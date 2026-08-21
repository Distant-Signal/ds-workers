//! Combine Knowledgebase incidents and LDBWS samples into one status
//! report per line. Ported from `src/aggregator.py`, adapted for the real
//! `common::IncidentMessage` shape (see module-level notes below).
//!
//! Two adaptations from the Python prototype, both because the real
//! `IncidentMessage` (built against confirmed RDM facts) differs from
//! what the prototype assumed:
//! 1. No `severity_hint` field exists — `priority: i32`'s meaning is an
//!    unresolved RDM gap, so severity classification uses keyword text
//!    only, dropping the Python version's `severity_hint == "major"`/
//!    `"minor"` branches.
//! 2. `IncidentMessage.validity` is a `Vec<ValidityPeriod>`, not a single
//!    optional pair — `validity_for_output` below picks one period for the
//!    (still-singular) `LineStatus.validity` field.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Datelike, Duration, NaiveTime, TimeZone, Utc};
use common::{
    AffectedRoute, CustomLine, DataQuality, Defaults, Disruption, IncidentMessage, LineDefinition,
    LineStatus, LineStatusReport, SampleStats, Severity, StationDeparture, StationSample,
    ValidityPeriod, thresholds_for,
};
use serde::Deserialize;

use crate::matcher::{Match, MatchScope, lines_affected_by};
use crate::queries::LoadedIncident;
use crate::segments::SegmentRegistry;

/// Merges DB-stored custom lines into the static catalogue, converting
/// each into a `LineDefinition` (see `common::CustomLine`'s `From` impl) so
/// the rest of the pipeline — matcher, segment registry, LDBWS inference —
/// treats them identically to catalogue lines. Re-run every poll cycle
/// (`main.rs`) since custom lines can be created or deleted at any time,
/// unlike the static catalogue which is fixed at process startup.
pub fn merge_custom_lines(
    static_lines: &HashMap<String, LineDefinition>,
    custom_lines: Vec<CustomLine>,
) -> HashMap<String, LineDefinition> {
    let mut merged = static_lines.clone();
    for custom in custom_lines {
        merged.insert(custom.id.clone(), custom.into());
    }
    merged
}

pub fn aggregate(
    lines: &HashMap<String, LineDefinition>,
    incidents: &[LoadedIncident],
    samples: &HashMap<String, StationSample>,
    registry: &SegmentRegistry,
    defaults: &Defaults,
) -> HashMap<String, LineStatusReport> {
    let mut reports: HashMap<String, LineStatusReport> = lines
        .values()
        .map(|line| {
            (
                line.id.clone(),
                LineStatusReport {
                    id: line.id.clone(),
                    name: line.name.clone(),
                    mode_name: line.mode.clone(),
                    operators: line.operators.clone(),
                    statuses: vec![],
                },
            )
        })
        .collect();

    // Layer 1: incidents. Filtered through `is_active` first -- a cleared,
    // temporally-expired, or stale-past-the-rail-day-cutoff incident never
    // reaches the matcher, so its line falls through to Layer 2 exactly as
    // if the incident didn't exist.
    let now = Utc::now();
    for loaded in incidents.iter().filter(|loaded| is_active(&loaded.message, loaded.first_seen_at, now)) {
        for m in lines_affected_by(&loaded.message, lines, registry) {
            let status = status_from_incident(&m, loaded);
            reports.get_mut(&m.line.id).unwrap().statuses.push(status);
        }
    }

    // Layer 2: sample-derived stats. Always computed for every line. Used
    // as the status itself when a line has no incident-derived status
    // (unchanged behavior); attached as supplementary `sample_stats` on top
    // of the incident-derived status(es) otherwise, never overriding their
    // severity — incident-reported severity stays authoritative.
    for line in lines.values() {
        let report = reports.get_mut(&line.id).unwrap();
        if report.statuses.is_empty() {
            let inferred = infer_from_samples(line, samples, defaults);
            report.statuses.push(inferred.unwrap_or_else(good_service));
            continue;
        }
        if let Some(stats) = compute_sample_stats(line, samples, defaults) {
            for status in &mut report.statuses {
                status.sample_stats = Some(stats.clone());
            }
        }
    }

    reports
}

// --- Incident path ---

fn status_from_incident(m: &Match, loaded: &LoadedIncident) -> LineStatus {
    let incident = &loaded.message;
    let base_severity = severity_from_incident(incident);
    let (extracted_severity, extraction_annotation) = apply_extraction(base_severity, loaded, Utc::now());
    let severity = demote_for_scope(extracted_severity, m.scope);

    let affected_stations = m.evidence.stations.clone();
    let affected_routes = routes_from_stations(m.line, &affected_stations);

    let mut reason = incident.summary.clone();
    match m.scope {
        MatchScope::SharedSegment => reason.push_str(" (shared trunk — also affects other lines)"),
        MatchScope::OperatorOnly => reason.push_str(" (operator-wide report)"),
        _ => {}
    }
    if let Some(annotation) = extraction_annotation {
        reason.push_str(&format!(" ({annotation})"));
    }

    let disruption = Disruption {
        category: if incident.is_planned { "PlannedWork" } else { "RealTime" }.to_string(),
        description: if incident.description.is_empty() { incident.summary.clone() } else { incident.description.clone() },
        affected_stops: affected_stations,
        affected_routes,
        source: Some(format!("knowledgebase-incident-{}", incident.incident_id)),
    };

    LineStatus {
        severity,
        reason,
        validity: validity_for_output(&incident.validity),
        disruption: Some(disruption),
        data_quality: if incident.is_planned { DataQuality::Planned } else { DataQuality::Knowledgebase },
        sample_stats: None,
    }
}

/// Picks one `ValidityPeriod` for `LineStatus.validity` from an incident's
/// (possibly empty, possibly multi-entry) `validity` vec. See module docs
/// for why this exists — the real schema allows repeated validity periods,
/// the output type doesn't.
fn validity_for_output(periods: &[ValidityPeriod]) -> ValidityPeriod {
    if periods.is_empty() {
        return ValidityPeriod { from_date: Utc::now(), to_date: None, is_now: true };
    }
    let now = Utc::now();
    periods
        .iter()
        .find(|p| period_covers_now(p, now))
        .cloned()
        .unwrap_or_else(|| periods[0].clone())
}

/// The next UK rail "traffic day" boundary after `first_seen_at` -- 02:00
/// Europe/London, per Network Rail's timetable convention (a traffic day
/// runs 02:00-01:59, not a midnight-to-midnight calendar day). If
/// `first_seen_at`'s local time-of-day is before 02:00, it belongs to the
/// previous calendar day's rail day, so the boundary is that same calendar
/// day's 02:00; otherwise it's the next calendar day's 02:00.
///
/// UK clocks change exactly at the 01:00/02:00 boundary in both directions
/// (spring: 01:00 GMT -> 02:00 BST; autumn: 02:00 BST -> 01:00 GMT), so
/// local 02:00 itself is never ambiguous or missing on a transition day --
/// only 01:00-01:59 is. `LocalResult::Single` is therefore the only case
/// expected for real UK dates; anything else is treated as a defensive
/// failure rather than left to a confusing bare-unwrap panic.
fn next_rail_day_boundary(first_seen_at: DateTime<Utc>) -> DateTime<Utc> {
    let local = first_seen_at.with_timezone(&chrono_tz::Europe::London);
    let boundary_time = NaiveTime::from_hms_opt(2, 0, 0).expect("2:00:00 is a valid time");

    let boundary_date = if local.time() < boundary_time {
        local.date_naive()
    } else {
        local.date_naive() + Duration::days(1)
    };
    let boundary_naive = boundary_date.and_time(boundary_time);

    match chrono_tz::Europe::London.from_local_datetime(&boundary_naive) {
        chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
        other => panic!(
            "unexpected {other:?} resolving rail-day boundary {boundary_naive} in Europe/London; \
             02:00 local should never be ambiguous or missing"
        ),
    }
}

/// Whether `period` covers the instant `now`. Shared by `validity_for_output`
/// (picks which period to *display*) and `is_active` (decides whether an
/// incident is included at all) so the "is this period active" condition
/// has one definition, not two.
fn period_covers_now(period: &ValidityPeriod, now: DateTime<Utc>) -> bool {
    period.from_date <= now && period.to_date.map(|to| to > now).unwrap_or(true)
}

/// Whether an incident should still contribute a `LineStatus` to any line
/// it matches. `is_cleared` isn't rechecked here -- `queries::load_incidents`
/// already excludes cleared rows at the SQL layer, so by the time an
/// incident reaches this function it's already known not to be cleared.
fn is_active(incident: &IncidentMessage, first_seen_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    let validity_ok = incident.validity.is_empty()
        || incident.validity.iter().any(|p| period_covers_now(p, now));
    let age_ok = incident.is_planned || now < next_rail_day_boundary(first_seen_at);
    validity_ok && age_ok
}

fn severity_from_incident(incident: &IncidentMessage) -> Severity {
    if incident.is_planned {
        return Severity::PlannedClosure;
    }

    let text = format!("{} {}", incident.summary, incident.description).to_lowercase();
    if text.contains("suspended") || text.contains("no service") {
        return Severity::Suspended;
    }
    if text.contains("rail replacement") || text.contains("replacement bus") {
        return Severity::BusService;
    }
    if text.contains("lines blocked") || text.contains("all lines blocked") {
        return Severity::PartSuspended;
    }
    if text.contains("severe delays") || text.contains("major disruption") {
        return Severity::SevereDelays;
    }
    if text.contains("diverted") {
        return Severity::Diverted;
    }
    Severity::MinorDelays
}

/// Weaker evidence -> milder reported status. Lower severity numbers are
/// more disruptive, so capping "at Minor Delays or milder" means picking
/// whichever of (severity, floor) sorts later (higher number = milder).
fn demote_for_scope(severity: Severity, scope: MatchScope) -> Severity {
    match scope {
        MatchScope::ExclusiveSegment | MatchScope::StationHit | MatchScope::SharedSegment => severity,
        MatchScope::KeywordOnly => severity.max(Severity::SevereDelays),
        MatchScope::OperatorOnly => severity.max(Severity::MinorDelays),
    }
}

#[derive(Deserialize)]
struct ScheduleWindow {
    days_of_week: Vec<u8>,
    start_time: String,
    end_time: String,
}

/// Whether `now` (converted to Europe/London local time) falls inside
/// `window`. Handles overnight windows (e.g. 22:00-06:00, where
/// `start_time > end_time`) by wraparound: "inside" means at or after
/// `start_time` OR before `end_time`, rather than requiring both.
fn now_within_window(window: &ScheduleWindow, now: DateTime<Utc>) -> bool {
    let local = now.with_timezone(&chrono_tz::Europe::London);
    let weekday = local.weekday().number_from_monday() as u8; // 1=Monday..7=Sunday
    if !window.days_of_week.contains(&weekday) {
        return false;
    }
    let Ok(start) = NaiveTime::parse_from_str(&window.start_time, "%H:%M") else { return true };
    let Ok(end) = NaiveTime::parse_from_str(&window.end_time, "%H:%M") else { return true };
    let now_time = local.time();
    if start <= end {
        now_time >= start && now_time < end
    } else {
        now_time >= start || now_time < end
    }
}

/// Adjusts `severity` based on NLP-extracted signals, and returns an
/// optional annotation to append to the status's `reason` text. Runs
/// between `severity_from_incident` and `demote_for_scope` in
/// `status_from_incident`. Can only demote (raise the numeric severity,
/// since lower is worse) or leave `severity` unchanged -- never make it
/// more severe, and never signals suppression. A missing or
/// low-confidence extraction is always a no-op: the absence of a signal
/// must behave identically to this function not existing at all. See
/// docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md §7.
fn apply_extraction(severity: Severity, loaded: &LoadedIncident, now: DateTime<Utc>) -> (Severity, Option<String>) {
    let high_confidence = loaded.extraction_confidence.as_deref() == Some("high");

    if high_confidence {
        match loaded.extracted_resolution_status.as_deref() {
            Some("resolved") => {
                return (
                    severity.max(Severity::MinorDelays),
                    Some("reported resolved -- showing residual impact".to_string()),
                );
            }
            Some("residual") => {
                return (severity.max(Severity::Recovering), Some("reported as residual delays only".to_string()));
            }
            _ => {}
        }
    }

    if let Some(window_json) = &loaded.extracted_schedule_window {
        if let Ok(window) = serde_json::from_value::<ScheduleWindow>(window_json.clone()) {
            if !now_within_window(&window, now) {
                return (
                    severity.max(Severity::MinorDelays),
                    Some(format!("reported active {}-{} only", window.start_time, window.end_time)),
                );
            }
        }
    }

    if let Some(eta) = loaded.extracted_eta {
        if eta < now {
            return (
                severity.max(Severity::MinorDelays),
                Some(format!("expected to end by {}", eta.with_timezone(&chrono_tz::Europe::London).format("%H:%M"))),
            );
        }
    }

    (severity, None)
}

fn routes_from_stations(line: &LineDefinition, stations: &[String]) -> Vec<AffectedRoute> {
    if stations.len() < 2 {
        return vec![];
    }
    let line_order: Vec<&str> = line.stations.iter().map(|s| s.crs.as_str()).collect();
    let mut in_order: Vec<&String> = stations.iter().collect();
    in_order.sort_by_key(|c| line_order.iter().position(|o| *o == c.as_str()).unwrap_or(999));
    vec![AffectedRoute { from_crs: in_order[0].clone(), to_crs: in_order[in_order.len() - 1].clone() }]
}

// --- Inference path ---

/// Raw sample-derived numbers for a line: how many recently-sampled
/// departures were delayed/cancelled, and by how much on average. Computed
/// independently of whether the line also has an incident-derived status —
/// `aggregate()` attaches the result to a line's status either way.
/// `avg_delay_minutes` is averaged over non-cancelled ("running") sampled
/// departures only.
fn compute_sample_stats(
    line: &LineDefinition,
    samples: &HashMap<String, StationSample>,
    defaults: &Defaults,
) -> Option<SampleStats> {
    let thresholds = thresholds_for(defaults, &line.severity_overrides);

    let relevant: Vec<&StationDeparture> = line
        .sample_stations
        .iter()
        .filter_map(|crs| samples.get(crs))
        .flat_map(|sample| sample.departures.iter())
        .filter(|dep| belongs_to_line(dep, line))
        .collect();

    if (relevant.len() as i64) < thresholds.min_sample_size {
        return None;
    }

    let total = relevant.len();
    let cancelled = relevant.iter().filter(|d| d.is_cancelled).count();
    let delayed = relevant
        .iter()
        .filter(|d| !d.is_cancelled && d.delay_minutes as i64 >= thresholds.delay_threshold_minutes)
        .count();
    let line_stations: HashSet<&str> = line.stations.iter().map(|s| s.crs.as_str()).collect();
    let skipped = relevant
        .iter()
        .filter(|d| !d.is_cancelled && d.skipped_stations.iter().any(|crs| line_stations.contains(crs.as_str())))
        .count();
    let running: Vec<&&StationDeparture> = relevant.iter().filter(|d| !d.is_cancelled).collect();
    let avg_delay_minutes = if running.is_empty() {
        0.0
    } else {
        running.iter().map(|d| d.delay_minutes as f64).sum::<f64>() / running.len() as f64
    };

    Some(SampleStats { total, delayed, cancelled, skipped, avg_delay_minutes })
}

fn infer_from_samples(
    line: &LineDefinition,
    samples: &HashMap<String, StationSample>,
    defaults: &Defaults,
) -> Option<LineStatus> {
    let stats = compute_sample_stats(line, samples, defaults)?;
    let thresholds = thresholds_for(defaults, &line.severity_overrides);

    let cancel_rate = stats.cancelled as f64 / stats.total as f64;
    let delay_rate = stats.delayed as f64 / stats.total as f64;
    let skip_rate = stats.skipped as f64 / stats.total as f64;

    let (severity, mut reason) = classify(
        cancel_rate,
        delay_rate,
        skip_rate,
        &thresholds,
        stats.total,
        stats.cancelled,
        stats.delayed,
        stats.skipped,
    );
    if severity == Severity::GoodService {
        let mut status = good_service();
        status.sample_stats = Some(stats);
        return Some(status);
    }

    // `compute_sample_stats` only returns aggregate counts, not the raw
    // departures, so the "most cited reason" text below re-derives its own
    // small filtered view. Cheap: a handful of departures per line per
    // cycle, and keeps `compute_sample_stats` focused on just the numbers.
    let relevant: Vec<&StationDeparture> = line
        .sample_stations
        .iter()
        .filter_map(|crs| samples.get(crs))
        .flat_map(|sample| sample.departures.iter())
        .filter(|dep| belongs_to_line(dep, line))
        .collect();
    let reasons: Vec<&str> = relevant
        .iter()
        .filter_map(|d| d.delay_reason.as_deref().or(d.cancel_reason.as_deref()))
        .collect();
    if let Some(most_common) = most_common(&reasons) {
        reason.push_str(&format!(" (most cited: {most_common})"));
    }

    // `samples` is a fresh `HashMap` every poll cycle with a randomized
    // per-process hash seed, so its iteration order is not stable across
    // cycles even for identical input. Sorting here makes the serialized
    // `affected_stops` array deterministic, which `normalize_for_diff`
    // (queries.rs) relies on to avoid writing spurious `line_status_history`
    // rows when nothing has actually changed.
    let mut affected_stops: Vec<String> = samples.keys().cloned().collect();
    affected_stops.sort();

    Some(LineStatus {
        severity,
        reason: reason.clone(),
        validity: ValidityPeriod { from_date: Utc::now(), to_date: None, is_now: true },
        disruption: Some(Disruption {
            category: "RealTime".to_string(),
            description: reason,
            affected_stops,
            affected_routes: vec![],
            source: Some("ldbws-sampling".to_string()),
        }),
        data_quality: DataQuality::LdbwsInferred,
        sample_stats: Some(stats),
    })
}

/// Operator filter is mandatory; destination-CRS/headcode-prefix filters
/// are optional narrowing, used at shared-trunk sample stations.
fn belongs_to_line(dep: &StationDeparture, line: &LineDefinition) -> bool {
    if !line.operators.contains(&dep.operator) {
        return false;
    }
    if !line.destination_crs_filter.is_empty() && !line.destination_crs_filter.contains(&dep.destination_crs) {
        return false;
    }
    if !line.headcode_prefixes.is_empty() {
        let Some(headcode) = &dep.headcode else { return false };
        if !line.headcode_prefixes.iter().any(|p| headcode.starts_with(p.as_str())) {
            return false;
        }
    }
    true
}

fn classify(
    cancel_rate: f64,
    delay_rate: f64,
    skip_rate: f64,
    thresholds: &Defaults,
    total: usize,
    cancelled: usize,
    delayed: usize,
    skipped: usize,
) -> (Severity, String) {
    if cancel_rate >= thresholds.part_suspended_pct {
        return (Severity::PartSuspended, format!("{cancelled} of {total} sampled services cancelled."));
    }
    if cancel_rate >= thresholds.reduced_service_pct {
        return (Severity::ReducedService, format!("{cancelled} of {total} sampled services cancelled."));
    }

    let delay_severity = if delay_rate >= thresholds.severe_delays_pct {
        Some(Severity::SevereDelays)
    } else if delay_rate >= thresholds.minor_delays_pct {
        Some(Severity::MinorDelays)
    } else {
        None
    };
    let skip_severity = if skip_rate >= thresholds.severe_delays_skip_pct {
        Some(Severity::SevereDelays)
    } else if skip_rate >= thresholds.minor_delays_skip_pct {
        Some(Severity::MinorDelays)
    } else {
        None
    };

    match (delay_severity, skip_severity) {
        (Some(d), Some(s)) if d == s => (
            d,
            format!(
                "{delayed} of {total} sampled services delayed, {skipped} of {total} sampled services skipping a scheduled stop."
            ),
        ),
        (Some(d), Some(s)) if d < s => (d, format!("{delayed} of {total} sampled services delayed.")),
        (Some(_), Some(s)) => (s, format!("{skipped} of {total} sampled services skipping a scheduled stop.")),
        (Some(d), None) => (d, format!("{delayed} of {total} sampled services delayed.")),
        (None, Some(s)) => (s, format!("{skipped} of {total} sampled services skipping a scheduled stop.")),
        (None, None) => (Severity::GoodService, "Good Service".to_string()),
    }
}

fn good_service() -> LineStatus {
    LineStatus {
        severity: Severity::GoodService,
        reason: "Good Service".to_string(),
        validity: ValidityPeriod { from_date: Utc::now(), to_date: None, is_now: true },
        disruption: None,
        data_quality: DataQuality::LdbwsInferred,
        sample_stats: None,
    }
}

fn most_common<'a>(items: &[&'a str]) -> Option<&'a str> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for item in items {
        *counts.entry(item).or_insert(0) += 1;
    }
    // `counts` iterates in a randomized, per-process order, so on a tie
    // `max_by_key` over `count` alone would pick a different "most cited"
    // reason on different poll cycles for identical input. Breaking ties
    // alphabetically by the reason string itself makes the result
    // deterministic (same input -> same output every time), which is what
    // `normalize_for_diff` needs to avoid spurious history rows.
    counts.into_iter().max_by_key(|(reason, count)| (*count, *reason)).map(|(item, _)| item)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn load_all_lines() -> HashMap<String, LineDefinition> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        LineDefinition::from_dir(&dir)
            .expect("lines/ directory should parse")
            .into_iter()
            .map(|l| (l.id.clone(), l))
            .collect()
    }

    fn incident(id: &str, summary: &str, description: &str, operators: &[&str], affected_stations: &[&str]) -> IncidentMessage {
        IncidentMessage {
            incident_id: id.to_string(),
            summary: summary.to_string(),
            description: description.to_string(),
            operators: operators.iter().map(|s| s.to_string()).collect(),
            affected_stations: affected_stations.iter().map(|s| s.to_string()).collect(),
            priority: 0,
            validity: vec![],
            is_planned: false,
            is_cleared: false,
        }
    }

    fn aggregate_with_defaults(
        lines: &HashMap<String, LineDefinition>,
        incidents: &[IncidentMessage],
    ) -> HashMap<String, LineStatusReport> {
        let registry = SegmentRegistry::new(lines);
        let defaults = Defaults::default();
        let loaded: Vec<LoadedIncident> = incidents
            .iter()
            .cloned()
            .map(|message| LoadedIncident {
                message,
                first_seen_at: Utc::now(),
                extracted_resolution_status: None,
                extraction_confidence: None,
                extracted_schedule_window: None,
                extracted_eta: None,
            })
            .collect();
        aggregate(lines, &loaded, &HashMap::new(), &registry, &defaults)
    }

    #[test]
    fn aggregator_propagates_severity_through_shared_trunk() {
        // Description text already contains "severe delays", which the
        // keyword classifier alone resolves to SevereDelays — the Python
        // original's `severity_hint="major"` was redundant with this text,
        // not load-bearing, so dropping it changes nothing about the result.
        let lines = load_all_lines();
        let inc = incident(
            "SWR-3",
            "Signal failure at Woking",
            "Severe delays expected on SWR services.",
            &["SW"],
            &["WOK"],
        );
        let reports = aggregate_with_defaults(&lines, &[inc]);
        for line_id in ["swr-south-west-main", "swr-portsmouth-direct", "swr-alton"] {
            let worst = reports[line_id].worst_severity();
            assert!(
                (worst as i32) <= (Severity::SevereDelays as i32),
                "{line_id} should have severe-or-worse severity, got {worst:?}"
            );
        }
    }

    #[test]
    fn aggregator_isolates_exclusive_incident() {
        // "minor delays" appears twice in the summary+description text, so
        // the keyword classifier alone reaches MinorDelays — the Python
        // original's `severity_hint="minor"` was likewise redundant here.
        let lines = load_all_lines();
        let inc = incident(
            "SWR-4",
            "Minor delays on Alton line",
            "A power supply problem at Alton is causing minor delays.",
            &["SW"],
            &["AON"],
        );
        let reports = aggregate_with_defaults(&lines, &[inc]);
        assert_eq!(reports["swr-alton"].worst_severity(), Severity::MinorDelays);
        assert_eq!(reports["swr-south-west-main"].worst_severity(), Severity::GoodService);
        assert_eq!(reports["swr-portsmouth-direct"].worst_severity(), Severity::GoodService);
    }

    #[test]
    fn operator_only_match_is_demoted_to_minor() {
        let lines = load_all_lines();
        let inc = incident(
            "OP-1",
            "SWR services suspended",
            "No service on SWR following an earlier incident.",
            &["SW"],
            &[], // no stations -> operator-only
        );
        let reports = aggregate_with_defaults(&lines, &[inc]);
        for line_id in ["swr-south-west-main", "swr-portsmouth-direct", "swr-alton"] {
            assert_eq!(
                reports[line_id].worst_severity(),
                Severity::MinorDelays,
                "{line_id} should be capped at Minor Delays"
            );
        }
    }

    #[test]
    fn no_incident_no_samples_yields_good_service() {
        let lines = load_all_lines();
        let reports = aggregate_with_defaults(&lines, &[]);
        for report in reports.values() {
            assert_eq!(report.worst_severity(), Severity::GoodService);
        }
    }

    #[test]
    fn validity_for_output_uses_now_when_no_periods_given() {
        let period = validity_for_output(&[]);
        assert!(period.is_now);
        assert!(period.to_date.is_none());
    }

    #[test]
    fn validity_for_output_picks_the_currently_active_period() {
        let now = Utc::now();
        let expired = ValidityPeriod {
            from_date: now - chrono::Duration::days(2),
            to_date: Some(now - chrono::Duration::days(1)),
            is_now: false,
        };
        let active = ValidityPeriod {
            from_date: now - chrono::Duration::hours(1),
            to_date: None,
            is_now: true,
        };
        let chosen = validity_for_output(&[expired.clone(), active.clone()]);
        assert_eq!(chosen.from_date, active.from_date);
    }

    #[test]
    fn validity_for_output_falls_back_to_first_when_none_are_active() {
        let now = Utc::now();
        let future = ValidityPeriod {
            from_date: now + chrono::Duration::days(1),
            to_date: None,
            is_now: false,
        };
        let chosen = validity_for_output(std::slice::from_ref(&future));
        assert_eq!(chosen.from_date, future.from_date);
    }

    // --- Inference-path tests ---
    //
    // The Python test suite (tests/test_matcher.py) never exercises
    // `_infer_from_samples`/`_belongs_to_line`/`_classify` at all — every
    // existing Python test is incident-path or matcher-only. There is no
    // Python original to port here, so these are new tests covering
    // logic that was faithfully ported but never had test coverage
    // upstream. Found and added during this plan's self-review.

    fn departure(destination_crs: &str, delay_minutes: i32, is_cancelled: bool) -> StationDeparture {
        StationDeparture {
            service_id: "svc".to_string(),
            operator: "SW".to_string(),
            destination_crs: destination_crs.to_string(),
            scheduled: "10:00".to_string(),
            estimated: "10:00".to_string(),
            is_cancelled,
            delay_minutes,
            cancel_reason: if is_cancelled { Some("fault".to_string()) } else { None },
            delay_reason: if !is_cancelled && delay_minutes > 0 { Some("signal failure".to_string()) } else { None },
            headcode: None,
            skipped_stations: vec![],
        }
    }

    #[test]
    fn belongs_to_line_filters_by_operator() {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let matching = departure("AON", 0, false);
        let wrong_operator = StationDeparture { operator: "XX".to_string(), ..matching.clone() };
        assert!(belongs_to_line(&matching, alton));
        assert!(!belongs_to_line(&wrong_operator, alton));
    }

    #[test]
    fn belongs_to_line_filters_by_destination_crs() {
        // swr-alton.toml: destination_crs_filter = ["AON", "BTL", "FRM", "AHT"]
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let matching = departure("AON", 0, false);
        let wrong_destination = StationDeparture { destination_crs: "WOK".to_string(), ..matching.clone() };
        assert!(belongs_to_line(&matching, alton));
        assert!(!belongs_to_line(&wrong_destination, alton));
    }

    #[test]
    fn infer_from_samples_returns_none_below_min_sample_size() {
        // swr-alton.toml: sample_stations = ["AHT", "FRM", "AON"]
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let mut samples = HashMap::new();
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                // Only 2 relevant departures, below the default min_sample_size of 3.
                departures: vec![departure("AON", 0, false), departure("AON", 0, false)],
            },
        );
        assert!(infer_from_samples(alton, &samples, &defaults).is_none());
    }

    #[test]
    fn infer_from_samples_classifies_severe_delays() {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let mut samples = HashMap::new();
        // 4 departures, 3 delayed >= 5 minutes -> 75% delay rate, above the
        // default severe_delays_pct of 0.50.
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    departure("AON", 10, false),
                    departure("AON", 12, false),
                    departure("AON", 8, false),
                    departure("AON", 0, false),
                ],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults).expect("should classify");
        assert_eq!(status.severity, Severity::SevereDelays);
        assert_eq!(status.data_quality, DataQuality::LdbwsInferred);
    }

    #[test]
    fn infer_from_samples_classifies_severe_skip_rate() {
        // swr-alton.toml: WOK is on the line's full `stations` list (part
        // of the shared trunk) but is not a sample station or in
        // destination_crs_filter — proves skip-relevance is checked
        // against `line.stations`, not the narrower sample/filter lists.
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let skipping = StationDeparture { skipped_stations: vec!["WOK".to_string()], ..departure("AON", 0, false) };
        let mut samples = HashMap::new();
        // 3 of 4 skip WOK -> 75% skip rate, above the default
        // severe_delays_skip_pct of 0.50, with delay_rate at 0%.
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![skipping.clone(), skipping.clone(), skipping, departure("AON", 0, false)],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults).expect("should classify");
        assert_eq!(status.severity, Severity::SevereDelays);
        assert_eq!(status.data_quality, DataQuality::LdbwsInferred);
        assert_eq!(status.sample_stats.expect("stats").skipped, 3);
    }

    #[test]
    fn infer_from_samples_classifies_minor_skip_rate() {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let skipping = StationDeparture { skipped_stations: vec!["WOK".to_string()], ..departure("AON", 0, false) };
        let mut samples = HashMap::new();
        // 1 of 4 skips WOK -> 25% skip rate, exactly at the default
        // minor_delays_skip_pct of 0.25.
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![skipping, departure("AON", 0, false), departure("AON", 0, false), departure("AON", 0, false)],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults).expect("should classify");
        assert_eq!(status.severity, Severity::MinorDelays);
    }

    #[test]
    fn infer_from_samples_ignores_skip_of_station_not_on_line() {
        // "ZZZ" isn't anywhere in swr-alton's `stations` list, so this skip
        // must not count towards skip_rate at all.
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let skipping_unrelated = StationDeparture { skipped_stations: vec!["ZZZ".to_string()], ..departure("AON", 0, false) };
        let mut samples = HashMap::new();
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    skipping_unrelated.clone(),
                    skipping_unrelated.clone(),
                    skipping_unrelated.clone(),
                    skipping_unrelated,
                ],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults).expect("should classify");
        assert_eq!(status.severity, Severity::GoodService);
        assert_eq!(status.sample_stats.expect("stats").skipped, 0);
    }

    #[test]
    fn infer_from_samples_excludes_cancelled_departures_from_skip_count() {
        // Darwin commonly marks the downstream subsequentCallingPoints of a
        // wholesale-cancelled service as isCancelled: true too, so a fully
        // cancelled service can still carry a non-empty skipped_stations
        // list. That must count towards `cancelled` only, never `skipped` —
        // skip detection is meant to be an independent signal from
        // cancellation, not a shadow of it.
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let cancelled_with_skips =
            StationDeparture { skipped_stations: vec!["WOK".to_string()], ..departure("AON", 0, true) };
        let mut samples = HashMap::new();
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    cancelled_with_skips,
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                ],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults).expect("should classify");
        let stats = status.sample_stats.expect("stats");
        assert_eq!(stats.skipped, 0);
        assert_eq!(stats.cancelled, 1);
    }

    #[test]
    fn classify_prefers_more_severe_of_delay_and_skip_candidates() {
        // skip_rate (75%, >= severe_delays_skip_pct 0.50) is more severe
        // than delay_rate (25%, only >= minor_delays_pct 0.25) -> the
        // overall severity must be the skip candidate's SevereDelays, not
        // the delay candidate's MinorDelays.
        let (severity, reason) = classify(0.0, 0.25, 0.75, &Defaults::default(), 4, 0, 1, 3);
        assert_eq!(severity, Severity::SevereDelays);
        assert!(reason.contains("skipping"), "reason was: {reason}");
    }

    #[test]
    fn classify_prefers_delay_when_more_severe_than_skip() {
        // delay_rate (75%, >= severe_delays_pct 0.50) is more severe than
        // skip_rate (30%, >= minor_delays_skip_pct 0.25 but < severe_delays_skip_pct
        // 0.50) -> the overall severity must be the delay candidate's
        // SevereDelays, not the skip candidate's MinorDelays.
        let (severity, reason) = classify(0.0, 0.75, 0.30, &Defaults::default(), 4, 0, 3, 1);
        assert_eq!(severity, Severity::SevereDelays);
        assert!(reason.contains("delayed"), "reason was: {reason}");
        assert!(!reason.contains("skipping"), "reason was: {reason}");
    }

    #[test]
    fn classify_combines_reason_when_delay_and_skip_tie() {
        // Both candidates land on MinorDelays (delay_rate 30% >= 0.25,
        // skip_rate 30% >= 0.25, neither >= their severe threshold) ->
        // combined message naming both counts.
        let (severity, reason) = classify(0.0, 0.30, 0.30, &Defaults::default(), 10, 0, 3, 3);
        assert_eq!(severity, Severity::MinorDelays);
        assert!(reason.contains("delayed"), "reason was: {reason}");
        assert!(reason.contains("skipping"), "reason was: {reason}");
    }

    #[test]
    fn classify_cancel_rate_still_takes_priority_over_skip_and_delay() {
        // cancel_rate alone (70%, >= part_suspended_pct 0.60) must win
        // even though skip_rate and delay_rate would also qualify for a
        // milder tier on their own.
        let (severity, _) = classify(0.70, 0.75, 0.75, &Defaults::default(), 10, 7, 7, 7);
        assert_eq!(severity, Severity::PartSuspended);
    }

    #[test]
    fn most_common_breaks_ties_deterministically() {
        // "b" and "a" tie at 2 occurrences each; without a deterministic
        // tie-break this could flip between "a" and "b" across runs
        // depending on HashMap iteration order. Alphabetical tie-break
        // always picks "b" (max_by_key: highest count, then lexicographic
        // reason) for this input, and repeated calls must agree.
        let items = ["a", "b", "b", "a"];
        let first = most_common(&items);
        for _ in 0..10 {
            assert_eq!(most_common(&items), first, "most_common must be deterministic across calls");
        }
        assert_eq!(first, Some("b"));
    }

    #[test]
    fn infer_from_samples_affected_stops_are_sorted_and_deterministic() {
        // swr-alton.toml: sample_stations = ["AHT", "FRM", "AON"]. Insert
        // samples in an order that would NOT be alphabetical if it leaked
        // through raw HashMap iteration, to prove the output is sorted
        // rather than incidentally ordered.
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let severe = vec![
            departure("AON", 10, false),
            departure("AON", 12, false),
            departure("AON", 8, false),
            departure("AON", 0, false),
        ];
        let mut samples = HashMap::new();
        samples.insert(
            "FRM".to_string(),
            StationSample { crs: "FRM".to_string(), polled_at: Utc::now(), departures: severe.clone() },
        );
        samples.insert(
            "AHT".to_string(),
            StationSample { crs: "AHT".to_string(), polled_at: Utc::now(), departures: severe },
        );

        let status = infer_from_samples(alton, &samples, &defaults).expect("should classify");
        let stops = status.disruption.expect("severe delays should produce a disruption").affected_stops;
        assert_eq!(stops, vec!["AHT".to_string(), "FRM".to_string()], "affected_stops must be sorted alphabetically");
    }

    #[test]
    fn infer_from_samples_returns_good_service_when_below_thresholds() {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let mut samples = HashMap::new();
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![departure("AON", 0, false), departure("AON", 0, false), departure("AON", 0, false)],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults).expect("should still classify (Good Service)");
        assert_eq!(status.severity, Severity::GoodService);
    }

    #[test]
    fn sample_stats_are_attached_alongside_an_active_incident_without_changing_severity() {
        let lines = load_all_lines();
        let inc = incident(
            "SWR-5",
            "Minor delays on Alton line",
            "A points failure at Alton is causing minor delays.",
            &["SW"],
            &["AON"],
        );
        let registry = SegmentRegistry::new(&lines);
        let defaults = Defaults::default();
        let mut samples = HashMap::new();
        // 4 departures, 3 delayed >= 5 minutes -> would classify as SevereDelays
        // on its own (75% delay rate, above the 50% severe_delays_pct default),
        // but the incident's MinorDelays severity must still win.
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    departure("AON", 10, false),
                    departure("AON", 12, false),
                    departure("AON", 8, false),
                    departure("AON", 0, false),
                ],
            },
        );
        let loaded = LoadedIncident {
            message: inc,
            first_seen_at: Utc::now(),
            extracted_resolution_status: None,
            extraction_confidence: None,
            extracted_schedule_window: None,
            extracted_eta: None,
        };
        let reports = aggregate(&lines, &[loaded], &samples, &registry, &defaults);
        let alton = &reports["swr-alton"];
        assert_eq!(
            alton.worst_severity(),
            Severity::MinorDelays,
            "incident severity must stay authoritative"
        );
        let stats = alton.statuses[0]
            .sample_stats
            .as_ref()
            .expect("sample stats should be attached even though an incident is active");
        assert_eq!(stats.total, 4);
        assert_eq!(stats.delayed, 3);
        assert_eq!(stats.cancelled, 0);
    }

    #[test]
    fn merge_custom_lines_adds_custom_without_touching_static() {
        let lines = load_all_lines();
        let static_count = lines.len();
        let custom = vec![CustomLine {
            id: "custom-my-commute".to_string(),
            name: "My Commute".to_string(),
            operators: vec!["SW".to_string()],
            stations: vec!["WOK".to_string(), "AON".to_string()],
            headcode_prefixes: vec![],
            destination_crs_filter: vec![],
        }];
        let merged = merge_custom_lines(&lines, custom);
        assert_eq!(merged.len(), static_count + 1);
        assert!(merged.contains_key("swr-alton"));
        assert_eq!(merged["custom-my-commute"].name, "My Commute");
        assert_eq!(merged["custom-my-commute"].category, "custom");
    }

    #[test]
    fn next_rail_day_boundary_on_a_plain_midweek_day() {
        // 2026-07-15 13:00 UTC is 14:00 BST (July is daylight saving) --
        // still well before that rail day's 02:00-the-next-day end, so the
        // boundary is 2026-07-16 02:00 BST = 2026-07-16 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-07-15T13:00:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(boundary, "2026-07-16T01:00:00Z".parse::<DateTime<Utc>>().unwrap());
    }

    #[test]
    fn next_rail_day_boundary_just_before_local_0200_stays_in_the_earlier_rail_day() {
        // 2026-07-16 00:30 UTC is 01:30 BST -- still inside the rail day
        // that started 2026-07-15 02:00 BST, so the boundary is only 30
        // local minutes away: 2026-07-16 02:00 BST = 2026-07-16 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-07-16T00:30:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(boundary, "2026-07-16T01:00:00Z".parse::<DateTime<Utc>>().unwrap());
    }

    #[test]
    fn next_rail_day_boundary_just_after_local_0200_rolls_to_the_next_rail_day() {
        // 2026-07-16 01:05 UTC is 02:05 BST -- just past that day's 02:00,
        // so it belongs to the rail day that just started, and the next
        // boundary is a full rail day away: 2026-07-17 02:00 BST = 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-07-16T01:05:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(boundary, "2026-07-17T01:00:00Z".parse::<DateTime<Utc>>().unwrap());
    }

    #[test]
    fn next_rail_day_boundary_across_the_spring_forward_transition() {
        // UK clocks spring forward at 01:00 UTC on the last Sunday in March
        // (2026-03-29), jumping local time from 01:00 GMT straight to 02:00
        // BST. 2026-03-29 00:30 UTC is *before* that jump, so local time is
        // still 00:30 GMT -- before that day's local 02:00, so the boundary
        // is that same day's 02:00, which (having just jumped) is already
        // BST: 2026-03-29 02:00 BST = 2026-03-29 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-03-29T00:30:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(boundary, "2026-03-29T01:00:00Z".parse::<DateTime<Utc>>().unwrap());
    }

    #[test]
    fn next_rail_day_boundary_across_the_autumn_fallback_transition() {
        // UK clocks fall back at 02:00 BST -> 01:00 GMT on the last Sunday
        // in October (2026-10-25). 2026-10-25 00:30 UTC is 01:30 BST --
        // before that day's local 02:00, which (after the fallback
        // completes) resolves as GMT -- so the boundary is 2026-10-25
        // 02:00 GMT = 2026-10-25 02:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-10-25T00:30:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(boundary, "2026-10-25T02:00:00Z".parse::<DateTime<Utc>>().unwrap());
    }

    #[test]
    fn period_covers_now_true_when_now_falls_inside_the_period() {
        let now = Utc::now();
        let period = ValidityPeriod { from_date: now - Duration::hours(1), to_date: Some(now + Duration::hours(1)), is_now: true };
        assert!(period_covers_now(&period, now));
    }

    #[test]
    fn period_covers_now_false_once_to_date_has_passed() {
        let now = Utc::now();
        let period = ValidityPeriod { from_date: now - Duration::days(2), to_date: Some(now - Duration::days(1)), is_now: false };
        assert!(!period_covers_now(&period, now));
    }

    #[test]
    fn is_active_true_for_fresh_incident_with_no_validity_periods() {
        let inc = incident("T1", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        assert!(is_active(&inc, now, now));
    }

    #[test]
    fn is_active_false_when_the_only_validity_period_has_elapsed() {
        let mut inc = incident("T2", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        inc.validity = vec![ValidityPeriod {
            from_date: now - Duration::days(2),
            to_date: Some(now - Duration::days(1)),
            is_now: false,
        }];
        assert!(!is_active(&inc, now - Duration::days(2), now));
    }

    #[test]
    fn is_active_true_when_a_validity_period_covers_now() {
        let mut inc = incident("T3", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        inc.validity = vec![ValidityPeriod { from_date: now - Duration::hours(1), to_date: None, is_now: true }];
        assert!(is_active(&inc, now - Duration::hours(1), now));
    }

    #[test]
    fn is_active_false_for_non_planned_incident_aged_past_the_rail_day_boundary() {
        let inc = incident("T4", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        let first_seen_at = now - Duration::days(2);
        assert!(!is_active(&inc, first_seen_at, now));
    }

    #[test]
    fn is_active_true_for_planned_incident_aged_past_the_rail_day_boundary() {
        let mut inc = incident("T5", "Engineering work", "Planned engineering work", &[], &[]);
        inc.is_planned = true;
        let now = Utc::now();
        let first_seen_at = now - Duration::days(2);
        assert!(is_active(&inc, first_seen_at, now));
    }

    #[test]
    fn is_active_true_when_one_of_several_validity_periods_covers_now() {
        // Real Knowledgebase incidents can carry more than one validity
        // window; the first is already over, the second is current.
        let mut inc = incident("T6", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        inc.validity = vec![
            ValidityPeriod { from_date: now - Duration::days(2), to_date: Some(now - Duration::days(1)), is_now: false },
            ValidityPeriod { from_date: now - Duration::hours(1), to_date: None, is_now: true },
        ];
        assert!(is_active(&inc, now, now));
    }

    #[test]
    fn is_active_false_for_planned_incident_whose_validity_has_expired() {
        // is_planned only exempts the rail-day age cutoff, not the
        // validity-window check -- a planned closure whose own stated
        // window has already ended should still be excluded, the same as
        // any other incident with expired validity.
        let mut inc = incident("T7", "Engineering work", "Planned engineering work", &[], &[]);
        inc.is_planned = true;
        let now = Utc::now();
        inc.validity = vec![ValidityPeriod {
            from_date: now - Duration::days(2),
            to_date: Some(now - Duration::days(1)),
            is_now: false,
        }];
        assert!(!is_active(&inc, now - Duration::days(2), now));
    }

    #[test]
    fn stale_non_planned_incident_falls_back_to_good_service() {
        let lines = load_all_lines();
        let inc = incident(
            "SWR-STALE",
            "Signal failure at Woking",
            "Residual delays continue.",
            &["SW"],
            &["WOK"],
        );
        let registry = SegmentRegistry::new(&lines);
        let defaults = Defaults::default();
        let loaded = LoadedIncident {
            message: inc,
            first_seen_at: Utc::now() - Duration::days(5),
            extracted_resolution_status: None,
            extraction_confidence: None,
            extracted_schedule_window: None,
            extracted_eta: None,
        };
        let reports = aggregate(&lines, &[loaded], &HashMap::new(), &registry, &defaults);
        for line_id in ["swr-south-west-main", "swr-portsmouth-direct", "swr-alton"] {
            assert_eq!(
                reports[line_id].worst_severity(),
                Severity::GoodService,
                "{line_id} should fall back to Good Service once the incident is stale"
            );
        }
    }

    fn loaded_with_extraction(
        resolution_status: Option<&str>,
        confidence: Option<&str>,
        schedule_window: Option<serde_json::Value>,
        eta: Option<DateTime<Utc>>,
    ) -> LoadedIncident {
        LoadedIncident {
            message: incident("EXT1", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_resolution_status: resolution_status.map(str::to_string),
            extraction_confidence: confidence.map(str::to_string),
            extracted_schedule_window: schedule_window,
            extracted_eta: eta,
        }
    }

    #[test]
    fn apply_extraction_is_a_no_op_with_no_extraction() {
        let loaded = loaded_with_extraction(None, None, None, None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, Utc::now());
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_ignores_low_confidence_resolved() {
        let loaded = loaded_with_extraction(Some("resolved"), Some("low"), None, None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, Utc::now());
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_demotes_high_confidence_resolved_to_minor_delays() {
        let loaded = loaded_with_extraction(Some("resolved"), Some("high"), None, None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, Utc::now());
        assert_eq!(severity, Severity::MinorDelays);
        assert!(annotation.unwrap().contains("resolved"));
    }

    #[test]
    fn apply_extraction_never_demotes_resolved_below_minor_delays() {
        // "Demote" means push toward a MILDER (higher-ordinal) severity,
        // never a more severe one. GoodService's ordinal (10) is already
        // higher/milder than MinorDelays' (9), so `.max(MinorDelays)` must
        // leave it unchanged, not pull it back down to 9.
        let loaded = loaded_with_extraction(Some("resolved"), Some("high"), None, None);
        let (severity, _) = apply_extraction(Severity::GoodService, &loaded, Utc::now());
        assert_eq!(severity, Severity::GoodService);
    }

    #[test]
    fn apply_extraction_demotes_high_confidence_residual_to_recovering() {
        let loaded = loaded_with_extraction(Some("residual"), Some("high"), None, None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, Utc::now());
        assert_eq!(severity, Severity::Recovering);
        assert!(annotation.is_some());
    }

    #[test]
    fn apply_extraction_ongoing_is_a_no_op() {
        let loaded = loaded_with_extraction(Some("ongoing"), Some("high"), None, None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, Utc::now());
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_demotes_when_now_is_outside_the_schedule_window() {
        // Window is 22:00-06:00 every day; "now" is fixed at a UTC instant
        // that's midday in London.
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap(); // 13:00 BST
        let window = serde_json::json!({ "days_of_week": [1,2,3,4,5,6,7], "start_time": "22:00", "end_time": "06:00" });
        let loaded = loaded_with_extraction(None, None, Some(window), None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::MinorDelays);
        assert!(annotation.is_some());
    }

    #[test]
    fn apply_extraction_no_op_when_now_is_inside_an_overnight_schedule_window() {
        let now: DateTime<Utc> = "2026-06-15T23:00:00Z".parse().unwrap(); // 00:00 BST, inside 22:00-06:00
        let window = serde_json::json!({ "days_of_week": [1,2,3,4,5,6,7], "start_time": "22:00", "end_time": "06:00" });
        let loaded = loaded_with_extraction(None, None, Some(window), None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_demotes_when_eta_has_already_passed() {
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let eta = now - Duration::hours(1);
        let loaded = loaded_with_extraction(None, None, None, Some(eta));
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::MinorDelays);
        assert!(annotation.unwrap().contains("expected to end"));
    }

    #[test]
    fn apply_extraction_no_op_when_eta_is_in_the_future() {
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let eta = now + Duration::hours(1);
        let loaded = loaded_with_extraction(None, None, None, Some(eta));
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn planned_work_is_exempt_from_the_rail_day_cutoff() {
        let lines = load_all_lines();
        let mut inc = incident(
            "SWR-PLANNED",
            "Engineering work at Woking",
            "Planned engineering work.",
            &["SW"],
            &["WOK"],
        );
        inc.is_planned = true;
        let registry = SegmentRegistry::new(&lines);
        let defaults = Defaults::default();
        let loaded = LoadedIncident {
            message: inc,
            first_seen_at: Utc::now() - Duration::days(5),
            extracted_resolution_status: None,
            extraction_confidence: None,
            extracted_schedule_window: None,
            extracted_eta: None,
        };
        let reports = aggregate(&lines, &[loaded], &HashMap::new(), &registry, &defaults);
        assert_eq!(reports["swr-alton"].worst_severity(), Severity::PlannedClosure);
    }
}
