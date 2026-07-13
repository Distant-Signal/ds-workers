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

use chrono::Utc;
use common::{
    AffectedRoute, CustomLine, DataQuality, Defaults, Disruption, IncidentMessage, LineDefinition,
    LineStatus, LineStatusReport, SampleStats, Severity, StationDeparture, StationSample,
    ValidityPeriod, thresholds_for,
};

use crate::matcher::{Match, MatchScope, lines_affected_by};
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
    incidents: &[IncidentMessage],
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

    // Layer 1: incidents.
    for incident in incidents {
        for m in lines_affected_by(incident, lines, registry) {
            let status = status_from_incident(&m, incident);
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

fn status_from_incident(m: &Match, incident: &IncidentMessage) -> LineStatus {
    let base_severity = severity_from_incident(incident);
    let severity = demote_for_scope(base_severity, m.scope);

    let affected_stations = m.evidence.stations.clone();
    let affected_routes = routes_from_stations(m.line, &affected_stations);

    let mut reason = incident.summary.clone();
    match m.scope {
        MatchScope::SharedSegment => reason.push_str(" (shared trunk — also affects other lines)"),
        MatchScope::OperatorOnly => reason.push_str(" (operator-wide report)"),
        _ => {}
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
        .find(|p| p.from_date <= now && p.to_date.map(|to| to > now).unwrap_or(true))
        .cloned()
        .unwrap_or_else(|| periods[0].clone())
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
        .filter(|d| d.skipped_stations.iter().any(|crs| line_stations.contains(crs.as_str())))
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
        (Some(_), Some(_)) => (
            skip_severity.expect("skip_severity is Some in this arm"),
            format!("{skipped} of {total} sampled services skipping a scheduled stop."),
        ),
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
        aggregate(lines, incidents, &HashMap::new(), &registry, &defaults)
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
        let reports = aggregate(&lines, &[inc], &samples, &registry, &defaults);
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
}
