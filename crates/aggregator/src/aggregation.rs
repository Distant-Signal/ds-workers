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
    LineStatus, LineStatusReport, SampleAvailability, SampleStats, Severity, StationDeparture,
    StationSample, ValidityPeriod, severity_rank, thresholds_for,
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
    for loaded in incidents.iter().filter(|loaded| is_active(loaded, now)) {
        for m in lines_affected_by(&loaded.message, lines, registry) {
            let status = status_from_incident(&m, loaded, now);
            reports.get_mut(&m.line.id).unwrap().statuses.push(status);
        }
    }

    // Layer 2: sample-derived stats. Always computed for every line. Used
    // as the status itself when a line has no incident-derived status
    // (unchanged behavior); attached as supplementary `sample_stats` on top
    // of the incident-derived status(es) otherwise, AND (2026-08-21) allowed
    // to escalate their severity -- never demote it -- when live delay/
    // cancellation data implies something worse than the incident text
    // alone accounted for. See `escalate_from_sample_stats`.
    for line in lines.values() {
        let report = reports.get_mut(&line.id).unwrap();
        if report.statuses.is_empty() {
            report
                .statuses
                .push(infer_from_samples(line, samples, defaults));
            continue;
        }
        let availability = compute_sample_availability(line, samples, defaults);
        for status in &mut report.statuses {
            status.sample_availability = availability.clone();
        }
        if let Some(stats) = availability.sample_stats() {
            let thresholds = thresholds_for(defaults, &line.severity_overrides);
            for status in &mut report.statuses {
                let (escalated, annotation) =
                    escalate_from_sample_stats(status.severity, &stats, &thresholds);
                status.severity = escalated;
                if let Some(annotation) = annotation {
                    status.reason.push_str(&format!(" ({annotation})"));
                }
                status.sample_stats = Some(stats.clone());
            }
        }
    }

    reports
}

// --- Incident path ---

/// `now` is threaded in from `aggregate()`'s single `Utc::now()` rather than
/// re-read here, so every incident in one aggregation pass is judged against
/// the same instant that `is_active` already filtered them by.
fn status_from_incident(m: &Match, loaded: &LoadedIncident, now: DateTime<Utc>) -> LineStatus {
    let incident = &loaded.message;
    let base_severity = severity_from_incident(incident);
    let (extracted_severity, extraction_annotation) = apply_extraction(base_severity, loaded, now);
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
        category: if incident.is_planned {
            "PlannedWork"
        } else {
            "RealTime"
        }
        .to_string(),
        description: if incident.description.is_empty() {
            incident.summary.clone()
        } else {
            incident.description.clone()
        },
        affected_stops: affected_stations,
        affected_routes,
        source: Some(format!("knowledgebase-incident-{}", incident.incident_id)),
        impact_type: governing_impact_type(loaded, now),
    };

    LineStatus {
        severity,
        reason,
        validity: validity_for_output(&incident.validity),
        disruption: Some(disruption),
        data_quality: if incident.is_planned {
            DataQuality::Planned
        } else {
            DataQuality::Knowledgebase
        },
        sample_stats: None,
        sample_availability: SampleAvailability::NoCoverage, // always overwritten by aggregate()'s Layer 2, immediately after construction
    }
}

/// Picks one `ValidityPeriod` for `LineStatus.validity` from an incident's
/// (possibly empty, possibly multi-entry) `validity` vec. See module docs
/// for why this exists — the real schema allows repeated validity periods,
/// the output type doesn't.
fn validity_for_output(periods: &[ValidityPeriod]) -> ValidityPeriod {
    if periods.is_empty() {
        return ValidityPeriod {
            from_date: Utc::now(),
            to_date: None,
            is_now: true,
        };
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
fn is_active(loaded: &LoadedIncident, now: DateTime<Utc>) -> bool {
    let incident = &loaded.message;
    let validity_ok =
        incident.validity.is_empty() || incident.validity.iter().any(|p| period_covers_now(p, now));
    let age_ok = incident.is_planned
        || has_recurring_schedule(loaded, now)
        || now < next_rail_day_boundary(loaded.first_seen_at);
    validity_ok && age_ok
}

/// Whether `loaded` carries at least one currently-`Active` period with a
/// high-confidence, successfully-parsed `schedule_window` -- evidence of a
/// genuinely recurring, time-bounded disruption (e.g. nightly rail
/// replacement while a fault is repaired) rather than the "SWR forgot about
/// it" case the rail-day cutoff exists to catch. A real-time
/// (non-`is_planned`) incident like that would otherwise still get evicted
/// by the age cutoff the first time `now` crosses the next rail-day
/// boundary after `first_seen_at`, even though it recurs every night for
/// weeks -- so this exempts it from that cutoff the same way `is_planned`
/// already is, in `is_active` above.
///
/// **Filtering to `Active` periods only is load-bearing, not incidental**
/// (design doc §4): checking any period in the raw array, regardless of
/// phase, would let an incident whose only recurring-schedule period has
/// already elapsed keep exempting itself from the rail-day cutoff forever
/// -- exactly the "SWR forgot about it" failure mode this cutoff exists to
/// catch. An `Elapsed` period is allowed to contribute its synthetic
/// demotion floor in `apply_extraction`, but must never be allowed to
/// contribute a recurring-schedule *exemption* here.
///
/// Malformed or absent schedule-window data does NOT count: unlike
/// `now_within_window`'s fail-safe direction (bad data must never
/// manufacture a *demotion*), granting an age-cutoff *exemption* from bad
/// data would be the unsafe direction here, so this requires an actual
/// successful parse (a period whose `schedule_window` JSON doesn't match
/// `ScheduleWindow`'s shape fails the whole `extracted_periods` parse via
/// `parse_periods`, and is therefore excluded along with every other period
/// on that incident -- the same fail-safe direction, just applied one level
/// up).
fn has_recurring_schedule(loaded: &LoadedIncident, now: DateTime<Utc>) -> bool {
    parse_periods(loaded).iter().any(|period| {
        period_phase(period, now) == PeriodPhase::Active
            && period.resolution_status_confidence == "high"
            && period.schedule_window.is_some()
    })
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
        MatchScope::ExclusiveSegment | MatchScope::StationHit | MatchScope::SharedSegment => {
            severity
        }
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

/// Mirrors `enricher`'s `DateRange`
/// (docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md
/// §1), but keeps `from_date`/`to_date` as raw strings rather than
/// `DateTime<Utc>`. A `#[derive(Deserialize)]` `DateTime<Utc>` field would
/// fail the entire `extracted_periods` parse on one malformed date; keeping
/// them as `String` lets `period_phase` below fail safe to `Active` for
/// just the one period with an unparseable date instead, matching
/// `now_within_window`'s existing fail-safe treatment of unparseable
/// `start_time`/`end_time`.
#[derive(Deserialize)]
struct DateRange {
    from_date: Option<String>,
    to_date: Option<String>,
}

/// Mirrors `enricher`'s `ExtractionPeriod` (design doc §1) -- one entry per
/// distinct period the primary extraction pass segmented an incident's
/// text into (always >= 1 in a well-formed row; the common single-fact
/// case is one element with `date_range: None`).
///
/// `#[serde(default)]` on the two confidence fields is load-bearing, not
/// decorative: the primary pass's own JSON schema never sends them (they
/// only exist once `enricher`'s combination step runs the two adversarial
/// passes), so without it, deserializing a row written between the primary
/// pass and the combination step -- or any row shaped by a version this
/// crate doesn't fully understand -- would hard-fail on a missing-field
/// error instead of degrading to "no extraction": an empty string never
/// equals `"high"`, so both confidence gates below fail closed exactly the
/// same as an absent extraction.
#[derive(Deserialize)]
struct ExtractionPeriod {
    scope_description: Option<String>,
    date_range: Option<DateRange>,
    schedule_window: Option<ScheduleWindow>,
    resolution_status: String,
    apparent_severity: String,
    #[serde(default)]
    resolution_status_confidence: String,
    #[serde(default)]
    severity_confidence: String,
    /// `#[serde(default)]` is load-bearing here for the identical reason
    /// as the two confidence fields above: a row written by an `enricher`
    /// process older than this field must still parse, degrading to
    /// `None` rather than failing the whole `extracted_periods` parse.
    #[serde(default)]
    impact_type: Option<String>,
}

/// Whether a period is currently relevant to `now`, and if not, *why* --
/// see
/// docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md §4.
/// Unlike a plain in/out-of-scope boolean, `apply_extraction` needs to tell
/// an elapsed period (which still contributes a synthetic demotion floor)
/// apart from one that hasn't started yet (which contributes nothing at
/// all).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeriodPhase {
    Active,
    Elapsed,
    NotStarted,
}

/// `None` `date_range` (the common single-fact case), or a `date_range`
/// that covers `now`, is `Active`. A `to_date` that has already passed is
/// `Elapsed` -- checked before `from_date`, matching the design doc's
/// stated precedence. A `from_date` still in the future is `NotStarted`.
/// A `from_date`/`to_date` string that's present but unparseable fails safe
/// to `Active` -- NOT `Elapsed` (which would manufacture a demotion out of
/// bad data) and NOT `NotStarted` (which would silently drop a period that
/// might genuinely be live right now), mirroring `now_within_window`'s
/// existing "malformed -> assume inside, no forced outcome" fail-safe
/// shape.
fn period_phase(period: &ExtractionPeriod, now: DateTime<Utc>) -> PeriodPhase {
    let Some(range) = &period.date_range else {
        return PeriodPhase::Active;
    };

    let parse = |raw: &Option<String>| -> Result<Option<DateTime<Utc>>, ()> {
        match raw {
            None => Ok(None),
            Some(s) => DateTime::parse_from_rfc3339(s)
                .map(|dt| Some(dt.with_timezone(&Utc)))
                .map_err(|_| ()),
        }
    };
    let (Ok(from), Ok(to)) = (parse(&range.from_date), parse(&range.to_date)) else {
        return PeriodPhase::Active;
    };

    let covers_now = from.is_none_or(|f| f <= now) && to.is_none_or(|t| t > now);
    if covers_now {
        return PeriodPhase::Active;
    }
    if let Some(to) = to
        && to <= now
    {
        return PeriodPhase::Elapsed;
    }
    if let Some(from) = from
        && from > now
    {
        return PeriodPhase::NotStarted;
    }
    PeriodPhase::Active
}

/// Deserializes `loaded.extracted_periods` into `Vec<ExtractionPeriod>`,
/// treating a missing column or any parse failure (wrong-shaped JSON, a
/// stale/foreign row, an empty array, ...) identically to "no periods at
/// all" -- the same fail-safe posture as every other extraction consumer in
/// this module: malformed or absent data never manufactures a demotion,
/// escalation, or age-cutoff exemption on its own.
fn parse_periods(loaded: &LoadedIncident) -> Vec<ExtractionPeriod> {
    loaded
        .extracted_periods
        .as_ref()
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

/// Prefixes `text` with `period.scope_description` when present, per design
/// doc §4's "platform 2 (...): reported active ..." annotation style --
/// keeps a multi-period incident's annotations attributable to the
/// specific period they came from once several are semicolon-joined into
/// one `reason` string.
fn scope_qualify(period: &ExtractionPeriod, text: String) -> String {
    match period.scope_description.as_deref() {
        Some(scope) if !scope.is_empty() => format!("{scope}: {text}"),
        _ => text,
    }
}

/// The synthetic annotation an `Elapsed` period contributes alongside its
/// `Severity::MinorDelays` floor -- mirrors the pre-multi-period design's
/// eta-passed annotation ("expected to end by HH:MM"), reusing the period's
/// own `date_range.to_date` (exactly the old flat `eta` field, once folded
/// into a period per design doc §1).
fn elapsed_annotation(period: &ExtractionPeriod) -> String {
    let text = period
        .date_range
        .as_ref()
        .and_then(|range| range.to_date.as_deref())
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| {
            format!(
                "expected to end by {}",
                dt.with_timezone(&chrono_tz::Europe::London).format("%H:%M")
            )
        })
        .unwrap_or_else(|| "reported period has ended".to_string());
    scope_qualify(period, text)
}

/// Whether `now` (converted to Europe/London local time) falls inside
/// `window`. Handles overnight windows (e.g. 22:00-06:00, where
/// `start_time > end_time`) by wraparound: "inside" means at or after
/// `start_time` OR before `end_time`, rather than requiring both.
///
/// `days_of_week` is matched against the day the *active window instance
/// started on*, which for an overnight window in its early-morning tail is
/// yesterday, not today. See the `window_start_date` comment below.
fn now_within_window(window: &ScheduleWindow, now: DateTime<Utc>) -> bool {
    // An empty `days_of_week` is degenerate/malformed extraction data (the
    // enricher's JSON schema permits it -- no `minItems` constraint), not a
    // meaningful "never active" signal. Matching the fail-safe direction
    // used just below for unparsable times: malformed extraction data must
    // never be able to manufacture a demotion, so treat it as "inside the
    // window" (no demotion) rather than "every day is outside it".
    if window.days_of_week.is_empty() {
        return true;
    }
    let local = now.with_timezone(&chrono_tz::Europe::London);
    let Ok(start) = NaiveTime::parse_from_str(&window.start_time, "%H:%M") else {
        return true;
    };
    let Ok(end) = NaiveTime::parse_from_str(&window.end_time, "%H:%M") else {
        return true;
    };
    let now_time = local.time();

    // Which calendar day did the window instance covering `now` START on?
    // For a same-day window (start <= end) that is always today. For an
    // overnight window it is today during the evening portion, but
    // YESTERDAY once we are past midnight in the early-morning tail: at
    // 00:30 on a Saturday, a `days_of_week: [1,2,3,4,5]` "Mon-Fri nights"
    // window is Friday night's instance still running. Checking today's
    // weekday there would report "outside the window" and manufacture a
    // demotion for a disruption that is genuinely still active.
    let window_start_date = if start > end && now_time < end {
        local.date_naive() - Duration::days(1)
    } else {
        local.date_naive()
    };
    let weekday = window_start_date.weekday().number_from_monday() as u8; // 1=Monday..7=Sunday
    if !window.days_of_week.contains(&weekday) {
        return false;
    }

    if start <= end {
        now_time >= start && now_time < end
    } else {
        now_time >= start || now_time < end
    }
}

/// Maps a high-confidence `extracted_severity` hint to the `Severity`
/// ceiling the base classifier should be raised to. `normal` (or an
/// unrecognized value -- schema drift, a model version mismatch) never
/// escalates.
fn escalation_ceiling(hint: &str) -> Option<Severity> {
    match hint {
        "moderate_disruption" => Some(Severity::ReducedService),
        "severe_disruption" => Some(Severity::SevereDelays),
        "blocked_or_suspended" => Some(Severity::PartSuspended),
        _ => None,
    }
}

/// Adjusts `severity` based on NLP-extracted signals, and returns an
/// optional annotation to append to the status's `reason` text. Runs
/// between `severity_from_incident` and `demote_for_scope` in
/// `status_from_incident`.
///
/// Generalized (design doc
/// docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md §4)
/// from "one flat set of signals per incident" to "one set of signals per
/// period, combined across all of an incident's periods". For every
/// `Active` period this runs the *same* per-row checks the original,
/// single-period design established (resolved/residual floor,
/// schedule-window-excludes-now floor, severity-hint escalation ceiling),
/// scoped with that period's `scope_description` in the annotation text
/// when present. For every `Elapsed` period it contributes exactly one
/// synthetic `Severity::MinorDelays` floor and an "expected to end by
/// HH:MM"-style annotation, regardless of what that period's own
/// `resolution_status` asserts -- deterministic date arithmetic wins over
/// the model's textual guess about whether a period that's already over is
/// "resolved" or still "ongoing". `NotStarted` periods contribute nothing.
///
/// All fired floors/escalations across every `Active`/`Elapsed` period are
/// combined with the exact same rule the original single-period design
/// used for its independent demote-only rows -- most severe (highest
/// `common::severity_rank`) wins, every firing annotation is kept and
/// joined -- just generalized from "one row per rule" to "one row per
/// (period, rule) pair". This still runs escalation first (the most severe
/// firing `apparent_severity` ceiling across `Active` periods, if any
/// exceeds the base severity's rank) and then caps the result against the
/// most severe firing demote-only floor, so a "fault fixed, but severe
/// knock-on delays remain" period can still be escalated and then
/// correctly re-capped in the same pass, without the two fighting.
///
/// CONFIDENCE GATES EVERY SIGNAL WITHIN ITS OWN HALF, per period. A period's
/// `resolution_status_confidence` gates its resolved/residual/schedule-window
/// checks *and* its `Elapsed` synthetic floor (matching the original design's
/// eta-passed rule exactly: an untrustworthy overall extraction shouldn't
/// demote via any channel, elapsed-floor included); a period's
/// `severity_confidence` independently gates only its escalation candidate.
/// A missing or non-"high" confidence value is always a no-op for whichever
/// half it gates -- the absence of a trustworthy signal must behave
/// identically to that signal not existing.
///
/// "Most severe" and "demote" are both measured with `common::severity_rank`,
/// NOT with `Severity`'s discriminant order: `Diverted = 21` and
/// `PartClosed = 11` are numerically high but genuinely severe, so a raw
/// `severity.max(floor)` left them completely undemoted while still
/// appending a "reported resolved" annotation -- a passenger shown a severe
/// status whose own reason text says it is over. See `severity_rank`'s docs.
fn apply_extraction(
    severity: Severity,
    loaded: &LoadedIncident,
    now: DateTime<Utc>,
) -> (Severity, Option<String>) {
    let periods = parse_periods(loaded);

    // Escalation candidates: one per `Active` period at high
    // `severity_confidence` whose `apparent_severity` maps to a ceiling
    // strictly more severe than the base `severity`. Combined the same way
    // as the floors below -- most severe candidate wins. `Elapsed`/
    // `NotStarted` periods never escalate: once a period is no longer live,
    // its own severity read is exactly the kind of model claim the
    // elapsed-floor logic below already distrusts in favor of computed
    // date arithmetic.
    let escalation_candidates: Vec<(Severity, String)> = periods
        .iter()
        .filter(|period| period_phase(period, now) == PeriodPhase::Active)
        .filter(|period| period.severity_confidence == "high")
        .filter_map(|period| {
            let ceiling = escalation_ceiling(&period.apparent_severity)?;
            if severity_rank(ceiling) <= severity_rank(severity) {
                return None;
            }
            let annotation = scope_qualify(
                period,
                format!(
                    "reported more severe than automatically classified: {}",
                    ceiling.description().to_lowercase()
                ),
            );
            Some((ceiling, annotation))
        })
        .collect();

    let (severity, escalation_annotation) = escalation_candidates
        .into_iter()
        .max_by_key(|(ceiling, _)| (severity_rank(*ceiling), std::cmp::Reverse(*ceiling)))
        .map_or((severity, None), |(ceiling, annotation)| {
            (ceiling, Some(annotation))
        });

    let mut floors: Vec<Severity> = Vec::new();
    let mut annotations: Vec<String> = escalation_annotation.into_iter().collect();

    for period in &periods {
        match period_phase(period, now) {
            PeriodPhase::NotStarted => {}
            PeriodPhase::Elapsed => {
                if period.resolution_status_confidence == "high" {
                    floors.push(Severity::MinorDelays);
                    annotations.push(elapsed_annotation(period));
                }
            }
            PeriodPhase::Active => {
                if period.resolution_status_confidence != "high" {
                    continue;
                }
                match period.resolution_status.as_str() {
                    "resolved" => {
                        floors.push(Severity::MinorDelays);
                        annotations.push(scope_qualify(
                            period,
                            "reported resolved — showing residual impact".to_string(),
                        ));
                    }
                    "residual" => {
                        floors.push(Severity::Recovering);
                        annotations.push(scope_qualify(
                            period,
                            "reported as residual delays only".to_string(),
                        ));
                    }
                    _ => {}
                }
                if let Some(window) = &period.schedule_window
                    && !now_within_window(window, now)
                {
                    floors.push(Severity::MinorDelays);
                    annotations.push(scope_qualify(
                        period,
                        format!(
                            "reported active {}-{} only",
                            window.start_time, window.end_time
                        ),
                    ));
                }
            }
        }
    }

    // The binding floor is the most severe of the firing floors: highest
    // `severity_rank` first, and among equal ranks the lowest discriminant,
    // which is the spec's literal "lowest-numbered". Capping against the
    // single most severe floor is equivalent to capping per row and taking
    // the most severe result, because the cap is monotonic in the floor.
    let Some(&binding_floor) = floors
        .iter()
        .max_by_key(|floor| (severity_rank(**floor), std::cmp::Reverse(**floor)))
    else {
        return (
            severity,
            if annotations.is_empty() {
                None
            } else {
                Some(annotations.join("; "))
            },
        );
    };

    // Demote-only, on the rank scale: if the current (possibly escalated)
    // severity is already strictly milder than the floor, leave it alone.
    // Otherwise -- whether `severity` is more severe than the floor, or
    // merely at the *same* rank as it -- land on the floor itself, since the
    // floor is the specific named severity the rule table calls for (e.g.
    // `MinorDelays` for `resolved`), not just "some severity in that rank
    // bucket". Equal rank must still land on the floor: `ReducedService` and
    // `MinorDelays` share the mild rank, but a `resolved` extraction against
    // a `ReducedService` base must still land on `MinorDelays`
    // specifically, not stay at `ReducedService`, or the displayed status
    // keeps a "showing residual impact" annotation stapled to a severity the
    // annotation wasn't actually written for. Never raises the rank, exactly
    // the intent the old `severity.max(floor)` had before the non-monotonic
    // discriminants broke it for Diverted/PartClosed.
    let demoted = if severity_rank(severity) < severity_rank(binding_floor) {
        severity
    } else {
        binding_floor
    };

    (demoted, Some(annotations.join("; ")))
}

/// Picks the `impact_type` to attach to `common::Disruption` from among an
/// incident's currently-relevant periods -- see
/// docs/superpowers/specs/2026-09-01-disruption-impact-type-design.md
/// Decision 4. Filters to `Active` periods (matching `apply_extraction`'s
/// own definition of "currently relevant"), then to periods that actually
/// state an `impact_type`, then prefers one whose `schedule_window` is
/// either absent or currently matching `now` -- the same real-time
/// refinement `apply_extraction`'s own schedule-window demotion check
/// already applies, reused here as a filter rather than a demotion
/// trigger. Resolves the Saturday-bus/Sunday-no-service case: on a
/// Saturday only the bus period's window matches; on a Sunday only the
/// no-service period's does; on a weekday matching neither, this returns
/// `None`, correctly reflecting that neither stated fact currently
/// applies. Takes the FIRST (array/text order) remaining candidate --
/// deliberately not a severity-like ranking across the three values (see
/// Decision 4's "Alternative considered and rejected"); a real, named,
/// unresolved tie-break gap for two simultaneously-eligible periods with
/// no `schedule_window` to disambiguate them (design doc Open
/// questions/risks item 1).
fn governing_impact_type(loaded: &LoadedIncident, now: DateTime<Utc>) -> Option<String> {
    parse_periods(loaded)
        .into_iter()
        .filter(|period| period_phase(period, now) == PeriodPhase::Active)
        .filter(|period| period.impact_type.is_some())
        .find(|period| match &period.schedule_window {
            None => true,
            Some(window) => now_within_window(window, now),
        })
        .and_then(|period| period.impact_type)
}

fn routes_from_stations(line: &LineDefinition, stations: &[String]) -> Vec<AffectedRoute> {
    if stations.len() < 2 {
        return vec![];
    }
    let line_order: Vec<&str> = line.stations.iter().map(|s| s.crs.as_str()).collect();
    let mut in_order: Vec<&String> = stations.iter().collect();
    in_order.sort_by_key(|c| {
        line_order
            .iter()
            .position(|o| *o == c.as_str())
            .unwrap_or(999)
    });
    vec![AffectedRoute {
        from_crs: in_order[0].clone(),
        to_crs: in_order[in_order.len() - 1].clone(),
    }]
}

// --- Inference path ---

/// Raw sample-derived numbers for a line: how many recently-sampled
/// departures were delayed/cancelled, and by how much on average. Computed
/// independently of whether the line also has an incident-derived status —
/// `aggregate()` attaches the result to a line's status either way.
/// `avg_delay_minutes` is averaged over non-cancelled ("running") sampled
/// departures only.
/// Departures at `line`'s configured `sample_stations` that belong to
/// `line` (operator/destination/headcode filtered via `belongs_to_line`).
/// Extracted so this exact filtering definition has one place, not several:
/// shared by `compute_sample_stats`, `infer_from_samples`'s "most cited
/// reason" pass, and (`pub(crate)`) `dedup::dedup_new_sample_stats`, which
/// needs the identical relevance filter before applying its own
/// per-service-id dedup on top.
pub(crate) fn relevant_departures<'a>(
    line: &LineDefinition,
    samples: &'a HashMap<String, StationSample>,
) -> Vec<&'a StationDeparture> {
    line.sample_stations
        .iter()
        .filter_map(|crs| samples.get(crs))
        .flat_map(|sample| sample.departures.iter())
        .filter(|dep| belongs_to_line(dep, line))
        .collect()
}

/// Turns an already-filtered list of departures into raw `SampleStats`
/// counts, given `line`'s (already-merged) classification `thresholds`.
/// Extracted from the old `compute_sample_stats` body so the actual
/// delayed/cancelled/skipped/avg-delay arithmetic has one definition,
/// shared by `compute_sample_stats` (raw, per-cycle, every visible
/// departure -- correct for live severity classification, which wants "what
/// does the window look like right now") and `dedup::dedup_new_sample_stats`
/// (only the departures not already counted this period -- correct for a
/// real "distinct trains" rollup). See `crate::dedup`'s module docs for why
/// these two must stay separate rather than sharing one `SampleStats`.
pub(crate) fn stats_from_departures(
    departures: &[&StationDeparture],
    line: &LineDefinition,
    thresholds: &Defaults,
) -> SampleStats {
    let total = departures.len();
    let cancelled = departures.iter().filter(|d| d.is_cancelled).count();
    let delayed = departures
        .iter()
        .filter(|d| !d.is_cancelled && d.delay_minutes as i64 >= thresholds.delay_threshold_minutes)
        .count();
    let line_stations: HashSet<&str> = line.stations.iter().map(|s| s.crs.as_str()).collect();
    let skipped = departures
        .iter()
        .filter(|d| {
            !d.is_cancelled
                && d.skipped_stations
                    .iter()
                    .any(|crs| line_stations.contains(crs.as_str()))
        })
        .count();
    let running: Vec<&&StationDeparture> = departures.iter().filter(|d| !d.is_cancelled).collect();
    let avg_delay_minutes = if running.is_empty() {
        0.0
    } else {
        running.iter().map(|d| d.delay_minutes as f64).sum::<f64>() / running.len() as f64
    };

    SampleStats {
        total,
        delayed,
        cancelled,
        skipped,
        avg_delay_minutes,
    }
}

/// `has_any_row` is deliberately not folded into `relevant_departures`
/// itself, per Decision 2 -- that function's shared job
/// (`compute_sample_availability`, `infer_from_samples`'s "most cited
/// reason" pass at line 777, and `dedup::dedup_new_sample_stats`) is "give
/// me the relevant departures," and a second return channel would change
/// its signature for two callers that don't need the distinction.
fn compute_sample_availability(
    line: &LineDefinition,
    samples: &HashMap<String, StationSample>,
    defaults: &Defaults,
) -> common::SampleAvailability {
    let thresholds = thresholds_for(defaults, &line.severity_overrides);
    let has_any_row = line
        .sample_stations
        .iter()
        .any(|crs| samples.contains_key(crs));
    if !has_any_row {
        return common::SampleAvailability::NoCoverage;
    }

    let relevant = relevant_departures(line, samples);
    if (relevant.len() as i64) < thresholds.min_sample_size {
        return common::SampleAvailability::BelowThreshold {
            observed: relevant.len(),
            required: thresholds.min_sample_size,
        };
    }

    common::SampleAvailability::Available(stats_from_departures(&relevant, line, &thresholds))
}

fn infer_from_samples(
    line: &LineDefinition,
    samples: &HashMap<String, StationSample>,
    defaults: &Defaults,
) -> LineStatus {
    let availability = compute_sample_availability(line, samples, defaults);
    let Some(stats) = availability.sample_stats() else {
        // NoCoverage or BelowThreshold: severity is unchanged (still
        // GoodService, same as today's `.unwrap_or_else(good_service)`
        // fallback), but -- unlike today -- the reason it's absent is no
        // longer discarded here. This is the core fix this plan exists for.
        let mut status = good_service();
        status.sample_availability = availability;
        return status;
    };
    let thresholds = thresholds_for(defaults, &line.severity_overrides);

    let cancel_rate = stats.cancelled as f64 / stats.total as f64;
    let delay_rate = stats.delayed as f64 / stats.total as f64;
    let skip_rate = stats.skipped as f64 / stats.total as f64;

    let (severity, mut reason) = classify(
        ClassifyCounts {
            cancel_rate,
            delay_rate,
            skip_rate,
            total: stats.total,
            cancelled: stats.cancelled,
            delayed: stats.delayed,
            skipped: stats.skipped,
        },
        &thresholds,
    );
    if severity == Severity::GoodService {
        let mut status = good_service();
        status.sample_stats = Some(stats);
        status.sample_availability = availability;
        return status;
    }

    // `compute_sample_availability` only returns aggregate counts, not the
    // raw departures, so the "most cited reason" text below re-derives its
    // own small filtered view via the shared `relevant_departures` helper.
    // Cheap: a handful of departures per line per cycle.
    let relevant = relevant_departures(line, samples);
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

    LineStatus {
        severity,
        reason: reason.clone(),
        validity: ValidityPeriod {
            from_date: Utc::now(),
            to_date: None,
            is_now: true,
        },
        disruption: Some(Disruption {
            category: "RealTime".to_string(),
            description: reason,
            affected_stops,
            affected_routes: vec![],
            source: Some("ldbws-sampling".to_string()),
            impact_type: None, // LDBWS sampling never runs the enricher's extraction pipeline
        }),
        data_quality: DataQuality::LdbwsInferred,
        sample_stats: Some(stats),
        sample_availability: availability,
    }
}

/// Operator filter is mandatory; destination-CRS/headcode-prefix filters
/// are optional narrowing, used at shared-trunk sample stations.
fn belongs_to_line(dep: &StationDeparture, line: &LineDefinition) -> bool {
    if !line.operators.contains(&dep.operator) {
        return false;
    }
    if !line.destination_crs_filter.is_empty()
        && !line.destination_crs_filter.contains(&dep.destination_crs)
    {
        return false;
    }
    if !line.headcode_prefixes.is_empty() {
        let Some(headcode) = &dep.headcode else {
            return false;
        };
        if !line
            .headcode_prefixes
            .iter()
            .any(|p| headcode.starts_with(p.as_str()))
        {
            return false;
        }
    }
    true
}

/// Bundles `classify`'s rate/count inputs so the function itself stays
/// under clippy's `too_many_arguments` threshold. Rates and counts are kept
/// as separate fields (not re-derived from `cancelled/total` etc. inside
/// `classify`) so tests can exercise the threshold logic with rates that
/// don't have to arithmetically match the counts used in the reason
/// string -- see e.g. `classify_prefers_delay_when_more_severe_than_skip`.
struct ClassifyCounts {
    cancel_rate: f64,
    delay_rate: f64,
    skip_rate: f64,
    total: usize,
    cancelled: usize,
    delayed: usize,
    skipped: usize,
}

fn classify(counts: ClassifyCounts, thresholds: &Defaults) -> (Severity, String) {
    let ClassifyCounts {
        cancel_rate,
        delay_rate,
        skip_rate,
        total,
        cancelled,
        delayed,
        skipped,
    } = counts;
    if cancel_rate >= thresholds.part_suspended_pct {
        return (
            Severity::PartSuspended,
            format!("{cancelled} of {total} sampled services cancelled."),
        );
    }
    if cancel_rate >= thresholds.reduced_service_pct {
        return (
            Severity::ReducedService,
            format!("{cancelled} of {total} sampled services cancelled."),
        );
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
        (Some(d), Some(s)) if d < s => {
            (d, format!("{delayed} of {total} sampled services delayed."))
        }
        (Some(_), Some(s)) => (
            s,
            format!("{skipped} of {total} sampled services skipping a scheduled stop."),
        ),
        (Some(d), None) => (d, format!("{delayed} of {total} sampled services delayed.")),
        (None, Some(s)) => (
            s,
            format!("{skipped} of {total} sampled services skipping a scheduled stop."),
        ),
        (None, None) => (Severity::GoodService, "Good Service".to_string()),
    }
}

/// Lets live LDBWS sample stats (average delay, cancellation rate) escalate
/// an incident-derived status's severity -- never demote it, mirroring
/// `escalate_from_severity_hint`'s escalate-only shape. Reuses `classify`,
/// the same thresholds/severity mapping `infer_from_samples` applies when a
/// line has no incident-derived status at all, so live delay data is held
/// to one consistent standard regardless of whether an incident also
/// happens to be reported for the line: a "Minor Delays" incident whose
/// live samples actually show severe cancellations displays as severe, not
/// as a footnote under a status that undersells it.
///
/// Only escalates on a STRICTLY higher `severity_rank` than the status
/// already carries, same tie-break rule as the severity-hint escalation --
/// at an equal or lower rank this is a no-op, so a status already at least
/// as severe as what the samples imply is left untouched.
fn escalate_from_sample_stats(
    severity: Severity,
    stats: &SampleStats,
    thresholds: &Defaults,
) -> (Severity, Option<String>) {
    if stats.total == 0 {
        return (severity, None);
    }
    let cancel_rate = stats.cancelled as f64 / stats.total as f64;
    let delay_rate = stats.delayed as f64 / stats.total as f64;
    let skip_rate = stats.skipped as f64 / stats.total as f64;
    let (sample_severity, reason) = classify(
        ClassifyCounts {
            cancel_rate,
            delay_rate,
            skip_rate,
            total: stats.total,
            cancelled: stats.cancelled,
            delayed: stats.delayed,
            skipped: stats.skipped,
        },
        thresholds,
    );

    if severity_rank(sample_severity) <= severity_rank(severity) {
        return (severity, None);
    }
    (
        sample_severity,
        Some(format!("live samples show: {reason}")),
    )
}

fn good_service() -> LineStatus {
    LineStatus {
        severity: Severity::GoodService,
        reason: "Good Service".to_string(),
        validity: ValidityPeriod {
            from_date: Utc::now(),
            to_date: None,
            is_now: true,
        },
        disruption: None,
        data_quality: DataQuality::LdbwsInferred,
        sample_stats: None,
        sample_availability: SampleAvailability::NoCoverage, // placeholder; always overwritten by every caller
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
    counts
        .into_iter()
        .max_by_key(|(reason, count)| (*count, *reason))
        .map(|(item, _)| item)
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

    fn incident(
        id: &str,
        summary: &str,
        description: &str,
        operators: &[&str],
        affected_stations: &[&str],
    ) -> IncidentMessage {
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
                extracted_periods: None,
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
        assert_eq!(
            reports["swr-south-west-main"].worst_severity(),
            Severity::GoodService
        );
        assert_eq!(
            reports["swr-portsmouth-direct"].worst_severity(),
            Severity::GoodService
        );
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

    fn departure(
        destination_crs: &str,
        delay_minutes: i32,
        is_cancelled: bool,
    ) -> StationDeparture {
        StationDeparture {
            service_id: "svc".to_string(),
            operator: "SW".to_string(),
            destination_crs: destination_crs.to_string(),
            scheduled: "10:00".to_string(),
            estimated: "10:00".to_string(),
            is_cancelled,
            delay_minutes,
            cancel_reason: if is_cancelled {
                Some("fault".to_string())
            } else {
                None
            },
            delay_reason: if !is_cancelled && delay_minutes > 0 {
                Some("signal failure".to_string())
            } else {
                None
            },
            headcode: None,
            skipped_stations: vec![],
        }
    }

    #[test]
    fn belongs_to_line_filters_by_operator() {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let matching = departure("AON", 0, false);
        let wrong_operator = StationDeparture {
            operator: "XX".to_string(),
            ..matching.clone()
        };
        assert!(belongs_to_line(&matching, alton));
        assert!(!belongs_to_line(&wrong_operator, alton));
    }

    #[test]
    fn belongs_to_line_filters_by_destination_crs() {
        // swr-alton.toml: destination_crs_filter = ["AON", "BTL", "FRM", "AHT"]
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let matching = departure("AON", 0, false);
        let wrong_destination = StationDeparture {
            destination_crs: "WOK".to_string(),
            ..matching.clone()
        };
        assert!(belongs_to_line(&matching, alton));
        assert!(!belongs_to_line(&wrong_destination, alton));
    }

    #[test]
    fn belongs_to_line_gatwick_express_operator_folds_in_via_operators_list() {
        // Task 5.6 (southern-brighton-main-line.toml). Its own header
        // comment folds Gatwick Express (`GX`) into `operators = ["SN",
        // "GX"]` rather than a separate file. This is the test that
        // actually exercises what that fold-in buys in production: LDBWS
        // sample classification (`belongs_to_line`, used by
        // `infer_from_samples`) reads `operators`, unlike the incident
        // matcher's station-hit path in `matcher.rs` (`match_one`), which
        // matches on `line.has_station(crs)` alone and never consults
        // `operators` - so a station-hit test there (see
        // `southern_bml_gatwick_express_operator_folds_into_brighton_main_line`
        // in `matcher.rs`) would pass identically whether or not `GX` were
        // ever added to this file's `operators` list. This test, by
        // contrast, would fail without it.
        let lines = load_all_lines();
        let bml = &lines["southern-brighton-main-line"];
        // `departure()` defaults `operator` to "SW" (built for the SWR
        // fixtures above), so it's overridden explicitly for both cases
        // exercised here instead of relying on the helper's default.
        let base = departure("BTN", 0, false);
        let southern = StationDeparture {
            operator: "SN".to_string(),
            ..base.clone()
        };
        let gatwick_express = StationDeparture {
            operator: "GX".to_string(),
            ..base.clone()
        };
        let wrong_operator = StationDeparture {
            operator: "XX".to_string(),
            ..base
        };
        assert!(belongs_to_line(&southern, bml));
        assert!(belongs_to_line(&gatwick_express, bml));
        assert!(!belongs_to_line(&wrong_operator, bml));
    }

    #[test]
    fn infer_from_samples_returns_below_threshold_availability_with_the_correct_counts() {
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
        let status = infer_from_samples(alton, &samples, &defaults);
        assert_eq!(
            status.severity,
            Severity::GoodService,
            "severity behavior is unchanged by this plan"
        );
        assert_eq!(
            status.sample_availability,
            SampleAvailability::BelowThreshold {
                observed: 2,
                required: 3
            }
        );
        assert_eq!(status.sample_stats, None);
    }

    #[test]
    fn infer_from_samples_returns_no_coverage_when_no_sample_station_has_a_row() {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let samples = HashMap::new(); // no rows for AHT/FRM/AON at all
        let status = infer_from_samples(alton, &samples, &defaults);
        assert_eq!(status.severity, Severity::GoodService);
        assert_eq!(status.sample_availability, SampleAvailability::NoCoverage);
        assert_eq!(status.sample_stats, None);
    }

    #[test]
    fn infer_from_samples_at_or_above_threshold_yields_available_matching_compute_sample_stats_today()
     {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let mut samples = HashMap::new();
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                ],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults);
        let SampleAvailability::Available(stats) = &status.sample_availability else {
            panic!("expected Available, got {:?}", status.sample_availability);
        };
        assert_eq!(status.sample_stats.as_ref(), Some(stats));
    }

    #[test]
    fn compute_sample_availability_below_threshold_reports_the_line_specific_override() {
        // Mirrors the existing severity_overrides tests at
        // crates/common/src/lib.rs:839-849: a line with min_sample_size
        // overridden to 5 should report `required: 5`, not the global
        // default of 3.
        let lines = load_all_lines();
        let mut alton = lines["swr-alton"].clone();
        alton
            .severity_overrides
            .insert("min_sample_size".to_string(), 5.0);
        let defaults = Defaults::default();
        let mut samples = HashMap::new();
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                ],
            },
        );
        let availability = compute_sample_availability(&alton, &samples, &defaults);
        assert_eq!(
            availability,
            SampleAvailability::BelowThreshold {
                observed: 4,
                required: 5
            }
        );
    }

    #[test]
    fn aggregate_attaches_no_coverage_to_incident_derived_statuses_with_zero_sample_coverage() {
        // Today this information is dropped entirely at this call site --
        // the direct regression test for aggregate()'s second Layer-2
        // branch (the has-an-incident path).
        let lines = load_all_lines();
        let inc = incident(
            "SWR-99",
            "Points failure on Alton line",
            "A points failure on the Alton line is causing disruption.",
            &["SW"],
            &["AHT"],
        );
        let reports = aggregate_with_defaults(&lines, &[inc]);
        let status = &reports["swr-alton"].statuses[0];
        assert_eq!(status.sample_availability, SampleAvailability::NoCoverage);
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
        let status = infer_from_samples(alton, &samples, &defaults);
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
        let skipping = StationDeparture {
            skipped_stations: vec!["WOK".to_string()],
            ..departure("AON", 0, false)
        };
        let mut samples = HashMap::new();
        // 3 of 4 skip WOK -> 75% skip rate, above the default
        // severe_delays_skip_pct of 0.50, with delay_rate at 0%.
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    skipping.clone(),
                    skipping.clone(),
                    skipping,
                    departure("AON", 0, false),
                ],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults);
        assert_eq!(status.severity, Severity::SevereDelays);
        assert_eq!(status.data_quality, DataQuality::LdbwsInferred);
        assert_eq!(status.sample_stats.expect("stats").skipped, 3);
    }

    #[test]
    fn infer_from_samples_classifies_minor_skip_rate() {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let skipping = StationDeparture {
            skipped_stations: vec!["WOK".to_string()],
            ..departure("AON", 0, false)
        };
        let mut samples = HashMap::new();
        // 1 of 4 skips WOK -> 25% skip rate, exactly at the default
        // minor_delays_skip_pct of 0.25.
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    skipping,
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                ],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults);
        assert_eq!(status.severity, Severity::MinorDelays);
    }

    #[test]
    fn infer_from_samples_ignores_skip_of_station_not_on_line() {
        // "ZZZ" isn't anywhere in swr-alton's `stations` list, so this skip
        // must not count towards skip_rate at all.
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let defaults = Defaults::default();
        let skipping_unrelated = StationDeparture {
            skipped_stations: vec!["ZZZ".to_string()],
            ..departure("AON", 0, false)
        };
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
        let status = infer_from_samples(alton, &samples, &defaults);
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
        let cancelled_with_skips = StationDeparture {
            skipped_stations: vec!["WOK".to_string()],
            ..departure("AON", 0, true)
        };
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
        let status = infer_from_samples(alton, &samples, &defaults);
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
        let (severity, reason) = classify(
            ClassifyCounts {
                cancel_rate: 0.0,
                delay_rate: 0.25,
                skip_rate: 0.75,
                total: 4,
                cancelled: 0,
                delayed: 1,
                skipped: 3,
            },
            &Defaults::default(),
        );
        assert_eq!(severity, Severity::SevereDelays);
        assert!(reason.contains("skipping"), "reason was: {reason}");
    }

    #[test]
    fn classify_prefers_delay_when_more_severe_than_skip() {
        // delay_rate (75%, >= severe_delays_pct 0.50) is more severe than
        // skip_rate (30%, >= minor_delays_skip_pct 0.25 but < severe_delays_skip_pct
        // 0.50) -> the overall severity must be the delay candidate's
        // SevereDelays, not the skip candidate's MinorDelays.
        let (severity, reason) = classify(
            ClassifyCounts {
                cancel_rate: 0.0,
                delay_rate: 0.75,
                skip_rate: 0.30,
                total: 4,
                cancelled: 0,
                delayed: 3,
                skipped: 1,
            },
            &Defaults::default(),
        );
        assert_eq!(severity, Severity::SevereDelays);
        assert!(reason.contains("delayed"), "reason was: {reason}");
        assert!(!reason.contains("skipping"), "reason was: {reason}");
    }

    #[test]
    fn classify_combines_reason_when_delay_and_skip_tie() {
        // Both candidates land on MinorDelays (delay_rate 30% >= 0.25,
        // skip_rate 30% >= 0.25, neither >= their severe threshold) ->
        // combined message naming both counts.
        let (severity, reason) = classify(
            ClassifyCounts {
                cancel_rate: 0.0,
                delay_rate: 0.30,
                skip_rate: 0.30,
                total: 10,
                cancelled: 0,
                delayed: 3,
                skipped: 3,
            },
            &Defaults::default(),
        );
        assert_eq!(severity, Severity::MinorDelays);
        assert!(reason.contains("delayed"), "reason was: {reason}");
        assert!(reason.contains("skipping"), "reason was: {reason}");
    }

    #[test]
    fn classify_cancel_rate_still_takes_priority_over_skip_and_delay() {
        // cancel_rate alone (70%, >= part_suspended_pct 0.60) must win
        // even though skip_rate and delay_rate would also qualify for a
        // milder tier on their own.
        let (severity, _) = classify(
            ClassifyCounts {
                cancel_rate: 0.70,
                delay_rate: 0.75,
                skip_rate: 0.75,
                total: 10,
                cancelled: 7,
                delayed: 7,
                skipped: 7,
            },
            &Defaults::default(),
        );
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
            assert_eq!(
                most_common(&items),
                first,
                "most_common must be deterministic across calls"
            );
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
            StationSample {
                crs: "FRM".to_string(),
                polled_at: Utc::now(),
                departures: severe.clone(),
            },
        );
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: severe,
            },
        );

        let status = infer_from_samples(alton, &samples, &defaults);
        let stops = status
            .disruption
            .expect("severe delays should produce a disruption")
            .affected_stops;
        assert_eq!(
            stops,
            vec!["AHT".to_string(), "FRM".to_string()],
            "affected_stops must be sorted alphabetically"
        );
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
                departures: vec![
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                ],
            },
        );
        let status = infer_from_samples(alton, &samples, &defaults);
        assert_eq!(status.severity, Severity::GoodService);
    }

    #[test]
    fn sample_stats_escalate_an_incident_status_that_undersells_live_delay_data() {
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
        // 4 departures, 3 delayed >= 5 minutes -> classifies as SevereDelays
        // on its own (75% delay rate, above the 50% severe_delays_pct
        // default). Live sample data is allowed to escalate an
        // incident-reported severity that undersells what's actually
        // happening -- see `escalate_from_sample_stats`.
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
            extracted_periods: None,
        };
        let reports = aggregate(&lines, &[loaded], &samples, &registry, &defaults);
        let alton = &reports["swr-alton"];
        assert_eq!(
            alton.worst_severity(),
            Severity::SevereDelays,
            "live sample data showing severe delays must escalate a status that undersells it"
        );
        let stats = alton.statuses[0]
            .sample_stats
            .as_ref()
            .expect("sample stats should be attached even though an incident is active");
        assert_eq!(stats.total, 4);
        assert_eq!(stats.delayed, 3);
        assert_eq!(stats.cancelled, 0);
        assert!(alton.statuses[0].reason.contains("live samples show"));
    }

    #[test]
    fn sample_stats_never_demote_an_already_more_severe_incident_status() {
        let lines = load_all_lines();
        let mut inc = incident(
            "SWR-6",
            "Signal failure blocking lines at Alton",
            "All lines blocked between Alton and Farnham.",
            &["SW"],
            &["AON"],
        );
        inc.summary = "lines blocked at Alton".to_string(); // matches severity_from_incident's keyword
        let registry = SegmentRegistry::new(&lines);
        let defaults = Defaults::default();
        let mut samples = HashMap::new();
        // Good service by sample data (0/4 delayed, 0/4 cancelled) -- must
        // never DEMOTE the incident's own severity, only ever escalate.
        samples.insert(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                    departure("AON", 0, false),
                ],
            },
        );
        let loaded = LoadedIncident {
            message: inc,
            first_seen_at: Utc::now(),
            extracted_periods: None,
        };
        let reports = aggregate(&lines, &[loaded], &samples, &registry, &defaults);
        assert_eq!(
            reports["swr-alton"].worst_severity(),
            Severity::PartSuspended,
            "good live sample data must never demote a more severe incident-reported status"
        );
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
        assert_eq!(
            boundary,
            "2026-07-16T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn next_rail_day_boundary_just_before_local_0200_stays_in_the_earlier_rail_day() {
        // 2026-07-16 00:30 UTC is 01:30 BST -- still inside the rail day
        // that started 2026-07-15 02:00 BST, so the boundary is only 30
        // local minutes away: 2026-07-16 02:00 BST = 2026-07-16 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-07-16T00:30:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(
            boundary,
            "2026-07-16T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn next_rail_day_boundary_just_after_local_0200_rolls_to_the_next_rail_day() {
        // 2026-07-16 01:05 UTC is 02:05 BST -- just past that day's 02:00,
        // so it belongs to the rail day that just started, and the next
        // boundary is a full rail day away: 2026-07-17 02:00 BST = 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-07-16T01:05:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(
            boundary,
            "2026-07-17T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
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
        assert_eq!(
            boundary,
            "2026-03-29T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
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
        assert_eq!(
            boundary,
            "2026-10-25T02:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn period_covers_now_true_when_now_falls_inside_the_period() {
        let now = Utc::now();
        let period = ValidityPeriod {
            from_date: now - Duration::hours(1),
            to_date: Some(now + Duration::hours(1)),
            is_now: true,
        };
        assert!(period_covers_now(&period, now));
    }

    #[test]
    fn period_covers_now_false_once_to_date_has_passed() {
        let now = Utc::now();
        let period = ValidityPeriod {
            from_date: now - Duration::days(2),
            to_date: Some(now - Duration::days(1)),
            is_now: false,
        };
        assert!(!period_covers_now(&period, now));
    }

    fn loaded_at(message: IncidentMessage, first_seen_at: DateTime<Utc>) -> LoadedIncident {
        LoadedIncident {
            message,
            first_seen_at,
            extracted_periods: None,
        }
    }

    #[test]
    fn is_active_true_for_fresh_incident_with_no_validity_periods() {
        let inc = incident("T1", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        assert!(is_active(&loaded_at(inc, now), now));
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
        assert!(!is_active(&loaded_at(inc, now - Duration::days(2)), now));
    }

    #[test]
    fn is_active_true_when_a_validity_period_covers_now() {
        let mut inc = incident("T3", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        inc.validity = vec![ValidityPeriod {
            from_date: now - Duration::hours(1),
            to_date: None,
            is_now: true,
        }];
        assert!(is_active(&loaded_at(inc, now - Duration::hours(1)), now));
    }

    #[test]
    fn is_active_false_for_non_planned_incident_aged_past_the_rail_day_boundary() {
        let inc = incident("T4", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        let first_seen_at = now - Duration::days(2);
        assert!(!is_active(&loaded_at(inc, first_seen_at), now));
    }

    #[test]
    fn is_active_true_for_planned_incident_aged_past_the_rail_day_boundary() {
        let mut inc = incident(
            "T5",
            "Engineering work",
            "Planned engineering work",
            &[],
            &[],
        );
        inc.is_planned = true;
        let now = Utc::now();
        let first_seen_at = now - Duration::days(2);
        assert!(is_active(&loaded_at(inc, first_seen_at), now));
    }

    #[test]
    fn is_active_true_when_one_of_several_validity_periods_covers_now() {
        // Real Knowledgebase incidents can carry more than one validity
        // window; the first is already over, the second is current.
        let mut inc = incident("T6", "Delay", "Delay description", &[], &[]);
        let now = Utc::now();
        inc.validity = vec![
            ValidityPeriod {
                from_date: now - Duration::days(2),
                to_date: Some(now - Duration::days(1)),
                is_now: false,
            },
            ValidityPeriod {
                from_date: now - Duration::hours(1),
                to_date: None,
                is_now: true,
            },
        ];
        assert!(is_active(&loaded_at(inc, now), now));
    }

    #[test]
    fn is_active_false_for_planned_incident_whose_validity_has_expired() {
        // is_planned only exempts the rail-day age cutoff, not the
        // validity-window check -- a planned closure whose own stated
        // window has already ended should still be excluded, the same as
        // any other incident with expired validity.
        let mut inc = incident(
            "T7",
            "Engineering work",
            "Planned engineering work",
            &[],
            &[],
        );
        inc.is_planned = true;
        let now = Utc::now();
        inc.validity = vec![ValidityPeriod {
            from_date: now - Duration::days(2),
            to_date: Some(now - Duration::days(1)),
            is_now: false,
        }];
        assert!(!is_active(&loaded_at(inc, now - Duration::days(2)), now));
    }

    #[test]
    fn is_active_true_for_non_planned_incident_aged_past_the_boundary_with_high_confidence_schedule_window()
     {
        // The nightly-rail-replacement case: not `is_planned`, but the
        // extraction found a genuine recurring schedule, so it's exempted
        // from the age cutoff the same way `is_planned` already is.
        let inc = incident(
            "T8",
            "Signal fault",
            "Rail replacement 23:00-05:00 nightly",
            &[],
            &[],
        );
        let now = Utc::now();
        let mut loaded = loaded_at(inc, now - Duration::days(10));
        loaded.extracted_periods = Some(serde_json::json!([{
            "scope_description": null,
            "date_range": null,
            "schedule_window": { "days_of_week": [1, 2, 3, 4, 5], "start_time": "23:00", "end_time": "05:00" },
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }]));
        assert!(is_active(&loaded, now));
    }

    #[test]
    fn is_active_false_for_non_planned_incident_with_low_confidence_schedule_window() {
        // A schedule window extracted at low confidence isn't trustworthy
        // enough to grant the age-cutoff exemption -- same confidence bar
        // `apply_extraction` already holds every other signal to.
        let inc = incident(
            "T9",
            "Signal fault",
            "Rail replacement 23:00-05:00 nightly",
            &[],
            &[],
        );
        let now = Utc::now();
        let mut loaded = loaded_at(inc, now - Duration::days(10));
        loaded.extracted_periods = Some(serde_json::json!([{
            "scope_description": null,
            "date_range": null,
            "schedule_window": { "days_of_week": [1, 2, 3, 4, 5], "start_time": "23:00", "end_time": "05:00" },
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "low",
            "severity_confidence": ""
        }]));
        assert!(!is_active(&loaded, now));
    }

    #[test]
    fn is_active_false_for_non_planned_incident_with_malformed_schedule_window() {
        // Bad extraction data must not manufacture an age-cutoff exemption
        // -- the opposite fail-safe direction from `now_within_window`,
        // where bad data must not manufacture a demotion. A malformed
        // `schedule_window` (missing days_of_week/start_time/end_time)
        // fails the *whole* `extracted_periods` parse -- see
        // `parse_periods` -- which `has_recurring_schedule` then correctly
        // treats as "no periods at all", same fail-safe direction one
        // level up.
        let inc = incident("T10", "Signal fault", "Rail replacement nightly", &[], &[]);
        let now = Utc::now();
        let mut loaded = loaded_at(inc, now - Duration::days(10));
        loaded.extracted_periods = Some(serde_json::json!([{
            "scope_description": null,
            "date_range": null,
            "schedule_window": { "not": "a schedule window" },
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }]));
        assert!(!is_active(&loaded, now));
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
            extracted_periods: None,
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

    /// Builds a `LoadedIncident` with a single extraction period -- the
    /// direct analogue of the pre-multi-period design's flat
    /// `extracted_resolution_status`/`extraction_confidence`/
    /// `extracted_schedule_window`/`extracted_eta` fields, now folded into
    /// one `ExtractionPeriod` JSON object (design doc §1). `eta` folds into
    /// `date_range.to_date` with no stated `from_date`, exactly as the
    /// design specifies -- so a past `eta` makes this period `Elapsed`, and
    /// a future (or absent) `eta` leaves it `Active`.
    fn loaded_with_extraction(
        resolution_status: Option<&str>,
        confidence: Option<&str>,
        schedule_window: Option<serde_json::Value>,
        eta: Option<DateTime<Utc>>,
    ) -> LoadedIncident {
        let date_range =
            eta.map(|eta| serde_json::json!({ "from_date": null, "to_date": eta.to_rfc3339() }));
        let period = serde_json::json!({
            "scope_description": null,
            "date_range": date_range,
            "schedule_window": schedule_window,
            "resolution_status": resolution_status.unwrap_or("ongoing"),
            "apparent_severity": "normal",
            "resolution_status_confidence": confidence.unwrap_or(""),
            "severity_confidence": "",
        });
        LoadedIncident {
            message: incident("EXT1", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(serde_json::Value::Array(vec![period])),
        }
    }

    /// Builds a `LoadedIncident` with a single `Active` extraction period
    /// carrying only an `apparent_severity`/`severity_confidence` --
    /// `resolution_status_confidence` is left empty (never `"high"`) so the
    /// demote-only half of `apply_extraction` can never fire, keeping these
    /// tests focused on the escalation half alone.
    fn loaded_with_severity(severity: Option<&str>, confidence: Option<&str>) -> LoadedIncident {
        let period = serde_json::json!({
            "scope_description": null,
            "date_range": null,
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": severity.unwrap_or("normal"),
            "resolution_status_confidence": "",
            "severity_confidence": confidence.unwrap_or(""),
        });
        LoadedIncident {
            message: incident("EXT2", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(serde_json::Value::Array(vec![period])),
        }
    }

    #[test]
    fn apply_extraction_is_a_no_op_with_no_extraction() {
        // extracted_periods is None entirely -- no extraction has ever
        // succeeded for this incident.
        let loaded = LoadedIncident {
            message: incident("EXT1", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: None,
        };
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, Utc::now());
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_is_a_no_op_with_an_empty_periods_array() {
        // A malformed/degenerate `extracted_periods: []` (which `enricher`
        // should never write, per design doc §1's "always >= 1 entry"
        // invariant, but this crate has no way to enforce that from the
        // read side) must behave identically to no extraction at all.
        let loaded = LoadedIncident {
            message: incident("EXT1", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(serde_json::json!([])),
        };
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
        // "Demote" means push toward a MILDER `severity_rank`, never a more
        // severe one. GoodService's rank is already strictly milder than
        // MinorDelays' rank, so the rank comparison must leave it unchanged
        // rather than pulling it down to the MinorDelays floor. (Contrast
        // with the equal-rank case, e.g. ReducedService + resolved, which
        // *does* land on the floor -- see the tie-break tests below.)
        let loaded = loaded_with_extraction(Some("resolved"), Some("high"), None, None);
        let (severity, _) = apply_extraction(Severity::GoodService, &loaded, Utc::now());
        assert_eq!(severity, Severity::GoodService);
    }

    #[test]
    fn apply_extraction_lands_equal_rank_resolved_on_minor_delays() {
        // ReducedService and MinorDelays share the mild `severity_rank` (3),
        // but they are not the same severity. A `resolved` extraction must
        // still land on the specific named floor (MinorDelays), not leave
        // `severity` unchanged just because the ranks already match --
        // otherwise the "reported resolved" annotation ends up stapled to a
        // ReducedService status whose own text never claims resolution.
        let loaded = loaded_with_extraction(Some("resolved"), Some("high"), None, None);
        let (severity, annotation) =
            apply_extraction(Severity::ReducedService, &loaded, Utc::now());
        assert_eq!(severity, Severity::MinorDelays);
        assert!(annotation.unwrap().contains("resolved"));
    }

    #[test]
    fn apply_extraction_lands_equal_rank_residual_on_recovering() {
        // MinorDelays and Recovering also share the mild rank (3). A
        // `residual` extraction must still land on Recovering specifically,
        // not leave a MinorDelays base severity unchanged.
        let loaded = loaded_with_extraction(Some("residual"), Some("high"), None, None);
        let (severity, annotation) = apply_extraction(Severity::MinorDelays, &loaded, Utc::now());
        assert_eq!(severity, Severity::Recovering);
        assert!(annotation.unwrap().contains("residual"));
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
        // that's midday in London. High confidence because EVERY signal is
        // confidence-gated, not just resolution status -- this test exercises
        // the window logic, so it has to clear that gate first.
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap(); // 13:00 BST
        let window = serde_json::json!({ "days_of_week": [1,2,3,4,5,6,7], "start_time": "22:00", "end_time": "06:00" });
        let loaded = loaded_with_extraction(None, Some("high"), Some(window), None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::MinorDelays);
        assert!(annotation.is_some());
    }

    #[test]
    fn apply_extraction_no_op_when_now_is_inside_an_overnight_schedule_window() {
        let now: DateTime<Utc> = "2026-06-15T23:00:00Z".parse().unwrap(); // 00:00 BST, inside 22:00-06:00
        let window = serde_json::json!({ "days_of_week": [1,2,3,4,5,6,7], "start_time": "22:00", "end_time": "06:00" });
        let loaded = loaded_with_extraction(None, Some("high"), Some(window), None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_no_op_in_the_early_morning_tail_of_a_weeknights_only_window() {
        // Regression: an overnight window on a PARTIAL day-of-week set.
        // 2026-06-19T23:30:00Z is 00:30 BST on Saturday the 20th -- but the
        // window instance still running is FRIDAY night's, which is in the
        // Mon-Fri set. Matching today's weekday (Saturday, 6) instead of the
        // window's start day (Friday, 5) reported "outside the window" and
        // demoted a disruption that is genuinely still active.
        let now: DateTime<Utc> = "2026-06-19T23:30:00Z".parse().unwrap();
        let window = serde_json::json!({ "days_of_week": [1,2,3,4,5], "start_time": "22:00", "end_time": "06:00" });
        let loaded = loaded_with_extraction(None, Some("high"), Some(window), None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_still_demotes_in_a_morning_tail_whose_window_never_started() {
        // The counterpart to the test above, proving the yesterday-lookup
        // didn't just make every early morning "inside". 2026-06-20T23:30:00Z
        // is 00:30 BST on SUNDAY; the window that would be running started
        // Saturday night, and Saturday (6) is not in the Mon-Fri set, so
        // nothing is active and the demotion still fires.
        let now: DateTime<Utc> = "2026-06-20T23:30:00Z".parse().unwrap();
        let window = serde_json::json!({ "days_of_week": [1,2,3,4,5], "start_time": "22:00", "end_time": "06:00" });
        let loaded = loaded_with_extraction(None, Some("high"), Some(window), None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::MinorDelays);
        assert!(annotation.is_some());
    }

    #[test]
    fn apply_extraction_demotes_when_eta_has_already_passed() {
        // Regression for design doc §4/testing plan: `eta` folds into a
        // single period's `date_range.to_date` (§1) with no stated
        // `from_date`, which makes the period `Elapsed` once that instant
        // passes -- and an `Elapsed` period's synthetic `MinorDelays` floor
        // must reproduce today's pre-multi-period `extracted_eta`-passed
        // rule exactly.
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let eta = now - Duration::hours(1);
        let loaded = loaded_with_extraction(None, Some("high"), None, Some(eta));
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::MinorDelays);
        assert!(annotation.unwrap().contains("expected to end"));
    }

    #[test]
    fn apply_extraction_no_op_when_eta_is_in_the_future() {
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let eta = now + Duration::hours(1);
        let loaded = loaded_with_extraction(None, Some("high"), None, Some(eta));
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_ignores_schedule_window_and_eta_below_high_confidence() {
        // The confidence gate covers EVERY signal, not just resolution
        // status -- and, per design doc §4, an `Elapsed` period's synthetic
        // floor too. Two SEPARATE periods are used here (one `Active` with
        // a demoting schedule window, one `Elapsed` via a passed
        // `date_range.to_date`, i.e. the folded-in ETA) rather than one
        // combined period, since an `Elapsed` period ignores its own
        // `schedule_window` entirely regardless of confidence (§4) -- the
        // only way to test both signals independently is two periods. At
        // anything below "high" confidence, NEITHER period's floor may
        // fire.
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap(); // 13:00 BST, outside 22:00-06:00
        let eta = now - Duration::hours(1);
        for confidence in ["", "low"] {
            let periods = serde_json::json!([
                {
                    "scope_description": null,
                    "date_range": null,
                    "schedule_window": { "days_of_week": [1,2,3,4,5,6,7], "start_time": "22:00", "end_time": "06:00" },
                    "resolution_status": "ongoing",
                    "apparent_severity": "normal",
                    "resolution_status_confidence": confidence,
                    "severity_confidence": ""
                },
                {
                    "scope_description": null,
                    "date_range": { "from_date": null, "to_date": eta.to_rfc3339() },
                    "schedule_window": null,
                    "resolution_status": "ongoing",
                    "apparent_severity": "normal",
                    "resolution_status_confidence": confidence,
                    "severity_confidence": ""
                }
            ]);
            let loaded = LoadedIncident {
                message: incident("EXT1", "Signal failure", "Delays expected", &[], &[]),
                first_seen_at: Utc::now(),
                extracted_periods: Some(periods),
            };
            let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
            assert_eq!(severity, Severity::Suspended, "confidence {confidence:?}");
            assert_eq!(annotation, None, "confidence {confidence:?}");
        }
    }

    #[test]
    fn apply_extraction_demotes_a_diverted_incident_reported_resolved() {
        // Regression for the discriminant-vs-rank bug. Diverted's
        // discriminant is 21, higher than MinorDelays' 9, so the old
        // `severity.max(MinorDelays)` left it at Diverted -- a passenger saw
        // a severe status carrying an annotation that said it was over.
        // Ranked properly, Diverted is severe (4) and MinorDelays mild (3),
        // so it demotes.
        let loaded = loaded_with_extraction(Some("resolved"), Some("high"), None, None);
        let (severity, annotation) = apply_extraction(Severity::Diverted, &loaded, Utc::now());
        assert_eq!(severity, Severity::MinorDelays);
        assert!(annotation.unwrap().contains("resolved"));
    }

    #[test]
    fn apply_extraction_demotes_a_part_closed_incident_reported_residual() {
        // Same bug, the other non-monotonic code (PartClosed = 11) and the
        // other floor (Recovering = 20, mild).
        let loaded = loaded_with_extraction(Some("residual"), Some("high"), None, None);
        let (severity, _) = apply_extraction(Severity::PartClosed, &loaded, Utc::now());
        assert_eq!(severity, Severity::Recovering);
    }

    #[test]
    fn apply_extraction_never_promotes_a_mild_status_via_a_severe_looking_floor() {
        // Demote-only in the rank direction: Recovering (rank 3, mild) is
        // already at the floor's rank, so a "residual" extraction must leave
        // it exactly where it is rather than swapping it for the floor.
        let loaded = loaded_with_extraction(Some("residual"), Some("high"), None, None);
        let (severity, _) = apply_extraction(Severity::Recovering, &loaded, Utc::now());
        assert_eq!(severity, Severity::Recovering);
    }

    #[test]
    fn apply_extraction_combines_multiple_firing_rows_taking_the_most_severe_floor() {
        // Two rows fire at once: high-confidence "residual" (floor
        // Recovering, ordinal 20) and an out-of-window schedule (floor
        // MinorDelays, ordinal 9). Per spec §7, "take the most severe
        // (lowest-numbered) resulting severity" -- MinorDelays (9) is more
        // severe than Recovering (20), so the combined result must be
        // MinorDelays, not Recovering from the resolution-status row alone.
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap(); // 13:00 BST
        let window = serde_json::json!({ "days_of_week": [1,2,3,4,5,6,7], "start_time": "22:00", "end_time": "06:00" });
        let loaded = loaded_with_extraction(Some("residual"), Some("high"), Some(window), None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::MinorDelays);
        let annotation = annotation.unwrap();
        assert!(
            annotation.contains("residual"),
            "annotation was: {annotation}"
        );
        assert!(annotation.contains("22:00"), "annotation was: {annotation}");
    }

    #[test]
    fn apply_extraction_no_op_when_schedule_window_days_of_week_is_empty() {
        // An empty days_of_week is malformed/degenerate extraction data
        // (the enricher's JSON schema allows it -- no minItems constraint),
        // not a meaningful "active on no day" signal. Must fail safe the
        // same direction as an unparsable start_time/end_time: never
        // manufacture a demotion out of malformed data.
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let window =
            serde_json::json!({ "days_of_week": [], "start_time": "22:00", "end_time": "06:00" });
        let loaded = loaded_with_extraction(None, Some("high"), Some(window), None);
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_escalates_a_high_confidence_severe_hint() {
        let loaded = loaded_with_severity(Some("severe_disruption"), Some("high"));
        let (severity, annotation) = apply_extraction(Severity::MinorDelays, &loaded, Utc::now());
        assert_eq!(severity, Severity::SevereDelays);
        assert!(annotation.unwrap().contains("more severe"));
    }

    #[test]
    fn apply_extraction_escalates_a_blocked_or_suspended_hint_to_part_suspended() {
        // The motivating case: "the lines...are blocked" doesn't match the
        // base classifier's literal "lines blocked" substring, so it falls
        // through to MinorDelays -- the severity-hint signal catches it.
        let loaded = loaded_with_severity(Some("blocked_or_suspended"), Some("high"));
        let (severity, annotation) = apply_extraction(Severity::MinorDelays, &loaded, Utc::now());
        assert_eq!(severity, Severity::PartSuspended);
        assert!(annotation.is_some());
    }

    #[test]
    fn apply_extraction_ignores_low_confidence_severity_hint() {
        let loaded = loaded_with_severity(Some("severe_disruption"), Some("low"));
        let (severity, annotation) = apply_extraction(Severity::MinorDelays, &loaded, Utc::now());
        assert_eq!(severity, Severity::MinorDelays);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_normal_severity_hint_never_escalates() {
        let loaded = loaded_with_severity(Some("normal"), Some("high"));
        let (severity, annotation) = apply_extraction(Severity::MinorDelays, &loaded, Utc::now());
        assert_eq!(severity, Severity::MinorDelays);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_never_escalates_an_already_equally_severe_status() {
        // Suspended and SevereDelays are both severity_rank 4 -- escalation
        // must not swap in a different rank-4 named value over one the
        // regex classifier already correctly identified as severe.
        let loaded = loaded_with_severity(Some("severe_disruption"), Some("high"));
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, Utc::now());
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_escalation_confidence_is_independent_of_resolution_confidence() {
        // resolution_status_confidence gates the demote-only signals only;
        // severity_confidence is its own gate, so a severity hint can
        // escalate even when resolution_status_confidence is low/absent.
        // `loaded_with_severity` already leaves resolution_status_confidence
        // empty (never "high"), so this is really just re-asserting that
        // fact explicitly as its own test.
        let loaded = loaded_with_severity(Some("blocked_or_suspended"), Some("high"));
        let (severity, annotation) = apply_extraction(Severity::MinorDelays, &loaded, Utc::now());
        assert_eq!(severity, Severity::PartSuspended);
        assert!(annotation.is_some());
    }

    #[test]
    fn apply_extraction_escalation_and_demotion_compose_in_one_pass() {
        // "The fault's fixed, but severe knock-on delays remain": the
        // severity hint escalates the regex-derived base up, and the
        // resolution-status floor then re-caps it back down to MinorDelays
        // -- the two signals don't fight, and both annotations survive.
        let period = serde_json::json!({
            "scope_description": null,
            "date_range": null,
            "schedule_window": null,
            "resolution_status": "resolved",
            "apparent_severity": "severe_disruption",
            "resolution_status_confidence": "high",
            "severity_confidence": "high",
        });
        let loaded = LoadedIncident {
            message: incident("EXT3", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(serde_json::Value::Array(vec![period])),
        };
        let (severity, annotation) = apply_extraction(Severity::MinorDelays, &loaded, Utc::now());
        assert_eq!(severity, Severity::MinorDelays);
        let annotation = annotation.unwrap();
        assert!(
            annotation.contains("more severe"),
            "escalation annotation missing: {annotation}"
        );
        assert!(
            annotation.contains("reported resolved"),
            "demotion annotation missing: {annotation}"
        );
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
            extracted_periods: None,
        };
        let reports = aggregate(&lines, &[loaded], &HashMap::new(), &registry, &defaults);
        assert_eq!(
            reports["swr-alton"].worst_severity(),
            Severity::PlannedClosure
        );
    }

    // --- Multi-period extraction tests ---
    //
    // See docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md
    // §4 and its testing plan.

    fn period_from_json(value: serde_json::Value) -> ExtractionPeriod {
        serde_json::from_value(value).expect("test period should deserialize")
    }

    #[test]
    fn period_phase_active_when_date_range_is_null() {
        // The common single-fact case -- no distinct date range at all.
        let period = period_from_json(serde_json::json!({
            "scope_description": null,
            "date_range": null,
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }));
        assert_eq!(period_phase(&period, Utc::now()), PeriodPhase::Active);
    }

    #[test]
    fn period_phase_active_when_date_range_covers_now() {
        let now = Utc::now();
        let period = period_from_json(serde_json::json!({
            "scope_description": null,
            "date_range": {
                "from_date": (now - Duration::hours(1)).to_rfc3339(),
                "to_date": (now + Duration::hours(1)).to_rfc3339()
            },
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }));
        assert_eq!(period_phase(&period, now), PeriodPhase::Active);
    }

    #[test]
    fn period_phase_elapsed_once_to_date_has_passed() {
        let now = Utc::now();
        let period = period_from_json(serde_json::json!({
            "scope_description": null,
            "date_range": { "from_date": null, "to_date": (now - Duration::hours(1)).to_rfc3339() },
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }));
        assert_eq!(period_phase(&period, now), PeriodPhase::Elapsed);
    }

    #[test]
    fn period_phase_not_started_when_from_date_is_in_the_future() {
        let now = Utc::now();
        let period = period_from_json(serde_json::json!({
            "scope_description": null,
            "date_range": { "from_date": (now + Duration::hours(1)).to_rfc3339(), "to_date": null },
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }));
        assert_eq!(period_phase(&period, now), PeriodPhase::NotStarted);
    }

    #[test]
    fn period_phase_fails_safe_to_active_on_malformed_dates() {
        // Malformed/unparseable dates must resolve to `Active`, NOT
        // `Elapsed` (which would manufacture a demotion out of bad data)
        // and NOT `NotStarted` (which would silently drop a period that
        // might genuinely be live right now).
        let now = Utc::now();
        let malformed_to = period_from_json(serde_json::json!({
            "scope_description": null,
            "date_range": { "from_date": null, "to_date": "not a date" },
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }));
        assert_eq!(period_phase(&malformed_to, now), PeriodPhase::Active);

        let malformed_from = period_from_json(serde_json::json!({
            "scope_description": null,
            "date_range": { "from_date": "not a date", "to_date": null },
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }));
        assert_eq!(period_phase(&malformed_from, now), PeriodPhase::Active);
    }

    #[test]
    fn apply_extraction_two_active_periods_both_fire_most_severe_wins_both_annotations_kept() {
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let periods = serde_json::json!([
            {
                "scope_description": "platform 2 closed, calls at platform 1",
                "date_range": null,
                "schedule_window": null,
                "resolution_status": "residual",
                "apparent_severity": "normal",
                "resolution_status_confidence": "high",
                "severity_confidence": ""
            },
            {
                "scope_description": "platform 3 closed, calls at platform 4",
                "date_range": null,
                "schedule_window": { "days_of_week": [1,2,3,4,5,6,7], "start_time": "22:00", "end_time": "06:00" },
                "resolution_status": "ongoing",
                "apparent_severity": "normal",
                "resolution_status_confidence": "high",
                "severity_confidence": ""
            }
        ]);
        let loaded = LoadedIncident {
            message: incident("MULTI1", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        // residual -> Recovering floor (rank 3); schedule-excludes-now ->
        // MinorDelays floor (also rank 3) -- same tie-break as the
        // single-period case picks MinorDelays.
        assert_eq!(severity, Severity::MinorDelays);
        let annotation = annotation.unwrap();
        assert!(
            annotation.contains("platform 2 closed"),
            "annotation was: {annotation}"
        );
        assert!(
            annotation.contains("platform 3 closed"),
            "annotation was: {annotation}"
        );
        assert!(
            annotation.contains("residual"),
            "annotation was: {annotation}"
        );
        assert!(annotation.contains("22:00"), "annotation was: {annotation}");
    }

    #[test]
    fn apply_extraction_elapsed_period_never_uses_its_own_resolution_status_or_severity_claims() {
        // One `Elapsed` period (whose own resolution_status/apparent_severity
        // claim severe/escalation-worthy things it must NOT be trusted for)
        // alongside one genuinely `Active` period, whose own checks still
        // run normally.
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let periods = serde_json::json!([
            {
                "scope_description": "phase 1",
                "date_range": { "from_date": null, "to_date": (now - Duration::hours(2)).to_rfc3339() },
                "schedule_window": null,
                "resolution_status": "ongoing",
                "apparent_severity": "severe_disruption",
                "resolution_status_confidence": "high",
                "severity_confidence": "high"
            },
            {
                "scope_description": "phase 2",
                "date_range": null,
                "schedule_window": null,
                "resolution_status": "residual",
                "apparent_severity": "normal",
                "resolution_status_confidence": "high",
                "severity_confidence": ""
            }
        ]);
        let loaded = LoadedIncident {
            message: incident("MULTI2", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        // Elapsed synthetic floor = MinorDelays (rank 3); Active residual
        // floor = Recovering (rank 3) -- tie-break picks MinorDelays.
        assert_eq!(severity, Severity::MinorDelays);
        let annotation = annotation.unwrap();
        assert!(
            annotation.contains("phase 1"),
            "annotation was: {annotation}"
        );
        assert!(
            annotation.contains("expected to end by"),
            "annotation was: {annotation}"
        );
        assert!(
            annotation.contains("phase 2"),
            "annotation was: {annotation}"
        );
        assert!(
            annotation.contains("residual"),
            "annotation was: {annotation}"
        );
        assert!(
            !annotation.contains("more severe"),
            "an Elapsed period's apparent_severity must never escalate, even at high severity_confidence: {annotation}"
        );
    }

    #[test]
    fn apply_extraction_elapsed_period_demotes_regardless_of_its_own_resolution_status_claim() {
        // "An ongoing-claiming elapsed period must demote identically to a
        // resolved-claiming one, since the claim is ignored either way."
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let to_date = now - Duration::hours(1);
        for claim in ["ongoing", "resolved", "residual"] {
            let periods = serde_json::json!([{
                "scope_description": null,
                "date_range": { "from_date": null, "to_date": to_date.to_rfc3339() },
                "schedule_window": null,
                "resolution_status": claim,
                "apparent_severity": "normal",
                "resolution_status_confidence": "high",
                "severity_confidence": ""
            }]);
            let loaded = LoadedIncident {
                message: incident("MULTI3", "Signal failure", "Delays expected", &[], &[]),
                first_seen_at: Utc::now(),
                extracted_periods: Some(periods),
            };
            let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
            assert_eq!(severity, Severity::MinorDelays, "claim {claim}");
            assert!(
                annotation.unwrap().contains("expected to end"),
                "claim {claim}"
            );
        }
    }

    #[test]
    fn apply_extraction_not_started_period_contributes_nothing() {
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let periods = serde_json::json!([{
            "scope_description": null,
            "date_range": { "from_date": (now + Duration::hours(1)).to_rfc3339(), "to_date": null },
            "schedule_window": null,
            "resolution_status": "resolved",
            "apparent_severity": "severe_disruption",
            "resolution_status_confidence": "high",
            "severity_confidence": "high"
        }]);
        let loaded = LoadedIncident {
            message: incident("MULTI4", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        let (severity, annotation) = apply_extraction(Severity::Suspended, &loaded, now);
        assert_eq!(severity, Severity::Suspended);
        assert_eq!(annotation, None);
    }

    #[test]
    fn apply_extraction_escalation_combines_across_periods_most_severe_wins() {
        let periods = serde_json::json!([
            {
                "scope_description": "phase 1",
                "date_range": null,
                "schedule_window": null,
                "resolution_status": "ongoing",
                "apparent_severity": "moderate_disruption",
                "resolution_status_confidence": "",
                "severity_confidence": "high"
            },
            {
                "scope_description": "phase 2",
                "date_range": null,
                "schedule_window": null,
                "resolution_status": "ongoing",
                "apparent_severity": "blocked_or_suspended",
                "resolution_status_confidence": "",
                "severity_confidence": "high"
            }
        ]);
        let loaded = LoadedIncident {
            message: incident("MULTI5", "Signal failure", "Delays expected", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        let (severity, annotation) = apply_extraction(Severity::MinorDelays, &loaded, Utc::now());
        assert_eq!(severity, Severity::PartSuspended);
        assert!(annotation.unwrap().contains("phase 2"));
    }

    #[test]
    fn has_recurring_schedule_elapsed_period_no_longer_exempts_from_rail_day_cutoff() {
        // The trap flagged in design doc §4: an incident whose only
        // recurring-schedule period has already elapsed must NOT keep
        // exempting itself from the rail-day cutoff -- exactly the "SWR
        // forgot about it" failure mode that cutoff exists to catch.
        let inc = incident(
            "T11",
            "Signal fault",
            "Rail replacement 23:00-05:00 nightly",
            &[],
            &[],
        );
        let now = Utc::now();
        let mut loaded = loaded_at(inc, now - Duration::days(10));
        loaded.extracted_periods = Some(serde_json::json!([{
            "scope_description": null,
            "date_range": { "from_date": null, "to_date": (now - Duration::days(1)).to_rfc3339() },
            "schedule_window": { "days_of_week": [1,2,3,4,5,6,7], "start_time": "23:00", "end_time": "05:00" },
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }]));
        assert!(
            !is_active(&loaded, now),
            "an elapsed recurring-schedule period must not exempt an incident from the rail-day cutoff"
        );
    }

    #[test]
    fn governing_impact_type_returns_none_with_no_periods() {
        let loaded = LoadedIncident {
            message: incident("GIT1", "Signal failure", "Delays", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: None,
        };
        assert_eq!(governing_impact_type(&loaded, Utc::now()), None);
    }

    #[test]
    fn governing_impact_type_returns_none_when_the_active_periods_impact_type_is_null() {
        let periods = serde_json::json!([{
            "scope_description": null,
            "date_range": null,
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": "",
            "impact_type": null
        }]);
        let loaded = LoadedIncident {
            message: incident("GIT2", "Signal failure", "Delays", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        assert_eq!(governing_impact_type(&loaded, Utc::now()), None);
    }

    #[test]
    fn governing_impact_type_returns_the_single_active_periods_value() {
        let periods = serde_json::json!([{
            "scope_description": null,
            "date_range": null,
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "severe_disruption",
            "resolution_status_confidence": "high",
            "severity_confidence": "high",
            "impact_type": "rail_replacement_bus"
        }]);
        let loaded = LoadedIncident {
            message: incident(
                "GIT3",
                "Buses replace trains",
                "Engineering works",
                &[],
                &[],
            ),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        assert_eq!(
            governing_impact_type(&loaded, Utc::now()),
            Some("rail_replacement_bus".to_string())
        );
    }

    #[test]
    fn governing_impact_type_ignores_a_pre_change_period_missing_the_key_entirely() {
        // Proves the aggregator mirror's #[serde(default)] backward-compat.
        let periods = serde_json::json!([{
            "scope_description": null,
            "date_range": null,
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": ""
        }]);
        let loaded = LoadedIncident {
            message: incident("GIT4", "Signal failure", "Delays", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        assert_eq!(governing_impact_type(&loaded, Utc::now()), None);
    }

    #[test]
    fn governing_impact_type_never_uses_an_elapsed_or_not_started_periods_value() {
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let periods = serde_json::json!([
            {
                "scope_description": "already over",
                "date_range": { "from_date": null, "to_date": (now - Duration::hours(1)).to_rfc3339() },
                "schedule_window": null,
                "resolution_status": "ongoing",
                "apparent_severity": "normal",
                "resolution_status_confidence": "high",
                "severity_confidence": "",
                "impact_type": "rail_replacement_bus"
            },
            {
                "scope_description": "not yet started",
                "date_range": { "from_date": (now + Duration::hours(1)).to_rfc3339(), "to_date": null },
                "schedule_window": null,
                "resolution_status": "ongoing",
                "apparent_severity": "normal",
                "resolution_status_confidence": "high",
                "severity_confidence": "",
                "impact_type": "diversion"
            }
        ]);
        let loaded = LoadedIncident {
            message: incident("GIT5", "Engineering works", "Various", &[], &[]),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        assert_eq!(governing_impact_type(&loaded, now), None);
    }

    #[test]
    fn governing_impact_type_saturday_sunday_case_picks_the_period_whose_window_matches_now() {
        // The exact Example 1 shape: one shared date_range, two periods
        // with different schedule_windows and different impact_types.
        let periods = serde_json::json!([
            {
                "scope_description": "Saturday bus",
                "date_range": null,
                "schedule_window": { "days_of_week": [6], "start_time": "00:00", "end_time": "23:59" },
                "resolution_status": "ongoing",
                "apparent_severity": "moderate_disruption",
                "resolution_status_confidence": "high",
                "severity_confidence": "",
                "impact_type": "rail_replacement_bus"
            },
            {
                "scope_description": "Sunday no service",
                "date_range": null,
                "schedule_window": { "days_of_week": [7], "start_time": "00:00", "end_time": "23:59" },
                "resolution_status": "ongoing",
                "apparent_severity": "blocked_or_suspended",
                "resolution_status_confidence": "high",
                "severity_confidence": "",
                "impact_type": "no_scheduled_service"
            }
        ]);
        let loaded = LoadedIncident {
            message: incident(
                "GIT6",
                "Buses replace trains",
                "Weekend engineering works",
                &[],
                &[],
            ),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };

        let saturday_noon: DateTime<Utc> = "2026-06-13T12:00:00Z".parse().unwrap(); // a Saturday
        assert_eq!(
            governing_impact_type(&loaded, saturday_noon),
            Some("rail_replacement_bus".to_string())
        );

        let sunday_noon: DateTime<Utc> = "2026-06-14T12:00:00Z".parse().unwrap(); // a Sunday
        assert_eq!(
            governing_impact_type(&loaded, sunday_noon),
            Some("no_scheduled_service".to_string())
        );

        let monday_noon: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap(); // neither window matches
        assert_eq!(governing_impact_type(&loaded, monday_noon), None);
    }

    #[test]
    fn status_from_incident_threads_governing_impact_type_into_the_disruption() {
        let lines = load_all_lines();
        let alton = &lines["swr-alton"];
        let now: DateTime<Utc> = "2026-06-15T12:00:00Z".parse().unwrap();
        let periods = serde_json::json!([{
            "scope_description": null,
            "date_range": null,
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "resolution_status_confidence": "high",
            "severity_confidence": "",
            "impact_type": "rail_replacement_bus"
        }]);
        let loaded = LoadedIncident {
            message: incident(
                "GIT7",
                "Engineering works",
                "Buses replace trains",
                &["SW"],
                &["AHT"],
            ),
            first_seen_at: Utc::now(),
            extracted_periods: Some(periods),
        };
        let m = crate::matcher::Match {
            line: alton,
            scope: MatchScope::ExclusiveSegment,
            evidence: crate::matcher::Evidence {
                stations: vec!["AHT".to_string()],
                segments: vec![],
                operators: vec![],
                keywords: vec![],
            },
        };

        let status = status_from_incident(&m, &loaded, now);

        assert_eq!(
            status.disruption.unwrap().impact_type.as_deref(),
            Some("rail_replacement_bus")
        );
    }
}
