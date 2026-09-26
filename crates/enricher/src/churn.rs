//! Churn instrumentation: measures how much an incident's LLM extraction
//! changes between two successive *text versions* of the same incident,
//! under the same model/prompt version. Purely observational -- nothing here
//! feeds back into what gets extracted or written. The question it exists to
//! answer is whether a diff-aware prompt ("here's what you said last time,
//! here's what changed") is worth building: if re-extraction from scratch
//! already reproduces the untouched parts of an incident faithfully, it
//! isn't.
//!
//! ## What counts as a re-run
//!
//! Only a *text-change* re-extraction ([`baseline_for_text_change_rerun`]):
//! a previous extraction exists, its `source_text_hash` differs from the
//! current text's hash, and its `extraction_model_version` equals the
//! current one. A model-version bump (the hourly sweep re-extracting
//! everything after a prompt/schema change) is excluded -- a different model
//! is expected to answer differently, which says nothing about churn.
//!
//! ## Pairing rule
//!
//! Periods have no stable identity across runs, so old and new periods are
//! paired in two passes ([`pair_periods`]):
//!
//! 1. **By key**: a new period pairs with the first still-unpaired old period
//!    with an identical `date_range` and an identical `scope_description`
//!    after trimming and ASCII-lowercasing (both `None` counts as equal).
//! 2. **By position**: whatever is still unpaired on each side is paired off
//!    in original order (i-th leftover old with i-th leftover new). This is
//!    what makes a re-worded scope or a shifted date range show up as a
//!    `scope_description`/`date_range` change on a pair rather than as one
//!    period removed plus one added -- and what pairs the overwhelmingly
//!    common single-period incident regardless of wording.
//!
//! Anything left over after both passes (only possible when the period
//! counts differ) is *unpaired*. Every compared field is then checked on
//! every pair; `scope_description`/`date_range` can only differ on a
//! positional pair, since key pairs match on them by construction.
//!
//! ## Metrics and log (emitted only after the write actually lands)
//!
//! - `distant_signal_enricher_extraction_rerun_total{changed="true"|"false"}`
//!   -- one per measured text-change re-run; `changed` is whether *any*
//!   field below changed.
//! - `distant_signal_enricher_extraction_churn_total{field=...}` -- at most
//!   one per re-run per field, so `churn{field=X} / rerun_total` is "the
//!   fraction of re-runs in which X changed". `field` is one of
//!   [`ChurnField::label`]'s fixed values.
//! - One `tracing::info!` line, message `"extraction re-run churn"`, with
//!   `incident_id`, old/new period counts, pair counts, and a
//!   comma-separated `changed_fields` list. No incident text is logged
//!   (`scope_description` is LLM-paraphrased incident text, so only whether
//!   it changed is recorded, never its value).

use std::collections::BTreeSet;

use crate::llm::ExtractionPeriod;
use crate::queries::IncidentState;

/// The previous (about-to-be-overwritten) extraction for an incident, parsed
/// out of `incidents.extracted_category`/`extracted_periods`.
#[derive(Debug, Clone, PartialEq)]
pub struct Baseline {
    pub category: Option<String>,
    pub periods: Vec<ExtractionPeriod>,
}

/// Returns the previous extraction to compare against iff this run is a
/// text-change re-extraction under the *same* model version -- see the
/// module doc. `None` for a first extraction, a model-version bump, a
/// same-text retry, or a stored `extracted_periods` value that doesn't parse
/// (logged at warn and otherwise ignored: measurement must never affect the
/// write path).
pub fn baseline_for_text_change_rerun(
    incident_id: &str,
    state: &IncidentState,
    new_text_hash: &str,
    current_model_version: &str,
) -> Option<Baseline> {
    let previous_hash = state.source_text_hash.as_deref()?;
    if previous_hash == new_text_hash {
        return None;
    }
    if state.extraction_model_version.as_deref() != Some(current_model_version) {
        return None;
    }
    let raw_periods = state.extracted_periods.as_ref()?;
    match serde_json::from_value::<Vec<ExtractionPeriod>>(raw_periods.clone()) {
        Ok(periods) => Some(Baseline {
            category: state.extracted_category.clone(),
            periods,
        }),
        Err(err) => {
            tracing::warn!(
                incident_id,
                error = %err,
                "could not parse the previous extraction for churn measurement; skipping it"
            );
            None
        }
    }
}

/// One thing that can change between two extractions. `label` values are
/// the `field` label of `enricher_extraction_churn_total` -- a fixed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChurnField {
    Category,
    PeriodCount,
    /// At least one period on either side had no counterpart.
    UnpairedPeriod,
    ScopeDescription,
    DateRange,
    ScheduleWindow,
    ResolutionStatus,
    ApparentSeverity,
    ImpactType,
}

impl ChurnField {
    pub fn label(self) -> &'static str {
        match self {
            ChurnField::Category => "category",
            ChurnField::PeriodCount => "period_count",
            ChurnField::UnpairedPeriod => "unpaired_period",
            ChurnField::ScopeDescription => "scope_description",
            ChurnField::DateRange => "date_range",
            ChurnField::ScheduleWindow => "schedule_window",
            ChurnField::ResolutionStatus => "resolution_status",
            ChurnField::ApparentSeverity => "apparent_severity",
            ChurnField::ImpactType => "impact_type",
        }
    }
}

/// Result of comparing one previous extraction with its replacement.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChurnReport {
    pub old_period_count: usize,
    pub new_period_count: usize,
    pub key_pairs: usize,
    pub positional_pairs: usize,
    pub unpaired_old: usize,
    pub unpaired_new: usize,
    /// Every field that changed anywhere in this re-run, deduplicated.
    pub changed: BTreeSet<ChurnField>,
}

/// Output of [`pair_periods`]: `(old_index, new_index)` pairs from each pass
/// plus the leftover indices on each side.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Pairing {
    pub by_key: Vec<(usize, usize)>,
    pub by_position: Vec<(usize, usize)>,
    pub unpaired_old: Vec<usize>,
    pub unpaired_new: Vec<usize>,
}

fn normalized_scope(period: &ExtractionPeriod) -> Option<String> {
    period
        .scope_description
        .as_deref()
        .map(|s| s.trim().to_ascii_lowercase())
}

fn same_key(a: &ExtractionPeriod, b: &ExtractionPeriod) -> bool {
    a.date_range == b.date_range && normalized_scope(a) == normalized_scope(b)
}

/// Pairs old and new periods -- see the module doc's "Pairing rule".
pub fn pair_periods(old: &[ExtractionPeriod], new: &[ExtractionPeriod]) -> Pairing {
    let mut old_taken = vec![false; old.len()];
    let mut new_taken = vec![false; new.len()];
    let mut pairing = Pairing::default();

    for (new_idx, new_period) in new.iter().enumerate() {
        let hit = old
            .iter()
            .enumerate()
            .find(|(old_idx, old_period)| !old_taken[*old_idx] && same_key(old_period, new_period))
            .map(|(old_idx, _)| old_idx);
        if let Some(old_idx) = hit {
            old_taken[old_idx] = true;
            new_taken[new_idx] = true;
            pairing.by_key.push((old_idx, new_idx));
        }
    }

    let old_left = (0..old.len()).filter(|i| !old_taken[*i]);
    let mut new_left = (0..new.len()).filter(|i| !new_taken[*i]);
    for old_idx in old_left {
        match new_left.next() {
            Some(new_idx) => pairing.by_position.push((old_idx, new_idx)),
            None => pairing.unpaired_old.push(old_idx),
        }
    }
    pairing.unpaired_new.extend(new_left);
    pairing
}

/// Compares a previous extraction with its replacement. Pure and
/// panic-free (indices come only from `pair_periods`, which draws them from
/// the same slices).
pub fn compare(
    old_category: Option<&str>,
    old_periods: &[ExtractionPeriod],
    new_category: &str,
    new_periods: &[ExtractionPeriod],
) -> ChurnReport {
    let pairing = pair_periods(old_periods, new_periods);
    let mut changed = BTreeSet::new();

    if old_category != Some(new_category) {
        changed.insert(ChurnField::Category);
    }
    if old_periods.len() != new_periods.len() {
        changed.insert(ChurnField::PeriodCount);
    }
    if !pairing.unpaired_old.is_empty() || !pairing.unpaired_new.is_empty() {
        changed.insert(ChurnField::UnpairedPeriod);
    }

    for &(old_idx, new_idx) in pairing.by_key.iter().chain(&pairing.by_position) {
        let (Some(a), Some(b)) = (old_periods.get(old_idx), new_periods.get(new_idx)) else {
            continue;
        };
        if normalized_scope(a) != normalized_scope(b) {
            changed.insert(ChurnField::ScopeDescription);
        }
        if a.date_range != b.date_range {
            changed.insert(ChurnField::DateRange);
        }
        if a.schedule_window != b.schedule_window {
            changed.insert(ChurnField::ScheduleWindow);
        }
        if a.resolution_status != b.resolution_status {
            changed.insert(ChurnField::ResolutionStatus);
        }
        if a.apparent_severity != b.apparent_severity {
            changed.insert(ChurnField::ApparentSeverity);
        }
        if a.impact_type != b.impact_type {
            changed.insert(ChurnField::ImpactType);
        }
    }

    ChurnReport {
        old_period_count: old_periods.len(),
        new_period_count: new_periods.len(),
        key_pairs: pairing.by_key.len(),
        positional_pairs: pairing.by_position.len(),
        unpaired_old: pairing.unpaired_old.len(),
        unpaired_new: pairing.unpaired_new.len(),
        changed,
    }
}

/// Emits the metrics and log line for one measured re-run -- see the module
/// doc. Call only once the new extraction has actually been written.
pub fn record(incident_id: &str, report: &ChurnReport) {
    metrics::counter!(
        common::metrics::metric_name("enricher_extraction_rerun_total"),
        "changed" => if report.changed.is_empty() { "false" } else { "true" }
    )
    .increment(1);
    for field in &report.changed {
        metrics::counter!(
            common::metrics::metric_name("enricher_extraction_churn_total"),
            "field" => field.label()
        )
        .increment(1);
    }
    let changed_fields = report
        .changed
        .iter()
        .map(|f| f.label())
        .collect::<Vec<_>>()
        .join(",");
    tracing::info!(
        incident_id,
        old_period_count = report.old_period_count,
        new_period_count = report.new_period_count,
        key_pairs = report.key_pairs,
        positional_pairs = report.positional_pairs,
        unpaired_old = report.unpaired_old,
        unpaired_new = report.unpaired_new,
        changed = !report.changed.is_empty(),
        changed_fields = %changed_fields,
        "extraction re-run churn"
    );
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::llm::{DateRange, ScheduleWindow};

    fn period(scope: &str, severity: &str) -> ExtractionPeriod {
        ExtractionPeriod {
            scope_description: Some(scope.to_string()),
            date_range: None,
            schedule_window: None,
            resolution_status: "ongoing".to_string(),
            apparent_severity: severity.to_string(),
            impact_type: None,
            resolution_status_confidence: "high".to_string(),
            severity_confidence: "high".to_string(),
        }
    }

    fn fields(report: &ChurnReport) -> Vec<&'static str> {
        report.changed.iter().map(|f| f.label()).collect()
    }

    #[test]
    fn identical_extractions_report_no_change() {
        let periods = vec![period("line closed", "blocked_or_suspended")];
        let report = compare(
            Some("engineering_works"),
            &periods,
            "engineering_works",
            &periods,
        );
        assert!(report.changed.is_empty(), "{report:?}");
        assert_eq!(report.key_pairs, 1);
        assert_eq!(report.positional_pairs, 0);
    }

    #[test]
    fn confidence_only_differences_are_not_churn() {
        let old = vec![period("line closed", "blocked_or_suspended")];
        let mut new = old.clone();
        new[0].severity_confidence = "low".to_string();
        new[0].resolution_status_confidence = "low".to_string();
        assert!(compare(Some("x"), &old, "x", &new).changed.is_empty());
    }

    #[test]
    fn severity_only_change_is_reported_as_just_severity() {
        let old = vec![period("line closed", "severe_disruption")];
        let new = vec![period("line closed", "moderate_disruption")];
        let report = compare(Some("x"), &old, "x", &new);
        assert_eq!(fields(&report), vec!["apparent_severity"]);
        assert_eq!(report.key_pairs, 1);
    }

    #[test]
    fn each_paired_field_is_detected() {
        let old = vec![period("a", "normal")];
        let mut new = old.clone();
        new[0].resolution_status = "resolved".to_string();
        new[0].impact_type = Some("rail_replacement_bus".to_string());
        new[0].schedule_window = Some(ScheduleWindow {
            days_of_week: vec![6, 7],
            start_time: "00:00".to_string(),
            end_time: "23:59".to_string(),
        });
        let report = compare(Some("x"), &old, "x", &new);
        assert_eq!(
            fields(&report),
            vec!["schedule_window", "resolution_status", "impact_type"]
        );
    }

    #[test]
    fn category_change_is_reported() {
        let periods = vec![period("a", "normal")];
        let report = compare(Some("signal_failure"), &periods, "trespass", &periods);
        assert_eq!(fields(&report), vec!["category"]);
        // A previous NULL category also counts as a change.
        let report = compare(None, &periods, "trespass", &periods);
        assert_eq!(fields(&report), vec!["category"]);
    }

    #[test]
    fn added_period_is_unpaired_and_changes_count() {
        let old = vec![period("northbound", "severe_disruption")];
        let new = vec![
            period("southbound", "moderate_disruption"),
            period("Northbound ", "severe_disruption"),
        ];
        let report = compare(Some("x"), &old, "x", &new);
        // "Northbound " key-matches "northbound" (trim + lowercase), so the
        // extra period is the unpaired one and the matched pair is unchanged.
        assert_eq!(report.key_pairs, 1);
        assert_eq!(report.unpaired_new, 1);
        assert_eq!(report.unpaired_old, 0);
        assert_eq!(fields(&report), vec!["period_count", "unpaired_period"]);
    }

    #[test]
    fn removed_period_is_unpaired_and_changes_count() {
        let old = vec![period("a", "normal"), period("b", "severe_disruption")];
        let new = vec![period("b", "severe_disruption")];
        let report = compare(Some("x"), &old, "x", &new);
        assert_eq!(report.key_pairs, 1);
        assert_eq!(report.unpaired_old, 1);
        assert_eq!(fields(&report), vec!["period_count", "unpaired_period"]);
    }

    #[test]
    fn reworded_scope_pairs_by_position_and_reports_scope_change() {
        let old = vec![period("platform 2 closed", "moderate_disruption")];
        let new = vec![period("platform two shut", "moderate_disruption")];
        let report = compare(Some("x"), &old, "x", &new);
        assert_eq!(report.key_pairs, 0);
        assert_eq!(report.positional_pairs, 1);
        assert_eq!(fields(&report), vec!["scope_description"]);
    }

    #[test]
    fn unpaired_leftovers_after_positional_pass_with_uneven_counts() {
        let old = vec![
            period("a", "normal"),
            period("b", "normal"),
            period("c", "normal"),
        ];
        let new = vec![period("c", "normal"), period("z", "severe_disruption")];
        let pairing = pair_periods(&old, &new);
        assert_eq!(pairing.by_key, vec![(2, 0)]);
        assert_eq!(pairing.by_position, vec![(0, 1)]);
        assert_eq!(pairing.unpaired_old, vec![1]);
        assert!(pairing.unpaired_new.is_empty());
        let report = compare(Some("x"), &old, "x", &new);
        assert_eq!(
            fields(&report),
            vec![
                "period_count",
                "unpaired_period",
                "scope_description",
                "apparent_severity"
            ]
        );
    }

    #[test]
    fn shifted_date_range_is_a_date_range_change_not_add_plus_remove() {
        let mut old = period("weekend closure", "blocked_or_suspended");
        old.date_range = Some(DateRange {
            from_date: Some(Utc.with_ymd_and_hms(2026, 10, 3, 0, 0, 0).unwrap()),
            to_date: Some(Utc.with_ymd_and_hms(2026, 10, 4, 0, 0, 0).unwrap()),
        });
        let mut new = old.clone();
        new.date_range = Some(DateRange {
            from_date: Some(Utc.with_ymd_and_hms(2026, 10, 10, 0, 0, 0).unwrap()),
            to_date: Some(Utc.with_ymd_and_hms(2026, 10, 11, 0, 0, 0).unwrap()),
        });
        let report = compare(Some("x"), &[old], "x", &[new]);
        assert_eq!(report.positional_pairs, 1);
        assert_eq!(fields(&report), vec!["date_range"]);
    }

    fn state(
        hash: Option<&str>,
        version: Option<&str>,
        periods: Option<serde_json::Value>,
    ) -> IncidentState {
        IncidentState {
            summary: String::new(),
            description: String::new(),
            source_text_hash: hash.map(str::to_string),
            extraction_model_version: version.map(str::to_string),
            first_seen_at: Utc::now(),
            extracted_category: Some("signal_failure".to_string()),
            extracted_periods: periods,
        }
    }

    fn stored_periods() -> serde_json::Value {
        serde_json::to_value(vec![period("a", "normal")]).unwrap()
    }

    #[test]
    fn text_change_under_the_same_model_version_is_measured() {
        let s = state(Some("old-hash"), Some("m@v2"), Some(stored_periods()));
        let baseline = baseline_for_text_change_rerun("I1", &s, "new-hash", "m@v2")
            .expect("a same-version text change must be measured");
        assert_eq!(baseline.category.as_deref(), Some("signal_failure"));
        assert_eq!(baseline.periods, vec![period("a", "normal")]);
    }

    #[test]
    fn model_version_bump_rerun_is_not_counted() {
        // Same text, new model version: the sweep's version-bump re-run.
        let s = state(Some("same-hash"), Some("m@v1"), Some(stored_periods()));
        assert_eq!(
            baseline_for_text_change_rerun("I1", &s, "same-hash", "m@v2"),
            None
        );
        // Text AND version changed together: still a different model, so
        // still excluded.
        let s = state(Some("old-hash"), Some("m@v1"), Some(stored_periods()));
        assert_eq!(
            baseline_for_text_change_rerun("I1", &s, "new-hash", "m@v2"),
            None
        );
    }

    #[test]
    fn first_extraction_and_same_text_retry_are_not_counted() {
        let s = state(None, None, None);
        assert_eq!(baseline_for_text_change_rerun("I1", &s, "h", "m@v2"), None);
        let s = state(Some("h"), Some("m@v2"), Some(stored_periods()));
        assert_eq!(baseline_for_text_change_rerun("I1", &s, "h", "m@v2"), None);
        // Hash present but no stored periods (pre-periods legacy row).
        let s = state(Some("old"), Some("m@v2"), None);
        assert_eq!(
            baseline_for_text_change_rerun("I1", &s, "new", "m@v2"),
            None
        );
    }

    #[test]
    fn unparseable_stored_periods_are_skipped_not_an_error() {
        let s = state(
            Some("old"),
            Some("m@v2"),
            Some(serde_json::json!({ "not": "an array" })),
        );
        assert_eq!(
            baseline_for_text_change_rerun("I1", &s, "new", "m@v2"),
            None
        );
    }
}
