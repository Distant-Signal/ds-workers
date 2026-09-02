//! Combines the primary extraction pass's per-period verdicts with the two
//! adversarial passes' per-period verdicts. See
//! docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md, §2.

use std::fmt;

use crate::llm::{AdversarialPeriodVerdict, ExtractionPeriod, SeverityAdversarialPeriodVerdict};

/// Combines one period's primary resolution-status verdict with the
/// adversarial pass's (which only ever argues for "ongoing" or agrees).
/// Returns `(resolution_status, confidence)` as the raw TEXT values to
/// store. Disagreement is treated as genuine ambiguity in the source text
/// -- low confidence, not an averaged or majority-vote answer -- per the
/// spec's asymmetric-risk reasoning: a false "resolved" is worse than a
/// missed one. Unchanged in substance from the original design's flat,
/// incident-level version -- it now just runs once per period index
/// instead of once per incident (see `combine_periods` below).
pub fn combine(primary_status: &str, adversarial_status: &str) -> (String, String) {
    if primary_status == "ongoing" {
        // No demotion is possible from "ongoing" either way, so the
        // adversarial pass's answer can't change the outcome.
        return (primary_status.to_string(), "high".to_string());
    }
    if adversarial_status == "ongoing" {
        (primary_status.to_string(), "low".to_string())
    } else {
        (primary_status.to_string(), "high".to_string())
    }
}

/// Ordering for `apparent_severity` values, most to least severe possible
/// escalation. `severe_disruption` and `blocked_or_suspended` rank equally
/// -- both map to `common::severity_rank`'s "severe" tier in
/// `aggregation::escalation_ceiling`, so neither is a milder read of the
/// other for the purpose of detecting disagreement here. `pub(crate)`
/// because `llm.rs`'s over-cap truncation (Decision 3,
/// docs/superpowers/specs/2026-09-01-enricher-period-cap-remediation-design.md)
/// reuses this exact ordering to rank which periods survive the
/// `MAX_PERIODS` cap -- the same relative severity order applies to both
/// "does the adversarial pass disagree" and "which periods are most
/// consequential to keep," so this is shared, not duplicated.
pub(crate) fn severity_hint_rank(hint: &str) -> u8 {
    match hint {
        "severe_disruption" | "blocked_or_suspended" => 2,
        "moderate_disruption" => 1,
        _ => 0, // "normal", or any unrecognized value -- fail toward no escalation.
    }
}

/// Combines one period's primary `apparent_severity` with the
/// severity-minimizing adversarial pass's answer. Returns
/// `(apparent_severity, confidence)` as the raw TEXT values to store.
/// Mirrors `combine`'s shape but for the opposite risk: here a false
/// escalation (needless alarm, unnecessary rerouting) is the failure this
/// guards against, so disagreement -- the adversarial pass finding a
/// materially milder honest reading -- is low confidence.
pub fn combine_severity(primary_severity: &str, adversarial_severity: &str) -> (String, String) {
    if primary_severity == "normal" {
        // No escalation is possible from "normal" either way.
        return (primary_severity.to_string(), "high".to_string());
    }
    if severity_hint_rank(adversarial_severity) < severity_hint_rank(primary_severity) {
        (primary_severity.to_string(), "low".to_string())
    } else {
        (primary_severity.to_string(), "high".to_string())
    }
}

/// Hard-failure reasons for `combine_periods`. Distinguished from a generic
/// `anyhow::Error` so callers (specifically `main.rs::process_incident`)
/// can tell "this is the length/ordinal-alignment risk flagged in design §7
/// items 3 and 4" apart from an ordinary LLM-call error, in order to
/// implement that risk item's operational-visibility requirement (log a
/// distinguishable message when this recurs for the same incident across
/// retries, since `temperature: 0.0` makes it deterministic against
/// unchanged text -- every retry reproduces the identical mismatch).
#[derive(Debug)]
pub enum CombineError {
    /// An adversarial array's length didn't match the primary pass's
    /// `periods` length.
    LengthMismatch { primary_periods: usize, resolution_adversarial: usize, severity_adversarial: usize },
    /// An adversarial array element's echoed `period_index` and/or
    /// `scope_description` didn't match what was sent to it at that
    /// position -- a length-preserving but reordered (or otherwise
    /// misaligned) response, the silent-failure risk design §7 item 4
    /// specifically calls out as harder to catch than a length mismatch.
    AlignmentMismatch { pass: &'static str, period_index: usize },
}

impl fmt::Display for CombineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CombineError::LengthMismatch { primary_periods, resolution_adversarial, severity_adversarial } => write!(
                f,
                "adversarial period-array length mismatch: primary={primary_periods} \
                 resolution_adversarial={resolution_adversarial} severity_adversarial={severity_adversarial}"
            ),
            CombineError::AlignmentMismatch { pass, period_index } => write!(
                f,
                "{pass} adversarial response failed ordinal-alignment check at period_index={period_index} \
                 (echoed period_index/scope_description did not match what was sent)"
            ),
        }
    }
}

impl std::error::Error for CombineError {}

/// Generalizes `combine`/`combine_severity` to operate elementwise over the
/// primary pass's periods and both adversarial passes' per-period verdict
/// arrays. A length mismatch between either adversarial array and the
/// primary period count, or an ordinal-alignment mismatch at any index
/// (the echoed `period_index`/`scope_description` not matching what was
/// sent), is a hard failure of the WHOLE extraction attempt -- the same
/// "no partial-credit storage" philosophy the original design already
/// applies to any schema-validation failure, extended to this new failure
/// mode (design §2).
pub fn combine_periods(
    primary_periods: &[ExtractionPeriod],
    resolution_adversarial: &[AdversarialPeriodVerdict],
    severity_adversarial: &[SeverityAdversarialPeriodVerdict],
) -> Result<Vec<ExtractionPeriod>, CombineError> {
    if resolution_adversarial.len() != primary_periods.len() || severity_adversarial.len() != primary_periods.len() {
        return Err(CombineError::LengthMismatch {
            primary_periods: primary_periods.len(),
            resolution_adversarial: resolution_adversarial.len(),
            severity_adversarial: severity_adversarial.len(),
        });
    }

    primary_periods
        .iter()
        .enumerate()
        .map(|(index, period)| {
            let resolution_verdict = &resolution_adversarial[index];
            if resolution_verdict.period_index != index || resolution_verdict.scope_description != period.scope_description {
                return Err(CombineError::AlignmentMismatch { pass: "resolution", period_index: index });
            }
            let severity_verdict = &severity_adversarial[index];
            if severity_verdict.period_index != index || severity_verdict.scope_description != period.scope_description {
                return Err(CombineError::AlignmentMismatch { pass: "severity", period_index: index });
            }

            let (resolution_status, resolution_status_confidence) = combine(&period.resolution_status, &resolution_verdict.resolution_status);
            let (apparent_severity, severity_confidence) = combine_severity(&period.apparent_severity, &severity_verdict.apparent_severity);

            Ok(ExtractionPeriod {
                scope_description: period.scope_description.clone(),
                date_range: period.date_range.clone(),
                schedule_window: period.schedule_window.clone(),
                resolution_status,
                apparent_severity,
                impact_type: period.impact_type.clone(),
                resolution_status_confidence,
                severity_confidence,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_ongoing_is_always_high_confidence() {
        assert_eq!(combine("ongoing", "ongoing"), ("ongoing".to_string(), "high".to_string()));
        assert_eq!(combine("ongoing", "resolved"), ("ongoing".to_string(), "high".to_string()));
    }

    #[test]
    fn primary_resolved_agreeing_adversarial_is_high_confidence() {
        assert_eq!(combine("resolved", "residual"), ("resolved".to_string(), "high".to_string()));
        assert_eq!(combine("resolved", "resolved"), ("resolved".to_string(), "high".to_string()));
    }

    #[test]
    fn primary_resolved_disagreeing_adversarial_is_low_confidence() {
        assert_eq!(combine("resolved", "ongoing"), ("resolved".to_string(), "low".to_string()));
    }

    #[test]
    fn primary_residual_disagreeing_adversarial_is_low_confidence() {
        assert_eq!(combine("residual", "ongoing"), ("residual".to_string(), "low".to_string()));
    }

    #[test]
    fn primary_residual_agreeing_adversarial_is_high_confidence() {
        assert_eq!(combine("residual", "resolved"), ("residual".to_string(), "high".to_string()));
    }

    #[test]
    fn primary_normal_severity_is_always_high_confidence() {
        assert_eq!(combine_severity("normal", "normal"), ("normal".to_string(), "high".to_string()));
        assert_eq!(combine_severity("normal", "blocked_or_suspended"), ("normal".to_string(), "high".to_string()));
    }

    #[test]
    fn severity_agreeing_or_more_severe_adversarial_is_high_confidence() {
        assert_eq!(
            combine_severity("moderate_disruption", "severe_disruption"),
            ("moderate_disruption".to_string(), "high".to_string())
        );
        assert_eq!(
            combine_severity("blocked_or_suspended", "severe_disruption"),
            ("blocked_or_suspended".to_string(), "high".to_string())
        );
    }

    #[test]
    fn severity_milder_adversarial_is_low_confidence() {
        assert_eq!(
            combine_severity("severe_disruption", "moderate_disruption"),
            ("severe_disruption".to_string(), "low".to_string())
        );
        assert_eq!(
            combine_severity("blocked_or_suspended", "normal"),
            ("blocked_or_suspended".to_string(), "low".to_string())
        );
    }

    fn period(scope: Option<&str>, resolution_status: &str, apparent_severity: &str) -> ExtractionPeriod {
        period_with_impact(scope, resolution_status, apparent_severity, None)
    }

    fn period_with_impact(
        scope: Option<&str>,
        resolution_status: &str,
        apparent_severity: &str,
        impact_type: Option<&str>,
    ) -> ExtractionPeriod {
        ExtractionPeriod {
            scope_description: scope.map(str::to_string),
            date_range: None,
            schedule_window: None,
            resolution_status: resolution_status.to_string(),
            apparent_severity: apparent_severity.to_string(),
            impact_type: impact_type.map(str::to_string),
            resolution_status_confidence: String::new(),
            severity_confidence: String::new(),
        }
    }

    fn resolution_verdict(index: usize, scope: Option<&str>, status: &str) -> AdversarialPeriodVerdict {
        AdversarialPeriodVerdict { period_index: index, scope_description: scope.map(str::to_string), resolution_status: status.to_string() }
    }

    fn severity_verdict(index: usize, scope: Option<&str>, severity: &str) -> SeverityAdversarialPeriodVerdict {
        SeverityAdversarialPeriodVerdict { period_index: index, scope_description: scope.map(str::to_string), apparent_severity: severity.to_string() }
    }

    #[test]
    fn combine_periods_combines_each_index_independently() {
        let primary = vec![period(None, "resolved", "normal"), period(Some("phase 2"), "ongoing", "severe_disruption")];
        let resolution = vec![resolution_verdict(0, None, "ongoing"), resolution_verdict(1, Some("phase 2"), "resolved")];
        let severity = vec![severity_verdict(0, None, "normal"), severity_verdict(1, Some("phase 2"), "moderate_disruption")];

        let result = combine_periods(&primary, &resolution, &severity).unwrap();

        assert_eq!(result.len(), 2);
        // Period 0: resolved + disagreeing adversarial ("ongoing") -> low confidence.
        assert_eq!(result[0].resolution_status, "resolved");
        assert_eq!(result[0].resolution_status_confidence, "low");
        // Period 1: ongoing primary is always high confidence regardless of adversarial answer.
        assert_eq!(result[1].resolution_status, "ongoing");
        assert_eq!(result[1].resolution_status_confidence, "high");
        // Period 0: primary "normal" is always high confidence.
        assert_eq!(result[0].apparent_severity, "normal");
        assert_eq!(result[0].severity_confidence, "high");
        // Period 1: severe primary + milder adversarial -> low confidence.
        assert_eq!(result[1].apparent_severity, "severe_disruption");
        assert_eq!(result[1].severity_confidence, "low");
    }

    #[test]
    fn combine_periods_copies_impact_type_through_unchanged() {
        let primary = vec![period_with_impact(None, "ongoing", "normal", Some("rail_replacement_bus"))];
        let resolution = vec![resolution_verdict(0, None, "ongoing")];
        let severity = vec![severity_verdict(0, None, "normal")];

        let result = combine_periods(&primary, &resolution, &severity).unwrap();

        assert_eq!(result[0].impact_type.as_deref(), Some("rail_replacement_bus"));
    }

    #[test]
    fn combine_periods_copies_a_null_impact_type_through_unchanged() {
        let primary = vec![period(None, "ongoing", "normal")]; // impact_type: None
        let resolution = vec![resolution_verdict(0, None, "ongoing")];
        let severity = vec![severity_verdict(0, None, "normal")];

        let result = combine_periods(&primary, &resolution, &severity).unwrap();

        assert_eq!(result[0].impact_type, None);
    }

    #[test]
    fn combine_periods_fails_on_resolution_length_mismatch() {
        let primary = vec![period(None, "ongoing", "normal"), period(None, "ongoing", "normal")];
        let resolution = vec![resolution_verdict(0, None, "ongoing")]; // only one, primary has two
        let severity = vec![severity_verdict(0, None, "normal"), severity_verdict(1, None, "normal")];

        let err = combine_periods(&primary, &resolution, &severity).unwrap_err();
        assert!(matches!(err, CombineError::LengthMismatch { .. }), "expected a length mismatch, got {err:?}");
    }

    #[test]
    fn combine_periods_fails_on_severity_length_mismatch() {
        let primary = vec![period(None, "ongoing", "normal")];
        let resolution = vec![resolution_verdict(0, None, "ongoing")];
        let severity = vec![]; // empty, primary has one

        let err = combine_periods(&primary, &resolution, &severity).unwrap_err();
        assert!(matches!(err, CombineError::LengthMismatch { .. }), "expected a length mismatch, got {err:?}");
    }

    #[test]
    fn combine_periods_fails_on_reordered_resolution_response() {
        // Length-preserving, but period_index 1's verdict was actually meant
        // for period 0 (its scope_description doesn't match what period 0
        // was sent) -- the reordering risk design §7 item 4 warns about.
        let primary = vec![period(Some("a"), "ongoing", "normal"), period(Some("b"), "ongoing", "normal")];
        let resolution = vec![resolution_verdict(0, Some("b"), "ongoing"), resolution_verdict(1, Some("a"), "ongoing")];
        let severity = vec![severity_verdict(0, Some("a"), "normal"), severity_verdict(1, Some("b"), "normal")];

        let err = combine_periods(&primary, &resolution, &severity).unwrap_err();
        assert!(matches!(err, CombineError::AlignmentMismatch { pass: "resolution", period_index: 0 }), "expected an alignment mismatch, got {err:?}");
    }

    #[test]
    fn combine_periods_fails_on_mismatched_period_index() {
        // scope_description matches by coincidence but the echoed
        // period_index is wrong -- still must be caught.
        let primary = vec![period(None, "ongoing", "normal")];
        let resolution = vec![AdversarialPeriodVerdict { period_index: 5, scope_description: None, resolution_status: "ongoing".to_string() }];
        let severity = vec![severity_verdict(0, None, "normal")];

        let err = combine_periods(&primary, &resolution, &severity).unwrap_err();
        assert!(matches!(err, CombineError::AlignmentMismatch { pass: "resolution", period_index: 0 }), "expected an alignment mismatch, got {err:?}");
    }

    #[test]
    fn combine_periods_fails_on_reordered_severity_response() {
        let primary = vec![period(Some("a"), "ongoing", "normal"), period(Some("b"), "ongoing", "severe_disruption")];
        let resolution = vec![resolution_verdict(0, Some("a"), "ongoing"), resolution_verdict(1, Some("b"), "ongoing")];
        let severity = vec![severity_verdict(0, Some("b"), "normal"), severity_verdict(1, Some("a"), "severe_disruption")];

        let err = combine_periods(&primary, &resolution, &severity).unwrap_err();
        assert!(matches!(err, CombineError::AlignmentMismatch { pass: "severity", period_index: 0 }), "expected an alignment mismatch, got {err:?}");
    }
}
