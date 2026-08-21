/// Combines the primary extraction's resolution-status verdict with the
/// adversarial pass's (which only ever argues for "ongoing" or agrees).
/// Returns `(resolution_status, confidence)` as the raw TEXT values to
/// store. Disagreement is treated as genuine ambiguity in the source text
/// -- low confidence, not an averaged or majority-vote answer -- per the
/// spec's asymmetric-risk reasoning: a false "resolved" is worse than a
/// missed one.
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
/// other for the purpose of detecting disagreement here.
fn severity_hint_rank(hint: &str) -> u8 {
    match hint {
        "severe_disruption" | "blocked_or_suspended" => 2,
        "moderate_disruption" => 1,
        _ => 0, // "normal", or any unrecognized value -- fail toward no escalation.
    }
}

/// Combines the primary extraction's `apparent_severity` with the
/// severity-minimizing adversarial pass's answer. Returns
/// `(apparent_severity, confidence)` as the raw TEXT values to store.
/// Mirrors `combine`'s shape but for the opposite risk: here a false
/// escalation (needless alarm, unnecessary rerouting) is the failure this
/// guards against, so disagreement -- the adversarial pass finding a
/// materially milder honest reading -- is low confidence, the same
/// "genuine ambiguity, not an averaged answer" treatment `combine` gives
/// resolution-status disagreement.
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
}
