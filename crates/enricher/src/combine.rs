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
}
