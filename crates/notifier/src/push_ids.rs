//! The URLs and tags a push carries for a tracked train, a journey leg or a
//! journey break (DS-534). Every one is built from a public id (`t_`, `l_`
//! or `j_` and 32 hex digits: `train_subscriptions.public_id` and its
//! siblings), never from a numeric sequence id: a tag is the source of the
//! push's `Topic` and, for a webhook, a field sent to a receiver the user
//! chose. The notifier's own state (hold keys, the notification state
//! tables) stays keyed on the numeric ids it always was.

/// A tracked train's page, `/train/by-id/<t_ id>`. The predicted-start push
/// used to link `/track/<n>`, a path that only redirects here.
pub(crate) fn train_url(train_public_id: &str) -> String {
    format!("/train/by-id/{train_public_id}")
}

/// A tracked train's tag: every push about the train replaces the last on
/// the device.
pub(crate) fn train_tag(train_public_id: &str) -> String {
    format!("train-{train_public_id}")
}

/// A skipped stop on a leg.
pub(crate) fn leg_skip_tag(leg_public_id: &str) -> String {
    format!("journey-leg-skip-{leg_public_id}")
}

/// A leg with no train found (the regular journey's attention push and the
/// pick reminder share it).
pub(crate) fn leg_unmatched_tag(leg_public_id: &str) -> String {
    format!("journey-leg-unmatched-{leg_public_id}")
}

/// A leave-by reminder.
pub(crate) fn leg_leave_tag(leg_public_id: &str) -> String {
    format!("journey-leg-leave-{leg_public_id}")
}

/// A journey break notice that has no trigger train.
pub(crate) fn journey_break_tag(journey_public_id: &str) -> String {
    format!("journey-break-{journey_public_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRAIN: &str = "t_0123456789abcdef0123456789abcdef";
    const LEG: &str = "l_0123456789abcdef0123456789abcdef";
    const JOURNEY: &str = "j_0123456789abcdef0123456789abcdef";

    #[test]
    fn urls_and_tags_are_built_from_the_public_ids() {
        assert_eq!(
            train_url(TRAIN),
            "/train/by-id/t_0123456789abcdef0123456789abcdef"
        );
        assert_eq!(train_tag(TRAIN), "train-t_0123456789abcdef0123456789abcdef");
        assert_eq!(
            leg_skip_tag(LEG),
            "journey-leg-skip-l_0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            leg_unmatched_tag(LEG),
            "journey-leg-unmatched-l_0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            leg_leave_tag(LEG),
            "journey-leg-leave-l_0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            journey_break_tag(JOURNEY),
            "journey-break-j_0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn each_kind_of_tag_is_distinct_for_the_same_id_text() {
        let tags = [
            leg_skip_tag(LEG),
            leg_unmatched_tag(LEG),
            leg_leave_tag(LEG),
            train_tag(LEG),
            journey_break_tag(LEG),
        ];
        let distinct: std::collections::HashSet<_> = tags.iter().collect();
        assert_eq!(distinct.len(), tags.len());
    }
}
