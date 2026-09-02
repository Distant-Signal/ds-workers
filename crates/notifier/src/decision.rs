//! Pure severity/status-transition decision logic -- no I/O, no database.
//! See docs/superpowers/specs/2026-09-02-line-status-notifications-design.md's
//! Decisions 2, 3, 4, 5 and this plan's Task 3 design notes for why lines
//! and trains use two different-shaped decision functions.
//!
//! Every rank used anywhere in this module is a `severity_rank`-style rank
//! (higher is worse), NEVER `common::Severity`'s own derived `Ord` --
//! `LineStatusReport::worst_severity()` (`crates/common/src/lib.rs`) uses
//! raw `Severity::min()` ordering, which is wrong for detecting real
//! severity transitions (see `crates/common/src/lib.rs`'s `severity_rank`
//! doc comment for the `Diverted`/`PartClosed`-vs-`MinorDelays` example).
//! This module never calls `worst_severity()` or compares `Severity`
//! values directly -- callers (`crates/notifier/src/queries.rs`) are
//! responsible for converting to a rank via `common::severity_rank` before
//! calling in here.

use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyDecision {
    Skip,
    NotifyNow,
}

/// Table-level filter, run once per new `line_status_history` row before
/// any per-user join. `previous_rank = None` means no preceding history
/// row exists for this `line_id` at all (Decision 3's cold-start guard --
/// must not be treated as "changed from nothing").
pub fn is_severity_transition(previous_rank: Option<u8>, new_rank: u8) -> bool {
    match previous_rank {
        None => false,
        Some(previous) => previous != new_rank,
    }
}

/// Per-user decision, called only for a row that already passed
/// `is_severity_transition`. `previous_rank`/`new_rank` are the line's own
/// objective transition (shared across every user pinning this line);
/// `last_notified_rank`/`last_notified_at` are this specific user's own
/// notification history for this line.
pub fn decide_user_notification(
    previous_rank: u8,
    new_rank: u8,
    last_notified_rank: Option<u8>,
    last_notified_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    cooldown: Duration,
) -> NotifyDecision {
    if last_notified_rank == Some(new_rank) {
        // Idempotency guard (Decision 3's note): this user's own state
        // already matches where the line ended up, regardless of cursor
        // position -- do not re-notify.
        return NotifyDecision::Skip;
    }
    let escalated = new_rank > previous_rank;
    if escalated {
        return NotifyDecision::NotifyNow;
    }
    match last_notified_at {
        Some(t) if now - t < cooldown => NotifyDecision::Skip,
        _ => NotifyDecision::NotifyNow,
    }
}

/// Maps a tracked train's derived state onto the same rank shape lines
/// use. Cancellation always outranks any delay reading.
pub fn train_severity_rank(status: &str, delay_minutes: Option<i32>, delay_threshold_minutes: i32) -> u8 {
    if status == "cancelled" {
        2
    } else if delay_minutes.unwrap_or(0) >= delay_threshold_minutes {
        1
    } else {
        0
    }
}

/// Escalation-only (see this plan's Task 3 design notes for why trains
/// don't get a de-escalation/cooldown branch).
pub fn decide_train_notification(previous_rank: u8, new_rank: u8) -> NotifyDecision {
    if new_rank > previous_rank { NotifyDecision::NotifyNow } else { NotifyDecision::Skip }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_prior_history_row_is_never_a_transition() {
        assert!(!is_severity_transition(None, 4));
        assert!(!is_severity_transition(None, 0));
    }

    #[test]
    fn same_rank_is_not_a_transition() {
        assert!(!is_severity_transition(Some(3), 3));
    }

    #[test]
    fn a_real_rank_change_is_a_transition() {
        assert!(is_severity_transition(Some(0), 4));
        assert!(is_severity_transition(Some(4), 0));
    }

    #[test]
    fn escalation_notifies_immediately_even_during_an_active_cooldown() {
        let now = Utc::now();
        let last_notified_at = Some(now - Duration::minutes(5)); // well inside a 20-min cooldown
        let decision = decide_user_notification(0, 4, Some(0), last_notified_at, now, Duration::minutes(20));
        assert_eq!(decision, NotifyDecision::NotifyNow);
    }

    #[test]
    fn deescalation_is_skipped_during_an_active_cooldown() {
        let now = Utc::now();
        let last_notified_at = Some(now - Duration::minutes(5));
        let decision = decide_user_notification(4, 0, Some(4), last_notified_at, now, Duration::minutes(20));
        assert_eq!(decision, NotifyDecision::Skip);
    }

    #[test]
    fn deescalation_notifies_once_the_cooldown_has_elapsed() {
        let now = Utc::now();
        let last_notified_at = Some(now - Duration::minutes(25));
        let decision = decide_user_notification(4, 0, Some(4), last_notified_at, now, Duration::minutes(20));
        assert_eq!(decision, NotifyDecision::NotifyNow);
    }

    #[test]
    fn a_first_ever_notification_for_this_user_is_not_gated_by_any_cooldown() {
        let now = Utc::now();
        let decision = decide_user_notification(0, 3, None, None, now, Duration::minutes(20));
        assert_eq!(decision, NotifyDecision::NotifyNow);
    }

    #[test]
    fn already_notified_this_exact_resulting_state_is_skipped() {
        let now = Utc::now();
        // last_notified_rank already equals new_rank -- e.g. two
        // consecutive equal transitions, or a watermark replay.
        let decision = decide_user_notification(0, 4, Some(4), Some(now - Duration::hours(1)), now, Duration::minutes(20));
        assert_eq!(decision, NotifyDecision::Skip);
    }

    #[test]
    fn train_cancelled_outranks_any_delay() {
        assert_eq!(train_severity_rank("cancelled", Some(2), 15), 2);
        assert!(train_severity_rank("cancelled", None, 15) > train_severity_rank("en_route", Some(999), 15));
    }

    #[test]
    fn train_delay_below_threshold_is_normal_rank() {
        assert_eq!(train_severity_rank("en_route", Some(14), 15), 0);
        assert_eq!(train_severity_rank("en_route", None, 15), 0);
    }

    #[test]
    fn train_delay_at_or_above_threshold_is_rank_one() {
        assert_eq!(train_severity_rank("en_route", Some(15), 15), 1);
        assert_eq!(train_severity_rank("en_route", Some(45), 15), 1);
    }

    #[test]
    fn train_escalation_notifies_deescalation_does_not() {
        assert_eq!(decide_train_notification(0, 1), NotifyDecision::NotifyNow);
        assert_eq!(decide_train_notification(0, 2), NotifyDecision::NotifyNow);
        assert_eq!(decide_train_notification(1, 0), NotifyDecision::Skip);
        assert_eq!(decide_train_notification(2, 1), NotifyDecision::Skip);
        assert_eq!(decide_train_notification(1, 1), NotifyDecision::Skip);
    }

    #[test]
    fn a_newly_tracked_already_delayed_train_does_notify_once() {
        // Status note: no cold-start guard for trains -- previous_rank=0
        // (no prior train_notification_state row) is the correct baseline,
        // not a skip.
        assert_eq!(decide_train_notification(0, 1), NotifyDecision::NotifyNow);
    }
}
