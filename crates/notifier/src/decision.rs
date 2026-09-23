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
pub fn train_severity_rank(
    status: &str,
    delay_minutes: Option<i32>,
    delay_threshold_minutes: i32,
) -> u8 {
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
    if new_rank > previous_rank {
        NotifyDecision::NotifyNow
    } else {
        NotifyDecision::Skip
    }
}

/// Escalation-only, same shape as [`decide_train_notification`]: fires the
/// instant a leg transitions from not-skipped to skipped, never on the
/// reverse (a skip that later resolves itself does not warrant a second
/// push -- matching every other de-escalation-is-silent decision in this
/// module). No cold-start guard, same as [`decide_train_notification`]'s
/// own documented posture: a leg that's ALREADY skipped the very first
/// time this notifier ever checks it (no prior
/// `journey_leg_notification_state` row, so `was_skipped` is `false` by
/// convention -- see `crates/notifier/src/queries.rs`'s
/// `skip_notification_state`) still notifies once immediately, the same
/// way "a newly tracked already-delayed train does notify once."
pub fn decide_skip_notification(was_skipped: bool, is_skipped: bool) -> NotifyDecision {
    if is_skipped && !was_skipped {
        NotifyDecision::NotifyNow
    } else {
        NotifyDecision::Skip
    }
}

/// Mon=1 (bit 0) .. Sun=64 (bit 6) -- the exact convention
/// `journey_templates.days_of_week` uses (spec §2.2). Mirrors the SQL
/// this plan's Task 4 writes as `1 << (EXTRACT(ISODOW FROM $1)::int - 1)`
/// -- ISODOW is 1=Monday..7=Sunday, matching `num_days_from_monday()`'s
/// 0=Monday..6=Sunday after the +1/-1 shift.
#[allow(dead_code)]
pub fn weekday_bit(date: chrono::NaiveDate) -> i16 {
    use chrono::Datelike;
    1i16 << date.weekday().num_days_from_monday()
}

/// True once `now` is within `lead_minutes` of `earliest_bound_utc` --
/// the leg's own `depart_after` (or `arrive_after` if no `depart_after`
/// was set), converted to an absolute instant. Stays true for the rest
/// of that leg's `service_date` (Judgment Call 3 -- the caller's own
/// `service_date = today` query scoping is what eventually stops this
/// from being consulted forever, not this function).
pub fn is_due_for_commit_check(
    now: DateTime<Utc>,
    earliest_bound_utc: DateTime<Utc>,
    lead_minutes: i64,
) -> bool {
    now >= earliest_bound_utc - Duration::minutes(lead_minutes)
}

/// Picks the index of the candidate scheduled time closest to `now_local`
/// by absolute distance -- ties broken toward the EARLIER candidate (a
/// deterministic, arbitrary-but-documented choice; the spec does not
/// resolve exact-tie behavior and an exact tie is vanishingly unlikely
/// against real CIF data, which never publishes two departures at the
/// identical minute for the same origin/destination pair in practice).
/// `None` only for an empty slice -- callers already filter to a leg with
/// >= 1 candidate before calling this.
pub fn pick_nearest_to_now_candidate(
    candidate_scheduled_times: &[chrono::NaiveTime],
    now_local: chrono::NaiveTime,
) -> Option<usize> {
    candidate_scheduled_times
        .iter()
        .enumerate()
        .min_by_key(|(_, t)| {
            let delta = t.signed_duration_since(now_local).num_seconds().abs();
            // Tie-break: (delta, scheduled_time) ordering makes an earlier
            // candidate win a tie, since NaiveTime: Ord.
            (delta, **t)
        })
        .map(|(i, _)| i)
}

/// Fires once, the first time an `'auto'`-mode leg's commit-check finds
/// zero candidates for today; never re-fires for the same leg (§4.2,
/// narrowly scoped per this plan's own Judgment Call 4).
pub fn decide_unmatched_notification(already_notified: bool) -> NotifyDecision {
    if already_notified {
        NotifyDecision::Skip
    } else {
        NotifyDecision::NotifyNow
    }
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
        let decision =
            decide_user_notification(0, 4, Some(0), last_notified_at, now, Duration::minutes(20));
        assert_eq!(decision, NotifyDecision::NotifyNow);
    }

    #[test]
    fn deescalation_is_skipped_during_an_active_cooldown() {
        let now = Utc::now();
        let last_notified_at = Some(now - Duration::minutes(5));
        let decision =
            decide_user_notification(4, 0, Some(4), last_notified_at, now, Duration::minutes(20));
        assert_eq!(decision, NotifyDecision::Skip);
    }

    #[test]
    fn deescalation_notifies_once_the_cooldown_has_elapsed() {
        let now = Utc::now();
        let last_notified_at = Some(now - Duration::minutes(25));
        let decision =
            decide_user_notification(4, 0, Some(4), last_notified_at, now, Duration::minutes(20));
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
        let decision = decide_user_notification(
            0,
            4,
            Some(4),
            Some(now - Duration::hours(1)),
            now,
            Duration::minutes(20),
        );
        assert_eq!(decision, NotifyDecision::Skip);
    }

    #[test]
    fn train_cancelled_outranks_any_delay() {
        assert_eq!(train_severity_rank("cancelled", Some(2), 15), 2);
        assert!(
            train_severity_rank("cancelled", None, 15)
                > train_severity_rank("en_route", Some(999), 15)
        );
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

#[cfg(test)]
mod skip_notification_tests {
    use super::*;

    #[test]
    fn not_skipped_to_skipped_notifies() {
        assert_eq!(
            decide_skip_notification(false, true),
            NotifyDecision::NotifyNow
        );
    }

    #[test]
    fn skipped_to_not_skipped_does_not_notify() {
        assert_eq!(decide_skip_notification(true, false), NotifyDecision::Skip);
    }

    #[test]
    fn staying_skipped_does_not_re_notify() {
        assert_eq!(decide_skip_notification(true, true), NotifyDecision::Skip);
    }

    #[test]
    fn staying_not_skipped_does_not_notify() {
        assert_eq!(decide_skip_notification(false, false), NotifyDecision::Skip);
    }

    #[test]
    fn a_leg_already_skipped_on_first_ever_check_notifies_once() {
        // Status note mirroring decide_train_notification's own equivalent
        // test: no cold-start guard -- was_skipped=false (no prior state
        // row) is the correct baseline, not a skip.
        assert_eq!(
            decide_skip_notification(false, true),
            NotifyDecision::NotifyNow
        );
    }
}

#[cfg(test)]
mod sweep_tests {
    use super::*;
    use chrono::{NaiveDate, NaiveTime, Utc};

    #[test]
    fn weekday_bit_monday_is_one() {
        let monday = NaiveDate::from_ymd_opt(2026, 9, 21).unwrap(); // 2026-09-21 is a Monday
        assert_eq!(weekday_bit(monday), 1);
    }

    #[test]
    fn weekday_bit_sunday_is_sixtyfour() {
        let sunday = NaiveDate::from_ymd_opt(2026, 9, 27).unwrap(); // 2026-09-27 is a Sunday
        assert_eq!(weekday_bit(sunday), 64);
    }

    #[test]
    fn weekday_bit_wednesday_is_four() {
        let wednesday = NaiveDate::from_ymd_opt(2026, 9, 23).unwrap(); // 2026-09-23 is a Wednesday
        assert_eq!(weekday_bit(wednesday), 4);
    }

    #[test]
    fn is_due_for_commit_check_false_well_before_lead_window() {
        let now = Utc::now();
        let earliest_bound_utc = now + Duration::hours(1);
        assert!(!is_due_for_commit_check(now, earliest_bound_utc, 15));
    }

    #[test]
    fn is_due_for_commit_check_false_one_minute_before_lead_boundary() {
        let now = Utc::now();
        let earliest_bound_utc = now + Duration::minutes(16);
        assert!(!is_due_for_commit_check(now, earliest_bound_utc, 15));
    }

    #[test]
    fn is_due_for_commit_check_true_exactly_at_lead_boundary() {
        let now = Utc::now();
        let earliest_bound_utc = now + Duration::minutes(15);
        assert!(is_due_for_commit_check(now, earliest_bound_utc, 15));
    }

    #[test]
    fn is_due_for_commit_check_true_well_after_lead_window() {
        let now = Utc::now();
        let earliest_bound_utc = now - Duration::minutes(30);
        assert!(is_due_for_commit_check(now, earliest_bound_utc, 15));
    }

    #[test]
    fn pick_nearest_to_now_candidate_empty_slice_returns_none() {
        let candidates: &[NaiveTime] = &[];
        let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert_eq!(pick_nearest_to_now_candidate(candidates, now), None);
    }

    #[test]
    fn pick_nearest_to_now_candidate_single_candidate_returns_index_zero() {
        let candidates = [NaiveTime::from_hms_opt(10, 30, 0).unwrap()];
        let now = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
        assert_eq!(pick_nearest_to_now_candidate(&candidates, now), Some(0));
    }

    #[test]
    fn pick_nearest_to_now_candidate_exact_tie_break_prefers_earlier_candidate() {
        let candidates = [
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(12, 10, 0).unwrap(),
        ];
        let now = NaiveTime::from_hms_opt(12, 5, 0).unwrap();
        assert_eq!(pick_nearest_to_now_candidate(&candidates, now), Some(0));
    }

    #[test]
    fn nearest_to_now_prefers_the_least_stale_candidate_over_the_earliest_one_when_now_has_already_passed_every_candidate()
     {
        let candidates = [
            NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
        ];
        let now = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
        // All candidates are in the past. The latest one (10:00) is closest to now.
        assert_eq!(pick_nearest_to_now_candidate(&candidates, now), Some(2));
    }

    #[test]
    fn decide_unmatched_notification_false_returns_notify_now() {
        assert_eq!(
            decide_unmatched_notification(false),
            NotifyDecision::NotifyNow
        );
    }

    #[test]
    fn decide_unmatched_notification_true_returns_skip() {
        assert_eq!(decide_unmatched_notification(true), NotifyDecision::Skip);
    }
}
