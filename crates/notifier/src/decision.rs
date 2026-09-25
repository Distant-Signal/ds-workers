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

/// What time information a commit-check leg actually carries -- the input
/// to BOTH "when does this leg become due for a commit-check" and "which
/// candidate should win it."
///
/// A journey leg materialized from a template leg can legitimately carry
/// any subset of the four window bounds, including none at all (see
/// `api::data::journey_templates::validate_template_leg`'s own doc
/// comment). Collapsing that to one `NaiveTime` with `NaiveTime::MIN` as
/// the fallback -- which is what this code did before -- is what caused the
/// bug described on [`commit_check_window`] below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitCheckWindow {
    /// The leg names its own EARLIEST acceptable time (`depart_after`, else
    /// `arrive_after`). The user has told us when they want to travel, so
    /// "nearest to now" is measured against that intent and a candidate
    /// that has already departed is still a legitimate answer (the sweep
    /// may simply be running a little late).
    EarliestKnown(chrono::NaiveTime),
    /// The leg names only a LATEST acceptable time (`depart_before`, else
    /// `arrive_before`) -- "any train, as long as it's before X."
    LatestOnly(chrono::NaiveTime),
    /// No time bound at all -- "any train, whenever."
    Open,
}

impl CommitCheckWindow {
    /// Whether the user named an earliest time of their own. `false` means
    /// this leg has NO lower bound, and so must be anchored on the sweep's
    /// own `now` instead -- see [`commit_check_window`].
    pub fn names_an_earliest_time(self) -> bool {
        matches!(self, Self::EarliestKnown(_))
    }

    /// The local time this leg's due-check should be measured against, or
    /// `None` for [`CommitCheckWindow::Open`] (the caller substitutes `now`
    /// -- there is no wall-clock time on the leg to convert).
    pub fn due_check_bound(self) -> Option<chrono::NaiveTime> {
        match self {
            Self::EarliestKnown(t) | Self::LatestOnly(t) => Some(t),
            Self::Open => None,
        }
    }
}

/// Classifies a commit-check leg's four (all-`Option`) window bounds.
///
/// **The bug this exists to fix.** This used to be
/// `depart_after.or(arrive_after).unwrap_or(NaiveTime::MIN)` -- i.e. a leg
/// with no LOWER bound was treated as due from LONDON MIDNIGHT. Since such
/// a leg's `service_date` is always today, `is_due_for_commit_check` was
/// then unconditionally true from the first sweep tick of the day, and
/// `pick_nearest_to_now_candidate` was asked to pick the candidate nearest
/// to THAT tick's own clock reading. With the sweep on its hourly default
/// that tick is somewhere in the 00:00-01:00 hour, so a commuter's "RDG to
/// PAD, no particular time" template got permanently committed -- flagged
/// `'auto'`, never re-evaluated -- to whatever overnight service happened to
/// run around then, and the user got delay/cancellation/skip pushes all day
/// for a train they were never on. Worse than the pre-fix behavior of never
/// committing such a leg at all: wrong AND loud. Note that the midnight
/// fallback also swallowed the leg that names only `depart_before`/
/// `arrive_before` -- "get me there before 09:00" is real, usable
/// information, and it was being discarded.
///
/// **The policy this implements instead.**
/// * A lower bound (`depart_after`, else `arrive_after`) is used exactly as
///   before -- [`CommitCheckWindow::EarliestKnown`], unchanged behavior.
/// * Only an upper bound (`depart_before`, else `arrive_before`) means the
///   leg becomes due `auto_commit_lead_minutes` before the user's LATEST
///   acceptable time, not at midnight -- and, having no lower bound, it
///   picks the next candidate UPCOMING at that point
///   ([`pick_next_upcoming_candidate`]), never one that has already
///   departed. Deliberately not "the latest candidate still inside the
///   window," which would also be defensible ("I need to be there by 9, so
///   give me the 08:40"): that is a product decision about what a
///   `*_before`-only template MEANS, and this fix is not the place to make
///   it. The next upcoming train inside the window satisfies the constraint
///   the user actually wrote down.
/// * No bound at all is [`CommitCheckWindow::Open`]: due whenever the sweep
///   looks (there is nothing to wait for), and committed to the next
///   UPCOMING candidate relative to that moment -- "the user wants any
///   train from here on," evaluated against when we are actually looking
///   rather than against midnight.
pub fn commit_check_window(
    depart_after: Option<chrono::NaiveTime>,
    depart_before: Option<chrono::NaiveTime>,
    arrive_after: Option<chrono::NaiveTime>,
    arrive_before: Option<chrono::NaiveTime>,
) -> CommitCheckWindow {
    if let Some(earliest) = depart_after.or(arrive_after) {
        return CommitCheckWindow::EarliestKnown(earliest);
    }
    match depart_before.or(arrive_before) {
        Some(latest) => CommitCheckWindow::LatestOnly(latest),
        None => CommitCheckWindow::Open,
    }
}

/// Picks the index of the candidate `(day_offset, scheduled_time)` nearest
/// to `now_local` -- but NOT by plain symmetric absolute distance. See the
/// "Low-severity residual" section below for the asymmetry this function
/// applies between a candidate that hasn't departed yet and one that has.
/// `None` only for an empty slice -- callers already filter to a leg with
/// >= 1 candidate before calling this.
///
/// `now_local` is always implicitly `day_offset` 0 -- the caller
/// (`main.rs`'s commit-check stage) only ever calls this for a leg whose
/// `service_date` is today, so "now" IS today, day zero, by construction.
/// Each candidate's own `day_offset` (`queries::schedule_candidates_for_leg`'s
/// `main.day_offset`, the schedule's day offset for the leg's origin stop,
/// same column/meaning `schedule_query::resolve`'s `assign_day_offsets` and
/// `schedule-reference`'s `schedule_network_departures_rows` sort by) is
/// folded into the distance as `day_offset * 86_400 + seconds_from_midnight`
/// seconds past midnight on `service_date`, mirroring this codebase's own
/// `(day_offset, time)` day-offset-aware comparison convention rather than
/// comparing bare `NaiveTime`s. A bare-time comparison (this function's
/// pre-fix behavior) has no notion of "day" at all: `NaiveTime`'s own `Sub`
/// is a plain seconds-from-midnight difference with no wraparound, so a
/// genuine overnight candidate just after midnight (`day_offset: 1`,
/// e.g. `00:05`) reads as ALMOST A FULL DAY away from a `now_local` late in
/// the evening (e.g. `23:58`) instead of the few minutes away it really is
/// -- the same "post-midnight time sorts as earlier/farther than it really
/// is" bug class already fixed for `schedule_network_departures_rows` and
/// `schedule-query::resolve`'s terminus-TIPLOC handling.
///
/// **Low-severity residual this function still has to account for.** This
/// is the ONLY selector [`commit_check_window`]'s `EarliestKnown` case
/// uses (a leg that names its own `depart_after`/`arrive_after`), precisely
/// because an already-departed candidate is meant to remain a legitimate
/// answer there -- the sweep may simply be running a little behind the
/// window the user chose. But plain symmetric absolute-distance comparison
/// treats "5 minutes late" and "5 minutes early" as equally good, which is
/// wrong: a candidate that has ALREADY LEFT is a strictly worse match than
/// one still to come, even at an identical (or smaller) numeric distance --
/// most likely to bite after an outage or delayed sweep tick catches up
/// against a leg affected by the (separately fixed) midnight-tick finding,
/// where a late-running sweep's `now` can land almost exactly between a
/// stale departed service and a genuine upcoming one. So this function
/// first tries [`pick_next_upcoming_candidate`] (nearest candidate that has
/// NOT yet departed) and only falls back to the nearest already-departed
/// candidate when literally every candidate has already gone -- preserving
/// "an already-departed candidate is still acceptable" as a last resort,
/// never as a preference over a comparably-close upcoming one.
pub fn pick_nearest_to_now_candidate(
    candidates: &[(u8, chrono::NaiveTime)],
    now_local: chrono::NaiveTime,
) -> Option<usize> {
    use chrono::Timelike;

    if let Some(upcoming) = pick_next_upcoming_candidate(candidates, now_local) {
        return Some(upcoming);
    }

    // Every candidate has already departed -- fall back to the least-stale
    // (nearest-to-now) one, ties broken toward the EARLIER candidate (a
    // deterministic, arbitrary-but-documented choice; an exact tie is
    // vanishingly unlikely against real CIF data, which never publishes two
    // departures at the identical minute for the same origin/destination
    // pair in practice).
    let now_secs = i64::from(now_local.num_seconds_from_midnight());
    candidates
        .iter()
        .enumerate()
        .min_by_key(|(_, (day_offset, t))| {
            let delta = (candidate_secs(*day_offset, *t) - now_secs).abs();
            (delta, *day_offset, *t)
        })
        .map(|(i, _)| i)
}

/// Candidate seconds past midnight on the leg's own `service_date` --
/// `day_offset * 86_400 + seconds_from_midnight`, the shared arithmetic
/// [`pick_nearest_to_now_candidate`] and [`pick_next_upcoming_candidate`]
/// both measure against `now_local` (itself always `day_offset` 0; see
/// either function's doc comment).
fn candidate_secs(day_offset: u8, at: chrono::NaiveTime) -> i64 {
    use chrono::Timelike;
    i64::from(day_offset) * 86_400 + i64::from(at.num_seconds_from_midnight())
}

/// Picks the index of the EARLIEST candidate that has not already departed
/// by `now_local` -- the selection rule for a leg with no lower time bound
/// of its own ([`CommitCheckWindow::names_an_earliest_time`] is `false`).
///
/// Distinct from [`pick_nearest_to_now_candidate`] in exactly one way, and
/// it is the way that matters for such a leg: absolute nearness is
/// direction-blind, so at 00:37 the nearest candidate to now can be the
/// 00:34 service that has ALREADY LEFT. For a leg whose only stated intent
/// is "any train," binding it to a departed train is never the right answer
/// -- the user is asking for the next reasonable service relative to when
/// we are actually looking. A leg that DOES name an earliest time keeps
/// using nearest-to-now, where an already-departed candidate remains
/// legitimate (the sweep may just be running a few minutes behind the
/// window the user chose).
///
/// `None` when every candidate is already in the past. The caller must then
/// leave the leg `'unmatched'` for this tick rather than committing it to a
/// departed train -- and must NOT treat that as "no service found" either
/// (candidates existed; they have simply all gone), so no push fires.
/// Day-offset-aware, same `(day_offset, time)` arithmetic and same
/// "`now_local` is always day 0" contract as
/// [`pick_nearest_to_now_candidate`].
pub fn pick_next_upcoming_candidate(
    candidates: &[(u8, chrono::NaiveTime)],
    now_local: chrono::NaiveTime,
) -> Option<usize> {
    use chrono::Timelike;

    let now_secs = i64::from(now_local.num_seconds_from_midnight());
    candidates
        .iter()
        .enumerate()
        .filter(|(_, (day_offset, at))| candidate_secs(*day_offset, *at) >= now_secs)
        .min_by_key(|(_, (day_offset, at))| candidate_secs(*day_offset, *at))
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
        let candidates: &[(u8, NaiveTime)] = &[];
        let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert_eq!(pick_nearest_to_now_candidate(candidates, now), None);
    }

    #[test]
    fn pick_nearest_to_now_candidate_single_candidate_returns_index_zero() {
        let candidates = [(0, NaiveTime::from_hms_opt(10, 30, 0).unwrap())];
        let now = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
        assert_eq!(pick_nearest_to_now_candidate(&candidates, now), Some(0));
    }

    #[test]
    fn pick_nearest_to_now_candidate_prefers_the_upcoming_candidate_on_an_exact_tie() {
        // Finding 1 (Low-severity residual): 12:00 already departed 5
        // minutes ago, 12:10 is 5 minutes away -- an exact tie by absolute
        // distance. Pre-fix this picked the departed 12:00 candidate
        // (ties broke toward the numerically/chronologically earlier one,
        // which in a past-vs-future tie is always the departed one); a
        // train that has already left is a strictly worse match than one
        // still to come, so the upcoming 12:10 must win instead.
        let candidates = [
            (0, NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
            (0, NaiveTime::from_hms_opt(12, 10, 0).unwrap()),
        ];
        let now = NaiveTime::from_hms_opt(12, 5, 0).unwrap();
        assert_eq!(pick_nearest_to_now_candidate(&candidates, now), Some(1));
    }

    #[test]
    fn pick_nearest_to_now_candidate_prefers_upcoming_even_when_it_is_numerically_farther() {
        // Finding 1's general (non-tied) shape: the departed candidate is
        // NUMERICALLY closer to `now` (3 minutes stale) than the upcoming
        // one is away (6 minutes out), so plain symmetric absolute-distance
        // comparison would still pick the departed one. An already-departed
        // train is a worse match regardless, so the upcoming candidate must
        // win even though it is farther by the raw clock-distance metric.
        let candidates = [
            (0, NaiveTime::from_hms_opt(9, 57, 0).unwrap()), // departed 3 min ago
            (0, NaiveTime::from_hms_opt(10, 6, 0).unwrap()), // 6 min from now, not yet departed
        ];
        let now = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        assert_eq!(pick_nearest_to_now_candidate(&candidates, now), Some(1));
    }

    #[test]
    fn nearest_to_now_prefers_the_least_stale_candidate_over_the_earliest_one_when_now_has_already_passed_every_candidate()
     {
        let candidates = [
            (0, NaiveTime::from_hms_opt(8, 0, 0).unwrap()),
            (0, NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            (0, NaiveTime::from_hms_opt(10, 0, 0).unwrap()),
        ];
        let now = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
        // All candidates are in the past. The latest one (10:00) is closest to now.
        assert_eq!(pick_nearest_to_now_candidate(&candidates, now), Some(2));
    }

    #[test]
    fn nearest_to_now_candidate_is_day_offset_aware_across_a_midnight_boundary() {
        // Same real overnight shape as `schedule_query::resolve`'s
        // `f49687_raw` fixture and `schedule-reference`'s
        // `schedule_network_departures_rows_sorts_by_day_offset_before_scheduled_time`
        // (Barking, F49687, `00:07`/day_offset 1): one candidate is a
        // stale same-day departure from hours ago (`08:00`, day_offset 0);
        // the other is a genuine post-midnight departure (`00:05`,
        // day_offset 1) that, on the REAL clock, is only 7 minutes from
        // now (`23:58`) -- `23:58` today plus 7 minutes rolls into
        // `00:05` tomorrow.
        //
        // Before this fix, `pick_nearest_to_now_candidate` compared bare
        // `NaiveTime`s with no day-offset context at all. `NaiveTime`'s own
        // `Sub` has no wraparound (verified directly against this crate's
        // pinned chrono: `00:05.signed_duration_since(23:58)` returns
        // -86_260 seconds, not -420), so the day_offset-1 candidate read as
        // ~23h51m away instead of 7 minutes away -- always losing to the
        // stale `08:00` candidate (~16h before `23:58`, still "closer" by
        // the buggy, day-blind math) even though `00:05` is the genuinely
        // nearest-to-now departure.
        let candidates = [
            (0, NaiveTime::from_hms_opt(8, 0, 0).unwrap()),
            (1, NaiveTime::from_hms_opt(0, 5, 0).unwrap()),
        ];
        let now = NaiveTime::from_hms_opt(23, 58, 0).unwrap();
        assert_eq!(
            pick_nearest_to_now_candidate(&candidates, now),
            Some(1),
            "the day_offset-1 00:05 candidate is only 7 real minutes from 23:58 -- genuinely \
             nearer than the day_offset-0 08:00 candidate's ~16 real hours, even though 08:00's \
             bare clock time looks closer to 23:58 than 00:05's does"
        );
    }

    // --- commit_check_window / open-ended-leg selection (finding 1) -------

    #[test]
    fn a_leg_with_depart_after_names_its_own_earliest_time() {
        let window = commit_check_window(
            Some(NaiveTime::from_hms_opt(8, 30, 0).unwrap()),
            Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            None,
            None,
        );
        assert_eq!(
            window,
            CommitCheckWindow::EarliestKnown(NaiveTime::from_hms_opt(8, 30, 0).unwrap())
        );
        assert!(window.names_an_earliest_time());
    }

    #[test]
    fn arrive_after_is_the_fallback_earliest_time() {
        let window = commit_check_window(
            None,
            None,
            Some(NaiveTime::from_hms_opt(9, 15, 0).unwrap()),
            None,
        );
        assert_eq!(
            window,
            CommitCheckWindow::EarliestKnown(NaiveTime::from_hms_opt(9, 15, 0).unwrap())
        );
    }

    #[test]
    fn a_leg_with_only_an_upper_bound_is_anchored_on_that_bound_not_on_midnight() {
        // The pre-fix code collapsed this to `NaiveTime::MIN` via
        // `depart_after.or(arrive_after).unwrap_or(MIN)`, so "any train that
        // gets me there before 09:00" became "due from midnight" and was
        // committed at the first tick after midnight. The upper bound is real
        // information and is now what the due-check is measured against.
        let window = commit_check_window(
            None,
            Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            None,
            None,
        );
        assert_eq!(
            window,
            CommitCheckWindow::LatestOnly(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
        );
        assert!(
            !window.names_an_earliest_time(),
            "an upper bound is not an earliest time, so this leg must use next-upcoming selection"
        );
        assert_eq!(
            window.due_check_bound(),
            Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            "NOT NaiveTime::MIN -- that is the whole bug"
        );
        assert_eq!(
            commit_check_window(
                None,
                None,
                None,
                Some(NaiveTime::from_hms_opt(9, 30, 0).unwrap())
            ),
            CommitCheckWindow::LatestOnly(NaiveTime::from_hms_opt(9, 30, 0).unwrap()),
            "arrive_before is the fallback upper bound"
        );
    }

    #[test]
    fn a_fully_open_leg_has_no_wall_clock_bound_at_all() {
        let window = commit_check_window(None, None, None, None);
        assert_eq!(window, CommitCheckWindow::Open);
        assert!(!window.names_an_earliest_time());
        assert_eq!(
            window.due_check_bound(),
            None,
            "the caller must substitute `now` here -- midnight is exactly the wrong answer"
        );
    }

    #[test]
    fn next_upcoming_candidate_never_picks_one_that_has_already_departed() {
        // The reported production shape, reduced: the sweep's first tick
        // after midnight (00:37) against an overnight 00:34 departure and the
        // next real service at 01:05. Pure absolute-distance nearness would
        // pick the 00:34 that has ALREADY LEFT (3 minutes behind beats 28
        // minutes ahead); for a leg whose only stated intent is "any train,"
        // that is never right.
        //
        // Both selectors must skip it: `pick_next_upcoming_candidate` by
        // construction (that's its whole job for an open/latest-only
        // window), and -- since finding 1's Low-severity fix --
        // `pick_nearest_to_now_candidate` too, for the SAME leg shape an
        // `EarliestKnown` window would present it with (an already-departed
        // candidate only wins there when NOTHING upcoming exists at all; see
        // that function's own doc comment).
        let candidates = [
            (0, NaiveTime::from_hms_opt(0, 34, 0).unwrap()),
            (0, NaiveTime::from_hms_opt(1, 5, 0).unwrap()),
        ];
        let now = NaiveTime::from_hms_opt(0, 37, 0).unwrap();
        assert_eq!(
            pick_nearest_to_now_candidate(&candidates, now),
            Some(1),
            "an already-departed candidate must never win over an upcoming one just because it \
             is numerically closer to now"
        );
        assert_eq!(
            pick_next_upcoming_candidate(&candidates, now),
            Some(1),
            "next-upcoming must skip the already-departed 00:34 and take the 01:05"
        );
    }

    #[test]
    fn next_upcoming_candidate_takes_the_soonest_of_several_future_candidates() {
        let candidates = [
            (0, NaiveTime::from_hms_opt(17, 0, 0).unwrap()),
            (0, NaiveTime::from_hms_opt(9, 12, 0).unwrap()),
            (0, NaiveTime::from_hms_opt(12, 30, 0).unwrap()),
        ];
        let now = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        assert_eq!(pick_next_upcoming_candidate(&candidates, now), Some(1));
    }

    #[test]
    fn next_upcoming_candidate_counts_a_candidate_at_exactly_now_as_upcoming() {
        let candidates = [(0, NaiveTime::from_hms_opt(9, 0, 0).unwrap())];
        let now = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        assert_eq!(pick_next_upcoming_candidate(&candidates, now), Some(0));
    }

    #[test]
    fn next_upcoming_candidate_is_day_offset_aware() {
        // Same overnight shape as
        // `nearest_to_now_candidate_is_day_offset_aware_across_a_midnight_boundary`:
        // at 23:58 the day_offset-1 00:05 departure is genuinely 7 minutes
        // AWAY (upcoming), while the day_offset-0 08:00 one is long gone --
        // a bare-clock-time comparison would read 00:05 as being in the past.
        let candidates = [
            (0, NaiveTime::from_hms_opt(8, 0, 0).unwrap()),
            (1, NaiveTime::from_hms_opt(0, 5, 0).unwrap()),
        ];
        let now = NaiveTime::from_hms_opt(23, 58, 0).unwrap();
        assert_eq!(pick_next_upcoming_candidate(&candidates, now), Some(1));
    }

    #[test]
    fn next_upcoming_candidate_is_none_when_every_candidate_has_departed() {
        let candidates = [
            (0, NaiveTime::from_hms_opt(6, 0, 0).unwrap()),
            (0, NaiveTime::from_hms_opt(7, 30, 0).unwrap()),
        ];
        let now = NaiveTime::from_hms_opt(23, 0, 0).unwrap();
        assert_eq!(
            pick_next_upcoming_candidate(&candidates, now),
            None,
            "the caller must leave the leg unmatched for this tick, NOT commit it to a train that \
             ran this morning and NOT fire 'no service found'"
        );
    }

    #[test]
    fn next_upcoming_candidate_empty_slice_returns_none() {
        let candidates: &[(u8, NaiveTime)] = &[];
        let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert_eq!(pick_next_upcoming_candidate(candidates, now), None);
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
