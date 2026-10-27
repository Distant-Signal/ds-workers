//! Quiet hours and snooze (DS-153): the last step between a decided push
//! and the push queue. A snooze ("pause notifications until") holds pushes
//! exactly as quiet hours do, until its time. Every cycle hands its decided pushes to [`dispatch`] instead of
//! straight to `PushQueue::enqueue`.
//!
//! # What is held
//!
//! While the user is in their quiet hours (`notification_preferences`'
//! `quiet_hours_start`..`quiet_hours_end`, local time in
//! `quiet_hours_timezone`; the window may cross midnight):
//!
//! - **a cancellation goes through** (a train push at the cancelled rank,
//!   or a break notice whose train is cancelled);
//! - **the grant-created notice is never held**: it is a security notice;
//! - **a line or train push is held** in `notification_holds`, one row per
//!   (user, target), replaced by each newer decision for that target. Both
//!   come from cursor-polled tables, which never decide the same change
//!   twice, so the hold is what keeps them;
//! - **every other push is not sent**: the skipped-stop, "needs attention"
//!   and break-notice cycles are full polls over current state, which
//!   decide them again once quiet hours end, if they still apply.
//!
//! # Release
//!
//! [`run_release_cycle`] looks at every hold each main poll. Once the
//! user's quiet hours are over it checks the hold again, against what has
//! been sent since and the current state, and enqueues it only when it is
//! still news ([`decide_release`]): the user still wants that kind of push,
//! a line push's rank is not the one they were last sent, a train push
//! still escalates past what they were last sent (a cancellation that went
//! through in the meantime outranks a held delay), the train has not
//! finished (or, for a predicted late start, not started), no open journey
//! break owns the train, and it is less than a day old. So the user gets
//! the latest state, deduplicated and escalation-checked, never a backlog.
//! A push decided after quiet hours end deletes its target's hold, so a
//! release never overtakes a newer decision.
//!
//! The queue's own rules still apply to a released push: the bookkeeping
//! (`*_notification_state`) is written only once it is delivered. A
//! release the queue refuses keeps its hold for the next cycle.

use chrono::{DateTime, Duration, NaiveTime, Utc};
use sqlx::PgPool;

use crate::decision::{NotifyDecision, decide_train_notification, train_severity_rank};
use crate::push_queue::{Delivered, EnqueueOutcome, PgBackend, PushJob, PushQueue};
use crate::send::NotificationPayload;

type Queue = PushQueue<PgBackend>;

/// How long a hold may wait before it is dropped unsent.
pub(crate) const HOLD_TTL: Duration = Duration::hours(24);

/// `notifier_push_holds_total{result}`: what happened to a push at the
/// quiet-hours step, and to each hold at release.
const HOLDS_METRIC: &str = "notifier_push_holds_total";
const RESULTS: &[&str] = &[
    "held",
    "not_sent_quiet",
    "cancellation_through",
    "released",
    "dropped_stale",
];

/// Registers every `result` at 0, so `increase()` sees the first one.
pub(crate) fn register_metrics() {
    for result in RESULTS {
        metrics::counter!(common::metrics::metric_name(HOLDS_METRIC), "result" => *result)
            .increment(0);
    }
}

fn count(result: &'static str) {
    metrics::counter!(common::metrics::metric_name(HOLDS_METRIC), "result" => result).increment(1);
}

/// A user's quiet hours, as stored (all three or none), and their snooze.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QuietHours {
    pub start: Option<NaiveTime>,
    pub end: Option<NaiveTime>,
    pub timezone: Option<String>,
    /// "Pause notifications until" (`notification_preferences.snoozed_until`):
    /// held exactly like quiet hours until then.
    pub snoozed_until: Option<DateTime<Utc>>,
}

/// Whether pushes wait at `now`: the user snoozed them until later, or
/// `now` is inside their quiet hours (`start` inclusive, `end` exclusive,
/// local time in `timezone`; a window whose end is before its start runs
/// through midnight). An unknown zone is read as Europe/London (the api
/// accepts only zones it knows).
pub(crate) fn in_quiet_hours(quiet: &QuietHours, now: DateTime<Utc>) -> bool {
    if quiet.snoozed_until.is_some_and(|until| now < until) {
        return true;
    }
    let (Some(start), Some(end)) = (quiet.start, quiet.end) else {
        return false;
    };
    let zone: chrono_tz::Tz = quiet
        .timezone
        .as_deref()
        .and_then(|name| name.parse().ok())
        .unwrap_or(chrono_tz::Europe::London);
    let local = now.with_timezone(&zone).time();
    match start.cmp(&end) {
        std::cmp::Ordering::Less => start <= local && local < end,
        std::cmp::Ordering::Greater => local >= start || local < end,
        std::cmp::Ordering::Equal => false,
    }
}

/// Whether a push goes through quiet hours: a cancellation.
pub(crate) fn is_cancellation(delivered: &Delivered) -> bool {
    match delivered {
        Delivered::Train { status, .. } => status == "cancelled",
        Delivered::JourneyBreak {
            trigger: Some(trigger),
            ..
        } => trigger.status == "cancelled",
        _ => false,
    }
}

/// What [`dispatch`] does with a push.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gate {
    /// Enqueue it now.
    Send,
    /// Keep it in `notification_holds` until quiet hours end.
    Hold,
    /// Do not send it; its full-poll cycle decides it again later.
    NotNow,
}

/// The quiet-hours rule for one push. Pure.
pub(crate) fn gate(delivered: &Delivered, quiet: bool) -> Gate {
    if matches!(delivered, Delivered::GrantCreated { .. }) || !quiet {
        return Gate::Send;
    }
    if is_cancellation(delivered) {
        return Gate::Send;
    }
    match delivered {
        Delivered::Line { .. } | Delivered::Train { .. } => Gate::Hold,
        _ => Gate::NotNow,
    }
}

/// The hold key of a line or train push, `None` for any other.
fn hold_target(delivered: &Delivered) -> Option<String> {
    match delivered {
        Delivered::Line { line_id, .. } => Some(format!("line:{line_id}")),
        Delivered::Train {
            tracked_train_id, ..
        } => Some(format!("train:{tracked_train_id}")),
        _ => None,
    }
}

type QuietHoursRow = (
    Option<NaiveTime>,
    Option<NaiveTime>,
    Option<String>,
    Option<DateTime<Utc>>,
);

async fn quiet_hours_for(pool: &PgPool, user_id: &str) -> anyhow::Result<QuietHours> {
    let row: Option<QuietHoursRow> = sqlx::query_as(
        "SELECT quiet_hours_start, quiet_hours_end, quiet_hours_timezone, snoozed_until \
         FROM notification_preferences WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    Ok(row
        .map(|(start, end, timezone, snoozed_until)| QuietHours {
            start,
            end,
            timezone,
            snoozed_until,
        })
        .unwrap_or_default())
}

/// Hands one decided push on: to the queue, to `notification_holds`, or
/// nowhere while the user's quiet hours last ([`gate`]). A push that goes
/// to the queue deletes its target's hold: it is newer.
pub(crate) async fn dispatch(pool: &PgPool, queue: &Queue, job: PushJob) -> anyhow::Result<()> {
    let quiet = if matches!(job.delivered, Delivered::GrantCreated { .. }) {
        false
    } else {
        in_quiet_hours(&quiet_hours_for(pool, &job.user_id).await?, job.decided_at)
    };
    match gate(&job.delivered, quiet) {
        Gate::Send => {
            if quiet {
                count("cancellation_through");
            }
            if let Some(target) = hold_target(&job.delivered) {
                sqlx::query("DELETE FROM notification_holds WHERE user_id = $1 AND target = $2")
                    .bind(&job.user_id)
                    .bind(&target)
                    .execute(pool)
                    .await?;
            }
            queue.enqueue(job);
        }
        Gate::Hold => {
            store_hold(pool, &job).await?;
            count("held");
            tracing::info!(user_id = %job.user_id, "push held for the user's quiet hours");
        }
        Gate::NotNow => {
            count("not_sent_quiet");
            tracing::debug!(
                user_id = %job.user_id,
                "push not sent in the user's quiet hours; its cycle decides it again later"
            );
        }
    }
    Ok(())
}

async fn store_hold(pool: &PgPool, job: &PushJob) -> anyhow::Result<()> {
    let Some(target) = hold_target(&job.delivered) else {
        return Ok(());
    };
    let (line_id, line_rank, tracked_train_id, train_status, train_delay_minutes) =
        match &job.delivered {
            Delivered::Line { line_id, rank } => (
                Some(line_id.as_str()),
                Some(i16::from(*rank)),
                None,
                None,
                None,
            ),
            Delivered::Train {
                tracked_train_id,
                status,
                delay_minutes,
                ..
            } => (
                None,
                None,
                Some(*tracked_train_id),
                Some(status.as_str()),
                *delay_minutes,
            ),
            _ => return Ok(()),
        };
    sqlx::query(
        "INSERT INTO notification_holds \
             (user_id, target, line_id, line_rank, tracked_train_id, train_status, \
              train_delay_minutes, title, body, url, tag, decided_at, expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (user_id, target) DO UPDATE SET \
             line_id = EXCLUDED.line_id, line_rank = EXCLUDED.line_rank, \
             tracked_train_id = EXCLUDED.tracked_train_id, \
             train_status = EXCLUDED.train_status, \
             train_delay_minutes = EXCLUDED.train_delay_minutes, \
             title = EXCLUDED.title, body = EXCLUDED.body, url = EXCLUDED.url, \
             tag = EXCLUDED.tag, decided_at = EXCLUDED.decided_at, \
             expires_at = EXCLUDED.expires_at \
         WHERE notification_holds.decided_at <= EXCLUDED.decided_at",
    )
    .bind(&job.user_id)
    .bind(&target)
    .bind(line_id)
    .bind(line_rank)
    .bind(tracked_train_id)
    .bind(train_status)
    .bind(train_delay_minutes)
    .bind(&job.payload.title)
    .bind(&job.payload.body)
    .bind(&job.payload.url)
    .bind(&job.payload.tag)
    .bind(job.decided_at)
    .bind(job.decided_at + HOLD_TTL)
    .execute(pool)
    .await?;
    Ok(())
}

/// One hold with everything [`decide_release`] checks it against.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct HoldRow {
    pub user_id: String,
    pub target: String,
    pub line_id: Option<String>,
    pub line_rank: Option<i16>,
    pub tracked_train_id: Option<i64>,
    pub train_status: Option<String>,
    pub train_delay_minutes: Option<i32>,
    pub title: String,
    pub body: String,
    pub url: String,
    pub tag: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub quiet_hours_start: Option<NaiveTime>,
    pub quiet_hours_end: Option<NaiveTime>,
    pub quiet_hours_timezone: Option<String>,
    pub snoozed_until: Option<DateTime<Utc>>,
    /// The user's switches still allow this push (and, for a train, its
    /// subscription still has its pushes on; for a line, it is still
    /// pinned with its alerts on).
    pub still_wanted: bool,
    /// The rank of the last line push delivered for this line.
    pub line_last_rank: Option<i16>,
    /// The last train push delivered for this subscription.
    pub last_notified_status: Option<String>,
    pub last_notified_delay_minutes: Option<i32>,
    /// The train's status now, `None` without a current-state row.
    pub current_train_status: Option<String>,
    pub delay_threshold_minutes: Option<i16>,
}

/// What [`decide_release`] decided for a hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Release {
    /// Still in quiet hours: keep it.
    Wait,
    /// Enqueue it.
    Send,
    /// Delete it unsent: no longer news.
    Drop(&'static str),
}

/// The release rule (module doc). Pure.
pub(crate) fn decide_release(
    hold: &HoldRow,
    now: DateTime<Utc>,
    default_threshold: i32,
) -> Release {
    if now >= hold.expires_at {
        return Release::Drop("expired");
    }
    let quiet = QuietHours {
        start: hold.quiet_hours_start,
        end: hold.quiet_hours_end,
        timezone: hold.quiet_hours_timezone.clone(),
        snoozed_until: hold.snoozed_until,
    };
    if in_quiet_hours(&quiet, now) {
        return Release::Wait;
    }
    if !hold.still_wanted {
        return Release::Drop("opted_out");
    }
    if let Some(rank) = hold.line_rank {
        return if hold.line_last_rank == Some(rank) {
            Release::Drop("already_sent")
        } else {
            Release::Send
        };
    }
    let Some(status) = hold.train_status.as_deref() else {
        return Release::Drop("unknown");
    };
    let threshold =
        crate::queries::user_delay_threshold(hold.delay_threshold_minutes, default_threshold);
    let held_rank = train_severity_rank(status, hold.train_delay_minutes, threshold);
    let last_rank = hold.last_notified_status.as_deref().map_or(0, |last| {
        train_severity_rank(last, hold.last_notified_delay_minutes, threshold)
    });
    if decide_train_notification(last_rank, held_rank) != NotifyDecision::NotifyNow {
        return Release::Drop("not_worse");
    }
    match (status, hold.current_train_status.as_deref()) {
        (_, Some("completed")) => Release::Drop("train_finished"),
        ("awaiting_activation", None | Some("awaiting_activation")) => Release::Send,
        ("awaiting_activation", Some(_)) => Release::Drop("train_started"),
        (_, None) => Release::Drop("train_gone"),
        _ => Release::Send,
    }
}

async fn held_rows(pool: &PgPool) -> anyhow::Result<Vec<HoldRow>> {
    Ok(sqlx::query_as(
        "SELECT h.user_id, h.target, h.line_id, h.line_rank, h.tracked_train_id, \
                h.train_status, h.train_delay_minutes, h.title, h.body, h.url, h.tag, \
                h.decided_at, h.expires_at, \
                p.quiet_hours_start, p.quiet_hours_end, p.quiet_hours_timezone, \
                p.snoozed_until, \
                CASE WHEN h.line_id IS NOT NULL THEN \
                         EXISTS (SELECT 1 FROM pinned_lines pl \
                                 WHERE pl.user_id = h.user_id AND pl.line_id = h.line_id \
                                   AND pl.notify) \
                         AND COALESCE(p.line_status, TRUE) \
                     ELSE COALESCE(ts.notifications_enabled, FALSE) \
                         AND CASE h.train_status \
                                 WHEN 'cancelled' THEN COALESCE(p.train_cancel, TRUE) \
                                 WHEN 'awaiting_activation' THEN COALESCE(p.predicted_start, TRUE) \
                                 ELSE COALESCE(p.train_delay, TRUE) END \
                END AS still_wanted, \
                lns.last_notified_severity_rank AS line_last_rank, \
                tns.last_notified_status, tns.last_notified_delay_minutes, \
                cs.status AS current_train_status, p.delay_threshold_minutes \
         FROM notification_holds h \
         LEFT JOIN notification_preferences p ON p.user_id = h.user_id \
         LEFT JOIN line_notification_state lns \
           ON lns.user_id = h.user_id AND lns.line_id = h.line_id \
         LEFT JOIN train_subscriptions ts ON ts.id = h.tracked_train_id \
         LEFT JOIN train_notification_state tns \
           ON tns.user_id = h.user_id AND tns.tracked_train_id = h.tracked_train_id \
         LEFT JOIN train_current_state cs ON cs.trains_id = ts.trains_id \
         ORDER BY h.decided_at, h.user_id, h.target \
         LIMIT 1000",
    )
    .fetch_all(pool)
    .await?)
}

/// Deletes the hold, unless a newer decision replaced it meanwhile.
async fn delete_hold(pool: &PgPool, hold: &HoldRow) -> anyhow::Result<()> {
    sqlx::query(
        "DELETE FROM notification_holds \
         WHERE user_id = $1 AND target = $2 AND decided_at = $3",
    )
    .bind(&hold.user_id)
    .bind(&hold.target)
    .bind(hold.decided_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// One release pass (module doc): every hold whose user's quiet hours are
/// over is sent, or dropped when it is no longer news.
pub(crate) async fn run_release_cycle(
    pool: &PgPool,
    queue: &Queue,
    now: DateTime<Utc>,
    default_threshold: i32,
) -> anyhow::Result<()> {
    for hold in held_rows(pool).await? {
        let mut release = decide_release(&hold, now, default_threshold);
        if release == Release::Send
            && let Some(tracked_train_id) = hold.tracked_train_id
            && crate::break_notices::enabled()
            && crate::break_notices::break_owns_train(pool, &hold.user_id, tracked_train_id).await?
        {
            release = Release::Drop("break_owns_train");
        }
        match release {
            Release::Wait => {}
            Release::Drop(reason) => {
                tracing::info!(user_id = %hold.user_id, target = %hold.target, reason, "held push dropped");
                count("dropped_stale");
                delete_hold(pool, &hold).await?;
            }
            Release::Send => {
                let Some(delivered) = delivered_of(&hold) else {
                    delete_hold(pool, &hold).await?;
                    continue;
                };
                let outcome = queue.enqueue(PushJob {
                    user_id: hold.user_id.clone(),
                    payload: NotificationPayload {
                        title: hold.title.clone(),
                        body: hold.body.clone(),
                        url: hold.url.clone(),
                        tag: hold.tag.clone(),
                    },
                    delivered,
                    decided_at: now,
                });
                if matches!(outcome, EnqueueOutcome::Dropped(_)) {
                    continue; // kept for the next cycle
                }
                tracing::info!(user_id = %hold.user_id, target = %hold.target, "held push released");
                count("released");
                delete_hold(pool, &hold).await?;
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a stored line rank is a severity rank, 0..=3"
)]
fn delivered_of(hold: &HoldRow) -> Option<Delivered> {
    if let (Some(line_id), Some(rank)) = (&hold.line_id, hold.line_rank) {
        return Some(Delivered::Line {
            line_id: line_id.clone(),
            rank: rank as u8,
        });
    }
    Some(Delivered::Train {
        tracked_train_id: hold.tracked_train_id?,
        status: hold.train_status.clone()?,
        delay_minutes: hold.train_delay_minutes,
        awaiting_reason: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn quiet(start: &str, end: &str, zone: &str) -> QuietHours {
        QuietHours {
            start: Some(start.parse().unwrap()),
            end: Some(end.parse().unwrap()),
            timezone: Some(zone.to_string()),
            snoozed_until: None,
        }
    }

    fn utc(h: u32, m: u32) -> DateTime<Utc> {
        // A winter day: London is UTC.
        Utc.with_ymd_and_hms(2099, 1, 10, h, m, 0).unwrap()
    }

    #[test]
    fn a_window_through_midnight_covers_both_sides_of_it() {
        let window = quiet("22:00:00", "07:00:00", "Europe/London");
        assert!(in_quiet_hours(&window, utc(22, 0)));
        assert!(in_quiet_hours(&window, utc(3, 0)));
        assert!(in_quiet_hours(&window, utc(6, 59)));
        assert!(!in_quiet_hours(&window, utc(7, 0)), "the end is exclusive");
        assert!(!in_quiet_hours(&window, utc(21, 59)));
    }

    #[test]
    fn a_daytime_window_and_the_users_time_zone() {
        let window = quiet("09:00:00", "17:00:00", "America/New_York");
        // 14:00 UTC is 09:00 in New York in January.
        assert!(in_quiet_hours(&window, utc(14, 0)));
        assert!(!in_quiet_hours(&window, utc(13, 59)));
        assert!(!in_quiet_hours(&window, utc(22, 0)));
    }

    #[test]
    fn no_quiet_hours_never_holds() {
        assert!(!in_quiet_hours(&QuietHours::default(), utc(3, 0)));
    }

    #[test]
    fn a_snooze_holds_until_its_time_with_or_without_quiet_hours() {
        let snoozed = QuietHours {
            snoozed_until: Some(utc(12, 0)),
            ..QuietHours::default()
        };
        assert!(in_quiet_hours(&snoozed, utc(11, 59)));
        assert!(!in_quiet_hours(&snoozed, utc(12, 0)));
        let both = QuietHours {
            snoozed_until: Some(utc(12, 0)),
            ..quiet("22:00:00", "07:00:00", "Europe/London")
        };
        assert!(in_quiet_hours(&both, utc(11, 0)), "snoozed");
        assert!(in_quiet_hours(&both, utc(23, 0)), "quiet");
        assert!(!in_quiet_hours(&both, utc(13, 0)));
    }

    #[test]
    fn cancellations_and_grant_notices_go_through_and_the_rest_is_held_or_waits() {
        let delay = Delivered::Train {
            tracked_train_id: 1,
            status: "en_route".into(),
            delay_minutes: Some(20),
            awaiting_reason: false,
        };
        let cancel = Delivered::Train {
            tracked_train_id: 1,
            status: "cancelled".into(),
            delay_minutes: None,
            awaiting_reason: true,
        };
        let line = Delivered::Line {
            line_id: "victoria".into(),
            rank: 2,
        };
        assert_eq!(gate(&delay, false), Gate::Send);
        assert_eq!(gate(&delay, true), Gate::Hold);
        assert_eq!(gate(&line, true), Gate::Hold);
        assert_eq!(gate(&cancel, true), Gate::Send);
        assert_eq!(
            gate(&Delivered::GrantCreated { grant_id: 1 }, true),
            Gate::Send
        );
        assert_eq!(
            gate(&Delivered::SkippedStop { journey_leg_id: 1 }, true),
            Gate::NotNow
        );
        assert_eq!(
            gate(&Delivered::Unmatched { journey_leg_id: 1 }, true),
            Gate::NotNow
        );
        let cancelled_break = Delivered::JourneyBreak {
            break_id: 1,
            severity: 3,
            trigger: Some(crate::push_queue::TriggerTrainState {
                tracked_train_id: 1,
                status: "cancelled".into(),
                delay_minutes: None,
            }),
        };
        assert_eq!(gate(&cancelled_break, true), Gate::Send);
        let missed_break = Delivered::JourneyBreak {
            break_id: 1,
            severity: 1,
            trigger: None,
        };
        assert_eq!(gate(&missed_break, true), Gate::NotNow);
    }

    fn train_hold(status: &str, delay: Option<i32>) -> HoldRow {
        HoldRow {
            user_id: "u".into(),
            target: "train:1".into(),
            line_id: None,
            line_rank: None,
            tracked_train_id: Some(1),
            train_status: Some(status.into()),
            train_delay_minutes: delay,
            title: "t".into(),
            body: "b".into(),
            url: "/u".into(),
            tag: "train-1".into(),
            decided_at: utc(23, 0) - Duration::days(1),
            expires_at: utc(23, 0),
            quiet_hours_start: Some("22:00:00".parse().unwrap()),
            quiet_hours_end: Some("07:00:00".parse().unwrap()),
            quiet_hours_timezone: Some("Europe/London".into()),
            snoozed_until: None,
            still_wanted: true,
            line_last_rank: None,
            last_notified_status: None,
            last_notified_delay_minutes: None,
            current_train_status: Some("en_route".into()),
            delay_threshold_minutes: None,
        }
    }

    #[test]
    fn a_hold_waits_out_quiet_hours_then_goes_if_still_news() {
        let hold = train_hold("en_route", Some(20));
        assert_eq!(decide_release(&hold, utc(6, 0), 15), Release::Wait);
        assert_eq!(decide_release(&hold, utc(7, 0), 15), Release::Send);
        assert_eq!(
            decide_release(&hold, utc(23, 0), 15),
            Release::Drop("expired")
        );
    }

    #[test]
    fn a_hold_already_outranked_or_overtaken_is_dropped() {
        // A cancellation went through meanwhile: the held delay is old news.
        let hold = HoldRow {
            last_notified_status: Some("cancelled".into()),
            ..train_hold("en_route", Some(20))
        };
        assert_eq!(
            decide_release(&hold, utc(7, 0), 15),
            Release::Drop("not_worse")
        );
        // The train finished overnight.
        let hold = HoldRow {
            current_train_status: Some("completed".into()),
            ..train_hold("en_route", Some(20))
        };
        assert_eq!(
            decide_release(&hold, utc(7, 0), 15),
            Release::Drop("train_finished")
        );
        // A predicted late start for a train that has since started.
        let hold = train_hold("awaiting_activation", Some(15));
        assert_eq!(
            decide_release(&hold, utc(7, 0), 15),
            Release::Drop("train_started")
        );
        // The user turned the kind off.
        let hold = HoldRow {
            still_wanted: false,
            ..train_hold("en_route", Some(20))
        };
        assert_eq!(
            decide_release(&hold, utc(7, 0), 15),
            Release::Drop("opted_out")
        );
        // A line already back at the rank the user was last told.
        let hold = HoldRow {
            line_id: Some("victoria".into()),
            line_rank: Some(0),
            tracked_train_id: None,
            train_status: None,
            line_last_rank: Some(0),
            ..train_hold("en_route", None)
        };
        assert_eq!(
            decide_release(&hold, utc(7, 0), 15),
            Release::Drop("already_sent")
        );
    }
}
