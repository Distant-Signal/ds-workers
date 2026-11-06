//! Leave-by reminders and the morning summary for regular journeys
//! (distant-signal's docs/superpowers/specs/2026-11-06-regular-journeys-design.md
//! §5.3), off unless `LEAVE_REMINDERS_ENABLED`.
//!
//! Each tick is a full poll of current state, like the skip check: it reads
//! the journeys with a station buffer whose first timed leg is today or
//! yesterday (overnight) and hasn't left, owned by a user with leave-by
//! reminders on, each with its leave-by from `ds_store::journeys::leave_by_sql`
//! (the same SQL the api serves, so the page and the push agree), its first
//! train's current state, any open break and its `regular_journey_push_state`.
//! [`crate::decision::decide_leave_reminder`] decides; a delivered reminder
//! is recorded by the push queue's worker (`Delivered::LeaveBy`), never
//! before (`push_queue`'s "Delivery semantics").
//!
//! - **One push per train.** A reminder whose text names the first train's
//!   delay at or over the user's threshold carries that train as its
//!   trigger: delivery writes the train's `train_notification_state`, and
//!   the queue drops a queued plain push for it. A later escalation still
//!   pushes on its own.
//! - **The cap** counts every regular-journey push of the day: the
//!   leave-by and its moves (`regular_journey_push_state.pushes`) and the
//!   pick-your-train reminders (`journey_leg_notification_state.last_notified_unmatched`).
//!   Train, skip and break pushes have their own rules and aren't counted.
//! - **Quiet hours** let a reminder through unless the user turned
//!   `leave_by_quiet_hours` off; a snooze stops it (`hold::gate`).
//! - **The morning summary** (off by default): one push per user and London
//!   day, 60 minutes before their first leave-by, listing the day's regular
//!   journeys. Recorded in `regular_journey_summaries`, pruned after 7 days
//!   by this cycle. Not counted in any day's cap.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;

use crate::decision::{LeaveReminderDecision, LeaveReminderInput, decide_leave_reminder};
use crate::push_queue::{Delivered, PgBackend, PushJob, PushQueue, TriggerTrainState};
use crate::send::NotificationPayload;

/// How long before the day's first leave-by the morning summary goes
/// (decision 13).
pub(crate) const SUMMARY_LEAD: Duration = Duration::minutes(60);

/// How long a summary's record is kept.
const SUMMARY_KEEP_DAYS: i64 = 7;

/// The cycle's settings (`LEAVE_REMINDERS_*`).
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaveReminderSettings {
    pub max_pushes: i64,
    pub move_threshold: Duration,
    pub ttl_cap_secs: u32,
    /// The notifier's default delay threshold, for a user without one.
    pub default_delay_threshold_minutes: i32,
}

/// One journey the cycle looks at.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct LeaveRow {
    pub journey_id: i64,
    pub user_id: String,
    pub leave_by_at: DateTime<Utc>,
    pub departure: DateTime<Utc>,
    pub basis: String,
    pub leg_id: i64,
    pub lead_minutes: i16,
    pub delay_threshold_minutes: Option<i16>,
    pub leave_by_sent: Option<DateTime<Utc>>,
    pub pushes: i64,
    pub open_break: bool,
    /// The first train leg's subscription, its state and route.
    pub train_leg_id: Option<i64>,
    pub tracked_train_id: Option<i64>,
    pub train_status: Option<String>,
    pub train_delay_minutes: Option<i32>,
    pub train_departure: Option<DateTime<Utc>>,
    pub origin_crs: Option<String>,
    pub origin_name: Option<String>,
    pub destination_crs: Option<String>,
    pub destination_name: Option<String>,
}

/// Every journey whose leave-by window is near `now` (London date
/// `today`): with a buffer, a leave-by at most 6 minutes ahead (the lead is
/// at most 5) and at most 10 behind, a departure still to come, a leg dated
/// today or yesterday, and an owner with leave-by reminders on.
pub(crate) async fn journeys_to_check(
    pool: &PgPool,
    now: DateTime<Utc>,
    today: NaiveDate,
) -> anyhow::Result<Vec<LeaveRow>> {
    Ok(sqlx::query_as(&format!(
        "SELECT j.id AS journey_id, j.user_id, lb.leave_by_at, lb.departure, lb.basis, lb.leg_id, \
                COALESCE(p.leave_by_lead_minutes, 0::SMALLINT) AS lead_minutes, \
                p.delay_threshold_minutes, ps.leave_by_sent, \
                COALESCE(ps.pushes, 0)::BIGINT \
                    + (SELECT COUNT(*) FROM journey_legs ul \
                       JOIN journey_leg_notification_state ns \
                         ON ns.journey_leg_id = ul.id AND ns.user_id = j.user_id \
                       WHERE ul.journey_id = j.id AND ns.last_notified_unmatched) AS pushes, \
                EXISTS (SELECT 1 FROM journey_breaks b \
                        WHERE b.journey_id = j.id AND b.status = 'open') AS open_break, \
                ft.leg_id AS train_leg_id, ft.tracked_train_id, ft.status AS train_status, \
                ft.delay_minutes AS train_delay_minutes, \
                ft.pin_scheduled_departure AS train_departure, \
                ft.origin_crs, so.name AS origin_name, ft.destination_crs, \
                sd.name AS destination_name \
         FROM journeys j \
         JOIN LATERAL ({leave_by}) lb ON TRUE \
         LEFT JOIN notification_preferences p ON p.user_id = j.user_id \
         LEFT JOIN regular_journey_push_state ps ON ps.journey_id = j.id \
         LEFT JOIN LATERAL ( \
             SELECT jl.id AS leg_id, ts.id AS tracked_train_id, cs.status, cs.delay_minutes, \
                    ts.pin_scheduled_departure, \
                    COALESCE(jl.origin_crs, ts.pin_origin_crs, tr.origin_crs) AS origin_crs, \
                    COALESCE(jl.destination_crs, ts.pin_destination_crs, tr.destination_crs) \
                        AS destination_crs \
             FROM journey_legs jl \
             JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id \
             LEFT JOIN trains tr ON tr.id = ts.trains_id \
             LEFT JOIN train_current_state cs ON cs.trains_id = ts.trains_id \
             WHERE jl.journey_id = j.id AND jl.kind = 'train' \
             ORDER BY jl.leg_order LIMIT 1 \
         ) ft ON TRUE \
         LEFT JOIN stations so ON so.crs = UPPER(ft.origin_crs)::bpchar \
         LEFT JOIN stations sd ON sd.crs = UPPER(ft.destination_crs)::bpchar \
         WHERE j.station_buffer_minutes IS NOT NULL \
           AND COALESCE(p.leave_by, TRUE) \
           AND EXISTS (SELECT 1 FROM journey_legs dl WHERE dl.journey_id = j.id \
                       AND dl.service_date BETWEEN $2 - 1 AND $2) \
           AND lb.departure > $1 \
           AND lb.leave_by_at <= $1 + INTERVAL '6 minutes' \
           AND lb.leave_by_at > $1 - INTERVAL '10 minutes' \
         ORDER BY lb.leave_by_at, j.id \
         LIMIT 500",
        leave_by = ds_store::journeys::leave_by_sql("j"),
    ))
    .bind(now)
    .bind(today)
    .fetch_all(pool)
    .await?)
}

fn london_clock(at: DateTime<Utc>) -> String {
    at.with_timezone(&chrono_tz::Europe::London)
        .format("%H:%M")
        .to_string()
}

/// "the 08:42 Woking to London Waterloo", or "the 08:42" without a route.
fn train_label(row: &LeaveRow) -> Option<String> {
    let departure = row.train_departure?;
    Some(
        match crate::push_route(
            row.origin_crs.as_deref(),
            row.origin_name.as_deref(),
            row.destination_crs.as_deref(),
            row.destination_name.as_deref(),
        ) {
            Some(route) => format!("the {} {route}", london_clock(departure)),
            None => format!("the {}", london_clock(departure)),
        },
    )
}

/// The first train's delay the reminder names, if it is late.
fn named_delay(row: &LeaveRow) -> Option<i32> {
    row.train_delay_minutes
        .filter(|delay| *delay > 0 && row.train_status.as_deref() != Some("cancelled"))
}

/// The reminder's text (§5.3's table): sentence case, no em dashes,
/// trains named as `lib/trainName.ts` does.
pub(crate) fn build_leave_by_payload(row: &LeaveRow, moved: bool) -> NotificationPayload {
    let leave = london_clock(row.leave_by_at);
    let (title, body) = if row.basis == ds_store::journeys::BASIS_SUGGESTION {
        let cancelled = row
            .train_departure
            .map_or_else(|| "train".to_string(), london_clock);
        (
            format!("Leave by {leave}"),
            format!(
                "Your {cancelled} is cancelled. The {} gets you there. Tap to switch.",
                london_clock(row.departure)
            ),
        )
    } else {
        let first_is_train = row.train_leg_id == Some(row.leg_id);
        let what = match (first_is_train, train_label(row)) {
            (true, Some(train)) => format!("For {train}"),
            _ => format!("For your journey at {}", london_clock(row.departure)),
        };
        if moved {
            (format!("Leave by {leave} now"), format!("{what} instead."))
        } else {
            let state = match named_delay(row) {
                Some(delay) => {
                    format!("Expected {delay} min late. Leave on time in case it makes up time.")
                }
                None if first_is_train => "On time.".to_string(),
                None => String::new(),
            };
            (
                format!("Leave by {leave}"),
                format!("{what}. {state}").trim_end().to_string(),
            )
        }
    };
    NotificationPayload {
        title,
        body,
        url: format!("/journeys/{}", row.journey_id),
        tag: format!("journey-leg-leave-{}", row.leg_id),
        renotify: moved,
    }
}

/// The trigger train a reminder carries: its first train, when the text
/// names a delay at or over the user's threshold.
fn trigger_for(row: &LeaveRow, default_threshold: i32) -> Option<TriggerTrainState> {
    let threshold =
        crate::queries::user_delay_threshold(row.delay_threshold_minutes, default_threshold);
    let delay = named_delay(row).filter(|delay| *delay >= threshold)?;
    Some(TriggerTrainState {
        tracked_train_id: row.tracked_train_id?,
        status: row
            .train_status
            .clone()
            .unwrap_or_else(|| "en_route".to_string()),
        delay_minutes: Some(delay),
    })
}

/// `notifier_leave_reminder_total{result}`, each (journey, leave-by,
/// result) counted once rather than every tick.
const METRIC: &str = "notifier_leave_reminder_total";
const RESULTS: &[&str] = &[
    "sent",
    "moved",
    "break_owns",
    "capped",
    "not_yet",
    "stale",
    "already_sent",
    "summary_sent",
];

pub(crate) fn register_metrics() {
    for result in RESULTS {
        metrics::counter!(ds_runtime::metrics::metric_name(METRIC), "result" => *result)
            .increment(0);
    }
}

type Counted = std::collections::HashSet<(i64, i64, &'static str)>;

static COUNTED: std::sync::LazyLock<std::sync::Mutex<Counted>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Counted::new()));

fn count_once(key: (i64, i64), result: &'static str) {
    if result == "not_yet" {
        return;
    }
    let mut counted = COUNTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if counted.len() > 10_000 {
        counted.clear();
    }
    if counted.insert((key.0, key.1, result)) {
        metrics::counter!(ds_runtime::metrics::metric_name(METRIC), "result" => result)
            .increment(1);
    }
}

/// Whether the reminder for `row` is owned by a break: an open break on
/// the journey, or one owning its first train's push.
async fn break_owns(pool: &PgPool, row: &LeaveRow) -> anyhow::Result<bool> {
    if row.open_break {
        return Ok(true);
    }
    match row.tracked_train_id {
        Some(train) => crate::break_notices::break_owns_train(pool, &row.user_id, train).await,
        None => Ok(false),
    }
}

/// One tick: decide every near journey's reminder and dispatch the ones
/// due, then the morning summaries, then prune old summary records.
pub(crate) async fn run_leave_reminder_cycle(
    pool: &PgPool,
    queue: &PushQueue<PgBackend>,
    now: DateTime<Utc>,
    settings: LeaveReminderSettings,
) -> anyhow::Result<()> {
    let today = now.with_timezone(&chrono_tz::Europe::London).date_naive();
    for row in journeys_to_check(pool, now, today).await? {
        let input = LeaveReminderInput {
            now,
            leave_by: row.leave_by_at,
            departure: row.departure,
            lead: Duration::minutes(i64::from(row.lead_minutes)),
            break_owns: break_owns(pool, &row).await?,
            sent: row.leave_by_sent,
            pushes: row.pushes,
            max_pushes: settings.max_pushes,
            move_threshold: settings.move_threshold,
        };
        let decision = decide_leave_reminder(&input);
        count_once(
            (row.journey_id, row.leave_by_at.timestamp()),
            decision.label(),
        );
        let LeaveReminderDecision::Send { moved } = decision else {
            continue;
        };
        tracing::info!(
            journey_id = row.journey_id,
            leg_id = row.leg_id,
            moved,
            "leave-by reminder"
        );
        crate::hold::dispatch(
            pool,
            queue,
            PushJob {
                user_id: row.user_id.clone(),
                payload: build_leave_by_payload(&row, moved),
                delivered: Delivered::LeaveBy {
                    journey_id: row.journey_id,
                    leg_id: row.leg_id,
                    leave_by: row.leave_by_at,
                    moved,
                    ttl_cap_secs: settings.ttl_cap_secs,
                    trigger: trigger_for(&row, settings.default_delay_threshold_minutes),
                },
                decided_at: now,
            },
        )
        .await?;
    }
    run_morning_summaries(pool, queue, now, today).await?;
    sqlx::query("DELETE FROM regular_journey_summaries WHERE service_date < $1")
        .bind(today - Duration::days(SUMMARY_KEEP_DAYS))
        .execute(pool)
        .await?;
    Ok(())
}

/// Records a delivered reminder for `(journey, leg, leave-by)`: the leg
/// and leave-by it named, and one more push against the day's cap. With a
/// trigger train, also that train's `train_notification_state` (the
/// reminder was the train's push).
pub(crate) async fn record_leave_by(
    pool: &PgPool,
    user_id: &str,
    (journey_id, leg_id, leave_by): (i64, i64, DateTime<Utc>),
    trigger: Option<&TriggerTrainState>,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    if let Some(trigger) = trigger {
        crate::queries::upsert_train_notification_state(
            pool,
            user_id,
            trigger.tracked_train_id,
            &trigger.status,
            trigger.delay_minutes,
            at,
            None,
        )
        .await?;
    }
    sqlx::query(
        "INSERT INTO regular_journey_push_state \
             (journey_id, user_id, leave_leg_id, leave_by_sent, leave_by_sent_at, pushes) \
         SELECT $1, $2, $3, $4, $5, 1 \
         WHERE EXISTS (SELECT 1 FROM journeys WHERE id = $1) \
         ON CONFLICT (journey_id) DO UPDATE SET \
             leave_leg_id = EXCLUDED.leave_leg_id, \
             leave_by_sent = EXCLUDED.leave_by_sent, \
             leave_by_sent_at = EXCLUDED.leave_by_sent_at, \
             pushes = regular_journey_push_state.pushes + 1",
    )
    .bind(journey_id)
    .bind(user_id)
    .bind(leg_id)
    .bind(leave_by)
    .bind(at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Records a delivered morning summary.
pub(crate) async fn record_summary(
    pool: &PgPool,
    user_id: &str,
    service_date: NaiveDate,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO regular_journey_summaries (user_id, service_date, sent_at) \
         VALUES ($1, $2, $3) ON CONFLICT (user_id, service_date) DO NOTHING",
    )
    .bind(user_id)
    .bind(service_date)
    .bind(at)
    .execute(pool)
    .await?;
    Ok(())
}

/// One regular journey of a user's day, for the summary.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct SummaryRow {
    pub user_id: String,
    pub leave_by_at: DateTime<Utc>,
    pub departure: DateTime<Utc>,
    pub destination_crs: Option<String>,
    pub destination_name: Option<String>,
    pub train_status: Option<String>,
    pub train_delay_minutes: Option<i32>,
}

/// Today's regular journeys (with a leave-by, a leg dated `today`, still
/// to leave) of every user with the morning summary on and none sent for
/// `today`, in departure order. Each is named by where the journey ends.
async fn summary_rows(
    pool: &PgPool,
    now: DateTime<Utc>,
    today: NaiveDate,
) -> anyhow::Result<Vec<SummaryRow>> {
    Ok(sqlx::query_as(&format!(
        "SELECT j.user_id, lb.leave_by_at, lb.departure, \
                last_leg.destination_crs, COALESCE(sd.name, last_leg.destination_place) \
                    AS destination_name, \
                ft.status AS train_status, ft.delay_minutes AS train_delay_minutes \
         FROM journeys j \
         JOIN notification_preferences p ON p.user_id = j.user_id AND p.morning_summary \
         JOIN LATERAL ({leave_by}) lb ON TRUE \
         LEFT JOIN LATERAL ( \
             SELECT COALESCE(jl.destination_crs, ts.pin_destination_crs) AS destination_crs, \
                    jl.destination_place \
             FROM journey_legs jl \
             LEFT JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id \
             WHERE jl.journey_id = j.id ORDER BY jl.leg_order DESC LIMIT 1 \
         ) last_leg ON TRUE \
         LEFT JOIN LATERAL ( \
             SELECT cs.status, cs.delay_minutes FROM journey_legs jl \
             JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id \
             LEFT JOIN train_current_state cs ON cs.trains_id = ts.trains_id \
             WHERE jl.journey_id = j.id AND jl.kind = 'train' \
             ORDER BY jl.leg_order LIMIT 1 \
         ) ft ON TRUE \
         LEFT JOIN stations sd ON sd.crs = UPPER(last_leg.destination_crs)::bpchar \
         WHERE j.source_template_id IS NOT NULL AND j.station_buffer_minutes IS NOT NULL \
           AND EXISTS (SELECT 1 FROM journey_legs dl WHERE dl.journey_id = j.id \
                       AND dl.service_date = $2) \
           AND lb.departure > $1 \
           AND NOT EXISTS (SELECT 1 FROM regular_journey_summaries s \
                           WHERE s.user_id = j.user_id AND s.service_date = $2) \
         ORDER BY j.user_id, lb.departure",
        leave_by = ds_store::journeys::leave_by_sql("j"),
    ))
    .bind(now)
    .bind(today)
    .fetch_all(pool)
    .await?)
}

/// "08:42 to London Waterloo, on time."
fn summary_line(row: &SummaryRow) -> String {
    let destination = row
        .destination_crs
        .as_deref()
        .map(|crs| crate::push_station(crs, row.destination_name.as_deref()))
        .or_else(|| row.destination_name.clone());
    let state = match (row.train_status.as_deref(), row.train_delay_minutes) {
        (Some("cancelled"), _) => "cancelled".to_string(),
        (_, Some(delay)) if delay > 0 => format!("{delay} min late"),
        _ => "on time".to_string(),
    };
    match destination {
        Some(destination) => format!("{} to {destination}, {state}.", london_clock(row.departure)),
        None => format!("{}, {state}.", london_clock(row.departure)),
    }
}

/// The summary for one user's day, from their rows in departure order.
pub(crate) fn build_summary_payload(rows: &[SummaryRow], today: NaiveDate) -> NotificationPayload {
    let lines: Vec<String> = rows.iter().map(summary_line).collect();
    NotificationPayload {
        title: "Your regular journeys today".to_string(),
        body: format!("Today: {}", lines.join(" ")),
        url: "/".to_string(),
        tag: format!("regular-summary-{today}"),
        renotify: false,
    }
}

/// Whether a user's summary is due: from an hour before their first
/// leave-by until it.
pub(crate) fn summary_due(first_leave_by: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now >= first_leave_by - SUMMARY_LEAD && now < first_leave_by
}

async fn run_morning_summaries(
    pool: &PgPool,
    queue: &PushQueue<PgBackend>,
    now: DateTime<Utc>,
    today: NaiveDate,
) -> anyhow::Result<()> {
    let rows = summary_rows(pool, now, today).await?;
    for user_rows in rows.chunk_by(|a, b| a.user_id == b.user_id) {
        let Some(first_leave_by) = user_rows.iter().map(|row| row.leave_by_at).min() else {
            continue;
        };
        if !summary_due(first_leave_by, now) {
            continue;
        }
        let user_id = user_rows[0].user_id.clone();
        count_once((0, first_leave_by.timestamp()), "summary_sent");
        crate::hold::dispatch(
            pool,
            queue,
            PushJob {
                user_id,
                payload: build_summary_payload(user_rows, today),
                delivered: Delivered::MorningSummary {
                    service_date: today,
                    first_leave_by,
                },
                decided_at: now,
            },
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn at(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 9, h, m, 0).unwrap()
    }

    fn row() -> LeaveRow {
        LeaveRow {
            journey_id: 7,
            user_id: "u".into(),
            leave_by_at: at(8, 20),
            departure: at(8, 42),
            basis: "booked".into(),
            leg_id: 11,
            lead_minutes: 0,
            delay_threshold_minutes: None,
            leave_by_sent: None,
            pushes: 0,
            open_break: false,
            train_leg_id: Some(11),
            tracked_train_id: Some(31),
            train_status: Some("awaiting_activation".into()),
            train_delay_minutes: None,
            train_departure: Some(at(8, 42)),
            origin_crs: Some("WOK".into()),
            origin_name: Some("Woking".into()),
            destination_crs: Some("WAT".into()),
            destination_name: Some("London Waterloo".into()),
        }
    }

    fn text(payload: &NotificationPayload) -> (String, String) {
        (payload.title.clone(), payload.body.clone())
    }

    #[test]
    fn the_copy_matches_the_design_table() {
        let on_time = build_leave_by_payload(&row(), false);
        assert_eq!(
            text(&on_time),
            (
                "Leave by 08:20".into(),
                "For the 08:42 Woking to London Waterloo. On time.".into()
            )
        );
        assert_eq!(on_time.tag, "journey-leg-leave-11");
        assert_eq!(on_time.url, "/journeys/7");
        assert!(!on_time.renotify);

        let late = LeaveRow {
            train_status: Some("en_route".into()),
            train_delay_minutes: Some(6),
            ..row()
        };
        assert_eq!(
            text(&build_leave_by_payload(&late, false)),
            (
                "Leave by 08:20".into(),
                "For the 08:42 Woking to London Waterloo. Expected 6 min late. Leave on time in \
                 case it makes up time."
                    .into()
            )
        );

        let moved = LeaveRow {
            leave_by_at: at(8, 5),
            train_departure: Some(at(8, 27)),
            ..row()
        };
        let moved = build_leave_by_payload(&moved, true);
        assert_eq!(
            text(&moved),
            (
                "Leave by 08:05 now".into(),
                "For the 08:27 Woking to London Waterloo instead.".into()
            )
        );
        assert!(moved.renotify);

        let suggestion = LeaveRow {
            basis: "suggestion".into(),
            leave_by_at: at(8, 35),
            departure: at(8, 57),
            ..row()
        };
        assert_eq!(
            text(&build_leave_by_payload(&suggestion, false)),
            (
                "Leave by 08:35".into(),
                "Your 08:42 is cancelled. The 08:57 gets you there. Tap to switch.".into()
            )
        );

        let walk_first = LeaveRow {
            leg_id: 10,
            departure: at(8, 30),
            leave_by_at: at(8, 25),
            ..row()
        };
        assert_eq!(
            text(&build_leave_by_payload(&walk_first, false)).1,
            "For your journey at 08:30."
        );
    }

    #[test]
    fn the_copy_is_in_london_time_across_the_clock_change() {
        // 25 Oct 2026: BST ends at 02:00. 07:20 UTC is 07:20 GMT.
        let after = LeaveRow {
            leave_by_at: Utc.with_ymd_and_hms(2026, 10, 25, 7, 20, 0).unwrap(),
            train_departure: Some(Utc.with_ymd_and_hms(2026, 10, 25, 7, 42, 0).unwrap()),
            ..row()
        };
        assert_eq!(
            build_leave_by_payload(&after, false).title,
            "Leave by 07:20"
        );
        // The day before, 07:20 UTC is 08:20 BST.
        let before = LeaveRow {
            leave_by_at: Utc.with_ymd_and_hms(2026, 10, 24, 7, 20, 0).unwrap(),
            ..row()
        };
        assert_eq!(
            build_leave_by_payload(&before, false).title,
            "Leave by 08:20"
        );
    }

    /// Copy rules (docs/style-guide.md, "Writing"): no em or en dashes,
    /// ellipses or arrows.
    #[test]
    fn copy_has_no_banned_characters() {
        let late = LeaveRow {
            train_status: Some("en_route".into()),
            train_delay_minutes: Some(6),
            ..row()
        };
        let suggestion = LeaveRow {
            basis: "suggestion".into(),
            ..row()
        };
        let summary = build_summary_payload(&[summary_row(Some(3))], at(7, 0).date_naive());
        for payload in [
            build_leave_by_payload(&row(), false),
            build_leave_by_payload(&late, false),
            build_leave_by_payload(&row(), true),
            build_leave_by_payload(&suggestion, false),
            summary,
        ] {
            for text in [&payload.title, &payload.body] {
                for banned in ['\u{2014}', '\u{2013}', '\u{2026}', '\u{2192}', '\u{2190}'] {
                    assert!(!text.contains(banned), "{text:?} contains {banned:?}");
                }
                assert!(!text.contains("..."), "{text:?}");
            }
        }
    }

    #[test]
    fn a_delay_at_the_threshold_makes_the_reminder_the_trains_push() {
        let late = |delay| LeaveRow {
            train_status: Some("en_route".into()),
            train_delay_minutes: Some(delay),
            delay_threshold_minutes: Some(10),
            ..row()
        };
        assert_eq!(trigger_for(&late(9), 15), None, "under the user's 10");
        assert_eq!(
            trigger_for(&late(10), 15),
            Some(TriggerTrainState {
                tracked_train_id: 31,
                status: "en_route".into(),
                delay_minutes: Some(10),
            })
        );
        assert_eq!(trigger_for(&row(), 15), None, "on time");
    }

    /// The state a delivered reminder writes for its train (rank 1, a
    /// delay) silences a plain delay push, but a later cancellation (rank
    /// 2) still pushes on its own.
    #[test]
    fn a_later_escalation_still_pushes() {
        use crate::decision::{NotifyDecision, decide_train_notification, train_severity_rank};
        let trigger = trigger_for(
            &LeaveRow {
                train_status: Some("en_route".into()),
                train_delay_minutes: Some(20),
                ..row()
            },
            15,
        )
        .expect("a trigger");
        let written = train_severity_rank(&trigger.status, trigger.delay_minutes, 15);
        assert_eq!(
            decide_train_notification(written, train_severity_rank("en_route", Some(25), 15)),
            NotifyDecision::Skip
        );
        assert_eq!(
            decide_train_notification(written, train_severity_rank("cancelled", None, 15)),
            NotifyDecision::NotifyNow
        );
    }

    fn summary_row(delay: Option<i32>) -> SummaryRow {
        SummaryRow {
            user_id: "u".into(),
            leave_by_at: at(8, 20),
            departure: at(8, 42),
            destination_crs: Some("WAT".into()),
            destination_name: Some("London Waterloo".into()),
            train_status: Some("en_route".into()),
            train_delay_minutes: delay,
        }
    }

    #[test]
    fn the_summary_lists_the_day_and_comes_an_hour_before_the_first_leave_by() {
        let evening = SummaryRow {
            leave_by_at: at(17, 30),
            departure: at(17, 48),
            destination_crs: Some("WOK".into()),
            destination_name: Some("Woking".into()),
            ..summary_row(None)
        };
        let payload = build_summary_payload(&[summary_row(None), evening], at(7, 0).date_naive());
        assert_eq!(
            payload.body,
            "Today: 08:42 to London Waterloo, on time. 17:48 to Woking, on time."
        );
        assert_eq!(payload.tag, "regular-summary-2026-11-09");
        assert!(summary_row(Some(6)).train_delay_minutes.is_some());
        assert_eq!(
            summary_line(&summary_row(Some(6))),
            "08:42 to London Waterloo, 6 min late."
        );
        assert!(!summary_due(at(8, 20), at(7, 19)));
        assert!(summary_due(at(8, 20), at(7, 20)));
        assert!(!summary_due(at(8, 20), at(8, 20)));
    }

    // --- Against a live database (DATABASE_URL, migrated) ---

    const USER: &str = "TEST-LEAVE-REMINDER-USER";

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        sqlx::postgres::PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    async fn exec(pool: &PgPool, sql: &str) {
        sqlx::query(sql)
            .bind(USER)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
    }

    async fn reset(pool: &PgPool) {
        exec(pool, "DELETE FROM journeys WHERE user_id = $1").await;
        exec(pool, "DELETE FROM train_subscriptions WHERE user_id = $1").await;
        exec(pool, "DELETE FROM journey_templates WHERE user_id = $1").await;
        exec(pool, "DELETE FROM users WHERE id = $1").await;
        exec(pool, "INSERT INTO users (id, name) VALUES ($1, $1)").await;
    }

    /// A journey with a 20-minute buffer whose one walk leg leaves at
    /// `departure` on London date `date`.
    async fn walk_journey(
        pool: &PgPool,
        date: NaiveDate,
        departure: DateTime<Utc>,
        template: Option<i64>,
    ) -> (i64, i64) {
        let journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, station_buffer_minutes, source_template_id) \
             VALUES ($1, 20, $2) RETURNING id",
        )
        .bind(USER)
        .bind(template)
        .fetch_one(pool)
        .await
        .unwrap();
        let leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs (journey_id, leg_order, service_date, kind, match_mode, \
                                       transit_mode, origin_place, destination_place, \
                                       planned_departure, planned_arrival) \
             VALUES ($1, 1, $2, 'untracked', 'manual', 'walk', 'Home', 'Station', $3, \
                     $3 + interval '10 minutes') RETURNING id",
        )
        .bind(journey_id)
        .bind(date)
        .bind(departure)
        .fetch_one(pool)
        .await
        .unwrap();
        (journey_id, leg_id)
    }

    fn input_for(row: &LeaveRow, now: DateTime<Utc>) -> LeaveReminderInput {
        LeaveReminderInput {
            now,
            leave_by: row.leave_by_at,
            departure: row.departure,
            lead: Duration::minutes(i64::from(row.lead_minutes)),
            break_owns: row.open_break,
            sent: row.leave_by_sent,
            pushes: row.pushes,
            max_pushes: 3,
            move_threshold: Duration::minutes(5),
        }
    }

    /// 25 Oct 2026, the day the clocks go back: a 07:40 GMT walk with a
    /// 20-minute buffer is due at 07:20 GMT (07:20 UTC), found by London's
    /// date. Delivery records it and counts it; a pick reminder counts
    /// too; an open break owns it; turning leave-by off hides it.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p notifier \
                leave_reminder -- --ignored --test-threads=1"]
    #[expect(clippy::too_many_lines, reason = "one walk through a day's reminder")]
    async fn a_reminder_is_found_recorded_counted_and_owned_across_the_clock_change() {
        let pool = connect().await;
        reset(&pool).await;
        let date = NaiveDate::from_ymd_opt(2026, 10, 25).unwrap();
        let departure = Utc.with_ymd_and_hms(2026, 10, 25, 7, 40, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 10, 25, 7, 20, 0).unwrap();
        let (journey_id, leg_id) = walk_journey(&pool, date, departure, None).await;
        let ours = |rows: Vec<LeaveRow>| rows.into_iter().find(|r| r.journey_id == journey_id);

        assert!(
            ours(
                journeys_to_check(&pool, now - Duration::hours(1), date)
                    .await
                    .unwrap()
            )
            .is_none(),
            "an hour early it isn't looked at"
        );
        let row = ours(journeys_to_check(&pool, now, date).await.unwrap()).expect("found");
        assert_eq!(row.leave_by_at, now);
        assert_eq!(row.leg_id, leg_id);
        assert_eq!(
            decide_leave_reminder(&input_for(&row, now)),
            LeaveReminderDecision::Send { moved: false }
        );
        assert_eq!(
            build_leave_by_payload(&row, false).title,
            "Leave by 07:20",
            "GMT after the change"
        );

        record_leave_by(
            &pool,
            USER,
            (journey_id, leg_id, row.leave_by_at),
            None,
            now,
        )
        .await
        .unwrap();
        let row = ours(journeys_to_check(&pool, now, date).await.unwrap()).unwrap();
        assert_eq!((row.leave_by_sent, row.pushes), (Some(now), 1));

        // A reminder naming a delay at the threshold is the train's push:
        // its delivery writes the train's state.
        let tracked_train_id: i64 = sqlx::query_scalar(
            "INSERT INTO train_subscriptions (user_id, service_date, resolution_status) \
             VALUES ($1, $2, 'pending') RETURNING id",
        )
        .bind(USER)
        .bind(date)
        .fetch_one(&pool)
        .await
        .unwrap();
        let trigger = TriggerTrainState {
            tracked_train_id,
            status: "en_route".into(),
            delay_minutes: Some(15),
        };
        record_leave_by(
            &pool,
            USER,
            (journey_id, leg_id, row.leave_by_at),
            Some(&trigger),
            now,
        )
        .await
        .unwrap();
        let written: (String, Option<i32>) = sqlx::query_as(
            "SELECT last_notified_status, last_notified_delay_minutes \
             FROM train_notification_state WHERE user_id = $1 AND tracked_train_id = $2",
        )
        .bind(USER)
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(written, ("en_route".to_string(), Some(15)));
        // Recording twice counts twice; reset to one for the cap checks below.
        exec(
            &pool,
            &format!(
                "UPDATE regular_journey_push_state SET pushes = 1 WHERE journey_id = {journey_id}"
            ),
        )
        .await;
        assert_eq!(
            decide_leave_reminder(&input_for(&row, now)),
            LeaveReminderDecision::AlreadySent
        );

        exec(
            &pool,
            &format!(
                "INSERT INTO journey_leg_notification_state \
                     (user_id, journey_leg_id, last_notified_skipped, last_notified_unmatched, \
                      last_notified_unmatched_at, last_notified_at) \
                 VALUES ($1, {leg_id}, FALSE, TRUE, now(), now())"
            ),
        )
        .await;
        let row = ours(journeys_to_check(&pool, now, date).await.unwrap()).unwrap();
        assert_eq!(row.pushes, 2, "the pick reminder counts towards the cap");

        exec(
            &pool,
            &format!(
                "INSERT INTO journey_breaks (journey_id, user_id, cause, fault, severity, \
                                             break_position, fingerprint) \
                 VALUES ({journey_id}, $1, 'cancelled', 'operator', 3, 1, 'f')"
            ),
        )
        .await;
        let row = ours(journeys_to_check(&pool, now, date).await.unwrap()).unwrap();
        assert!(row.open_break);
        assert_eq!(
            decide_leave_reminder(&input_for(&row, now)),
            LeaveReminderDecision::BreakOwns
        );

        exec(
            &pool,
            "INSERT INTO notification_preferences (user_id, leave_by) VALUES ($1, FALSE)",
        )
        .await;
        assert!(ours(journeys_to_check(&pool, now, date).await.unwrap()).is_none());
        reset(&pool).await;
        exec(&pool, "DELETE FROM users WHERE id = $1").await;
    }

    /// The summary looks at regular journeys only, of users who turned it
    /// on, until one is recorded for the day.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p notifier \
                leave_reminder -- --ignored --test-threads=1"]
    async fn the_morning_summary_is_for_regular_journeys_and_sent_once_a_day() {
        let pool = connect().await;
        reset(&pool).await;
        let date = NaiveDate::from_ymd_opt(2026, 11, 9).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 11, 9, 7, 30, 0).unwrap();
        let template: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, days_of_week) VALUES ($1, 31) RETURNING id",
        )
        .bind(USER)
        .fetch_one(&pool)
        .await
        .unwrap();
        walk_journey(&pool, date, at(8, 42), Some(template)).await;
        walk_journey(&pool, date, at(9, 0), None).await;
        let ours = |rows: Vec<SummaryRow>| -> Vec<SummaryRow> {
            rows.into_iter().filter(|r| r.user_id == USER).collect()
        };
        assert!(
            ours(summary_rows(&pool, now, date).await.unwrap()).is_empty(),
            "off"
        );

        exec(
            &pool,
            "INSERT INTO notification_preferences (user_id, morning_summary) VALUES ($1, TRUE)",
        )
        .await;
        let rows = ours(summary_rows(&pool, now, date).await.unwrap());
        assert_eq!(rows.len(), 1, "the one regular journey");
        assert_eq!(rows[0].leave_by_at, at(8, 22));
        assert!(summary_due(rows[0].leave_by_at, now));

        record_summary(&pool, USER, date, now).await.unwrap();
        assert!(
            ours(summary_rows(&pool, now, date).await.unwrap()).is_empty(),
            "sent"
        );
        reset(&pool).await;
        exec(&pool, "DELETE FROM users WHERE id = $1").await;
    }
}
