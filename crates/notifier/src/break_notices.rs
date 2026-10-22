//! Auto-suggest notices (docs/superpowers/specs/2026-10-22-mid-journey-changes-design.md
//! §6): when one of a user's journeys breaks, one push that says what
//! happened and the next option, deep-linking to the suggestions the api
//! already planned.
//!
//! The api's break sweep writes `journey_breaks` (with `suggestions`); this
//! cycle only reads the open ones and decides ([`decide_break_notice`]):
//!
//! - **one per break**, and again only when it gets worse (`severity`
//!   above `notified_severity`);
//! - **quiet once acted on**: an accepted, dismissed or recovered break is
//!   not open, and one opened in the last 10 minutes at the same severity
//!   is skipped;
//! - **confirmed**: a break seen by one sweep only waits for the next
//!   (about a minute), unless a train was cancelled (§11);
//! - **opt-outs**: the user's `notification_preferences.journey_suggestions`
//!   and the journey's `suggestions_muted` suppress the push (the in-app
//!   banner still shows, and the train's own push then goes out as usual);
//! - **owner only**, and never for "I missed this train" (the user is
//!   already in the app);
//! - **one push per user per train** (§6.3): the notice is that train's
//!   push. Its delivery writes the train's `train_notification_state` at
//!   the equivalent rank, the train path (delay and cancel pushes, and
//!   DS-127's predicted late starts) defers to an open break on the
//!   train ([`break_owns_train`]), and the queue drops a queued plain push
//!   for the train (`push_queue`'s supersede). Its `tag` is the train's, so
//!   a plain push that raced it is replaced on the phone.

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sqlx::PgPool;

use crate::send::NotificationPayload;

/// A break opened (by the owner, from the push or in the app) this
/// recently at its current severity counts as acted on.
pub(crate) const OPENED_QUIET: Duration = Duration::minutes(10);

/// One open break the cycle may notify.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct BreakNoticeRow {
    pub id: i64,
    pub journey_id: i64,
    pub user_id: String,
    pub cause: String,
    pub severity: i16,
    pub notified_severity: Option<i16>,
    pub opened_at: Option<DateTime<Utc>>,
    pub confirmations: i16,
    pub suggestions: Option<Value>,
    pub suggestions_muted: bool,
    pub journey_suggestions: bool,
    /// The trigger leg's subscription (the train whose push this replaces).
    pub trigger_tracked_train_id: Option<i64>,
    /// That train's current status, for its `train_notification_state`.
    pub trigger_status: Option<String>,
    pub trigger_delay_minutes: Option<i32>,
}

/// Open breaks with suggestions ready that are not yet notified at their
/// severity, oldest first.
pub(crate) async fn breaks_to_notify(pool: &PgPool) -> anyhow::Result<Vec<BreakNoticeRow>> {
    Ok(sqlx::query_as(
        "SELECT b.id, b.journey_id, b.user_id, b.cause, b.severity, b.notified_severity, \
                b.opened_at, b.confirmations, b.suggestions, j.suggestions_muted, \
                COALESCE(p.journey_suggestions, TRUE) AS journey_suggestions, \
                jl.train_subscription_id AS trigger_tracked_train_id, \
                cs.status AS trigger_status, cs.delay_minutes AS trigger_delay_minutes \
         FROM journey_breaks b \
         JOIN journeys j ON j.id = b.journey_id AND j.user_id = b.user_id \
         LEFT JOIN notification_preferences p ON p.user_id = b.user_id \
         LEFT JOIN journey_legs jl ON jl.id = b.trigger_leg_id \
         LEFT JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id \
         LEFT JOIN train_current_state cs ON cs.trains_id = ts.trains_id \
         WHERE b.status = 'open' AND b.suggestions IS NOT NULL AND b.cause <> 'user_missed' \
           AND (b.notified_severity IS NULL OR b.severity > b.notified_severity) \
         ORDER BY b.detected_at, b.id \
         LIMIT 200",
    )
    .fetch_all(pool)
    .await?)
}

/// Whether an open, notifiable break owns tracked train
/// `tracked_train_id`'s push: one of the user's journeys has a leg on that
/// subscription and an open break, with suggestions planned, whose push is
/// not opted out. The train path then stays quiet; the break notice says
/// more (§6.3). A break the api could not plan for yet owns nothing, so a
/// planning failure never silences the train.
pub(crate) async fn break_owns_train(
    pool: &PgPool,
    user_id: &str,
    tracked_train_id: i64,
) -> anyhow::Result<bool> {
    let (owned,): (bool,) = sqlx::query_as(
        "SELECT EXISTS ( \
             SELECT 1 FROM journey_breaks b \
             JOIN journeys j ON j.id = b.journey_id \
             JOIN journey_legs jl ON jl.journey_id = b.journey_id \
             LEFT JOIN notification_preferences p ON p.user_id = b.user_id \
             WHERE b.user_id = $1 AND b.status = 'open' AND b.cause <> 'user_missed' \
               AND b.suggestions IS NOT NULL \
               AND jl.train_subscription_id = $2 \
               AND NOT j.suggestions_muted AND COALESCE(p.journey_suggestions, TRUE))",
    )
    .bind(user_id)
    .bind(tracked_train_id)
    .fetch_one(pool)
    .await?;
    Ok(owned)
}

/// Records a delivered notice: the break's push bookkeeping.
pub(crate) async fn record_notified(
    pool: &PgPool,
    break_id: i64,
    severity: i16,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE journey_breaks SET notified_at = $2, \
             notified_severity = GREATEST(COALESCE(notified_severity, 0), $3) \
         WHERE id = $1",
    )
    .bind(break_id)
    .bind(at)
    .bind(severity)
    .execute(pool)
    .await?;
    Ok(())
}

/// What [`decide_break_notice`] decided, also the `result` label of
/// `notifier_journey_break_notice_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BreakDecision {
    Send,
    SuppressOptOut,
    SuppressMuted,
    SuppressNotWorse,
    SuppressActed,
    /// Seen by one sweep only, and not a cancellation: wait for the next.
    SuppressUnconfirmed,
    /// "I missed this train": the user is already in the app.
    SuppressInApp,
}

impl BreakDecision {
    pub(crate) fn label(self) -> &'static str {
        match self {
            BreakDecision::Send => "sent",
            BreakDecision::SuppressOptOut => "opt_out",
            BreakDecision::SuppressMuted => "muted",
            BreakDecision::SuppressNotWorse => "not_worse",
            BreakDecision::SuppressActed => "acted",
            BreakDecision::SuppressUnconfirmed => "unconfirmed",
            BreakDecision::SuppressInApp => "in_app",
        }
    }
}

/// The dedupe rule (§6.3). Pure.
pub(crate) fn decide_break_notice(row: &BreakNoticeRow, now: DateTime<Utc>) -> BreakDecision {
    if row.cause == "user_missed" {
        return BreakDecision::SuppressInApp;
    }
    if row
        .notified_severity
        .is_some_and(|notified| row.severity <= notified)
    {
        return BreakDecision::SuppressNotWorse;
    }
    if !row.journey_suggestions {
        return BreakDecision::SuppressOptOut;
    }
    if row.suggestions_muted {
        return BreakDecision::SuppressMuted;
    }
    // Opened recently and no worse since: the user is on it. A first
    // notice is never pre-empted this way only if it was opened before it
    // got worse, which `notified_severity` already covers.
    if row
        .opened_at
        .is_some_and(|opened| now - opened < OPENED_QUIET && row.notified_severity.is_none())
    {
        return BreakDecision::SuppressActed;
    }
    if row.confirmations < 2 && row.cause != "cancelled" {
        return BreakDecision::SuppressUnconfirmed;
    }
    BreakDecision::Send
}

fn london_clock(at: DateTime<Utc>) -> String {
    at.with_timezone(&chrono_tz::Europe::London)
        .format("%H:%M")
        .to_string()
}

fn text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn instant(value: &Value, key: &str) -> Option<DateTime<Utc>> {
    text(value, key)?.parse().ok()
}

/// The train a push names: "10:16 to Charing Cross".
fn train_name(departure: Option<DateTime<Utc>>, destination: Option<&str>) -> Option<String> {
    Some(format!("{} to {}", london_clock(departure?), destination?))
}

/// The notice's copy (§6.4, and the decided style: "You'll miss the 10:16
/// to Charing Cross. Next: 10:31, arrive 10:52. Tap to switch."), from the
/// stored suggestion set. The Recommended train is named only when it is
/// usually covered by the ticket; otherwise the count of options.
pub(crate) fn build_break_payload(row: &BreakNoticeRow) -> NotificationPayload {
    let empty = Value::Null;
    let set = row.suggestions.as_ref().unwrap_or(&empty);
    let trigger = set.get("trigger").unwrap_or(&empty);
    let trigger_train = train_name(
        instant(trigger, "departure"),
        text(trigger, "trainDestinationName").or_else(|| text(trigger, "destinationName")),
    );
    let station = text(set, "originName").or_else(|| text(set, "originCrs"));
    let destination = text(set, "destinationName").or_else(|| text(set, "destinationCrs"));
    let suggestions: &[Value] = set
        .get("suggestions")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice);

    let title = match (row.cause.as_str(), &trigger_train) {
        ("cancelled", Some(train)) => format!("{train} cancelled"),
        ("cancelled", None) => "Your train is cancelled".to_string(),
        ("part_cancelled" | "skipped_stop", Some(train)) => match station {
            Some(station) => format!("{train} no longer stops at {station}"),
            None => format!("{train} no longer runs as planned"),
        },
        ("part_cancelled" | "skipped_stop", None) => {
            "Your train no longer runs as planned".to_string()
        }
        (_, _) => match station {
            Some(station) => format!("Connection at {station} missed"),
            None => "Connection missed".to_string(),
        },
    };
    let title = if suggestions.is_empty() {
        match destination {
            Some(destination) => format!("No more trains to {destination} today"),
            None => "No more trains today".to_string(),
        }
    } else {
        title
    };

    let lead = match (row.cause.as_str(), &trigger_train) {
        ("connection_missed", Some(train)) => Some(format!("You'll miss the {train}.")),
        _ => None,
    };
    let best = suggestions.first();
    let covered = best.is_some_and(|s| text(s, "fit") == Some("covered"));
    let next = best.and_then(|s| {
        let departure = s
            .get("legs")
            .and_then(Value::as_array)
            .and_then(|legs| {
                legs.iter()
                    .find(|leg| text(leg, "kind") == Some("train"))
                    .and_then(|leg| instant(leg, "departure"))
            })
            .or_else(|| instant(s, "departure"))?;
        let arrival = instant(s, "arrival")?;
        Some((departure, arrival))
    });
    let body = match (next, covered) {
        (Some((departure, arrival)), true) => format!(
            "Next: {}, arrive {}. Tap to switch.",
            london_clock(departure),
            london_clock(arrival)
        ),
        (Some((_, arrival)), false) => {
            let count = suggestions.len();
            let ways = if count == 1 {
                "1 other way".to_string()
            } else {
                format!("{count} other ways")
            };
            match destination {
                Some(destination) => format!(
                    "{ways} to {destination}. First arrives {}. Tap to see them.",
                    london_clock(arrival)
                ),
                None => format!(
                    "{ways}. First arrives {}. Tap to see them.",
                    london_clock(arrival)
                ),
            }
        }
        (None, _) => "Tap to see your options.".to_string(),
    };
    let body = match lead {
        Some(lead) => format!("{lead} {body}"),
        None => body,
    };
    NotificationPayload {
        title,
        body,
        url: format!(
            "/journeys/{}?break={}&src=push#break",
            row.journey_id, row.id
        ),
        tag: match row.trigger_tracked_train_id {
            Some(tracked_train_id) => format!("train-{tracked_train_id}"),
            None => format!("journey-break-{}", row.journey_id),
        },
    }
}

/// The train state a delivered notice writes for its trigger train, so the
/// train's own escalation sees nothing new (§6.3): a cancellation as
/// cancelled (rank 2), anything else as at least the delay threshold
/// (rank 1).
pub(crate) fn equivalent_train_state(
    row: &BreakNoticeRow,
    delay_threshold_minutes: i32,
) -> (String, Option<i32>) {
    if row.cause == "cancelled" || row.trigger_status.as_deref() == Some("cancelled") {
        return ("cancelled".to_string(), row.trigger_delay_minutes);
    }
    (
        row.trigger_status
            .clone()
            .unwrap_or_else(|| "en_route".to_string()),
        Some(
            row.trigger_delay_minutes
                .unwrap_or(0)
                .max(delay_threshold_minutes),
        ),
    )
}

/// Whether break notices are on (`JOURNEY_BREAK_NOTICES_ENABLED`, set once
/// at startup). Off, the train path never defers to a break either.
static ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub(crate) fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn enabled() -> bool {
    ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

/// The (break, result) pairs already counted, so a suppression a break
/// keeps meeting every cycle (opened, unconfirmed, opted out) is counted
/// once, not every 15 seconds. Cleared when it grows large; a recount after
/// that is harmless.
/// A counted (break, severity, result).
type Counted = std::collections::HashSet<(i64, i16, &'static str)>;

static COUNTED: std::sync::LazyLock<std::sync::Mutex<Counted>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Counted::new()));

fn count_once(row: &BreakNoticeRow, decision: BreakDecision) {
    let mut counted = COUNTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if counted.len() > 10_000 {
        counted.clear();
    }
    if counted.insert((row.id, row.severity, decision.label())) {
        metrics::counter!(
            common::metrics::metric_name("notifier_journey_break_notice_total"),
            "result" => decision.label()
        )
        .increment(1);
    }
}

/// One cycle (§6.3): decide every notifiable open break and enqueue the
/// notices, counting each decision once per break and severity in
/// `notifier_journey_break_notice_total{result}`. A delivered notice is
/// recorded by the push queue's worker (`Delivered::JourneyBreak`).
pub(crate) async fn run_break_notice_cycle(
    pool: &PgPool,
    queue: &crate::push_queue::PushQueue<crate::push_queue::PgBackend>,
    now: DateTime<Utc>,
    delay_threshold_minutes: i32,
) -> anyhow::Result<()> {
    if !enabled() {
        return Ok(());
    }
    for row in breaks_to_notify(pool).await? {
        let decision = decide_break_notice(&row, now);
        count_once(&row, decision);
        if decision != BreakDecision::Send {
            tracing::debug!(
                break_id = row.id,
                result = decision.label(),
                "break notice not sent"
            );
            continue;
        }
        let trigger = row.trigger_tracked_train_id.map(|tracked_train_id| {
            let (status, delay_minutes) = equivalent_train_state(&row, delay_threshold_minutes);
            crate::push_queue::TriggerTrainState {
                tracked_train_id,
                status,
                delay_minutes,
            }
        });
        tracing::info!(
            break_id = row.id,
            journey_id = row.journey_id,
            severity = row.severity,
            "journey break notice"
        );
        queue.enqueue(crate::push_queue::PushJob {
            user_id: row.user_id.clone(),
            payload: build_break_payload(&row),
            delivered: crate::push_queue::Delivered::JourneyBreak {
                break_id: row.id,
                severity: row.severity,
                trigger,
            },
            decided_at: now,
        });
    }
    Ok(())
}

/// Registers every `result` of `notifier_journey_break_notice_total` at 0.
pub(crate) fn register_metrics() {
    for decision in [
        BreakDecision::Send,
        BreakDecision::SuppressOptOut,
        BreakDecision::SuppressMuted,
        BreakDecision::SuppressNotWorse,
        BreakDecision::SuppressActed,
        BreakDecision::SuppressUnconfirmed,
        BreakDecision::SuppressInApp,
    ] {
        metrics::counter!(
            common::metrics::metric_name("notifier_journey_break_notice_total"),
            "result" => decision.label()
        )
        .increment(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(hour: u32, minute: u32) -> DateTime<Utc> {
        // 2026-10-22 is BST: London = UTC + 1.
        Utc.with_ymd_and_hms(2026, 10, 22, hour, minute, 0).unwrap()
    }

    fn row(cause: &str) -> BreakNoticeRow {
        BreakNoticeRow {
            id: 7,
            journey_id: 42,
            user_id: "u".to_string(),
            cause: cause.to_string(),
            severity: 1,
            notified_severity: None,
            opened_at: None,
            confirmations: 2,
            suggestions: Some(serde_json::json!({
                "originName": "London Bridge",
                "destinationName": "Charing Cross",
                "trigger": {
                    "departure": at(9, 16).to_rfc3339(),
                    "trainDestinationName": "Charing Cross",
                },
                "suggestions": [{
                    "fit": "covered",
                    "departure": at(9, 31).to_rfc3339(),
                    "arrival": at(9, 52).to_rfc3339(),
                    "legs": [{"kind": "train", "departure": at(9, 31).to_rfc3339()}],
                }, {"fit": "covered", "arrival": at(10, 0).to_rfc3339()}],
            })),
            suggestions_muted: false,
            journey_suggestions: true,
            trigger_tracked_train_id: Some(118),
            trigger_status: Some("en_route".to_string()),
            trigger_delay_minutes: Some(9),
        }
    }

    #[test]
    fn the_decided_copy_names_the_next_train_when_it_is_usually_covered() {
        let payload = build_break_payload(&row("connection_missed"));
        assert_eq!(payload.title, "Connection at London Bridge missed");
        assert_eq!(
            payload.body,
            "You'll miss the 10:16 to Charing Cross. Next: 10:31, arrive 10:52. Tap to switch."
        );
        assert_eq!(payload.url, "/journeys/42?break=7&src=push#break");
        assert_eq!(payload.tag, "train-118");
    }

    #[test]
    fn an_uncovered_recommendation_gives_the_count_instead() {
        let mut r = row("cancelled");
        r.suggestions.as_mut().unwrap()["suggestions"][0]["fit"] =
            serde_json::json!("likelyNewTicket");
        let payload = build_break_payload(&r);
        assert_eq!(payload.title, "10:16 to Charing Cross cancelled");
        assert_eq!(
            payload.body,
            "2 other ways to Charing Cross. First arrives 10:52. Tap to see them."
        );
    }

    #[test]
    fn no_suggestions_says_so_and_a_break_with_no_train_has_its_own_tag() {
        let mut r = row("cancelled");
        r.suggestions.as_mut().unwrap()["suggestions"] = serde_json::json!([]);
        r.trigger_tracked_train_id = None;
        let payload = build_break_payload(&r);
        assert_eq!(payload.title, "No more trains to Charing Cross today");
        assert_eq!(payload.body, "Tap to see your options.");
        assert_eq!(payload.tag, "journey-break-42");
    }

    #[test]
    fn copy_has_no_banned_characters() {
        for cause in [
            "cancelled",
            "part_cancelled",
            "connection_missed",
            "skipped_stop",
        ] {
            let payload = build_break_payload(&row(cause));
            for text in [&payload.title, &payload.body] {
                for banned in ['\u{2014}', '\u{2026}', '\u{2192}'] {
                    assert!(!text.contains(banned), "{text}");
                }
            }
        }
    }

    #[test]
    fn the_dedupe_matrix() {
        let now = at(9, 30);
        let cases: Vec<(BreakNoticeRow, BreakDecision)> = vec![
            // First notice.
            (row("connection_missed"), BreakDecision::Send),
            // Same severity again.
            (
                BreakNoticeRow {
                    notified_severity: Some(1),
                    ..row("connection_missed")
                },
                BreakDecision::SuppressNotWorse,
            ),
            // Worse.
            (
                BreakNoticeRow {
                    notified_severity: Some(1),
                    severity: 3,
                    ..row("cancelled")
                },
                BreakDecision::Send,
            ),
            // Opened in the app a moment ago.
            (
                BreakNoticeRow {
                    opened_at: Some(now - Duration::minutes(2)),
                    ..row("connection_missed")
                },
                BreakDecision::SuppressActed,
            ),
            // Opened long ago and never notified: notify.
            (
                BreakNoticeRow {
                    opened_at: Some(now - Duration::minutes(30)),
                    ..row("connection_missed")
                },
                BreakDecision::Send,
            ),
            // Opt-out and mute.
            (
                BreakNoticeRow {
                    journey_suggestions: false,
                    ..row("connection_missed")
                },
                BreakDecision::SuppressOptOut,
            ),
            (
                BreakNoticeRow {
                    suggestions_muted: true,
                    ..row("connection_missed")
                },
                BreakDecision::SuppressMuted,
            ),
            // One sweep only: wait, unless cancelled.
            (
                BreakNoticeRow {
                    confirmations: 1,
                    ..row("connection_missed")
                },
                BreakDecision::SuppressUnconfirmed,
            ),
            (
                BreakNoticeRow {
                    confirmations: 1,
                    ..row("cancelled")
                },
                BreakDecision::Send,
            ),
            // The user's own "I missed this train".
            (row("user_missed"), BreakDecision::SuppressInApp),
        ];
        for (case, expected) in cases {
            assert_eq!(decide_break_notice(&case, now), expected, "{case:?}");
        }
    }

    #[test]
    fn the_notice_writes_the_trains_state_at_the_equivalent_rank() {
        assert_eq!(
            equivalent_train_state(&row("connection_missed"), 15),
            ("en_route".to_string(), Some(15))
        );
        assert_eq!(
            equivalent_train_state(&row("cancelled"), 15),
            ("cancelled".to_string(), Some(9))
        );
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    const USER: &str = "TEST-BREAK-NOTICE-USER";

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    async fn cleanup(pool: &PgPool) {
        for sql in [
            "DELETE FROM journeys WHERE user_id = $1",
            "DELETE FROM train_subscriptions WHERE user_id = $1",
            "DELETE FROM notification_preferences WHERE user_id = $1",
            "DELETE FROM users WHERE id = $1",
        ] {
            sqlx::query(sql).bind(USER).execute(pool).await.expect(sql);
        }
        sqlx::query(
            "DELETE FROM trains WHERE train_uid = 'TEST-BREAK-NOTICE' AND service_date = '2099-06-01'",
        )
        .execute(pool)
        .await
        .expect("cleanup train");
    }

    /// A journey with one leg on a cancelled train, and an open break on it
    /// with suggestions. Returns `(journey_id, break_id, tracked_train_id)`.
    async fn seed(pool: &PgPool) -> (i64, i64, i64) {
        sqlx::query("INSERT INTO users (id, name) VALUES ($1, $1)")
            .bind(USER)
            .execute(pool)
            .await
            .expect("user");
        let (trains_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-BREAK-NOTICE', '2099-06-01') \
             RETURNING id",
        )
        .fetch_one(pool)
        .await
        .expect("train");
        sqlx::query(
            "INSERT INTO train_current_state (trains_id, status, delay_minutes) \
             VALUES ($1, 'cancelled', NULL)",
        )
        .bind(trains_id)
        .execute(pool)
        .await
        .expect("state");
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, trains_id, resolution_status) \
             VALUES ($1, '2099-06-01', $2, 'resolved') RETURNING id",
        )
        .bind(USER)
        .bind(trains_id)
        .fetch_one(pool)
        .await
        .expect("subscription");
        let (journey_id,): (i64,) =
            sqlx::query_as("INSERT INTO journeys (user_id) VALUES ($1) RETURNING id")
                .bind(USER)
                .fetch_one(pool)
                .await
                .expect("journey");
        let (leg_id,): (i64,) = sqlx::query_as(
            "INSERT INTO journey_legs (journey_id, leg_order, origin_crs, destination_crs, \
                 service_date, train_subscription_id, match_mode) \
             VALUES ($1, 1, 'KGX', 'YRK', '2099-06-01', $2, 'manual') RETURNING id",
        )
        .bind(journey_id)
        .bind(tracked_train_id)
        .fetch_one(pool)
        .await
        .expect("leg");
        let (break_id,): (i64,) = sqlx::query_as(
            "INSERT INTO journey_breaks (journey_id, user_id, cause, fault, severity, \
                 trigger_leg_id, break_position, fingerprint, suggestions) \
             VALUES ($1, $2, 'cancelled', 'operator', 3, $3, 1, 'f', '{\"suggestions\": []}') \
             RETURNING id",
        )
        .bind(journey_id)
        .bind(USER)
        .bind(leg_id)
        .fetch_one(pool)
        .await
        .expect("break");
        (journey_id, break_id, tracked_train_id)
    }

    #[tokio::test]
    #[ignore = "requires a live database: DATABASE_URL"]
    async fn an_open_break_is_notified_once_and_owns_its_trains_push() {
        let pool = connect().await;
        cleanup(&pool).await;
        let (journey_id, break_id, tracked_train_id) = seed(&pool).await;

        let rows = breaks_to_notify(&pool).await.expect("breaks");
        let row = rows
            .iter()
            .find(|row| row.id == break_id)
            .expect("the open break is listed");
        assert_eq!(row.trigger_tracked_train_id, Some(tracked_train_id));
        assert_eq!(row.trigger_status.as_deref(), Some("cancelled"));
        assert!(row.journey_suggestions && !row.suggestions_muted);
        assert_eq!(decide_break_notice(row, Utc::now()), BreakDecision::Send);
        assert!(
            break_owns_train(&pool, USER, tracked_train_id)
                .await
                .expect("owns")
        );

        // Muted: the train pushes as before.
        sqlx::query("UPDATE journeys SET suggestions_muted = TRUE WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .expect("mute");
        assert!(
            !break_owns_train(&pool, USER, tracked_train_id)
                .await
                .expect("owns")
        );

        // Notified at its severity: no longer listed.
        record_notified(&pool, break_id, 3, Utc::now())
            .await
            .expect("record");
        let rows = breaks_to_notify(&pool).await.expect("breaks");
        assert!(rows.iter().all(|row| row.id != break_id));
        cleanup(&pool).await;
    }
}
