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
//!   so does turning the trigger train's own pushes off
//!   (`train_subscriptions.notifications_enabled`, DS-153): that train then
//!   sends nothing, suggestions included;
//! - **owner only**, and never for "I missed this train" (the user is
//!   already in the app);
//! - **one push per user per train** (§6.3): the notice is that train's
//!   push. Its delivery writes the train's `train_notification_state` at
//!   the equivalent rank, the train path (delay and cancel pushes, and
//!   DS-127's predicted late starts) defers to an open break triggered by
//!   the train whose notice is due ([`break_owns_train`]), and the queue
//!   drops a queued plain push
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
    /// `journey_breaks.public_id` (`b_` plus 32 hex, DS-534): the id the
    /// notice's URL carries. The column is nullable in the schema (a
    /// validated CHECK and a DEFAULT keep it set), so a NULL falls back to
    /// the numeric id, which the website still accepts.
    pub break_public_id: Option<String>,
    pub journey_id: i64,
    /// `journeys.public_id`: the id the website's journey URLs use (DS-534).
    pub journey_public_id: String,
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
    /// `train_subscriptions.public_id` of that subscription (DS-534): the
    /// notice's tag when a train triggered it.
    pub trigger_tracked_train_public_id: Option<String>,
    /// That train's current status, for its `train_notification_state`.
    pub trigger_status: Option<String>,
    pub trigger_delay_minutes: Option<i32>,
    /// Whether the trigger train's own pushes are on
    /// (`train_subscriptions.notifications_enabled`); TRUE without one.
    pub trigger_pushes_on: bool,
    /// The trigger train's shared `trains` row, for its cancellation reason.
    pub trigger_trains_id: Option<i64>,
    /// The trigger train's cancellation reason text (DS-184), filled by the
    /// cycle for a cancellation; the notice leads with it.
    #[sqlx(skip)]
    pub cancel_reason: Option<String>,
    /// The owner's own delay threshold (DS-153), NULL for the default.
    pub delay_threshold_minutes: Option<i16>,
}

/// The [`BreakNoticeRow`] columns and joins over open breaks with
/// suggestions ready (`b`), shared by [`breaks_to_notify`] and
/// [`train_break_state`] so both decide on the same row.
pub(crate) const BREAK_ROWS_SQL: &str = "SELECT b.id, b.public_id AS break_public_id, b.journey_id, j.public_id AS journey_public_id, b.user_id, b.cause, b.severity, b.notified_severity, \
                b.opened_at, b.confirmations, b.suggestions, j.suggestions_muted, \
                COALESCE(p.journey_suggestions, TRUE) AS journey_suggestions, \
                jl.train_subscription_id AS trigger_tracked_train_id, \
                ts.public_id AS trigger_tracked_train_public_id, \
                cs.status AS trigger_status, cs.delay_minutes AS trigger_delay_minutes, \
                COALESCE(ts.notifications_enabled, TRUE) AS trigger_pushes_on, \
                ts.trains_id AS trigger_trains_id, \
                p.delay_threshold_minutes \
         FROM journey_breaks b \
         JOIN journeys j ON j.id = b.journey_id AND j.user_id = b.user_id \
         LEFT JOIN notification_preferences p ON p.user_id = b.user_id \
         LEFT JOIN journey_legs jl ON jl.id = b.trigger_leg_id \
         LEFT JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id \
         LEFT JOIN train_current_state cs ON cs.trains_id = ts.trains_id \
         WHERE b.status = 'open' AND b.suggestions IS NOT NULL AND b.cause <> 'user_missed' ";

/// Open breaks with suggestions ready that are not yet notified at their
/// severity, oldest first.
pub(crate) async fn breaks_to_notify(pool: &PgPool) -> anyhow::Result<Vec<BreakNoticeRow>> {
    Ok(sqlx::query_as(&format!(
        "{BREAK_ROWS_SQL} \
           AND (b.notified_severity IS NULL OR b.severity > b.notified_severity) \
         ORDER BY b.detected_at, b.id \
         LIMIT 200"
    ))
    .fetch_all(pool)
    .await?)
}

/// What the user's open breaks say about one tracked train's pushes (§6.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TrainBreak {
    /// No break speaks for the train: its own pushes go out.
    None,
    /// A break the train triggered has a notice due (decided `Send`,
    /// waiting one sweep for its confirmation, or opened by the user in the
    /// last [`OPENED_QUIET`], after which it is sent): the notice will be
    /// the train's push.
    Due,
    /// A break the train triggered was notified at its severity. Its
    /// delivery wrote the train's state at the equivalent rank, so a plain
    /// push past that rank is news the notice did not carry.
    Notified { cause: String },
}

/// [`TrainBreak`] for a user's open breaks triggered by one train (`rows`),
/// each decided by [`decide_break_notice`] at `now`. Pure. A break whose
/// notice is suppressed for good (opt-out, mute, the train's own pushes
/// off) owns nothing, so a break that will not push never silences the
/// train (review finding N4). One the user opened in the app is only
/// quiet for [`OPENED_QUIET`] and then sent, so it is due: a plain push in
/// that window would be followed by the notice, two pushes for one train.
pub(crate) fn classify_train_break(rows: &[BreakNoticeRow], now: DateTime<Utc>) -> TrainBreak {
    let mut state = TrainBreak::None;
    for row in rows {
        match decide_break_notice(row, now) {
            BreakDecision::Send
            | BreakDecision::SuppressUnconfirmed
            | BreakDecision::SuppressActed => return TrainBreak::Due,
            BreakDecision::SuppressNotWorse => {
                state = TrainBreak::Notified {
                    cause: row.cause.clone(),
                };
            }
            BreakDecision::SuppressOptOut
            | BreakDecision::SuppressMuted
            | BreakDecision::SuppressTrainOff
            | BreakDecision::SuppressInApp => {}
        }
    }
    state
}

/// [`classify_train_break`] over the user's open breaks whose trigger leg
/// is on tracked train `tracked_train_id`. Only the trigger leg's train: a
/// break raised by leg 1 says nothing about leg 2's train, which keeps its
/// own pushes. A break the api could not plan for yet is not listed, so a
/// planning failure never silences the train.
pub(crate) async fn train_break_state(
    pool: &PgPool,
    user_id: &str,
    tracked_train_id: i64,
    now: DateTime<Utc>,
) -> anyhow::Result<TrainBreak> {
    let rows: Vec<BreakNoticeRow> = sqlx::query_as(&format!(
        "{BREAK_ROWS_SQL} AND b.user_id = $1 AND jl.train_subscription_id = $2 \
         ORDER BY b.id"
    ))
    .bind(user_id)
    .bind(tracked_train_id)
    .fetch_all(pool)
    .await?;
    Ok(classify_train_break(&rows, now))
}

/// Whether a break owns tracked train `tracked_train_id`'s next plain push
/// (a delay, a cancellation, a predicted late start, a held push): a break
/// the train triggered has its notice due, and the notice says more
/// (§6.3). The train path then stays quiet.
pub(crate) async fn break_owns_train(
    pool: &PgPool,
    user_id: &str,
    tracked_train_id: i64,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    Ok(train_break_state(pool, user_id, tracked_train_id, now).await? == TrainBreak::Due)
}

/// What a break means for a leg's skipped-stop push (review finding N3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkippedStopBreak {
    /// No break says it: the skip push goes out.
    None,
    /// The break's notice is due and will be the train's push: decide the
    /// skip again next cycle, without recording it (a notice that never
    /// goes leaves the skip push to go).
    Wait,
    /// A notice for a cancellation, a part-cancellation or a skipped stop
    /// was sent: it was this push. The skip is recorded as told, so the
    /// break closing (dismissed, or kept) does not send it a second time.
    Told,
}

/// [`SkippedStopBreak`] for the train's [`TrainBreak`]. Pure.
pub(crate) fn skipped_stop_break(state: &TrainBreak) -> SkippedStopBreak {
    match state {
        TrainBreak::Due => SkippedStopBreak::Wait,
        TrainBreak::Notified { cause }
            if matches!(
                cause.as_str(),
                "cancelled" | "part_cancelled" | "skipped_stop"
            ) =>
        {
            SkippedStopBreak::Told
        }
        // No break, or a missed connection's notice, which said nothing
        // about the stop.
        TrainBreak::None | TrainBreak::Notified { .. } => SkippedStopBreak::None,
    }
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
    /// The trigger train's own pushes are off (DS-153).
    SuppressTrainOff,
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
            BreakDecision::SuppressTrainOff => "train_off",
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
    if !row.trigger_pushes_on {
        return BreakDecision::SuppressTrainOff;
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
    common::zone::london_local(at).format("%H:%M").to_string()
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

/// A notice's body: why the train is not running first, when known
/// (DS-184: "Reason: Signal failure."), then the lead and the next option.
fn joined_body(cancel_reason: Option<&str>, lead: Option<String>, body: String) -> String {
    let reason = cancel_reason.map(|r| format!("Reason: {r}."));
    [reason, lead, Some(body)]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ")
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
    let body = joined_body(row.cancel_reason.as_deref(), lead, body);
    NotificationPayload {
        title,
        body,
        url: break_url(row),
        tag: match &row.trigger_tracked_train_public_id {
            Some(train_public_id) => crate::push_ids::train_tag(train_public_id),
            None => crate::push_ids::journey_break_tag(&row.journey_public_id),
        },
        renotify: false,
    }
}

/// The notice's link: the journey's public id and the break's, so no
/// numeric id leaves the service (DS-534). A break with no public id (not
/// expected) links its numeric id, which the website still accepts.
fn break_url(row: &BreakNoticeRow) -> String {
    let break_ref = match row.break_public_id.as_deref() {
        Some(public_id) if !public_id.is_empty() => public_id.to_string(),
        _ => row.id.to_string(),
    };
    format!(
        "/journeys/{}?break={break_ref}&src=push#break",
        row.journey_public_id
    )
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
            ds_runtime::metrics::metric_name("notifier_journey_break_notice_total"),
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
    for mut row in breaks_to_notify(pool).await? {
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
        // A cancelled trigger train's reason, when TRUST has given one. The
        // notice is not followed up when it comes later: its suggestions
        // are what it is for, and a plain follow-up would replace them.
        if (matches!(row.cause.as_str(), "cancelled" | "part_cancelled")
            || row.trigger_status.as_deref() == Some("cancelled"))
            && let Some(trains_id) = row.trigger_trains_id
        {
            let summary = crate::cancel_reason::summary_for_train(pool, trains_id).await;
            row.cancel_reason =
                crate::cancel_reason::CancelCopy::from_summary(summary.as_ref()).reason;
        }
        let trigger = row.trigger_tracked_train_id.map(|tracked_train_id| {
            let threshold = crate::queries::user_delay_threshold(
                row.delay_threshold_minutes,
                delay_threshold_minutes,
            );
            let (status, delay_minutes) = equivalent_train_state(&row, threshold);
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
        crate::hold::dispatch(
            pool,
            queue,
            crate::push_queue::PushJob {
                user_id: row.user_id.clone(),
                payload: build_break_payload(&row),
                delivered: crate::push_queue::Delivered::JourneyBreak {
                    break_id: row.id,
                    severity: row.severity,
                    trigger,
                },
                decided_at: now,
            },
        )
        .await?;
    }
    Ok(())
}

/// Registers every `result` of `notifier_journey_break_notice_total` at 0.
pub(crate) fn register_metrics() {
    for decision in [
        BreakDecision::Send,
        BreakDecision::SuppressOptOut,
        BreakDecision::SuppressMuted,
        BreakDecision::SuppressTrainOff,
        BreakDecision::SuppressNotWorse,
        BreakDecision::SuppressActed,
        BreakDecision::SuppressUnconfirmed,
        BreakDecision::SuppressInApp,
    ] {
        metrics::counter!(
            ds_runtime::metrics::metric_name("notifier_journey_break_notice_total"),
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
            break_public_id: Some("b_00000000000000000000000000000007".to_string()),
            journey_id: 42,
            journey_public_id: "j_00000000000000000000000000000042".to_string(),
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
            trigger_tracked_train_public_id: Some("t_00000000000000000000000000000118".to_string()),
            trigger_status: Some("en_route".to_string()),
            trigger_delay_minutes: Some(9),
            trigger_pushes_on: true,
            trigger_trains_id: None,
            cancel_reason: None,
            delay_threshold_minutes: None,
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
        assert_eq!(
            payload.url,
            "/journeys/j_00000000000000000000000000000042?break=b_00000000000000000000000000000007&src=push#break"
        );
        assert_eq!(payload.tag, "train-t_00000000000000000000000000000118");
    }

    /// DS-534: the link carries the break's public id, never its number.
    #[test]
    fn the_url_carries_the_public_break_id_and_no_numeric_one() {
        let url = break_url(&row("cancelled"));
        assert_eq!(
            url,
            "/journeys/j_00000000000000000000000000000042?break=b_00000000000000000000000000000007&src=push#break"
        );
        assert!(!url.contains("break=7"), "{url}");
    }

    /// A row whose `public_id` is NULL (the column allows it, the DEFAULT
    /// and CHECK keep it from happening) links its numeric id rather than
    /// a bare `b_`.
    #[test]
    fn a_break_without_a_public_id_falls_back_to_its_numeric_id() {
        for missing in [None, Some(String::new())] {
            let r = BreakNoticeRow {
                break_public_id: missing,
                ..row("cancelled")
            };
            assert_eq!(
                break_url(&r),
                "/journeys/j_00000000000000000000000000000042?break=7&src=push#break"
            );
            assert_eq!(build_break_payload(&r).url, break_url(&r));
        }
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
    fn a_cancelled_trains_reason_leads_the_notice() {
        let mut r = row("cancelled");
        r.cancel_reason = Some("Signal failure".to_string());
        let payload = build_break_payload(&r);
        assert_eq!(payload.title, "10:16 to Charing Cross cancelled");
        assert_eq!(
            payload.body,
            "Reason: Signal failure. Next: 10:31, arrive 10:52. Tap to switch."
        );
    }

    #[test]
    fn no_suggestions_says_so_and_a_break_with_no_train_has_its_own_tag() {
        let mut r = row("cancelled");
        r.suggestions.as_mut().unwrap()["suggestions"] = serde_json::json!([]);
        r.trigger_tracked_train_id = None;
        r.trigger_tracked_train_public_id = None;
        let payload = build_break_payload(&r);
        assert_eq!(payload.title, "No more trains to Charing Cross today");
        assert_eq!(payload.body, "Tap to see your options.");
        assert_eq!(
            payload.tag,
            "journey-break-j_00000000000000000000000000000042"
        );
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
            // The trigger train's own pushes are off (DS-153).
            (
                BreakNoticeRow {
                    trigger_pushes_on: false,
                    ..row("cancelled")
                },
                BreakDecision::SuppressTrainOff,
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

    /// Review finding N4: a break owns its train's plain push only while
    /// its notice is due; one that will not push owns nothing.
    #[test]
    fn a_break_owns_its_train_only_while_its_notice_is_due() {
        let now = at(9, 30);
        assert_eq!(classify_train_break(&[], now), TrainBreak::None);
        assert_eq!(
            classify_train_break(&[row("cancelled")], now),
            TrainBreak::Due
        );
        // Waiting one sweep for its confirmation: due next cycle.
        let unconfirmed = BreakNoticeRow {
            confirmations: 1,
            ..row("connection_missed")
        };
        assert_eq!(classify_train_break(&[unconfirmed], now), TrainBreak::Due);
        // Opened in the app a moment ago: quiet for OPENED_QUIET, then
        // sent, so it still owns the train meanwhile.
        let opened = BreakNoticeRow {
            opened_at: Some(now - Duration::minutes(2)),
            ..row("connection_missed")
        };
        assert_eq!(
            decide_break_notice(&opened, now),
            BreakDecision::SuppressActed
        );
        assert_eq!(classify_train_break(&[opened], now), TrainBreak::Due);
        // Already notified at its severity: the notice carried the train
        // up to its equivalent rank, so a plain push past it goes out.
        let notified = BreakNoticeRow {
            notified_severity: Some(1),
            ..row("connection_missed")
        };
        assert_eq!(
            classify_train_break(std::slice::from_ref(&notified), now),
            TrainBreak::Notified {
                cause: "connection_missed".into()
            }
        );
        // A break that will not push never silences the train.
        for silent in [
            BreakNoticeRow {
                journey_suggestions: false,
                ..row("cancelled")
            },
            BreakNoticeRow {
                suggestions_muted: true,
                ..row("cancelled")
            },
            BreakNoticeRow {
                trigger_pushes_on: false,
                ..row("cancelled")
            },
        ] {
            assert_eq!(
                classify_train_break(std::slice::from_ref(&silent), now),
                TrainBreak::None,
                "{silent:?}"
            );
        }
        // A due notice wins over a notified one.
        assert_eq!(
            classify_train_break(&[notified, row("cancelled")], now),
            TrainBreak::Due
        );
    }

    /// Review finding N3: the skipped-stop push defers to a break that
    /// says the train no longer stops where planned.
    #[test]
    fn a_skipped_stop_defers_to_a_break_that_already_says_so() {
        assert_eq!(
            skipped_stop_break(&TrainBreak::None),
            SkippedStopBreak::None
        );
        assert_eq!(skipped_stop_break(&TrainBreak::Due), SkippedStopBreak::Wait);
        for cause in ["skipped_stop", "part_cancelled", "cancelled"] {
            assert_eq!(
                skipped_stop_break(&TrainBreak::Notified {
                    cause: cause.into()
                }),
                SkippedStopBreak::Told,
                "{cause}"
            );
        }
        // A missed connection's notice said nothing about the stop.
        assert_eq!(
            skipped_stop_break(&TrainBreak::Notified {
                cause: "connection_missed".into()
            }),
            SkippedStopBreak::None
        );
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
        let public_id: String =
            sqlx::query_scalar("SELECT public_id FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read train_subscriptions.public_id");
        assert_eq!(
            row.trigger_tracked_train_public_id.as_deref(),
            Some(public_id.as_str())
        );
        assert!(
            public_id.starts_with("t_") && public_id.len() == 34,
            "{public_id}"
        );
        // DS-534: the query reads the break's own public id, and the
        // notice's link carries it.
        let break_public_id: String =
            sqlx::query_scalar("SELECT public_id FROM journey_breaks WHERE id = $1")
                .bind(break_id)
                .fetch_one(&pool)
                .await
                .expect("read journey_breaks.public_id");
        assert!(
            break_public_id.starts_with("b_") && break_public_id.len() == 34,
            "{break_public_id}"
        );
        assert_eq!(
            row.break_public_id.as_deref(),
            Some(break_public_id.as_str())
        );
        assert!(
            build_break_payload(row)
                .url
                .ends_with(&format!("?break={break_public_id}&src=push#break"))
        );
        assert_eq!(row.trigger_status.as_deref(), Some("cancelled"));
        assert!(row.journey_suggestions && !row.suggestions_muted);
        assert_eq!(decide_break_notice(row, Utc::now()), BreakDecision::Send);
        assert!(
            break_owns_train(&pool, USER, tracked_train_id, Utc::now())
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
            !break_owns_train(&pool, USER, tracked_train_id, Utc::now())
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

    /// Review finding N4: a break owns only its trigger leg's train, and
    /// only while its notice is due.
    #[tokio::test]
    #[ignore = "requires a live database: DATABASE_URL"]
    async fn a_break_owns_only_its_trigger_train_and_only_while_due() {
        let pool = connect().await;
        cleanup(&pool).await;
        let (journey_id, break_id, tracked_train_id) = seed(&pool).await;
        // A second leg on another train of the same journey.
        let (other_trains_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-BREAK-NOTICE-2', '2099-06-01') \
             RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .expect("train");
        let (other_tracked,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, trains_id, resolution_status) \
             VALUES ($1, '2099-06-01', $2, 'resolved') RETURNING id",
        )
        .bind(USER)
        .bind(other_trains_id)
        .fetch_one(&pool)
        .await
        .expect("subscription");
        sqlx::query(
            "INSERT INTO journey_legs (journey_id, leg_order, origin_crs, destination_crs, \
                 service_date, train_subscription_id, match_mode) \
             VALUES ($1, 2, 'YRK', 'EDB', '2099-06-01', $2, 'manual')",
        )
        .bind(journey_id)
        .bind(other_tracked)
        .execute(&pool)
        .await
        .expect("leg 2");
        let now = Utc::now();

        assert!(
            break_owns_train(&pool, USER, tracked_train_id, now)
                .await
                .expect("owns")
        );
        assert!(
            !break_owns_train(&pool, USER, other_tracked, now)
                .await
                .expect("owns"),
            "leg 2's train keeps its own pushes"
        );
        assert_eq!(
            train_break_state(&pool, USER, other_tracked, now)
                .await
                .expect("state"),
            TrainBreak::None
        );

        // Notified: the trigger train's next plain push is news past it.
        record_notified(&pool, break_id, 3, now)
            .await
            .expect("record");
        assert!(
            !break_owns_train(&pool, USER, tracked_train_id, now)
                .await
                .expect("owns")
        );
        assert_eq!(
            train_break_state(&pool, USER, tracked_train_id, now)
                .await
                .expect("state"),
            TrainBreak::Notified {
                cause: "cancelled".into()
            }
        );
        cleanup(&pool).await;
    }

    /// DS-153: a train with its own pushes off sends nothing, its break's
    /// suggestions included, and a break on it owns no other push.
    #[tokio::test]
    #[ignore = "requires a live database: DATABASE_URL"]
    async fn a_break_on_a_train_with_its_pushes_off_sends_no_suggestions() {
        let pool = connect().await;
        cleanup(&pool).await;
        let (_journey_id, break_id, tracked_train_id) = seed(&pool).await;
        sqlx::query("UPDATE train_subscriptions SET notifications_enabled = FALSE WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .expect("turn the train's pushes off");

        let rows = breaks_to_notify(&pool).await.expect("breaks");
        let row = rows
            .iter()
            .find(|row| row.id == break_id)
            .expect("the open break is still listed (the banner still shows)");
        assert!(!row.trigger_pushes_on);
        assert_eq!(
            decide_break_notice(row, Utc::now()),
            BreakDecision::SuppressTrainOff
        );
        assert!(
            !break_owns_train(&pool, USER, tracked_train_id, Utc::now())
                .await
                .expect("owns"),
            "a silenced break owns no push"
        );
        cleanup(&pool).await;
    }
}

/// Pins `london_clock` (the London wall-clock `HH:MM` shown in pushes) around
/// both clock changes of 2025 and 2026. Expected values come from an
/// independent implementation of the IANA rules.
#[cfg(test)]
mod london_clock_pinning_tests {
    use super::*;

    /// `(instant, London HH:MM)`.
    #[rustfmt::skip]
    const CASES: &[(&str, &str)] = &[
        ("2025-03-29T22:00:00Z", "22:00"),
        ("2025-03-29T23:00:00Z", "23:00"),
        ("2025-03-29T23:30:00Z", "23:30"),
        ("2025-03-30T00:00:00Z", "00:00"),
        ("2025-03-30T00:30:00Z", "00:30"),
        ("2025-03-30T00:59:00Z", "00:59"),
        ("2025-03-30T01:00:00Z", "02:00"),
        ("2025-03-30T01:01:00Z", "02:01"),
        ("2025-03-30T01:30:00Z", "02:30"),
        ("2025-03-30T01:59:00Z", "02:59"),
        ("2025-03-30T02:00:00Z", "03:00"),
        ("2025-03-30T02:30:00Z", "03:30"),
        ("2025-03-30T03:00:00Z", "04:00"),
        ("2025-03-30T12:00:00Z", "13:00"),
        ("2025-03-30T22:59:00Z", "23:59"),
        ("2025-03-30T23:00:00Z", "00:00"),
        ("2025-03-30T23:30:00Z", "00:30"),
        ("2025-03-31T00:30:00Z", "01:30"),
        ("2025-10-25T22:00:00Z", "23:00"),
        ("2025-10-25T23:00:00Z", "00:00"),
        ("2025-10-25T23:30:00Z", "00:30"),
        ("2025-10-26T00:00:00Z", "01:00"),
        ("2025-10-26T00:30:00Z", "01:30"),
        ("2025-10-26T00:59:00Z", "01:59"),
        ("2025-10-26T01:00:00Z", "01:00"),
        ("2025-10-26T01:01:00Z", "01:01"),
        ("2025-10-26T01:30:00Z", "01:30"),
        ("2025-10-26T01:59:00Z", "01:59"),
        ("2025-10-26T02:00:00Z", "02:00"),
        ("2025-10-26T02:30:00Z", "02:30"),
        ("2025-10-26T03:00:00Z", "03:00"),
        ("2025-10-26T12:00:00Z", "12:00"),
        ("2025-10-26T22:59:00Z", "22:59"),
        ("2025-10-26T23:00:00Z", "23:00"),
        ("2025-10-26T23:30:00Z", "23:30"),
        ("2025-10-27T00:30:00Z", "00:30"),
        ("2025-12-31T22:00:00Z", "22:00"),
        ("2025-12-31T22:59:00Z", "22:59"),
        ("2025-12-31T23:00:00Z", "23:00"),
        ("2025-12-31T23:30:00Z", "23:30"),
        ("2025-12-31T23:59:00Z", "23:59"),
        ("2026-01-01T00:00:00Z", "00:00"),
        ("2026-01-01T00:30:00Z", "00:30"),
        ("2026-01-01T00:59:00Z", "00:59"),
        ("2026-01-01T01:00:00Z", "01:00"),
        ("2026-01-01T01:30:00Z", "01:30"),
        ("2026-01-01T01:59:00Z", "01:59"),
        ("2026-01-01T02:00:00Z", "02:00"),
        ("2026-01-01T02:30:00Z", "02:30"),
        ("2026-01-15T22:00:00Z", "22:00"),
        ("2026-01-15T22:59:00Z", "22:59"),
        ("2026-01-15T23:00:00Z", "23:00"),
        ("2026-01-15T23:30:00Z", "23:30"),
        ("2026-01-15T23:59:00Z", "23:59"),
        ("2026-01-16T00:00:00Z", "00:00"),
        ("2026-01-16T00:30:00Z", "00:30"),
        ("2026-01-16T00:59:00Z", "00:59"),
        ("2026-01-16T01:00:00Z", "01:00"),
        ("2026-01-16T01:30:00Z", "01:30"),
        ("2026-01-16T01:59:00Z", "01:59"),
        ("2026-01-16T02:00:00Z", "02:00"),
        ("2026-01-16T02:30:00Z", "02:30"),
        ("2026-03-28T22:00:00Z", "22:00"),
        ("2026-03-28T23:00:00Z", "23:00"),
        ("2026-03-28T23:30:00Z", "23:30"),
        ("2026-03-29T00:00:00Z", "00:00"),
        ("2026-03-29T00:30:00Z", "00:30"),
        ("2026-03-29T00:59:00Z", "00:59"),
        ("2026-03-29T01:00:00Z", "02:00"),
        ("2026-03-29T01:01:00Z", "02:01"),
        ("2026-03-29T01:30:00Z", "02:30"),
        ("2026-03-29T01:59:00Z", "02:59"),
        ("2026-03-29T02:00:00Z", "03:00"),
        ("2026-03-29T02:30:00Z", "03:30"),
        ("2026-03-29T03:00:00Z", "04:00"),
        ("2026-03-29T12:00:00Z", "13:00"),
        ("2026-03-29T22:59:00Z", "23:59"),
        ("2026-03-29T23:00:00Z", "00:00"),
        ("2026-03-29T23:30:00Z", "00:30"),
        ("2026-03-30T00:30:00Z", "01:30"),
        ("2026-06-30T22:00:00Z", "23:00"),
        ("2026-06-30T22:59:00Z", "23:59"),
        ("2026-06-30T23:00:00Z", "00:00"),
        ("2026-06-30T23:30:00Z", "00:30"),
        ("2026-06-30T23:59:00Z", "00:59"),
        ("2026-07-01T00:00:00Z", "01:00"),
        ("2026-07-01T00:30:00Z", "01:30"),
        ("2026-07-01T00:59:00Z", "01:59"),
        ("2026-07-01T01:00:00Z", "02:00"),
        ("2026-07-01T01:30:00Z", "02:30"),
        ("2026-07-01T01:59:00Z", "02:59"),
        ("2026-07-01T02:00:00Z", "03:00"),
        ("2026-07-01T02:30:00Z", "03:30"),
        ("2026-07-15T22:00:00Z", "23:00"),
        ("2026-07-15T22:59:00Z", "23:59"),
        ("2026-07-15T23:00:00Z", "00:00"),
        ("2026-07-15T23:30:00Z", "00:30"),
        ("2026-07-15T23:59:00Z", "00:59"),
        ("2026-07-16T00:00:00Z", "01:00"),
        ("2026-07-16T00:30:00Z", "01:30"),
        ("2026-07-16T00:59:00Z", "01:59"),
        ("2026-07-16T01:00:00Z", "02:00"),
        ("2026-07-16T01:30:00Z", "02:30"),
        ("2026-07-16T01:59:00Z", "02:59"),
        ("2026-07-16T02:00:00Z", "03:00"),
        ("2026-07-16T02:30:00Z", "03:30"),
        ("2026-10-24T22:00:00Z", "23:00"),
        ("2026-10-24T23:00:00Z", "00:00"),
        ("2026-10-24T23:30:00Z", "00:30"),
        ("2026-10-25T00:00:00Z", "01:00"),
        ("2026-10-25T00:30:00Z", "01:30"),
        ("2026-10-25T00:59:00Z", "01:59"),
        ("2026-10-25T01:00:00Z", "01:00"),
        ("2026-10-25T01:01:00Z", "01:01"),
        ("2026-10-25T01:30:00Z", "01:30"),
        ("2026-10-25T01:59:00Z", "01:59"),
        ("2026-10-25T02:00:00Z", "02:00"),
        ("2026-10-25T02:30:00Z", "02:30"),
        ("2026-10-25T03:00:00Z", "03:00"),
        ("2026-10-25T12:00:00Z", "12:00"),
        ("2026-10-25T22:59:00Z", "22:59"),
        ("2026-10-25T23:00:00Z", "23:00"),
        ("2026-10-25T23:30:00Z", "23:30"),
        ("2026-10-26T00:30:00Z", "00:30"),
    ];

    #[test]
    fn london_clock_matches_the_table() {
        for &(at, expected) in CASES {
            assert_eq!(london_clock(at.parse().unwrap()), expected, "at {at}");
        }
    }
}
