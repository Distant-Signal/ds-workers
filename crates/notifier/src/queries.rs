//! Watermark polling and candidate joins over line_status_history /
//! train_movement_events. See
//! docs/superpowers/specs/2026-09-02-line-status-notifications-design.md's
//! Architecture section for the full per-cycle shape this implements.
//!
//! `line_status_history` has TWO independent writers elsewhere in this
//! workspace -- `crates/aggregator/src/queries.rs::write_line_status` and
//! `crates/api/src/data/queries.rs::upsert_tfl_line_status` -- this module
//! only ever reads the shared table, never hooks either writer.
//!
//! Every rank computed in this module goes through `common::severity_rank`
//! (never `LineStatusReport::worst_severity()`/raw `Severity` ordering) --
//! see `crate::decision`'s module doc for why.

use chrono::{DateTime, Utc};
use common::{LineStatus, severity_rank};
use sqlx::{PgPool, Row};

use crate::decision::train_severity_rank;

/// One `notifier_cursor` row: the committed watermark plus the
/// not-yet-promoted proposal behind the grace window
/// (`20260925215000_notifier_cursor_pending_watermark.sql`). See
/// [`advance_cursor_with_grace`] for the full two-phase mechanic and the
/// out-of-order-commit bug it closes.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CursorState {
    /// Every row at or below this id is genuinely processed-and-past --
    /// only this value bounds the next poll's `WHERE id > $1`.
    pub last_processed_id: i64,
    /// The highest id some EARLIER cycle observed, awaiting promotion into
    /// `last_processed_id` once it is older than the grace window. `None`
    /// on a brand-new cursor row (and on one last written by a pre-grace
    /// notifier build).
    pub pending_id: Option<i64>,
    pub pending_observed_at: Option<DateTime<Utc>>,
}

/// Upserts a zero row on first use -- the migration declares the table's
/// shape but deliberately does not seed rows (Task 1), so the first ever
/// poll cycle for a given `name` creates its own starting-at-zero cursor
/// here.
pub async fn read_cursor(pool: &PgPool, name: &str) -> anyhow::Result<CursorState> {
    let row = sqlx::query_as::<_, CursorState>(
        "INSERT INTO notifier_cursor (name, last_processed_id) VALUES ($1, 0) \
         ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name \
         RETURNING last_processed_id, pending_id, pending_observed_at",
    )
    .bind(name)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Advances a watermark through a GRACE WINDOW rather than straight to
/// "the maximum id this cycle happened to see."
///
/// The bug this closes: ids come off a sequence at INSERT time, but a row
/// only becomes visible at COMMIT time, so ids are NOT committed in order.
/// Under the old `SET last_processed_id = MAX(id) observed`, a transaction
/// holding id 100 that committed after a cycle had already observed (and
/// stepped past) id 101 was skipped FOREVER -- for line status that means a
/// user is never told about a severity escalation, and nothing downstream
/// ever re-checks it. Every writer of the three tables this crate polls is
/// transactional and can commit out of id order under concurrency (the
/// aggregator's `write_line_status`, `api`'s `upsert_tfl_line_status`, TRUST
/// ingest, and trust-consumer's own forwarding write).
///
/// The fix, two-phase: this cycle PROPOSES `observed_max_id`
/// (`pending_id`/`pending_observed_at`), and only PROMOTES a proposal made
/// by an earlier cycle into `last_processed_id` once that proposal has aged
/// past `grace`. The promoting cycle has, by construction, just re-read
/// every row above the old `last_processed_id` -- so a lower-id row that
/// committed late, inside the grace window, is in THAT read's candidate set
/// before the cursor ever moves past it. A row can therefore be read by
/// several consecutive cycles; every send path fed by these cursors is
/// already idempotent against that (`decision::decide_user_notification`'s
/// own "already notified this exact resulting state" guard, and the
/// escalation-only `decide_train_notification`).
///
/// Residual, deliberately accepted: a transaction that stays in flight for
/// LONGER than `grace` can still be missed. That is the standard tradeoff of
/// this pattern -- the alternative (never trusting an id watermark at all)
/// means re-scanning the whole table forever. `grace` is configurable
/// (`--cursor-grace-seconds`) precisely so it can be widened if a writer
/// ever grows a genuinely long-running transaction.
///
/// Returns the `last_processed_id` now stored, for the caller's logging.
pub async fn advance_cursor_with_grace(
    pool: &PgPool,
    name: &str,
    state: &CursorState,
    observed_max_id: i64,
    now: DateTime<Utc>,
    grace: chrono::Duration,
) -> anyhow::Result<i64> {
    let promoted = match (state.pending_id, state.pending_observed_at) {
        (Some(pending_id), Some(observed_at)) if now - observed_at >= grace => pending_id,
        _ => state.last_processed_id,
    };
    // `max` on both: a watermark must never move BACKWARDS, not even if a
    // stale proposal (or a deleted row shrinking `observed_max_id`) would
    // otherwise take it there.
    let new_last = promoted.max(state.last_processed_id);
    let new_pending = observed_max_id.max(new_last);

    sqlx::query(
        "UPDATE notifier_cursor \
         SET last_processed_id = $1, pending_id = $2, pending_observed_at = $3 \
         WHERE name = $4",
    )
    .bind(new_last)
    .bind(new_pending)
    .bind(now)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(new_last)
}

pub struct LineCandidate {
    pub id: i64,
    pub line_id: String,
    pub new_rank: u8,
    /// Always a real value by construction: only pushed below after
    /// `is_severity_transition` has already required `previous_rank` to be
    /// `Some` (Decision 3's cold-start guard already filtered out the
    /// `None` case) -- `u8`, not `Option<u8>`, so callers never need to
    /// unwrap an invariant that's already been proven true.
    pub previous_rank: u8,
}

/// Raw columns of one `poll_line_candidates` row, before the
/// JSON-decode-and-rank-and-filter step -- private, exists only to satisfy
/// `sqlx::FromRow`. Mirrors `crates/api/src/data/train_tracking.rs`'s
/// `TrackedTrainRow`/`TrackedTrainRef` two-struct precedent.
#[derive(Debug, sqlx::FromRow)]
struct LineHistoryRow {
    id: i64,
    line_id: String,
    statuses: serde_json::Value,
    previous_statuses: Option<serde_json::Value>,
}

fn worst_rank(statuses: &[LineStatus]) -> u8 {
    statuses
        .iter()
        .map(|s| severity_rank(s.severity))
        .min()
        .unwrap_or(0)
}

/// Decodes one polled `line_status_history` row into a candidate, or
/// `None` for "this row is not a candidate" -- which now deliberately
/// covers BOTH "no severity transition here" and "this row's own JSON does
/// not decode into `Vec<LineStatus>` at all."
///
/// The bug that second case closes: this logic used `serde_json::from_value(
/// ...)?` inline, so ONE undecodable `statuses` (or `previous_statuses`)
/// blob -- a partially-written row, a shape written by an older/newer
/// build of `common::LineStatus`, anything hand-edited -- returned `Err`
/// out of the whole poll, which aborted `run_cycle` BEFORE it reached
/// `advance_cursor`. The cursor therefore never moved past that row, and
/// every subsequent cycle failed at the exact same point, forever, for
/// EVERY user and for BOTH the line-status and the train-movement halves of
/// the cycle (trains are polled after this in the same function). A
/// permanent, silent, total notification outage caused by one bad row.
///
/// Skipping-and-logging instead is the same posture
/// `schedule_matching::run_schedule_match_sweep` already takes for one
/// malformed row inside a sweep ("a single row's failure ... is logged and
/// skipped, not propagated"), and is what the aggregator's equivalent
/// row-decode path does too. A genuine DB/connectivity failure still
/// propagates -- that is `fetch_all`'s own `?`, above, untouched.
fn line_candidate_from_row(row: LineHistoryRow) -> Option<LineCandidate> {
    let statuses: Vec<LineStatus> = match serde_json::from_value(row.statuses) {
        Ok(statuses) => statuses,
        Err(err) => {
            tracing::error!(
                error = ?err,
                line_status_history_id = row.id,
                line_id = %row.line_id,
                "skipping an undecodable line_status_history.statuses row -- the cursor still \
                 advances past it rather than stalling every notification forever"
            );
            return None;
        }
    };
    let new_rank = worst_rank(&statuses);

    let previous_rank = match row.previous_statuses {
        None => None,
        Some(previous_json) => match serde_json::from_value::<Vec<LineStatus>>(previous_json) {
            Ok(previous_statuses) => Some(worst_rank(&previous_statuses)),
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    line_status_history_id = row.id,
                    line_id = %row.line_id,
                    "skipping a line_status_history row whose PRECEDING row's statuses are \
                     undecodable -- there is no trustworthy previous rank to compare against"
                );
                return None;
            }
        },
    };

    if !crate::decision::is_severity_transition(previous_rank, new_rank) {
        return None;
    }
    // Safe: is_severity_transition returning true already requires
    // previous_rank to be Some (its None branch always returns false) --
    // see the LineCandidate.previous_rank field comment.
    Some(LineCandidate {
        id: row.id,
        line_id: row.line_id,
        new_rank,
        previous_rank: previous_rank.expect("checked by is_severity_transition"),
    })
}

/// One correlated subquery per row to find "the immediately preceding
/// line_status_history row for this same line_id" (Decision 3's guard --
/// NULL previous_statuses means none exists). This workspace's existing
/// data-volume scale ("single trusted personal instance", per DESIGN.md)
/// doesn't justify a window-function rewrite for this; revisit if line
/// count/history volume ever grows enough to matter.
///
/// Returns `(candidates, observed_max_id)`. The second element is the
/// highest id this poll actually SAW, not the highest id that turned out to
/// be a candidate -- same shape `poll_train_candidates` already returns, and
/// necessary for the same two reasons: an ordinary non-transition row (by
/// far the common case -- most `line_status_history` rows repeat the
/// previous severity) and a skipped undecodable row must BOTH let the
/// watermark move past them. `run_cycle` previously took `max` over the
/// CANDIDATE ids alone, so a cycle that found no candidate at all left the
/// cursor where it was and re-scanned the same rows on every subsequent
/// cycle for as long as no transition ever occurred.
pub async fn poll_line_candidates(
    pool: &PgPool,
    since_id: i64,
) -> anyhow::Result<(Vec<LineCandidate>, i64)> {
    let rows = sqlx::query_as::<_, LineHistoryRow>(
        "SELECT h.id, h.line_id, h.statuses AS statuses, \
                (SELECT h2.statuses FROM line_status_history h2 \
                   WHERE h2.line_id = h.line_id AND h2.id < h.id \
                   ORDER BY h2.id DESC LIMIT 1) AS previous_statuses \
         FROM line_status_history h \
         WHERE h.id > $1 \
         ORDER BY h.id",
    )
    .bind(since_id)
    .fetch_all(pool)
    .await?;

    let observed_max_id = rows.iter().map(|row| row.id).max().unwrap_or(since_id);
    let candidates = rows
        .into_iter()
        .filter_map(line_candidate_from_row)
        .collect();
    Ok((candidates, observed_max_id))
}

pub struct TrainCandidate {
    pub tracked_train_id: i64,
    pub trains_id: i64,
    pub user_id: String,
    pub new_rank: u8,
    pub previous_rank: u8,
}

/// The per-`trains_id` candidate-building body Task 12's `poll_train_candidates`
/// already had, extracted so Task 18's forward-queue cycle can reuse it
/// without a second, divergent copy of the cooldown/escalation lookup. One
/// `trains_id` can have MANY independent subscribers (`tracked_trains`
/// rows); this fans out to every one of them, each judged by its OWN
/// cooldown/escalation state in `train_notification_state` (still keyed by
/// `(user_id, tracked_train_id)` -- unchanged, since that's still each
/// user's own private escalation history for their own subscription row).
///
/// `AND notifications_enabled` on the subscriber query: 2026-09 security/
/// bug review finding (Medium) -- `train_subscriptions.notifications_enabled`
/// existed in the schema since `20260906100000_trains.sql` but was never
/// actually consulted anywhere before this fix, so it was pure dead
/// weight. The API's `journeys::set_leg_train_subscription` (a "Change
/// train" re-pick) now sets it `FALSE` on a leg's old subscription once it
/// becomes unreferenced, specifically so that subscription stops
/// generating notifications -- this filter is the other, equally
/// necessary half of that fix: without it, a deactivated subscription
/// would still show up here and get notified exactly as before.
pub async fn candidates_for_trains_id(
    pool: &PgPool,
    trains_id: i64,
    delay_threshold_minutes: i32,
) -> anyhow::Result<Vec<TrainCandidate>> {
    let current =
        sqlx::query("SELECT status, delay_minutes FROM train_current_state WHERE trains_id = $1")
            .bind(trains_id)
            .fetch_optional(pool)
            .await?;
    let Some(current) = current else {
        return Ok(Vec::new());
    }; // no current-state row yet -- nothing to compare

    let status: String = current.try_get("status")?;
    let delay_minutes: Option<i32> = current.try_get("delay_minutes")?;
    let new_rank = train_severity_rank(&status, delay_minutes, delay_threshold_minutes);

    let subscribers = sqlx::query(
        "SELECT id, user_id FROM train_subscriptions WHERE trains_id = $1 AND notifications_enabled",
    )
    .bind(trains_id)
    .fetch_all(pool)
    .await?;
    let mut candidates = Vec::new();
    for subscriber in subscribers {
        let tracked_train_id: i64 = subscriber.try_get("id")?;
        let user_id: String = subscriber.try_get("user_id")?;

        let previous = sqlx::query(
            "SELECT last_notified_status, last_notified_delay_minutes \
             FROM train_notification_state WHERE user_id = $1 AND tracked_train_id = $2",
        )
        .bind(&user_id)
        .bind(tracked_train_id)
        .fetch_optional(pool)
        .await?;
        let previous_rank = match previous {
            None => 0, // Task 3's design note: no cold-start guard for trains
            Some(previous) => {
                let previous_status: String = previous.try_get("last_notified_status")?;
                let previous_delay: Option<i32> =
                    previous.try_get("last_notified_delay_minutes")?;
                train_severity_rank(&previous_status, previous_delay, delay_threshold_minutes)
            }
        };

        if crate::decision::decide_train_notification(previous_rank, new_rank)
            == crate::decision::NotifyDecision::NotifyNow
        {
            candidates.push(TrainCandidate {
                tracked_train_id,
                trains_id,
                user_id,
                new_rank,
                previous_rank,
            });
        }
    }
    Ok(candidates)
}

/// Watermark now advances over `train_movement_events.trains_id` (Task 9),
/// the shared, per-physical-train identity -- NOT `tracked_train_id`, which
/// stops being written by new events as of Task 11.
///
/// Since `trust-backlog-consumer` started writing `train_movement_events`
/// for every train touched NATIONALLY (not just tracked ones), a plain
/// `SELECT DISTINCT trains_id ... WHERE id > $1` here would return every
/// train touched since the watermark -- almost all with zero subscribers,
/// each then costing >= 2 more sequential round trips in
/// `candidates_for_trains_id` for nothing. The `JOIN` below pushes the
/// "does this train have a subscriber at all" filter into this one query,
/// so a trains_id with zero rows in `train_subscriptions` never reaches
/// `candidates_for_trains_id` in the first place -- a pure query-efficiency
/// change, the per-subscriber cooldown/escalation join in
/// `candidates_for_trains_id` below is untouched.
pub async fn poll_train_candidates(
    pool: &PgPool,
    since_id: i64,
    delay_threshold_minutes: i32,
) -> anyhow::Result<(Vec<TrainCandidate>, i64)> {
    // The watermark must advance past EVERY event since $1 (subscribed or
    // not), or unsubscribed-train events would be re-scanned by the
    // `touched` query below forever -- so this is deliberately computed
    // over the whole table, NOT joined/filtered by subscriber the way
    // `touched` is. `None` (no rows at all past $1) means don't move the
    // cursor.
    let max_id: Option<i64> =
        sqlx::query_scalar("SELECT MAX(id) FROM train_movement_events WHERE id > $1")
            .bind(since_id)
            .fetch_one(pool)
            .await?;
    let Some(max_id) = max_id else {
        return Ok((Vec::new(), since_id));
    };

    let touched: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT tme.trains_id FROM train_movement_events tme \
         JOIN train_subscriptions ts ON ts.trains_id = tme.trains_id \
         WHERE tme.id > $1 AND tme.trains_id IS NOT NULL",
    )
    .bind(since_id)
    .fetch_all(pool)
    .await?;

    let mut candidates = Vec::new();
    for trains_id in touched {
        candidates
            .extend(candidates_for_trains_id(pool, trains_id, delay_threshold_minutes).await?);
    }
    Ok((candidates, max_id))
}

/// The forward queue's own watermark poll -- same shape as
/// `poll_train_candidates`'s own `train_movement_events` watermark, over
/// `notifier_forward_queue` instead. Advanced via its own, separate
/// `notifier_cursor` row (name `"notifier_forward_queue"`), independent of
/// the `"train_movement_events"` cursor `poll_train_candidates` advances.
pub async fn poll_forward_queue(pool: &PgPool, since_id: i64) -> anyhow::Result<(Vec<i64>, i64)> {
    let touched: Vec<i64> =
        sqlx::query_scalar("SELECT DISTINCT trains_id FROM notifier_forward_queue WHERE id > $1")
            .bind(since_id)
            .fetch_all(pool)
            .await?;
    if touched.is_empty() {
        return Ok((Vec::new(), since_id));
    }
    let max_id: i64 =
        sqlx::query_scalar("SELECT MAX(id) FROM notifier_forward_queue WHERE id > $1")
            .bind(since_id)
            .fetch_one(pool)
            .await?;
    Ok((touched, max_id))
}

pub async fn pinned_users_for_line(pool: &PgPool, line_id: &str) -> anyhow::Result<Vec<String>> {
    let rows =
        sqlx::query_scalar::<_, String>("SELECT user_id FROM pinned_lines WHERE line_id = $1")
            .bind(line_id)
            .fetch_all(pool)
            .await?;
    Ok(rows)
}

#[derive(Debug, sqlx::FromRow)]
struct LineNotificationStateRow {
    last_notified_severity_rank: i16,
    last_notified_at: DateTime<Utc>,
}

pub async fn line_notification_state(
    pool: &PgPool,
    user_id: &str,
    line_id: &str,
) -> anyhow::Result<Option<(u8, DateTime<Utc>)>> {
    let row = sqlx::query_as::<_, LineNotificationStateRow>(
        "SELECT last_notified_severity_rank, last_notified_at FROM line_notification_state \
         WHERE user_id = $1 AND line_id = $2",
    )
    .bind(user_id)
    .bind(line_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| (row.last_notified_severity_rank as u8, row.last_notified_at)))
}

pub async fn upsert_line_notification_state(
    pool: &PgPool,
    user_id: &str,
    line_id: &str,
    rank: u8,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO line_notification_state (user_id, line_id, last_notified_severity_rank, last_notified_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (user_id, line_id) DO UPDATE SET \
           last_notified_severity_rank = EXCLUDED.last_notified_severity_rank, last_notified_at = EXCLUDED.last_notified_at",
    )
    .bind(user_id)
    .bind(line_id)
    .bind(rank as i16)
    .bind(at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_train_notification_state(
    pool: &PgPool,
    user_id: &str,
    tracked_train_id: i64,
    status: &str,
    delay_minutes: Option<i32>,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO train_notification_state \
           (user_id, tracked_train_id, last_notified_status, last_notified_delay_minutes, last_notified_at) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (user_id, tracked_train_id) DO UPDATE SET \
           last_notified_status = EXCLUDED.last_notified_status, \
           last_notified_delay_minutes = EXCLUDED.last_notified_delay_minutes, \
           last_notified_at = EXCLUDED.last_notified_at",
    )
    .bind(user_id)
    .bind(tracked_train_id)
    .bind(status)
    .bind(delay_minutes)
    .bind(at)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
pub struct JourneyLegContext {
    pub journey_id: i64,
    pub journey_name: Option<String>,
    pub leg_order: i32,
    pub total_legs: i64,
    pub origin_crs: Option<String>,
    pub destination_crs: Option<String>,
}

/// Finds the journey/leg context for a `train_subscriptions.id`, if one
/// exists -- absent for every legacy tracked train until Phase 1's own
/// migration wraps it in a one-row journey (§7.1 of the design spec), and
/// for any train tracked outside the journeys flow, if that path stays
/// open at all. `notify_train_candidates` falls back to today's exact
/// copy/URL when this returns `None`, OR when it returns `Some` with
/// `total_legs == 1` (Judgment Call 5 -- a one-leg journey's notification
/// copy is indistinguishable in value from today's plain tracked-train
/// copy, so this plan doesn't change it).
///
/// `ORDER BY jl.id LIMIT 1`: a known, accepted edge case -- if the SAME
/// physical train (`trains_id`) is tracked via two different legs (legal:
/// `create_subscription_for_train` is idempotent by `(user_id, trains_id)`,
/// so a second leg pointing at the same trains_id reuses the same
/// `train_subscriptions` row -- design spec §0.1), this query returns only
/// the first-created leg's context, so the payload describes only one of
/// the two legs even though both legs' owners (if different users) are
/// notified via the existing per-trains_id fan-out. Rare, and no worse
/// than the ambiguity already inherent in "one physical train, several
/// subscribers" today.
pub async fn journey_leg_for_train_subscription(
    pool: &PgPool,
    tracked_train_id: i64,
) -> anyhow::Result<Option<JourneyLegContext>> {
    let row = sqlx::query_as::<_, JourneyLegContext>(
        "SELECT j.id AS journey_id, j.custom_name AS journey_name, jl.leg_order, \
                (SELECT COUNT(*) FROM journey_legs jl2 WHERE jl2.journey_id = jl.journey_id) AS total_legs, \
                jl.origin_crs, jl.destination_crs \
         FROM journey_legs jl \
         JOIN journeys j ON j.id = jl.journey_id \
         WHERE jl.train_subscription_id = $1 \
         ORDER BY jl.id LIMIT 1",
    )
    .bind(tracked_train_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

#[derive(Debug, sqlx::FromRow)]
pub struct PushSubscriptionRow {
    pub id: i64,
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
}

pub async fn push_subscriptions_for_user(
    pool: &PgPool,
    user_id: &str,
) -> anyhow::Result<Vec<PushSubscriptionRow>> {
    let rows = sqlx::query_as::<_, PushSubscriptionRow>(
        "SELECT id, endpoint, p256dh, auth FROM push_subscriptions WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Error handling: called on a 404/410 from the push service (Task 6) --
/// self-healing cleanup, mirroring users.rs's own "every write takes out
/// its own trash" posture cited by the spec.
pub async fn delete_push_subscription(pool: &PgPool, id: i64) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM push_subscriptions WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// This crate's own copy of `crates/api/src/data/queries.rs`'s
/// `latest_station_sample` -- necessarily duplicated, not imported, since
/// `crates/notifier` does not (and per this plan's Global Constraints,
/// must not) depend on `crates/api`. Same table (`station_samples`,
/// wholesale-replaced per poll, one row per station, no history), same
/// "None means no sample for this CRS yet" contract.
pub async fn station_sample_for_crs(
    pool: &PgPool,
    crs: &str,
) -> anyhow::Result<Option<common::StationSample>> {
    let row = sqlx::query("SELECT crs, polled_at, departures FROM station_samples WHERE crs = $1")
        .bind(crs)
        .fetch_optional(pool)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let departures_json: serde_json::Value = row.try_get("departures")?;
    Ok(Some(common::StationSample {
        crs: row.try_get("crs")?,
        polled_at: row.try_get("polled_at")?,
        departures: serde_json::from_value(departures_json)?,
    }))
}

pub struct CommittedLeg {
    pub journey_leg_id: i64,
    pub journey_id: i64,
    pub user_id: String,
    pub origin_crs: String,
    pub destination_crs: String,
    pub trains_id: i64,
    pub pin_destination_crs: Option<String>,
    pub next_calling_point: Option<String>,
    pub train_origin_crs: Option<String>,
}

/// Every leg worth station-skip-checking today: bound to a real train
/// (`train_subscription_id IS NOT NULL`), resolved to a shared `trains`
/// row (`ts.trains_id IS NOT NULL` -- an unresolved pin has no departure
/// board to check against), with a known own origin/destination (§7.1's
/// nullability correction), on `today` (Judgment Call 3 -- bounds this
/// full poll to journeys actually happening today, since `station_samples`
/// is a current-snapshot table with no watermark to diff against). One row
/// per leg, already carrying everything `skip_check::leg_is_skipped`
/// (Task 9) needs -- no further per-leg query required.
pub async fn list_committed_legs_for_today(
    pool: &PgPool,
    today: chrono::NaiveDate,
) -> anyhow::Result<Vec<CommittedLeg>> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT jl.id AS journey_leg_id, jl.journey_id, j.user_id, \
                jl.origin_crs, jl.destination_crs, ts.trains_id, \
                ts.pin_destination_crs, cs.next_calling_point, tr.origin_crs AS train_origin_crs \
         FROM journey_legs jl \
         JOIN journeys j ON j.id = jl.journey_id \
         JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id \
         LEFT JOIN train_current_state cs ON cs.trains_id = ts.trains_id \
         LEFT JOIN trains tr ON tr.id = ts.trains_id \
         WHERE jl.train_subscription_id IS NOT NULL \
           AND ts.trains_id IS NOT NULL \
           AND jl.service_date = $1 \
           AND jl.origin_crs IS NOT NULL \
           AND jl.destination_crs IS NOT NULL",
    )
    .bind(today)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(CommittedLeg {
                journey_leg_id: row.try_get("journey_leg_id")?,
                journey_id: row.try_get("journey_id")?,
                user_id: row.try_get("user_id")?,
                origin_crs: row.try_get("origin_crs")?,
                destination_crs: row.try_get("destination_crs")?,
                trains_id: row.try_get("trains_id")?,
                pin_destination_crs: row.try_get("pin_destination_crs")?,
                next_calling_point: row.try_get("next_calling_point")?,
                train_origin_crs: row.try_get("train_origin_crs")?,
            })
        })
        .collect()
}

pub async fn skip_notification_state(
    pool: &PgPool,
    user_id: &str,
    journey_leg_id: i64,
) -> anyhow::Result<Option<bool>> {
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT last_notified_skipped FROM journey_leg_notification_state \
         WHERE user_id = $1 AND journey_leg_id = $2",
    )
    .bind(user_id)
    .bind(journey_leg_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(skipped,)| skipped))
}

pub async fn upsert_skip_notification_state(
    pool: &PgPool,
    user_id: &str,
    journey_leg_id: i64,
    skipped: bool,
    at: chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO journey_leg_notification_state \
           (user_id, journey_leg_id, last_notified_skipped, last_notified_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (user_id, journey_leg_id) DO UPDATE SET \
           last_notified_skipped = EXCLUDED.last_notified_skipped, \
           last_notified_at = EXCLUDED.last_notified_at",
    )
    .bind(user_id)
    .bind(journey_leg_id)
    .bind(skipped)
    .bind(at)
    .execute(pool)
    .await?;
    Ok(())
}

/// One due template's `journeys`/`journey_legs` row(s), minted idempotently.
/// Deliberately duplicates the shape of Phase B's own
/// `crates/api/src/data/journey_templates.rs::materialize_template` --
/// per `crates/notifier`'s hard "never depend on crates/api" constraint,
/// this crate cannot import that function and must keep its own copy of
/// the same INSERT shapes instead. This function's SQL has since been
/// diffed against Phase B's real `materialize_template` (once by the
/// controller running this branch's SDD process, again independently by
/// the final-review reviewer): no divergence was found in the INSERT
/// column shapes.
///
/// One deliberate difference IS worth stating explicitly rather than
/// leaving implicit: the real `materialize_template` has NO idempotency
/// guard at all -- it mints unconditionally every time it's called,
/// because a human pressing "Run now" twice for the same date is a
/// legitimate, supported case there. THIS function's own `WHERE NOT
/// EXISTS` guard below is a deliberate, necessary addition on top of that
/// shared shape -- the sweep calling this on an hourly, unattended timer
/// needs the guard that the manual, attended "Run now" route intentionally
/// does not have.
///
/// Every leg is minted `'unmatched'` regardless of the template's
/// `default_match_mode` -- `'auto'`-vs-`'manual'` behavior is entirely this
/// crate's stage-2 commit-check's job (see `unmatched_auto_legs_for_commit_check`
/// below), never decided at mint time (spec §3.2's own 2026-09-22 addendum:
/// this is the whole point of the two-stage split).
///
/// Idempotency: the INSERT's own `WHERE NOT EXISTS` guard, single
/// statement, same idiom as `find_or_create_train`'s `ON CONFLICT DO UPDATE
/// ... RETURNING` and `create_subscription_for_train`'s CTE -- safe under
/// this crate's normal single-process sequential-tick execution; a true
/// concurrent double-mint is the same accepted, documented residual race
/// `create_subscription_for_train`'s own doc comment already names for this
/// codebase ("closes the ordinary repeat case, not a true concurrent
/// double-submit").
///
/// Returns `Ok(None)` if this template already has an occurrence for
/// `today` (no-op, not an error), if the user explicitly DISCARDED today's
/// occurrence (see the tombstone guard below), or if the template has zero
/// legs (should be unreachable given Phase B's own validation, but
/// defensively a no-op rather than a partially-minted journey).
///
/// The tombstone guard (`journey_template_skipped_dates`, migration
/// `20260925214500`) closes a real, loud bug: the "already materialized"
/// half of this guard is satisfied only while the minted `journeys` row
/// still EXISTS, so a user deleting today's auto-minted occurrence (they
/// aren't travelling today) had it re-minted by the very next sweep tick --
/// within the hour -- complete with a fresh auto-commit and fresh pushes
/// for a journey they had explicitly discarded, with no way to make it stop
/// other than pausing the whole template. `api`'s own delete paths
/// (`data::journeys::delete_journey`/`delete_leg`) now record a
/// `(template_id, service_date)` tombstone as they delete, and an explicit
/// re-materialize of the same date clears it again
/// (`data::journey_templates::materialize_template`).
///
/// Called from `main.rs`'s `run_template_sweep_cycle` (Task 5, stage 1) --
/// also exercised directly by this module's own `sweep_tests`.
pub async fn materialize_due_template_occurrence(
    pool: &PgPool,
    template_id: i64,
    user_id: &str,
    custom_name: Option<&str>,
    today: chrono::NaiveDate,
) -> anyhow::Result<Option<i64>> {
    #[allow(clippy::type_complexity)]
    let legs: Vec<(
        i32,
        Option<String>,
        Option<String>,
        Option<chrono::NaiveTime>,
        Option<chrono::NaiveTime>,
        Option<chrono::NaiveTime>,
        Option<chrono::NaiveTime>,
    )> = sqlx::query_as(
        "SELECT leg_order, origin_crs, destination_crs, depart_after, depart_before, \
                arrive_after, arrive_before \
         FROM journey_template_legs WHERE template_id = $1 ORDER BY leg_order",
    )
    .bind(template_id)
    .fetch_all(pool)
    .await?;
    if legs.is_empty() {
        return Ok(None);
    }

    let mut tx = pool.begin().await?;
    let journey_id: Option<i64> = sqlx::query_scalar(
        "INSERT INTO journeys (user_id, custom_name, source_template_id) \
         SELECT $1, $2, $3 \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM journeys j JOIN journey_legs jl ON jl.journey_id = j.id \
             WHERE j.source_template_id = $3 AND jl.service_date = $4 \
         ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM journey_template_skipped_dates s \
             WHERE s.template_id = $3 AND s.service_date = $4 \
         ) \
         RETURNING id",
    )
    .bind(user_id)
    .bind(custom_name)
    .bind(template_id)
    .bind(today)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(journey_id) = journey_id else {
        tx.rollback().await?;
        return Ok(None); // already materialized today -- idempotent no-op
    };

    for (
        leg_order,
        origin_crs,
        destination_crs,
        depart_after,
        depart_before,
        arrive_after,
        arrive_before,
    ) in legs
    {
        sqlx::query(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, \
                 depart_after, depart_before, arrive_after, arrive_before, match_mode) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'unmatched')",
        )
        .bind(journey_id)
        .bind(leg_order)
        .bind(origin_crs)
        .bind(destination_crs)
        .bind(today)
        .bind(depart_after)
        .bind(depart_before)
        .bind(arrive_after)
        .bind(arrive_before)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(Some(journey_id))
}

/// Every active template due to materialize `today` -- active, today's
/// weekday bit set, today within [starts_on, ends_on]. Does NOT itself
/// check the idempotency guard (that's `materialize_due_template_occurrence`'s
/// own job, per-template) -- this just narrows the sweep's per-tick
/// candidate set. `days_of_week IS NULL` (a one-shot, non-recurring
/// template, spec §2.2) is correctly excluded by the AND below (NULL &
/// anything is NULL, never non-zero).
///
/// This struct and `due_templates_for` below are called from `main.rs`'s
/// `run_template_sweep_cycle` (Task 5, stage 1) -- also exercised directly
/// by this module's own `sweep_tests`.
#[derive(Debug, sqlx::FromRow)]
pub struct DueTemplate {
    pub id: i64,
    pub user_id: String,
    pub custom_name: Option<String>,
}

pub async fn due_templates_for(
    pool: &PgPool,
    today: chrono::NaiveDate,
) -> anyhow::Result<Vec<DueTemplate>> {
    let rows = sqlx::query_as::<_, DueTemplate>(
        "SELECT id, user_id, custom_name FROM journey_templates \
         WHERE active \
           AND days_of_week IS NOT NULL \
           AND (days_of_week & (1 << (EXTRACT(ISODOW FROM $1::date)::int - 1))) != 0 \
           AND $1::date >= COALESCE(starts_on, $1::date) \
           AND $1::date <= COALESCE(ends_on, $1::date)",
    )
    .bind(today)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// A leg eligible for this cycle's commit-check -- `'unmatched'`,
/// `service_date = today`, belonging to a journey whose source template
/// has `default_match_mode = 'auto'` AND is still `active` -- a paused
/// template (`active = false`) must not have its already-minted legs
/// auto-committed just because the user paused it after today's occurrence
/// was already stamped; see this module's own
/// `unmatched_auto_legs_for_commit_check_excludes_paused_templates` test.
/// Does NOT itself apply the `auto_commit_lead_minutes` lead-time gate
/// (see `decision::is_due_for_commit_check`, applied per-row by the caller
/// in Task 5) -- keeping that check in Rust, not SQL, keeps it
/// unit-testable in isolation (Task 1) without a DB.
///
/// Deliberately does NOT require `depart_after`/`arrive_after` to be set.
/// A template leg is allowed to carry no time window at all (see
/// `api::data::journey_templates::validate_template_leg`'s own doc
/// comment: "a template leg is never itself 'matched,' so the ambiguity
/// [requiring a window bound] exists to prevent for an ordinary journey
/// leg doesn't apply here"), and a materialized leg copies that verbatim
/// (`materialize_template`/`materialize_due_template_occurrence`) -- so a
/// fully-open-window `journey_legs` row is a real, reachable state this
/// query must not silently exclude. An earlier version of this query
/// required `depart_after IS NOT NULL OR arrive_after IS NOT NULL`, which
/// meant an `'auto'`-mode leg with no window at all was NEVER returned
/// here and therefore never auto-committed by the sweep -- see this
/// module's own `unmatched_auto_legs_for_commit_check_includes_a_fully_open_window_leg`
/// regression test, and `main.rs`'s own commit-check loop for how the
/// caller now treats a leg with neither bound set (midnight, not a skip).
///
/// This struct and `unmatched_auto_legs_for_commit_check` below are called
/// from `main.rs`'s `run_template_sweep_cycle` (Task 5, stage 2) -- also
/// exercised directly by this module's own `sweep_tests`.
#[derive(Debug, sqlx::FromRow)]
pub struct CommitCheckLeg {
    pub journey_leg_id: i64,
    pub journey_id: i64,
    pub user_id: String,
    pub origin_crs: String,
    pub destination_crs: String,
    pub service_date: chrono::NaiveDate,
    pub depart_after: Option<chrono::NaiveTime>,
    pub depart_before: Option<chrono::NaiveTime>,
    pub arrive_after: Option<chrono::NaiveTime>,
    pub arrive_before: Option<chrono::NaiveTime>,
}

pub async fn unmatched_auto_legs_for_commit_check(
    pool: &PgPool,
    today: chrono::NaiveDate,
) -> anyhow::Result<Vec<CommitCheckLeg>> {
    let rows = sqlx::query_as::<_, CommitCheckLeg>(
        "SELECT jl.id AS journey_leg_id, jl.journey_id, j.user_id, \
                jl.origin_crs, jl.destination_crs, jl.service_date, \
                jl.depart_after, jl.depart_before, jl.arrive_after, jl.arrive_before \
         FROM journey_legs jl \
         JOIN journeys j ON j.id = jl.journey_id \
         JOIN journey_templates jt ON jt.id = j.source_template_id \
         WHERE jl.match_mode = 'unmatched' \
           AND jl.service_date = $1 \
           AND jt.default_match_mode = 'auto' \
           AND jt.active \
           AND jl.origin_crs IS NOT NULL \
           AND jl.destination_crs IS NOT NULL",
    )
    .bind(today)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// One `(train_uid, day_offset, scheduled departure at the leg's own
/// origin)` candidate -- `day_offset` is `main.day_offset`, the SAME
/// column/meaning `schedule_query::resolve`'s `assign_day_offsets` and
/// `schedule-reference`'s `schedule_network_departures_rows` already sort
/// by: how many calendar days past `service_date` this departure at the
/// leg's origin really falls on (0 for an ordinary same-day departure, 1
/// or more for a schedule whose calling points have already regressed
/// past midnight by the time they reach this leg's origin stop). Surfaced
/// here, rather than dropped the way this function used to, so
/// `decision::pick_nearest_to_now_candidate` can compare candidates
/// day-offset-aware instead of by bare clock time alone -- see that
/// function's own doc comment for the bug this fixes.
///
/// A deliberately slimmed sibling of
/// `crates/api::data::queries::search_journey_leg_candidates` -- drops
/// that function's cursor pagination and leg-destination-arrival
/// subqueries (this stage only needs enough to pick a train, never
/// renders a candidate to a human), keeps its WHERE-clause reachability
/// logic (a candidate's route must actually call at `destination_crs`
/// after `origin_crs`) and window-bound logic verbatim, duplicated per
/// this crate's established crate-boundary constraint. `LIMIT 100` is a
/// safety cap, not true pagination -- this crate never needs a second
/// page.
///
/// `#[allow(clippy::too_many_arguments)]`: same eight-argument shape as
/// the real `search_journey_leg_candidates` this duplicates (that
/// function carries the same allow, plus `clippy::type_complexity` for
/// its wider return type, which this slimmed version doesn't need).
/// Called from `main.rs`'s `run_template_sweep_cycle` (Task 5, stage 2) --
/// also exercised directly by this module's own `sweep_tests`.
#[allow(clippy::too_many_arguments)]
pub async fn schedule_candidates_for_leg(
    pool: &PgPool,
    origin_crs: &str,
    destination_crs: &str,
    service_date: chrono::NaiveDate,
    depart_after: Option<chrono::NaiveTime>,
    depart_before: Option<chrono::NaiveTime>,
    arrive_after: Option<chrono::NaiveTime>,
    arrive_before: Option<chrono::NaiveTime>,
) -> anyhow::Result<Vec<(String, u8, chrono::NaiveTime)>> {
    let rows: Vec<(String, i16, chrono::NaiveTime)> = sqlx::query_as(
        "SELECT main.train_uid, main.day_offset, main.scheduled \
         FROM schedule_destination_departures main \
         WHERE main.service_date = $1 \
           AND main.origin_crs = $2 \
           AND ($3::time IS NULL OR main.scheduled >= $3) \
           AND ($4::time IS NULL OR main.scheduled <= $4) \
           AND ( \
                 main.destination_crs = $5 \
                 OR EXISTS ( \
                     SELECT 1 FROM schedule_destination_departures stop \
                     WHERE stop.service_date = $1 AND stop.train_uid = main.train_uid \
                       AND stop.origin_crs = $5 \
                       AND (stop.day_offset, stop.scheduled) > (main.day_offset, main.scheduled) \
                 ) \
           ) \
           AND ( \
                 ($6::time IS NULL AND $7::time IS NULL) \
                 OR ( \
                     main.destination_crs = $5 \
                     AND ($6::time IS NULL OR main.destination_arrival >= $6) \
                     AND ($7::time IS NULL OR main.destination_arrival <= $7) \
                 ) \
                 OR EXISTS ( \
                     SELECT 1 FROM schedule_destination_departures stop \
                     WHERE stop.service_date = $1 AND stop.train_uid = main.train_uid \
                       AND stop.origin_crs = $5 \
                       AND (stop.day_offset, stop.scheduled) > (main.day_offset, main.scheduled) \
                       AND ($6::time IS NULL OR stop.calling_point_arrival >= $6) \
                       AND ($7::time IS NULL OR stop.calling_point_arrival <= $7) \
                 ) \
           ) \
         ORDER BY main.day_offset, main.scheduled, main.train_uid \
         LIMIT 100",
    )
    .bind(service_date)
    .bind(origin_crs)
    .bind(depart_after)
    .bind(depart_before)
    .bind(destination_crs)
    .bind(arrive_after)
    .bind(arrive_before)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        // `schedule_destination_departures.day_offset` is a Postgres
        // `SMALLINT` (`i16`); every other day_offset field in this
        // codebase (`RawCallingPoint::day_offset`,
        // `trip_planning::CallingPointRow::day_offset` at its own call
        // site) is `u8` -- same `unwrap_or(0)` fallback-to-same-day
        // conversion convention as those.
        .map(|(uid, day_offset, scheduled)| (uid, u8::try_from(day_offset).unwrap_or(0), scheduled))
        .collect())
}

/// Duplicates `crates/api::data::trains::find_or_create_train` --
/// necessarily, per this crate's crate-boundary constraint. Keep this in
/// sync with that function's exact ON CONFLICT shape if it ever changes.
///
/// Prefer [`find_or_create_train_with_cif_schedule`] below for the
/// auto-commit path: a BARE row created here has no
/// `origin_crs`/`destination_crs`/`scheduled_departure` at all, which
/// silently disables station-skip detection for the leg committed to it
/// (see that function's own doc comment). This plain version is kept
/// because the enriching one falls back to it whenever CIF has nothing to
/// enrich WITH, and because it is the exact shape being duplicated from
/// `crates/api`.
pub async fn find_or_create_train<'e, E>(
    executor: E,
    train_uid: &str,
    service_date: chrono::NaiveDate,
) -> anyhow::Result<i64>
where
    E: sqlx::PgExecutor<'e>,
{
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO trains (train_uid, service_date) VALUES ($1, $2) \
         ON CONFLICT (train_uid, service_date) DO UPDATE SET train_uid = EXCLUDED.train_uid \
         RETURNING id",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_one(executor)
    .await?;
    Ok(row.0)
}

/// The schedule shape CIF already publishes for a train this crate is about
/// to auto-commit a leg to: its own true origin departure, and its own
/// final destination. Both come straight out of
/// `schedule_destination_departures` -- the SAME table the auto-commit
/// candidate itself came from, so if a candidate existed at all, this data
/// exists too.
struct CifTrainSchedule {
    true_origin_crs: String,
    scheduled: chrono::NaiveTime,
    /// The train's own TERMINUS, not the leg's destination -- `None` only if
    /// no row for this train carries a resolvable arrival at all.
    destination_crs: Option<String>,
}

/// Reads `(true origin, its booked departure, terminus)` for one
/// `(train_uid, service_date)` out of CIF.
///
/// `origin_crs = true_origin_crs` is the same predicate
/// `crates/api::data::reconciliation::true_origin_departure` uses to pick a
/// schedule's OWN origin row out of the several rows this flattened table
/// holds per train (one per departure-bearing calling point x destination
/// pair). The terminus is the row with the latest resolvable arrival --
/// `(destination_arrival_day_offset, destination_arrival)` DESC, day-offset
/// leading so an overnight schedule's post-midnight terminus does not sort
/// as the earliest stop (the same `(day_offset, time)` ordering convention
/// `schedule_candidates_for_leg` above and `schedule-reference`'s own
/// publisher already use).
async fn cif_train_schedule(
    pool: &PgPool,
    train_uid: &str,
    service_date: chrono::NaiveDate,
) -> anyhow::Result<Option<CifTrainSchedule>> {
    let origin: Option<(String, chrono::NaiveTime)> = sqlx::query_as(
        "SELECT origin_crs, scheduled FROM schedule_destination_departures \
         WHERE train_uid = $1 AND service_date = $2 AND origin_crs = true_origin_crs \
         ORDER BY day_offset, scheduled LIMIT 1",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    let Some((true_origin_crs, scheduled)) = origin else {
        return Ok(None);
    };

    let destination_crs: Option<String> = sqlx::query_scalar(
        "SELECT destination_crs FROM schedule_destination_departures \
         WHERE train_uid = $1 AND service_date = $2 AND destination_arrival IS NOT NULL \
         ORDER BY destination_arrival_day_offset DESC, destination_arrival DESC LIMIT 1",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;

    Ok(Some(CifTrainSchedule {
        true_origin_crs,
        scheduled,
        destination_crs,
    }))
}

/// [`find_or_create_train`], but also mirrors CIF's own already-published
/// schedule for this train onto the shared `trains` row -- the auto-commit
/// path's counterpart to the enrichment the MANUAL "pick a train" route
/// runs (`api::routes::journeys::post_leg_train` ->
/// `routes::train::enrich_shared_train`).
///
/// The bug this closes: the sweep's auto-commit called the BARE
/// `find_or_create_train`, so an auto-committed leg's shared row kept
/// `origin_crs`/`destination_crs`/`scheduled_departure` NULL. That is not
/// cosmetic -- `create_subscription_for_train` below copies exactly those
/// three columns into the new subscription's `pin_*` columns, and
/// `skip_check::leg_is_skipped` matches a live Darwin departure board by
/// `pin_destination_crs`, so with them NULL station-skip detection for every
/// auto-committed leg could never fire at all. `api`'s own periodic
/// `reconciliation::retry_schedule_enrichment_for_nr_primary_trains` sweep
/// does eventually enrich such a row, but only once the train's origin
/// departure is already `schedule_enrichment_grace_minutes` in the PAST --
/// far too late for skip detection on a leg committed ~2h before departure.
///
/// Deliberately CIF-only, and deliberately not a duplicate of
/// `enrich_shared_train`:
///
/// * The identity is already KNOWN here -- `train_uid` came out of
///   `schedule_candidates_for_leg`, i.e. out of CIF itself. The api-side
///   `schedule_matching::find_schedule_match` heuristic exists to DISCOVER
///   an unknown uid from a (CRS, time) pin against
///   `schedule_line_population`; it has nothing to add once the uid is a
///   given, and duplicating it here would also drag the static
///   `lines/*.toml` catalogue into this crate.
/// * `trains.calling_points` is deliberately left alone rather than
///   rebuilt from `schedule_calling_points_full`. The journey timeline
///   already falls back to that table directly when the column is NULL
///   (`api::data::journey::build_journey_stops`, the 2026-09-23 fallback
///   fix), and every schedule column on this row is written with
///   `COALESCE(existing, new)` -- so writing a lossier blob here (that
///   table carries no `is_half_minute_*` flags) would PERMANENTLY block the
///   richer one a real api-side schedule match writes later.
/// * TRUST backlog replay (`enrich_shared_train`'s other half) is likewise
///   left to the api: a leg is auto-committed BEFORE its train has run, so
///   there is no retained history to replay yet, and once it does run
///   `trust-consumer` feeds this shared `trains_id` live anyway.
///
/// Every column is `COALESCE`d against the existing value, exactly like
/// `api::data::trains::find_or_create_train_with_schedule_match` -- so this
/// never clobbers richer data an api-side match already wrote, and is safe
/// to call repeatedly. Falls back to a bare `find_or_create_train` when CIF
/// has nothing published for this train at all (best-effort, never fatal:
/// the auto-commit itself must still happen).
pub async fn find_or_create_train_with_cif_schedule(
    pool: &PgPool,
    train_uid: &str,
    service_date: chrono::NaiveDate,
) -> anyhow::Result<i64> {
    let Some(schedule) = cif_train_schedule(pool, train_uid, service_date).await? else {
        return find_or_create_train(pool, train_uid, service_date).await;
    };
    // A nonexistent local time (the spring-forward gap) means there is no
    // instant to store; the rest of the enrichment is still worth writing.
    let scheduled_departure = crate::london_to_utc(service_date.and_time(schedule.scheduled));

    let row: (i64,) = sqlx::query_as(
        "INSERT INTO trains \
            (train_uid, service_date, origin_crs, scheduled_departure, destination_crs, \
             schedule_matched_at) \
         VALUES ($1, $2, $3, $4, $5, NOW()) \
         ON CONFLICT (train_uid, service_date) DO UPDATE SET \
            train_uid           = EXCLUDED.train_uid, \
            origin_crs          = COALESCE(trains.origin_crs, EXCLUDED.origin_crs), \
            scheduled_departure = COALESCE(trains.scheduled_departure, EXCLUDED.scheduled_departure), \
            destination_crs     = COALESCE(trains.destination_crs, EXCLUDED.destination_crs), \
            schedule_matched_at = COALESCE(trains.schedule_matched_at, EXCLUDED.schedule_matched_at) \
         RETURNING id",
    )
    .bind(train_uid)
    .bind(service_date)
    .bind(&schedule.true_origin_crs)
    .bind(scheduled_departure)
    .bind(schedule.destination_crs.as_deref())
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Duplicates `crates/api::data::train_tracking::create_subscription_for_train`
/// -- same CTE idempotency idiom, same accepted "ordinary repeat case
/// only" concurrency caveat as the original's own doc comment states.
///
/// Generic over the executor (not `&PgPool`) so
/// [`auto_commit_leg_to_train`] can run it inside its own transaction
/// alongside the commit it must be atomic with.
pub async fn create_subscription_for_train<'e, E>(
    executor: E,
    trains_id: i64,
    user_id: &str,
) -> anyhow::Result<i64>
where
    E: sqlx::PgExecutor<'e>,
{
    let row: (i64,) = sqlx::query_as(
        "WITH existing AS ( \
             SELECT id FROM train_subscriptions \
             WHERE user_id = $1 AND trains_id = $2 ORDER BY id LIMIT 1 \
         ), inserted AS ( \
             INSERT INTO train_subscriptions \
                 (user_id, trains_id, service_date, pin_origin_crs, pin_scheduled_departure, pin_destination_crs) \
             SELECT $1, tr.id, tr.service_date, tr.origin_crs, tr.scheduled_departure, tr.destination_crs \
             FROM trains tr WHERE tr.id = $2 AND NOT EXISTS (SELECT 1 FROM existing) \
             RETURNING id \
         ) \
         SELECT id FROM existing UNION ALL SELECT id FROM inserted",
    )
    .bind(user_id)
    .bind(trains_id)
    .fetch_one(executor)
    .await?;
    Ok(row.0)
}

/// Commits a leg to a train working -- the auto-commit sibling of
/// `crates/api::data::journeys::set_leg_train_subscription`, but sets
/// `match_mode = 'auto'` (never `'manual'`) and is guarded by `AND
/// match_mode = 'unmatched'` so a leg already committed by a concurrent
/// tick (or since raced-and-lost) is a silent no-op, not a double write.
///
/// Generic over the executor for the same reason
/// [`create_subscription_for_train`] is -- [`auto_commit_leg_to_train`]
/// runs both inside ONE transaction. Still called directly (with a plain
/// `&PgPool`) by this module's own `sweep_tests`.
pub async fn commit_leg_to_train<'e, E>(
    executor: E,
    journey_leg_id: i64,
    train_subscription_id: i64,
) -> anyhow::Result<bool>
where
    E: sqlx::PgExecutor<'e>,
{
    let result = sqlx::query(
        "UPDATE journey_legs SET train_subscription_id = $1, match_mode = 'auto' \
         WHERE id = $2 AND match_mode = 'unmatched'",
    )
    .bind(train_subscription_id)
    .bind(journey_leg_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// "Subscribe this user to this train AND commit this leg to that
/// subscription" as ONE atomic step -- `Ok(Some(train_subscription_id))` if
/// the leg really was committed, `Ok(None)` if it had already been
/// committed by someone else and nothing was written at all.
///
/// The bug this closes: the sweep used to call
/// [`create_subscription_for_train`] and then [`commit_leg_to_train`] as two
/// independent statements. On the no-op path -- the leg was committed
/// between those two calls by a concurrent actor, in practice the user
/// themselves picking a train by hand via `POST
/// /Journeys/{j}/legs/{l}/train` -- the subscription STAYED, orphaned: no
/// journey leg referenced it, nothing in the UI's journey view explained it,
/// and `candidates_for_trains_id`'s per-subscriber fan-out kept generating
/// delay/cancellation pushes off it for a train the user never chose to
/// track. The same applied to any error raised after the subscription
/// insert.
///
/// Rolling back is specifically safe for BOTH shapes
/// `create_subscription_for_train`'s CTE can take: if a subscription for
/// `(user_id, trains_id)` already existed, that CTE's `existing` branch only
/// SELECTs it (no write to undo), so the rollback cannot destroy a
/// subscription this sweep did not create -- it only ever discards its own
/// brand-new INSERT.
pub async fn auto_commit_leg_to_train(
    pool: &PgPool,
    journey_leg_id: i64,
    trains_id: i64,
    user_id: &str,
) -> anyhow::Result<Option<i64>> {
    let mut tx = pool.begin().await?;
    let train_subscription_id = create_subscription_for_train(&mut *tx, trains_id, user_id).await?;
    let committed = commit_leg_to_train(&mut *tx, journey_leg_id, train_subscription_id).await?;
    if !committed {
        tx.rollback().await?;
        return Ok(None);
    }
    tx.commit().await?;
    Ok(Some(train_subscription_id))
}

/// Whether CIF has published ANY schedule row at all for `service_date` --
/// this crate's own copy of `api::data::queries`'s
/// `schedule_destination_departures_published_for` (necessarily duplicated,
/// per this crate's crate-boundary constraint; that one is private to its
/// own module anyway). One indexed lookup: `service_date` leads
/// `schedule_destination_departures`' primary key.
///
/// Used by the commit-check's zero-candidate branch to tell "we genuinely
/// have today's timetable and nothing in it matches this leg" apart from "we
/// do not have today's timetable yet" -- the exact same 404-versus-`200 []`
/// distinction the api draws on the read side, applied here to a
/// notification that fires at most once per leg and can therefore not be
/// taken back.
pub async fn schedule_published_for(
    pool: &PgPool,
    service_date: chrono::NaiveDate,
) -> anyhow::Result<bool> {
    let probe: Option<(i32,)> = sqlx::query_as(
        "SELECT 1 FROM schedule_destination_departures WHERE service_date = $1 LIMIT 1",
    )
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    Ok(probe.is_some())
}

/// Called from `main.rs`'s `run_template_sweep_cycle` (Task 5, stage 2) --
/// also exercised directly by this module's own `sweep_tests`.
pub async fn unmatched_notification_state(
    pool: &PgPool,
    user_id: &str,
    journey_leg_id: i64,
) -> anyhow::Result<Option<bool>> {
    let row: Option<(Option<bool>,)> = sqlx::query_as(
        "SELECT last_notified_unmatched FROM journey_leg_notification_state \
         WHERE user_id = $1 AND journey_leg_id = $2",
    )
    .bind(user_id)
    .bind(journey_leg_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(v,)| v))
}

/// Only touches the two unmatched-specific columns on conflict -- never
/// clobbers `last_notified_skipped`/`last_notified_at` if the skip-check
/// cycle (Phase 3) already has a row here. On a genuine first INSERT for
/// this `(user_id, journey_leg_id)`, supplies `last_notified_skipped =
/// FALSE` -- not a placeholder but the literally correct value: a still-
/// unmatched leg has no bound train to be "skipped" against yet.
///
/// Called from `main.rs`'s `run_template_sweep_cycle` (Task 5, stage 2) --
/// also exercised directly by this module's own `sweep_tests`.
pub async fn upsert_unmatched_notification_state(
    pool: &PgPool,
    user_id: &str,
    journey_leg_id: i64,
    at: chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO journey_leg_notification_state \
            (user_id, journey_leg_id, last_notified_skipped, last_notified_at, \
             last_notified_unmatched, last_notified_unmatched_at) \
         VALUES ($1, $2, FALSE, $3, TRUE, $3) \
         ON CONFLICT (user_id, journey_leg_id) DO UPDATE SET \
           last_notified_unmatched = EXCLUDED.last_notified_unmatched, \
           last_notified_unmatched_at = EXCLUDED.last_notified_unmatched_at",
    )
    .bind(user_id)
    .bind(journey_leg_id)
    .bind(at)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    async fn seed_user(pool: &PgPool, user_id: &str) {
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind(format!("{user_id}@example.com"))
        .bind(user_id)
        .execute(pool)
        .await
        .expect("seed fixture user");
    }

    async fn cleanup_line_history(pool: &PgPool, line_id: &str) {
        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1")
            .bind(line_id)
            .execute(pool)
            .await
            .expect("cleanup history");
        sqlx::query("DELETE FROM notifier_cursor WHERE name = 'line_status_history'")
            .execute(pool)
            .await
            .expect("cleanup cursor");
    }

    /// Real `common::LineStatus` values, serialized the same way
    /// `write_line_status`/`upsert_tfl_line_status` actually write this
    /// column (`serde_json::to_value(&Vec<LineStatus>)`) -- hand-rolled
    /// JSON here would silently disagree with `Severity`'s
    /// `Serialize_repr`/`DataQuality`'s kebab-case tagging and mask a real
    /// round-trip bug.
    fn status_json(severity: common::Severity) -> serde_json::Value {
        let status = common::LineStatus {
            severity,
            reason: String::new(),
            validity: common::ValidityPeriod {
                from_date: chrono::Utc::now(),
                to_date: None,
                is_now: true,
            },
            disruption: None,
            data_quality: common::DataQuality::default(),
            sample_stats: None,
            sample_availability: common::SampleAvailability::NoCoverage,
            full_coverage_stats: None,
            full_coverage_availability: common::FullCoverageAvailability::NotEnabled,
        };
        serde_json::to_value(vec![status]).expect("serialize fixture LineStatus")
    }

    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p notifier \
                a_second_poll_over_an_unchanged_table_finds_no_new_candidates -- --ignored`"]
    async fn a_second_poll_over_an_unchanged_table_finds_no_new_candidates() {
        let pool = connect().await;
        let line_id = "TEST-NOTIFIER-CURSOR-LINE";
        cleanup_line_history(&pool, line_id).await;

        let good = status_json(common::Severity::GoodService);
        let severe = status_json(common::Severity::SevereDelays);
        sqlx::query("INSERT INTO line_status_history (line_id, statuses, computed_at) VALUES ($1, $2, NOW())")
            .bind(line_id)
            .bind(&good)
            .execute(&pool)
            .await
            .expect("seed first history row");
        sqlx::query("INSERT INTO line_status_history (line_id, statuses, computed_at) VALUES ($1, $2, NOW())")
            .bind(line_id)
            .bind(&severe)
            .execute(&pool)
            .await
            .expect("seed second (transitioned) history row");

        let cursor_name = "line_status_history";
        let cursor = read_cursor(&pool, cursor_name).await.expect("read cursor");
        let (first_pass, observed_max_id) = poll_line_candidates(&pool, cursor.last_processed_id)
            .await
            .expect("first poll");
        let candidate = first_pass
            .iter()
            .find(|c| c.line_id == line_id)
            .expect("the transition must be a candidate");
        assert_eq!(candidate.previous_rank, 0);
        assert!(candidate.new_rank > 0);

        // `Duration::zero()` grace: this test is about the POLL's own
        // since-id behavior, so the watermark is promoted immediately here.
        // The grace window itself is covered by
        // `advance_cursor_with_grace_only_promotes_a_proposal_once_it_has_aged`
        // and its sibling below.
        let advanced = advance_cursor_with_grace(
            &pool,
            cursor_name,
            &cursor,
            observed_max_id,
            Utc::now(),
            chrono::Duration::zero(),
        )
        .await
        .expect("advance");

        let (second_pass, _) = poll_line_candidates(&pool, advanced.max(observed_max_id))
            .await
            .expect("second poll");
        assert!(
            second_pass.iter().all(|c| c.line_id != line_id),
            "an unchanged table must produce zero new candidates for this line on a repeat poll"
        );

        cleanup_line_history(&pool, line_id).await;
    }

    /// Finding 2's regression test: ONE undecodable `statuses` row must not
    /// stall notifications for everyone, for ever.
    ///
    /// Seeds a row whose `statuses` JSONB is not a `Vec<LineStatus>` at all
    /// (the shape a partially-written row, or one written by a different
    /// build of `common::LineStatus`, really takes) on one line, plus a
    /// genuine severity transition on ANOTHER line with a HIGHER id. Before
    /// this fix, `serde_json::from_value(...)?` inside the row loop returned
    /// `Err` out of `poll_line_candidates`, which aborted `run_cycle` before
    /// `advance_cursor` -- so the bad row was never passed, the transition
    /// after it was never seen, and every subsequent cycle failed at the
    /// identical point, for every user, for both the line and the train half
    /// of the cycle.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p notifier \
                one_undecodable_row_is_skipped_and_never_stalls_the_poll -- --ignored \
                --test-threads=1`"]
    async fn one_undecodable_row_is_skipped_and_never_stalls_the_poll() {
        let pool = connect().await;
        let bad_line = "TEST-NOTIFIER-BADROW-LINE";
        let good_line = "TEST-NOTIFIER-GOODROW-LINE";
        cleanup_line_history(&pool, bad_line).await;
        cleanup_line_history(&pool, good_line).await;

        let start = read_cursor(&pool, "line_status_history")
            .await
            .expect("read cursor")
            .last_processed_id;

        let bad_id: i64 = sqlx::query_scalar(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) \
             VALUES ($1, '{\"nope\": \"not a LineStatus array\"}'::jsonb, NOW()) RETURNING id",
        )
        .bind(bad_line)
        .fetch_one(&pool)
        .await
        .expect("seed the undecodable row");

        // A real transition on a DIFFERENT line, after the bad row.
        sqlx::query("INSERT INTO line_status_history (line_id, statuses, computed_at) VALUES ($1, $2, NOW())")
            .bind(good_line)
            .bind(status_json(common::Severity::GoodService))
            .execute(&pool)
            .await
            .expect("seed first good row");
        let good_id: i64 = sqlx::query_scalar(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) \
             VALUES ($1, $2, NOW()) RETURNING id",
        )
        .bind(good_line)
        .bind(status_json(common::Severity::SevereDelays))
        .fetch_one(&pool)
        .await
        .expect("seed the transitioned good row");

        let (candidates, observed_max_id) = poll_line_candidates(&pool, start)
            .await
            .expect("the poll must SUCCEED despite an undecodable row -- this is the whole fix");

        assert!(
            candidates.iter().all(|c| c.line_id != bad_line),
            "the undecodable row must be skipped, not turned into a candidate"
        );
        let good = candidates
            .iter()
            .find(|c| c.line_id == good_line)
            .expect("the real transition AFTER the bad row must still be found");
        assert_eq!(good.id, good_id);
        assert!(good.new_rank > 0);
        assert!(
            observed_max_id >= good_id && observed_max_id > bad_id,
            "the watermark this poll reports must cover every row it SAW (including the skipped \
             one), so the cursor can move past the bad row instead of re-reading it for ever"
        );

        cleanup_line_history(&pool, bad_line).await;
        cleanup_line_history(&pool, good_line).await;
    }

    /// Finding 6's regression test: the cursor must not jump straight to the
    /// maximum id it saw, because ids are allocated at INSERT time and become
    /// visible at COMMIT time -- so a lower id can still be in flight when a
    /// higher one has already been observed. Asserts the two-phase promotion:
    /// a proposal inside the grace window does NOT move `last_processed_id`,
    /// and the same proposal DOES once it has aged past the window.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p notifier \
                advance_cursor_with_grace_only_promotes_a_proposal_once_it_has_aged -- --ignored \
                --test-threads=1`"]
    async fn advance_cursor_with_grace_only_promotes_a_proposal_once_it_has_aged() {
        let pool = connect().await;
        let cursor_name = "TEST-NOTIFIER-GRACE-CURSOR";
        sqlx::query("DELETE FROM notifier_cursor WHERE name = $1")
            .bind(cursor_name)
            .execute(&pool)
            .await
            .expect("clear fixture cursor");

        let grace = chrono::Duration::seconds(120);
        let t0 = Utc::now();

        let fresh = read_cursor(&pool, cursor_name).await.expect("read cursor");
        assert_eq!(fresh.last_processed_id, 0);
        assert_eq!(fresh.pending_id, None);

        // First cycle: observes id 100 and only PROPOSES it.
        let advanced = advance_cursor_with_grace(&pool, cursor_name, &fresh, 100, t0, grace)
            .await
            .expect("first advance");
        assert_eq!(
            advanced, 0,
            "an unaged proposal must not move the real watermark -- a transaction holding id 99 \
             may still be in flight, and nothing ever re-checks a row the cursor has passed"
        );
        let after_first = read_cursor(&pool, cursor_name)
            .await
            .expect("re-read cursor");
        assert_eq!(after_first.last_processed_id, 0);
        assert_eq!(after_first.pending_id, Some(100));

        // Second cycle, still inside the grace window: still no promotion.
        let inside = advance_cursor_with_grace(
            &pool,
            cursor_name,
            &after_first,
            100,
            t0 + chrono::Duration::seconds(60),
            grace,
        )
        .await
        .expect("second advance");
        assert_eq!(inside, 0, "60s < 120s grace -- still not promoted");

        // Third cycle, past the window: the proposal is promoted, and this
        // cycle's own observation becomes the new proposal.
        let after_second = read_cursor(&pool, cursor_name)
            .await
            .expect("re-read cursor");
        let promoted = advance_cursor_with_grace(
            &pool,
            cursor_name,
            &after_second,
            140,
            t0 + chrono::Duration::seconds(300),
            grace,
        )
        .await
        .expect("third advance");
        assert_eq!(
            promoted, 100,
            "once the proposal has aged past the grace window -- and this cycle has itself just \
             re-read everything above the old watermark -- it is safe to promote"
        );
        let after_third = read_cursor(&pool, cursor_name)
            .await
            .expect("re-read cursor");
        assert_eq!(after_third.last_processed_id, 100);
        assert_eq!(after_third.pending_id, Some(140));

        // A watermark must never move backwards, even given a smaller
        // observation (rows deleted, or a stale proposal).
        let backwards = advance_cursor_with_grace(
            &pool,
            cursor_name,
            &after_third,
            5,
            t0 + chrono::Duration::seconds(600),
            grace,
        )
        .await
        .expect("fourth advance");
        assert_eq!(backwards, 140);
        let after_fourth = read_cursor(&pool, cursor_name)
            .await
            .expect("re-read cursor");
        assert_eq!(after_fourth.pending_id, Some(140));

        sqlx::query("DELETE FROM notifier_cursor WHERE name = $1")
            .bind(cursor_name)
            .execute(&pool)
            .await
            .ok();
    }

    /// Finding 6, the property that actually matters: a row that becomes
    /// visible AFTER a higher id was already observed is still picked up,
    /// because the cursor is still behind it while the proposal ages.
    ///
    /// Inserts the "late" row with a LOWER id than an already-observed one by
    /// exploiting the same mechanic the real bug does -- an open transaction
    /// holds its id until commit -- and asserts the next poll (run from the
    /// cursor's own `last_processed_id`, not from the observed maximum) still
    /// returns it. Under the pre-fix "MAX(id) observed" watermark the cursor
    /// would already be past this row and it would never be seen again.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p notifier \
                a_late_committing_lower_id_row_is_still_polled_inside_the_grace_window \
                -- --ignored --test-threads=1`"]
    async fn a_late_committing_lower_id_row_is_still_polled_inside_the_grace_window() {
        let pool = connect().await;
        let cursor_name = "TEST-NOTIFIER-LATECOMMIT-CURSOR";
        let line_id = "TEST-NOTIFIER-LATECOMMIT-LINE";
        sqlx::query("DELETE FROM notifier_cursor WHERE name = $1")
            .bind(cursor_name)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1")
            .bind(line_id)
            .execute(&pool)
            .await
            .ok();

        let grace = chrono::Duration::seconds(120);
        let cursor = read_cursor(&pool, cursor_name).await.expect("read cursor");

        // A transaction that has taken its id but has NOT committed yet --
        // exactly the writer the old watermark lost.
        let mut slow_tx = pool.begin().await.expect("begin the slow writer");
        let late_id: i64 = sqlx::query_scalar(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) \
             VALUES ($1, $2, NOW()) RETURNING id",
        )
        .bind(line_id)
        .bind(status_json(common::Severity::GoodService))
        .fetch_one(&mut *slow_tx)
        .await
        .expect("the slow writer takes its id");

        // Meanwhile a LATER transaction commits, and a cycle observes it.
        let visible_id: i64 = sqlx::query_scalar(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) \
             VALUES ($1, $2, NOW()) RETURNING id",
        )
        .bind(line_id)
        .bind(status_json(common::Severity::MinorDelays))
        .fetch_one(&pool)
        .await
        .expect("the fast writer commits immediately");
        assert!(
            visible_id > late_id,
            "the still-uncommitted row must hold the LOWER id for this test to mean anything"
        );

        let (_, observed_max_id) = poll_line_candidates(&pool, cursor.last_processed_id)
            .await
            .expect("first poll");
        assert!(
            observed_max_id >= visible_id,
            "the cycle observes the committed, higher id"
        );
        let advanced = advance_cursor_with_grace(
            &pool,
            cursor_name,
            &cursor,
            observed_max_id,
            Utc::now(),
            grace,
        )
        .await
        .expect("advance");
        assert!(
            advanced < late_id,
            "the real watermark must still be BEHIND the row that had not committed yet -- this \
             is precisely what 'MAX(id) observed' got wrong"
        );

        // The slow writer finally commits, out of id order.
        slow_tx
            .commit()
            .await
            .expect("the slow writer commits late");

        let after = read_cursor(&pool, cursor_name)
            .await
            .expect("re-read cursor");
        let (candidates, _) = poll_line_candidates(&pool, after.last_processed_id)
            .await
            .expect("second poll");
        assert!(
            candidates.iter().any(|c| c.id == visible_id)
                || poll_line_candidates(&pool, after.last_processed_id)
                    .await
                    .expect("re-poll")
                    .1
                    >= late_id,
            "the late row is inside the still-unpassed range, so a later cycle sees it"
        );
        let (_, observed_after) = poll_line_candidates(&pool, after.last_processed_id)
            .await
            .expect("third poll");
        assert!(
            observed_after >= late_id,
            "the late-committing row is still within this poll's range and is therefore evaluated \
             -- under the pre-fix watermark the cursor was already past it for ever"
        );

        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1")
            .bind(line_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM notifier_cursor WHERE name = $1")
            .bind(cursor_name)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p notifier \
                push_subscriptions_round_trip_and_self_cleanup_on_delete -- --ignored`"]
    async fn push_subscriptions_round_trip_and_self_cleanup_on_delete() {
        // Mirrors users.rs's own session_round_trip_creates_looks_up_and_deletes
        // shape -- this is the automated half of the 404/410 self-cleanup
        // path Task 5/6 exercise for real; Task 10's manual pass confirms
        // the real HTTP 404/410 trigger, this confirms the DB side of
        // "delete on expired" alone.
        let pool = connect().await;
        seed_user(&pool, "TEST-NOTIFIER-CLEANUP-USER").await;

        sqlx::query(
            "INSERT INTO push_subscriptions (user_id, endpoint, p256dh, auth, created_at, last_seen_at) \
             VALUES ($1, $2, $3, $4, NOW(), NOW())",
        )
        .bind("TEST-NOTIFIER-CLEANUP-USER")
        .bind("https://push.example/ep-cleanup-test")
        .bind("p256dh")
        .bind("auth")
        .execute(&pool)
        .await
        .expect("seed subscription");

        let subscriptions = push_subscriptions_for_user(&pool, "TEST-NOTIFIER-CLEANUP-USER")
            .await
            .expect("list");
        let seeded = subscriptions
            .iter()
            .find(|s| s.endpoint == "https://push.example/ep-cleanup-test")
            .expect("seeded row must be listed");

        delete_push_subscription(&pool, seeded.id)
            .await
            .expect("delete");

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM push_subscriptions WHERE endpoint = $1")
                .bind("https://push.example/ep-cleanup-test")
                .fetch_one(&pool)
                .await
                .expect("count after delete");
        assert_eq!(
            remaining, 0,
            "delete_push_subscription must actually remove the row"
        );

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind("TEST-NOTIFIER-CLEANUP-USER")
            .execute(&pool)
            .await
            .expect("cleanup fixture user");
    }

    #[tokio::test]
    #[ignore = "requires a live database with Phase 1's journeys/journey_legs tables \
                already migrated; run with `DATABASE_URL=... cargo test -p notifier \
                list_committed_legs_for_today_finds_a_bound_leg_with_a_live_sample \
                -- --ignored --test-threads=1`"]
    async fn list_committed_legs_for_today_finds_a_bound_leg_with_a_live_sample() {
        let pool = connect().await;
        let service_date: chrono::NaiveDate = "2026-09-22".parse().unwrap();
        seed_user(&pool, "TEST-SKIP-LEG-USER").await;

        let trains_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date, origin_crs) \
             VALUES ('TEST-SKIP-LEG-UID', $1, 'PAD') RETURNING id",
        )
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed trains row");

        let tracked_train_id: i64 = sqlx::query_scalar(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, trains_id, resolution_status) \
             VALUES ($1, $2, 'RDG', $3, $4, 'resolved') RETURNING id",
        )
        .bind("TEST-SKIP-LEG-USER")
        .bind(service_date)
        .bind(service_date.and_hms_opt(9, 0, 0).unwrap().and_utc())
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed train_subscriptions row");

        let journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name) VALUES ($1, NULL) RETURNING id",
        )
        .bind("TEST-SKIP-LEG-USER")
        .fetch_one(&pool)
        .await
        .expect("seed journeys row");

        let journey_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, train_subscription_id, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, $3, 'manual') RETURNING id",
        )
        .bind(journey_id)
        .bind(service_date)
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .expect("seed journey_legs row");

        let legs = list_committed_legs_for_today(&pool, service_date)
            .await
            .expect("list_committed_legs_for_today");
        let found = legs
            .iter()
            .find(|leg| leg.journey_leg_id == journey_leg_id)
            .expect("the seeded leg must be returned");
        assert_eq!(found.origin_crs, "RDG");
        assert_eq!(found.destination_crs, "WOK");
        assert_eq!(found.trains_id, trains_id);
        assert_eq!(found.train_origin_crs.as_deref(), Some("PAD"));

        sqlx::query("DELETE FROM journey_legs WHERE id = $1")
            .bind(journey_leg_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user_skip(&pool, "TEST-SKIP-LEG-USER").await;
    }

    async fn cleanup_user_skip(pool: &PgPool, user_id: &str) {
        sqlx::query("DELETE FROM journey_leg_notification_state WHERE user_id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database with Phase 1's journeys/journey_legs tables \
                already migrated; run with `DATABASE_URL=... cargo test -p notifier \
                skip_notification_state_round_trips -- --ignored --test-threads=1`"]
    async fn skip_notification_state_round_trips() {
        let pool = connect().await;
        seed_user(&pool, "TEST-SKIP-STATE-USER").await;
        let journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name) VALUES ($1, NULL) RETURNING id",
        )
        .bind("TEST-SKIP-STATE-USER")
        .fetch_one(&pool)
        .await
        .expect("seed journeys row");
        let journey_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs (journey_id, leg_order, origin_crs, destination_crs, service_date, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', '2026-09-22', 'unmatched') RETURNING id",
        )
        .bind(journey_id)
        .fetch_one(&pool)
        .await
        .expect("seed journey_legs row");

        assert_eq!(
            skip_notification_state(&pool, "TEST-SKIP-STATE-USER", journey_leg_id)
                .await
                .expect("read before any write"),
            None
        );

        let now = Utc::now();
        upsert_skip_notification_state(&pool, "TEST-SKIP-STATE-USER", journey_leg_id, true, now)
            .await
            .expect("first upsert");
        assert_eq!(
            skip_notification_state(&pool, "TEST-SKIP-STATE-USER", journey_leg_id)
                .await
                .expect("read after first upsert"),
            Some(true)
        );

        upsert_skip_notification_state(&pool, "TEST-SKIP-STATE-USER", journey_leg_id, false, now)
            .await
            .expect("second upsert (resolved)");
        assert_eq!(
            skip_notification_state(&pool, "TEST-SKIP-STATE-USER", journey_leg_id)
                .await
                .expect("read after second upsert"),
            Some(false)
        );

        sqlx::query("DELETE FROM journey_legs WHERE id = $1")
            .bind(journey_leg_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user_skip(&pool, "TEST-SKIP-STATE-USER").await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p notifier \
                poll_train_candidates_fans_out_one_trains_id_to_every_subscriber \
                -- --ignored --test-threads=1`"]
    async fn poll_train_candidates_fans_out_one_trains_id_to_every_subscriber() {
        let pool = connect().await;
        let service_date: chrono::NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-FANOUT-UID', $1) \
             RETURNING id",
        )
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed trains row");

        for user_id in ["TEST-FANOUT-USER-A", "TEST-FANOUT-USER-B"] {
            sqlx::query(
                "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
            )
            .bind(user_id)
            .bind(format!("{user_id}@example.com"))
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("seed fixture user");
            sqlx::query(
                "INSERT INTO train_subscriptions \
                    (user_id, service_date, pin_origin_crs, pin_scheduled_departure, trains_id, resolution_status) \
                 VALUES ($1, $2, 'EUS', $3, $4, 'resolved')",
            )
            .bind(user_id)
            .bind(service_date)
            .bind(service_date.and_hms_opt(19, 15, 0).unwrap().and_utc())
            .bind(trains_id)
            .execute(&pool)
            .await
            .expect("seed a subscription pointing at the shared train");
        }

        let (event_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_movement_events (trains_id, dedup_key, msg_type, raw_body) \
             VALUES ($1, 'test-fanout-dedup', '0003', '{}'::jsonb) RETURNING id",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed a movement event for the shared train");
        sqlx::query(
            "INSERT INTO train_current_state (trains_id, status, delay_minutes) VALUES ($1, 'en_route', 20)",
        )
        .bind(trains_id)
        .execute(&pool)
        .await
        .expect("seed current state showing a real delay");

        let (candidates, max_id) = poll_train_candidates(&pool, event_id - 1, 15)
            .await
            .expect("poll_train_candidates");
        assert_eq!(max_id, event_id);
        assert_eq!(
            candidates.len(),
            2,
            "one physical train's delay must notify BOTH of its independent subscribers"
        );
        assert!(candidates.iter().all(|c| c.trains_id == trains_id));

        sqlx::query("DELETE FROM train_subscriptions WHERE trains_id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        for user_id in ["TEST-FANOUT-USER-A", "TEST-FANOUT-USER-B"] {
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(&pool)
                .await
                .ok();
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p notifier \
                candidates_for_trains_id_excludes_a_deactivated_subscription \
                -- --ignored --test-threads=1`"]
    async fn candidates_for_trains_id_excludes_a_deactivated_subscription() {
        // 2026-09 review finding (Medium), the notifier-side half of the
        // "Change train" orphan-subscription fix: `journeys::
        // set_leg_train_subscription` (crates/api) now sets
        // `notifications_enabled = FALSE` on a leg's old subscription once
        // it becomes unreferenced. This pins down that this query actually
        // honours that flag -- before this fix, the column was never
        // consulted here at all, so a "deactivated" subscription would
        // still generate notifications forever.
        let pool = connect().await;
        let service_date: chrono::NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-DISABLED-SUB-UID', $1) \
             RETURNING id",
        )
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed trains row");

        for user_id in [
            "TEST-DISABLED-SUB-USER-ENABLED",
            "TEST-DISABLED-SUB-USER-DISABLED",
        ] {
            sqlx::query(
                "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
            )
            .bind(user_id)
            .bind(format!("{user_id}@example.com"))
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("seed fixture user");
        }
        sqlx::query(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, trains_id, resolution_status) \
             VALUES ($1, $2, 'EUS', $3, $4, 'resolved')",
        )
        .bind("TEST-DISABLED-SUB-USER-ENABLED")
        .bind(service_date)
        .bind(service_date.and_hms_opt(19, 15, 0).unwrap().and_utc())
        .bind(trains_id)
        .execute(&pool)
        .await
        .expect("seed the still-enabled subscription");
        let (disabled_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, trains_id, resolution_status, notifications_enabled) \
             VALUES ($1, $2, 'EUS', $3, $4, 'resolved', FALSE) RETURNING id",
        )
        .bind("TEST-DISABLED-SUB-USER-DISABLED")
        .bind(service_date)
        .bind(service_date.and_hms_opt(19, 15, 0).unwrap().and_utc())
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed the deactivated (orphaned) subscription");

        sqlx::query(
            "INSERT INTO train_current_state (trains_id, status, delay_minutes) VALUES ($1, 'en_route', 20)",
        )
        .bind(trains_id)
        .execute(&pool)
        .await
        .expect("seed current state showing a real delay");

        let candidates = candidates_for_trains_id(&pool, trains_id, 15)
            .await
            .expect("candidates_for_trains_id");
        assert_eq!(
            candidates.len(),
            1,
            "the deactivated subscription must not produce a candidate"
        );
        assert_eq!(candidates[0].user_id, "TEST-DISABLED-SUB-USER-ENABLED");
        assert!(
            candidates.iter().all(|c| c.tracked_train_id != disabled_id),
            "the deactivated subscription's own id must not appear at all"
        );

        sqlx::query("DELETE FROM train_current_state WHERE trains_id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE trains_id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        for user_id in [
            "TEST-DISABLED-SUB-USER-ENABLED",
            "TEST-DISABLED-SUB-USER-DISABLED",
        ] {
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(&pool)
                .await
                .ok();
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p notifier \
                poll_forward_queue_returns_distinct_touched_trains_ids -- --ignored --test-threads=1`"]
    async fn poll_forward_queue_returns_distinct_touched_trains_ids() {
        let pool = connect().await;
        let service_date: chrono::NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-FORWARD-QUEUE-UID', $1) RETURNING id",
        )
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed trains row");

        let (queue_id,): (i64,) = sqlx::query_as(
            "INSERT INTO notifier_forward_queue (trains_id, event_summary) VALUES ($1, 'en_route at WAT') RETURNING id",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed a forward-queue row");

        let (touched, max_id) = poll_forward_queue(&pool, queue_id - 1)
            .await
            .expect("poll_forward_queue");
        assert_eq!(touched, vec![trains_id]);
        assert_eq!(max_id, queue_id);

        let (touched_again, max_id_again) = poll_forward_queue(&pool, max_id)
            .await
            .expect("poll_forward_queue again from the new watermark");
        assert!(touched_again.is_empty());
        assert_eq!(max_id_again, max_id);

        sqlx::query("DELETE FROM notifier_forward_queue WHERE id = $1")
            .bind(queue_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// A `tracing_subscriber::Layer` counting `sqlx`'s own per-query
    /// tracing events (target `"sqlx::query"`, emitted once per executed
    /// statement -- see `sqlx-core`'s `logger.rs`). Used below to prove
    /// `poll_train_candidates` never does a per-train existence check for
    /// trains with zero subscribers -- a plain "are the right candidates
    /// returned" assertion can't distinguish a join-filtered single query
    /// from N wasted round trips that all correctly return nothing.
    struct SqlxQueryCounter(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for SqlxQueryCounter {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() == "sqlx::query" {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p notifier \
                poll_train_candidates_never_queries_trains_without_subscribers \
                -- --ignored --test-threads=1`"]
    async fn poll_train_candidates_never_queries_trains_without_subscribers() {
        use tracing_subscriber::layer::SubscriberExt;

        let pool = connect().await;
        let service_date: chrono::NaiveDate = "2026-09-06".parse().unwrap();
        const UNSUBSCRIBED_COUNT: i64 = 20;

        // Defensive: clear any leftovers from a previously-aborted run.
        sqlx::query("DELETE FROM train_movement_events WHERE dedup_key LIKE 'test-nosub-%'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM trains WHERE train_uid LIKE 'TEST-NOSUB-%' OR train_uid = 'TEST-NOSUB-SUBSCRIBED-UID'",
        )
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM users WHERE id = 'TEST-NOSUB-SUBSCRIBER'")
            .execute(&pool)
            .await
            .ok();

        // One train WITH a subscriber -- must still produce a candidate.
        let subscribed_trains_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-NOSUB-SUBSCRIBED-UID', $1) \
             RETURNING id",
        )
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed subscribed train");

        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind("TEST-NOSUB-SUBSCRIBER")
        .bind("test-nosub-subscriber@example.com")
        .bind("TEST-NOSUB-SUBSCRIBER")
        .execute(&pool)
        .await
        .expect("seed fixture user");

        sqlx::query(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, trains_id, resolution_status) \
             VALUES ($1, $2, 'EUS', $3, $4, 'resolved')",
        )
        .bind("TEST-NOSUB-SUBSCRIBER")
        .bind(service_date)
        .bind(service_date.and_hms_opt(19, 15, 0).unwrap().and_utc())
        .bind(subscribed_trains_id)
        .execute(&pool)
        .await
        .expect("seed a subscription for the subscribed train");

        sqlx::query(
            "INSERT INTO train_current_state (trains_id, status, delay_minutes) VALUES ($1, 'en_route', 20)",
        )
        .bind(subscribed_trains_id)
        .execute(&pool)
        .await
        .expect("seed current state showing a real delay");

        let (subscribed_event_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_movement_events (trains_id, dedup_key, msg_type, raw_body) \
             VALUES ($1, 'test-nosub-subscribed', '0003', '{}'::jsonb) RETURNING id",
        )
        .bind(subscribed_trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed a movement event for the subscribed train");

        // Twenty trains with movement events but ZERO subscribers -- these
        // must never be looked up individually.
        let mut unsubscribed_trains_ids = Vec::new();
        for i in 0..UNSUBSCRIBED_COUNT {
            let trains_id: i64 = sqlx::query_scalar(
                "INSERT INTO trains (train_uid, service_date) VALUES ($1, $2) RETURNING id",
            )
            .bind(format!("TEST-NOSUB-UID-{i}"))
            .bind(service_date)
            .fetch_one(&pool)
            .await
            .expect("seed an unsubscribed train");
            unsubscribed_trains_ids.push(trains_id);

            sqlx::query(
                "INSERT INTO train_movement_events (trains_id, dedup_key, msg_type, raw_body) \
                 VALUES ($1, $2, '0003', '{}'::jsonb)",
            )
            .bind(trains_id)
            .bind(format!("test-nosub-{i}"))
            .execute(&pool)
            .await
            .expect("seed a movement event for an unsubscribed train");
        }

        let since_id = subscribed_event_id - 1;

        let query_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(SqlxQueryCounter(query_count.clone()));
        let guard = tracing::subscriber::set_default(subscriber);
        let (candidates, max_id) = poll_train_candidates(&pool, since_id, 15)
            .await
            .expect("poll_train_candidates");
        drop(guard);

        assert_eq!(
            candidates.len(),
            1,
            "only the trains_id with a real subscriber must produce a candidate"
        );
        assert_eq!(candidates[0].trains_id, subscribed_trains_id);
        assert!(max_id > since_id);

        let observed = query_count.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            observed < 10,
            "poll_train_candidates must not do a per-train round trip for each of the {} \
             unsubscribed trains -- observed {observed} sqlx queries for one subscribed train \
             plus {} unsubscribed ones",
            UNSUBSCRIBED_COUNT,
            UNSUBSCRIBED_COUNT
        );

        sqlx::query("DELETE FROM train_movement_events WHERE dedup_key LIKE 'test-nosub-%'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE trains_id = $1")
            .bind(subscribed_trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_current_state WHERE trains_id = $1")
            .bind(subscribed_trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(subscribed_trains_id)
            .execute(&pool)
            .await
            .ok();
        for trains_id in unsubscribed_trains_ids {
            sqlx::query("DELETE FROM trains WHERE id = $1")
                .bind(trains_id)
                .execute(&pool)
                .await
                .ok();
        }
        sqlx::query("DELETE FROM users WHERE id = 'TEST-NOSUB-SUBSCRIBER'")
            .execute(&pool)
            .await
            .ok();
    }
}

/// The two-stage recurrence sweep's DB layer -- mint (stage 1) and
/// commit-check (stage 2). A separate module from `mod tests` above
/// (mirroring `decision.rs`'s own `tests`/`skip_notification_tests`/
/// `sweep_tests` three-way split), not because these tests need different
/// machinery, but to keep this plan's own addition reviewable as one
/// self-contained unit.
#[cfg(test)]
mod sweep_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    async fn seed_user(pool: &PgPool, user_id: &str) {
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind(format!("{user_id}@example.com"))
        .bind(user_id)
        .execute(pool)
        .await
        .expect("seed fixture user");
    }

    async fn cleanup_user(pool: &PgPool, user_id: &str) {
        sqlx::query("DELETE FROM journey_leg_notification_state WHERE user_id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
    }

    /// Finding 3's regression test: once a template's occurrence for a date
    /// has been explicitly discarded by its owner, the sweep must not mint
    /// it again.
    ///
    /// Reproduces the real sequence: the sweep mints today's occurrence, the
    /// user deletes it (which `api::data::journeys::delete_journey` now
    /// records as a `journey_template_skipped_dates` tombstone -- simulated
    /// here by the same INSERT that function performs, since this crate
    /// cannot call it), and the next tick runs. Before this fix the mint
    /// guard was purely "does a journey from this template have a leg on this
    /// date," which the delete had just made false again -- so the occurrence
    /// came straight back, within the hour, along with its auto-commit and
    /// its notifications. Also asserts the tombstone is DATE-scoped: tomorrow
    /// still mints.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                a_discarded_occurrence_is_not_reminted_by_the_next_sweep_tick \
                -- --ignored --test-threads=1`"]
    async fn a_discarded_occurrence_is_not_reminted_by_the_next_sweep_tick() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-SKIPDAY-USER";
        seed_user(&pool, user_id).await;
        let today: chrono::NaiveDate = "2026-09-25".parse().unwrap();
        let tomorrow = today.succ_opt().expect("a next day exists");

        let template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name) \
             VALUES ($1, 'Test Skip Day Template') RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed journey_templates row");
        sqlx::query(
            "INSERT INTO journey_template_legs \
                (template_id, leg_order, origin_crs, destination_crs, depart_after) \
             VALUES ($1, 1, 'RDG', 'WOK', '09:00:00')",
        )
        .bind(template_id)
        .execute(&pool)
        .await
        .expect("seed journey_template_legs row");

        let journey_id =
            materialize_due_template_occurrence(&pool, template_id, user_id, None, today)
                .await
                .expect("first materialize call")
                .expect("the first call must mint a journey");

        // The user discards today's occurrence. This is exactly what
        // `journeys::delete_journey` does, in one transaction: tombstone the
        // (template, date) pair, then delete the journey.
        sqlx::query(
            "INSERT INTO journey_template_skipped_dates (template_id, service_date) \
             VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(template_id)
        .bind(today)
        .execute(&pool)
        .await
        .expect("record the skip tombstone");
        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .expect("delete the occurrence");

        let reminted =
            materialize_due_template_occurrence(&pool, template_id, user_id, None, today)
                .await
                .expect("the post-delete materialize call must not error");
        assert_eq!(
            reminted, None,
            "a deleted occurrence must stay deleted -- re-minting it is what made the user get \
             pushes again, within the hour, for a journey they had explicitly thrown away"
        );
        let journeys_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM journeys WHERE source_template_id = $1")
                .bind(template_id)
                .fetch_one(&pool)
                .await
                .expect("count journeys");
        assert_eq!(journeys_count, 0, "and no journeys row may exist for it");

        let tomorrows =
            materialize_due_template_occurrence(&pool, template_id, user_id, None, tomorrow)
                .await
                .expect("tomorrow's materialize call");
        assert!(
            tomorrows.is_some(),
            "skipping TODAY must not suppress tomorrow's occurrence of the same commute -- the \
             tombstone is scoped to (template_id, service_date)"
        );

        sqlx::query("DELETE FROM journeys WHERE source_template_id = $1")
            .bind(template_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journey_template_skipped_dates WHERE template_id = $1")
            .bind(template_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journey_templates WHERE id = $1")
            .bind(template_id)
            .execute(&pool)
            .await
            .ok(); // cascades journey_template_legs
        cleanup_user(&pool, user_id).await;
    }

    /// Finding 4's regression test: when the leg has already been committed
    /// by someone else, `auto_commit_leg_to_train` must leave NO subscription
    /// behind.
    ///
    /// Reproduces the real race: the user picks a train by hand
    /// (`POST /Journeys/{j}/legs/{l}/train`, which sets `match_mode =
    /// 'manual'`) at the same moment as a sweep tick. Before this fix the
    /// sweep created its subscription FIRST and only then discovered its own
    /// `UPDATE ... AND match_mode = 'unmatched'` had affected zero rows -- so
    /// the subscription stayed, referenced by nothing, and
    /// `candidates_for_trains_id`'s per-subscriber fan-out kept sending that
    /// user delay/cancellation pushes for a train they never chose to track.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                a_lost_auto_commit_race_leaves_no_orphaned_subscription \
                -- --ignored --test-threads=1`"]
    async fn a_lost_auto_commit_race_leaves_no_orphaned_subscription() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-ORPHAN-USER";
        seed_user(&pool, user_id).await;
        let service_date: chrono::NaiveDate = "2026-09-25".parse().unwrap();

        let auto_trains_id =
            find_or_create_train(&pool, "TEST-SWEEP-ORPHAN-AUTO-UID", service_date)
                .await
                .expect("the train the sweep would have chosen");
        let manual_trains_id =
            find_or_create_train(&pool, "TEST-SWEEP-ORPHAN-MANUAL-UID", service_date)
                .await
                .expect("the train the user chose by hand");
        let manual_subscription_id =
            create_subscription_for_train(&pool, manual_trains_id, user_id)
                .await
                .expect("the user's own manual subscription");

        let journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name) VALUES ($1, NULL) RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed journeys row");
        // Already 'manual': the user won the race, exactly as
        // `set_leg_train_subscription` would have left it.
        let journey_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, \
                 train_subscription_id, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, $3, 'manual') RETURNING id",
        )
        .bind(journey_id)
        .bind(service_date)
        .bind(manual_subscription_id)
        .fetch_one(&pool)
        .await
        .expect("seed an already-committed journey_legs row");

        let outcome = auto_commit_leg_to_train(&pool, journey_leg_id, auto_trains_id, user_id)
            .await
            .expect("auto_commit_leg_to_train must not error on the no-op path");
        assert_eq!(
            outcome, None,
            "committing an already-committed leg must report the no-op, not a success"
        );

        let orphans: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM train_subscriptions WHERE user_id = $1 AND trains_id = $2",
        )
        .bind(user_id)
        .bind(auto_trains_id)
        .fetch_one(&pool)
        .await
        .expect("count subscriptions for the train the sweep tried to commit");
        assert_eq!(
            orphans, 0,
            "the subscription created inside the rolled-back transaction must be gone -- leaving \
             it behind is what kept pushing notifications for a train the user never chose"
        );

        let (still_manual, still_pointed_at): (String, Option<i64>) = sqlx::query_as(
            "SELECT match_mode, train_subscription_id FROM journey_legs WHERE id = $1",
        )
        .bind(journey_leg_id)
        .fetch_one(&pool)
        .await
        .expect("read the leg back");
        assert_eq!(still_manual, "manual", "the user's own pick must survive");
        assert_eq!(still_pointed_at, Some(manual_subscription_id));

        // And the user's pre-existing subscription for the OTHER train must
        // not have been rolled back either -- the CTE only SELECTs an
        // existing row, so there is nothing of theirs to undo.
        let manual_still_there: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM train_subscriptions WHERE id = $1")
                .bind(manual_subscription_id)
                .fetch_one(&pool)
                .await
                .expect("count the manual subscription");
        assert_eq!(manual_still_there, 1);

        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id IN ($1, $2)")
            .bind(auto_trains_id)
            .bind(manual_trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// Finding 4's other half: when the commit really does happen, the
    /// subscription is committed with it and the leg comes out `'auto'`.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                a_won_auto_commit_persists_both_the_subscription_and_the_leg \
                -- --ignored --test-threads=1`"]
    async fn a_won_auto_commit_persists_both_the_subscription_and_the_leg() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-COMMITTX-USER";
        seed_user(&pool, user_id).await;
        let service_date: chrono::NaiveDate = "2026-09-25".parse().unwrap();

        let trains_id = find_or_create_train(&pool, "TEST-SWEEP-COMMITTX-UID", service_date)
            .await
            .expect("find_or_create_train");
        let journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name) VALUES ($1, NULL) RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed journeys row");
        let journey_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, 'unmatched') RETURNING id",
        )
        .bind(journey_id)
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed an unmatched journey_legs row");

        let tracking_id = auto_commit_leg_to_train(&pool, journey_leg_id, trains_id, user_id)
            .await
            .expect("auto_commit_leg_to_train")
            .expect("an unmatched leg must be committed");

        let (match_mode, train_subscription_id): (String, Option<i64>) = sqlx::query_as(
            "SELECT match_mode, train_subscription_id FROM journey_legs WHERE id = $1",
        )
        .bind(journey_leg_id)
        .fetch_one(&pool)
        .await
        .expect("read the leg back");
        assert_eq!(match_mode, "auto");
        assert_eq!(train_subscription_id, Some(tracking_id));
        let subscriptions: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM train_subscriptions WHERE id = $1")
                .bind(tracking_id)
                .fetch_one(&pool)
                .await
                .expect("count the committed subscription");
        assert_eq!(
            subscriptions, 1,
            "the transaction must have COMMITTED the subscription, not rolled it back"
        );

        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// Finding 5's unit-level regression test for the enrichment itself:
    /// `find_or_create_train_with_cif_schedule` must copy CIF's own true
    /// origin, booked departure and TERMINUS onto the shared `trains` row,
    /// and `create_subscription_for_train` must then carry them into the
    /// subscription's `pin_*` columns.
    ///
    /// The terminus assertion is the load-bearing one: the seeded schedule
    /// has the flattened table's ordinary several-rows-per-train shape
    /// (RDG->WOK, RDG->BSK, WOK->BSK), so "the destination of the first row"
    /// would be wrong -- the terminus is the latest resolvable arrival.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                find_or_create_train_with_cif_schedule_writes_the_true_origin_and_terminus \
                -- --ignored --test-threads=1`"]
    async fn find_or_create_train_with_cif_schedule_writes_the_true_origin_and_terminus() {
        let pool = connect().await;
        let train_uid = "TEST-CIF-ENRICH-UID";
        let service_date: chrono::NaiveDate = "2026-09-25".parse().unwrap();
        let user_id = "TEST-CIF-ENRICH-USER";
        seed_user(&pool, user_id).await;
        sqlx::query("DELETE FROM schedule_destination_departures WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .ok();

        sqlx::query(
            "INSERT INTO schedule_destination_departures \
                (service_date, destination_crs, scheduled, day_offset, train_uid, origin_crs, \
                 true_origin_crs, destination_arrival, destination_arrival_day_offset) \
             VALUES ($1, 'WOK', '09:05:00', 0, $2, 'RDG', 'RDG', '09:25:00', 0), \
                    ($1, 'BSK', '09:05:00', 0, $2, 'RDG', 'RDG', '09:58:00', 0), \
                    ($1, 'BSK', '09:26:00', 0, $2, 'WOK', 'RDG', '09:58:00', 0)",
        )
        .bind(service_date)
        .bind(train_uid)
        .execute(&pool)
        .await
        .expect("seed a realistic multi-row CIF shape for one train");

        let trains_id = find_or_create_train_with_cif_schedule(&pool, train_uid, service_date)
            .await
            .expect("find_or_create_train_with_cif_schedule");

        let (origin_crs, destination_crs, scheduled_departure): (
            Option<String>,
            Option<String>,
            Option<DateTime<Utc>>,
        ) = sqlx::query_as(
            "SELECT origin_crs, destination_crs, scheduled_departure FROM trains WHERE id = $1",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("read the trains row back");
        assert_eq!(
            origin_crs.as_deref(),
            Some("RDG"),
            "the true origin is the row whose origin_crs IS its true_origin_crs, not the \
             WOK->BSK leg of the same schedule"
        );
        assert_eq!(
            destination_crs.as_deref(),
            Some("BSK"),
            "the terminus is the latest resolvable arrival (09:58 BSK), not the first row's WOK"
        );
        assert_eq!(
            scheduled_departure,
            crate::london_to_utc(service_date.and_hms_opt(9, 5, 0).unwrap()),
            "the booked departure is stored as a real UTC instant"
        );

        let tracking_id = create_subscription_for_train(&pool, trains_id, user_id)
            .await
            .expect("create_subscription_for_train");
        let (pin_origin_crs, pin_destination_crs): (Option<String>, Option<String>) =
            sqlx::query_as(
                "SELECT pin_origin_crs, pin_destination_crs FROM train_subscriptions WHERE id = $1",
            )
            .bind(tracking_id)
            .fetch_one(&pool)
            .await
            .expect("read the subscription back");
        assert_eq!(pin_origin_crs.as_deref(), Some("RDG"));
        assert_eq!(
            pin_destination_crs.as_deref(),
            Some("BSK"),
            "skip_check::leg_is_skipped matches Darwin by this column -- NULL here (the pre-fix \
             behavior of the bare find_or_create_train) means skip detection can never fire"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracking_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM schedule_destination_departures WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                materialize_due_template_occurrence_is_idempotent_on_a_second_call \
                -- --ignored --test-threads=1`"]
    async fn materialize_due_template_occurrence_is_idempotent_on_a_second_call() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-MINT-USER";
        seed_user(&pool, user_id).await;
        let today: chrono::NaiveDate = "2026-09-23".parse().unwrap();

        let template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name) \
             VALUES ($1, 'Test Mint Template') RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed journey_templates row");

        sqlx::query(
            "INSERT INTO journey_template_legs \
                (template_id, leg_order, origin_crs, destination_crs, depart_after) \
             VALUES ($1, 1, 'RDG', 'WOK', '09:00:00')",
        )
        .bind(template_id)
        .execute(&pool)
        .await
        .expect("seed journey_template_legs row");

        let first = materialize_due_template_occurrence(&pool, template_id, user_id, None, today)
            .await
            .expect("first materialize call");
        let journey_id = first.expect("the first call must mint a journey");

        let second = materialize_due_template_occurrence(&pool, template_id, user_id, None, today)
            .await
            .expect("second materialize call");
        assert_eq!(
            second, None,
            "a second call for the same template/day must be an idempotent no-op"
        );

        let journeys_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM journeys WHERE source_template_id = $1")
                .bind(template_id)
                .fetch_one(&pool)
                .await
                .expect("count journeys");
        assert_eq!(
            journeys_count, 1,
            "exactly one journeys row must exist after two calls"
        );

        let legs_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM journey_legs WHERE journey_id = $1")
                .bind(journey_id)
                .fetch_one(&pool)
                .await
                .expect("count journey_legs");
        assert_eq!(
            legs_count, 1,
            "exactly one journey_legs row must exist after two calls"
        );

        sqlx::query("DELETE FROM journey_legs WHERE journey_id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journey_template_legs WHERE template_id = $1")
            .bind(template_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journey_templates WHERE id = $1")
            .bind(template_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                due_templates_for_respects_days_of_week_active_and_date_range \
                -- --ignored --test-threads=1`"]
    async fn due_templates_for_respects_days_of_week_active_and_date_range() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-DUE-USER";
        seed_user(&pool, user_id).await;

        // 2026-09-23 is a Wednesday (weekday_bit = 4); 2026-09-21 is a
        // Monday (weekday_bit = 1) -- same convention decision.rs's own
        // weekday_bit tests use.
        let today: chrono::NaiveDate = "2026-09-23".parse().unwrap();
        let wednesday_bit: i16 = 4;
        let monday_bit: i16 = 1;

        let due_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, days_of_week, active) \
             VALUES ($1, 'Due', $2, TRUE) RETURNING id",
        )
        .bind(user_id)
        .bind(wednesday_bit)
        .fetch_one(&pool)
        .await
        .expect("seed due template");

        let inactive_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, days_of_week, active) \
             VALUES ($1, 'Inactive', $2, FALSE) RETURNING id",
        )
        .bind(user_id)
        .bind(wednesday_bit)
        .fetch_one(&pool)
        .await
        .expect("seed inactive template");

        let wrong_day_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, days_of_week, active) \
             VALUES ($1, 'WrongDay', $2, TRUE) RETURNING id",
        )
        .bind(user_id)
        .bind(monday_bit)
        .fetch_one(&pool)
        .await
        .expect("seed wrong-day template");

        let ended_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, days_of_week, active, ends_on) \
             VALUES ($1, 'Ended', $2, TRUE, $3) RETURNING id",
        )
        .bind(user_id)
        .bind(wednesday_bit)
        .bind(today.pred_opt().unwrap())
        .fetch_one(&pool)
        .await
        .expect("seed ended template");

        let due = due_templates_for(&pool, today)
            .await
            .expect("due_templates_for");
        let due_ids: Vec<i64> = due.iter().map(|t| t.id).collect();

        assert!(
            due_ids.contains(&due_id),
            "the active/right-weekday/in-range template must be due"
        );
        assert!(
            !due_ids.contains(&inactive_id),
            "an inactive template must not be due"
        );
        assert!(
            !due_ids.contains(&wrong_day_id),
            "a template without today's weekday bit set must not be due"
        );
        assert!(
            !due_ids.contains(&ended_id),
            "a template whose ends_on is before today must not be due"
        );

        for id in [due_id, inactive_id, wrong_day_id, ended_id] {
            sqlx::query("DELETE FROM journey_templates WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .ok();
        }
        cleanup_user(&pool, user_id).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                unmatched_auto_legs_for_commit_check_excludes_manual_mode_templates \
                -- --ignored --test-threads=1`"]
    async fn unmatched_auto_legs_for_commit_check_excludes_manual_mode_templates() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-COMMIT-CHECK-USER";
        seed_user(&pool, user_id).await;
        let today: chrono::NaiveDate = "2026-09-23".parse().unwrap();

        let auto_template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, default_match_mode) \
             VALUES ($1, 'Auto Template', 'auto') RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed auto template");

        let manual_template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, default_match_mode) \
             VALUES ($1, 'Manual Template', 'manual') RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed manual template");

        let auto_journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name, source_template_id) \
             VALUES ($1, NULL, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(auto_template_id)
        .fetch_one(&pool)
        .await
        .expect("seed auto journey");

        let manual_journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name, source_template_id) \
             VALUES ($1, NULL, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(manual_template_id)
        .fetch_one(&pool)
        .await
        .expect("seed manual journey");

        let auto_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, depart_after, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, '09:00:00', 'unmatched') RETURNING id",
        )
        .bind(auto_journey_id)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("seed auto leg");

        let manual_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, depart_after, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, '09:00:00', 'unmatched') RETURNING id",
        )
        .bind(manual_journey_id)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("seed manual leg");

        let legs = unmatched_auto_legs_for_commit_check(&pool, today)
            .await
            .expect("unmatched_auto_legs_for_commit_check");
        let leg_ids: Vec<i64> = legs.iter().map(|l| l.journey_leg_id).collect();
        assert!(
            leg_ids.contains(&auto_leg_id),
            "the auto-template's leg must be a commit-check candidate"
        );
        assert!(
            !leg_ids.contains(&manual_leg_id),
            "the manual-template's leg must be excluded"
        );

        sqlx::query("DELETE FROM journey_legs WHERE id IN ($1, $2)")
            .bind(auto_leg_id)
            .bind(manual_leg_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journeys WHERE id IN ($1, $2)")
            .bind(auto_journey_id)
            .bind(manual_journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journey_templates WHERE id IN ($1, $2)")
            .bind(auto_template_id)
            .bind(manual_template_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                unmatched_auto_legs_for_commit_check_includes_a_fully_open_window_leg \
                -- --ignored --test-threads=1`"]
    async fn unmatched_auto_legs_for_commit_check_includes_a_fully_open_window_leg() {
        // Regression test: a template leg is allowed to carry NO time
        // window at all -- see `api::data::journey_templates::validate_template_leg`'s
        // own doc comment ("a template leg is never itself 'matched,' so
        // the ambiguity [requiring a window bound] exists to prevent for
        // an ordinary journey leg doesn't apply here"). A materialized leg
        // copies that verbatim, so `depart_after`/`depart_before`/
        // `arrive_after`/`arrive_before` can all genuinely be NULL on a
        // real `journey_legs` row under an `'auto'`-mode template.
        //
        // This query used to additionally require
        // `depart_after IS NOT NULL OR arrive_after IS NOT NULL`, which
        // meant such a leg was NEVER returned here -- and therefore never
        // auto-committed by the sweep at all, silently, forever. That
        // requirement is gone; this test proves a fully-open-window leg is
        // a real commit-check candidate now.
        let pool = connect().await;
        let user_id = "TEST-SWEEP-OPEN-WINDOW-COMMIT-CHECK-USER";
        seed_user(&pool, user_id).await;
        let today: chrono::NaiveDate = "2026-09-23".parse().unwrap();

        let auto_template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, default_match_mode) \
             VALUES ($1, 'Open Window Auto Template', 'auto') RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed auto template");

        let auto_journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name, source_template_id) \
             VALUES ($1, NULL, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(auto_template_id)
        .fetch_one(&pool)
        .await
        .expect("seed auto journey");

        // No depart_after/depart_before/arrive_after/arrive_before bound
        // at all -- every one of the four stays NULL by omission.
        let open_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, 'unmatched') RETURNING id",
        )
        .bind(auto_journey_id)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("seed fully-open-window leg");

        let legs = unmatched_auto_legs_for_commit_check(&pool, today)
            .await
            .expect("unmatched_auto_legs_for_commit_check");
        let leg_ids: Vec<i64> = legs.iter().map(|l| l.journey_leg_id).collect();
        assert!(
            leg_ids.contains(&open_leg_id),
            "a leg with no time window at all must still be a commit-check candidate"
        );
        let open_leg = legs
            .iter()
            .find(|l| l.journey_leg_id == open_leg_id)
            .expect("the fully-open-window leg's own row");
        assert_eq!(open_leg.depart_after, None);
        assert_eq!(open_leg.depart_before, None);
        assert_eq!(open_leg.arrive_after, None);
        assert_eq!(open_leg.arrive_before, None);

        sqlx::query("DELETE FROM journey_legs WHERE id = $1")
            .bind(open_leg_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(auto_journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journey_templates WHERE id = $1")
            .bind(auto_template_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                unmatched_auto_legs_for_commit_check_excludes_paused_templates \
                -- --ignored --test-threads=1`"]
    async fn unmatched_auto_legs_for_commit_check_excludes_paused_templates() {
        // Final-review Finding 1: a paused template's ('active = false')
        // already-minted 'unmatched' leg must NOT be a commit-check
        // candidate -- otherwise flipping the Pause toggle after today's
        // occurrence was already stamped is silently ignored by the sweep
        // and the leg gets auto-committed to a real train anyway. The
        // sibling `unmatched_auto_legs_for_commit_check_excludes_manual_mode_templates`
        // test above already covers the `active = true` (default) case
        // returning a row, so this test only adds the negative `active =
        // false` case.
        let pool = connect().await;
        let user_id = "TEST-SWEEP-PAUSED-COMMIT-CHECK-USER";
        seed_user(&pool, user_id).await;
        let today: chrono::NaiveDate = "2026-09-23".parse().unwrap();

        let paused_template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, default_match_mode, active) \
             VALUES ($1, 'Paused Auto Template', 'auto', FALSE) RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed paused auto template");

        let paused_journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name, source_template_id) \
             VALUES ($1, NULL, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(paused_template_id)
        .fetch_one(&pool)
        .await
        .expect("seed paused journey");

        let paused_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, depart_after, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, '09:00:00', 'unmatched') RETURNING id",
        )
        .bind(paused_journey_id)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("seed paused leg");

        let legs = unmatched_auto_legs_for_commit_check(&pool, today)
            .await
            .expect("unmatched_auto_legs_for_commit_check");
        let leg_ids: Vec<i64> = legs.iter().map(|l| l.journey_leg_id).collect();
        assert!(
            !leg_ids.contains(&paused_leg_id),
            "a paused ('active = false') template's already-minted leg must be excluded \
             from the commit-check, even though it is still 'unmatched' and under an \
             'auto'-mode template"
        );

        sqlx::query("DELETE FROM journey_legs WHERE id = $1")
            .bind(paused_leg_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(paused_journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journey_templates WHERE id = $1")
            .bind(paused_template_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                commit_leg_to_train_is_a_no_op_once_already_committed \
                -- --ignored --test-threads=1`"]
    async fn commit_leg_to_train_is_a_no_op_once_already_committed() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-COMMIT-USER";
        seed_user(&pool, user_id).await;
        let today: chrono::NaiveDate = "2026-09-23".parse().unwrap();

        let journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name) VALUES ($1, NULL) RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed journey");

        let journey_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, 'unmatched') RETURNING id",
        )
        .bind(journey_id)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("seed journey leg");

        let sub1_id: i64 = sqlx::query_scalar(
            "INSERT INTO train_subscriptions (user_id, service_date) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("seed first train_subscriptions row");

        let sub2_id: i64 = sqlx::query_scalar(
            "INSERT INTO train_subscriptions (user_id, service_date) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("seed second train_subscriptions row");

        let first_commit = commit_leg_to_train(&pool, journey_leg_id, sub1_id)
            .await
            .expect("first commit");
        assert!(first_commit, "the first commit must succeed");

        let second_commit = commit_leg_to_train(&pool, journey_leg_id, sub2_id)
            .await
            .expect("second commit attempt");
        assert!(
            !second_commit,
            "a second commit attempt on an already-committed leg must be a no-op"
        );

        let (train_subscription_id, match_mode): (Option<i64>, String) = sqlx::query_as(
            "SELECT train_subscription_id, match_mode FROM journey_legs WHERE id = $1",
        )
        .bind(journey_leg_id)
        .fetch_one(&pool)
        .await
        .expect("read leg after both commit attempts");
        assert_eq!(
            train_subscription_id,
            Some(sub1_id),
            "the FIRST train_subscription_id must survive a second commit attempt"
        );
        assert_eq!(match_mode, "auto");

        sqlx::query("DELETE FROM journey_legs WHERE id = $1")
            .bind(journey_leg_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE id IN ($1, $2)")
            .bind(sub1_id)
            .bind(sub2_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                unmatched_notification_state_round_trips_without_clobbering_skip_state \
                -- --ignored --test-threads=1`"]
    async fn unmatched_notification_state_round_trips_without_clobbering_skip_state() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-UNMATCHED-STATE-USER";
        seed_user(&pool, user_id).await;
        let today: chrono::NaiveDate = "2026-09-23".parse().unwrap();

        let journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name) VALUES ($1, NULL) RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed journey");

        let journey_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, 'unmatched') RETURNING id",
        )
        .bind(journey_id)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("seed journey leg");

        let now = Utc::now();
        upsert_skip_notification_state(&pool, user_id, journey_leg_id, true, now)
            .await
            .expect("seed a skip-state row via Phase 3's existing upsert");

        upsert_unmatched_notification_state(&pool, user_id, journey_leg_id, now)
            .await
            .expect("upsert_unmatched_notification_state");

        assert_eq!(
            skip_notification_state(&pool, user_id, journey_leg_id)
                .await
                .expect("read skip state after the unmatched upsert"),
            Some(true),
            "upsert_unmatched_notification_state must not clobber the existing skip-state row"
        );
        assert_eq!(
            unmatched_notification_state(&pool, user_id, journey_leg_id)
                .await
                .expect("read unmatched state"),
            Some(true)
        );

        sqlx::query("DELETE FROM journey_legs WHERE id = $1")
            .bind(journey_leg_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }
}
