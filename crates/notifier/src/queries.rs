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

/// Upserts a zero row on first use -- the migration declares the table's
/// shape but deliberately does not seed rows (Task 1), so the first ever
/// poll cycle for a given `name` creates its own starting-at-zero cursor
/// here.
pub async fn read_cursor(pool: &PgPool, name: &str) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO notifier_cursor (name, last_processed_id) VALUES ($1, 0) \
         ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name \
         RETURNING last_processed_id",
    )
    .bind(name)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

pub async fn advance_cursor(pool: &PgPool, name: &str, new_value: i64) -> anyhow::Result<()> {
    sqlx::query("UPDATE notifier_cursor SET last_processed_id = $1 WHERE name = $2")
        .bind(new_value)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
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

/// One correlated subquery per row to find "the immediately preceding
/// line_status_history row for this same line_id" (Decision 3's guard --
/// NULL previous_statuses means none exists). This workspace's existing
/// data-volume scale ("single trusted personal instance", per DESIGN.md)
/// doesn't justify a window-function rewrite for this; revisit if line
/// count/history volume ever grows enough to matter.
pub async fn poll_line_candidates(
    pool: &PgPool,
    since_id: i64,
) -> anyhow::Result<Vec<LineCandidate>> {
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

    let mut candidates = Vec::new();
    for row in rows {
        let statuses: Vec<LineStatus> = serde_json::from_value(row.statuses)?;
        let new_rank = worst_rank(&statuses);

        let previous_rank = match row.previous_statuses {
            None => None,
            Some(previous_json) => {
                let previous_statuses: Vec<LineStatus> = serde_json::from_value(previous_json)?;
                Some(worst_rank(&previous_statuses))
            }
        };

        if crate::decision::is_severity_transition(previous_rank, new_rank) {
            // Safe: is_severity_transition returning true already requires
            // previous_rank to be Some (its None branch always returns
            // false) -- see the LineCandidate.previous_rank field comment.
            candidates.push(LineCandidate {
                id: row.id,
                line_id: row.line_id,
                new_rank,
                previous_rank: previous_rank.expect("checked by is_severity_transition"),
            });
        }
    }
    Ok(candidates)
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

    let subscribers =
        sqlx::query("SELECT id, user_id FROM train_subscriptions WHERE trains_id = $1")
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
/// `today` (no-op, not an error) or if the template has zero legs (should
/// be unreachable given Phase B's own validation, but defensively a no-op
/// rather than a partially-minted journey).
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
           AND jl.destination_crs IS NOT NULL \
           AND (jl.depart_after IS NOT NULL OR jl.arrive_after IS NOT NULL)",
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
/// Called from `main.rs`'s `run_template_sweep_cycle` (Task 5, stage 2) --
/// also exercised directly by this module's own `sweep_tests`.
pub async fn find_or_create_train(
    pool: &PgPool,
    train_uid: &str,
    service_date: chrono::NaiveDate,
) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO trains (train_uid, service_date) VALUES ($1, $2) \
         ON CONFLICT (train_uid, service_date) DO UPDATE SET train_uid = EXCLUDED.train_uid \
         RETURNING id",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Duplicates `crates/api::data::train_tracking::create_subscription_for_train`
/// -- same CTE idempotency idiom, same accepted "ordinary repeat case
/// only" concurrency caveat as the original's own doc comment states.
///
/// Called from `main.rs`'s `run_template_sweep_cycle` (Task 5, stage 2) --
/// also exercised directly by this module's own `sweep_tests`.
pub async fn create_subscription_for_train(
    pool: &PgPool,
    trains_id: i64,
    user_id: &str,
) -> anyhow::Result<i64> {
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
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Commits a leg to a train working -- the auto-commit sibling of
/// `crates/api::data::journeys::set_leg_train_subscription`, but sets
/// `match_mode = 'auto'` (never `'manual'`) and is guarded by `AND
/// match_mode = 'unmatched'` so a leg already committed by a concurrent
/// tick (or since raced-and-lost) is a silent no-op, not a double write.
///
/// Called from `main.rs`'s `run_template_sweep_cycle` (Task 5, stage 2) --
/// also exercised directly by this module's own `sweep_tests`.
pub async fn commit_leg_to_train(
    pool: &PgPool,
    journey_leg_id: i64,
    train_subscription_id: i64,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE journey_legs SET train_subscription_id = $1, match_mode = 'auto' \
         WHERE id = $2 AND match_mode = 'unmatched'",
    )
    .bind(train_subscription_id)
    .bind(journey_leg_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
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
        let start = read_cursor(&pool, cursor_name).await.expect("read cursor");
        let first_pass = poll_line_candidates(&pool, start)
            .await
            .expect("first poll");
        let candidate = first_pass
            .iter()
            .find(|c| c.line_id == line_id)
            .expect("the transition must be a candidate");
        assert_eq!(candidate.previous_rank, 0);
        assert!(candidate.new_rank > 0);

        let max_id = first_pass.iter().map(|c| c.id).max().unwrap_or(start);
        advance_cursor(&pool, cursor_name, max_id)
            .await
            .expect("advance");

        let second_pass = poll_line_candidates(&pool, max_id)
            .await
            .expect("second poll");
        assert!(
            second_pass.iter().all(|c| c.line_id != line_id),
            "an unchanged table must produce zero new candidates for this line on a repeat poll"
        );

        cleanup_line_history(&pool, line_id).await;
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
