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
    statuses.iter().map(|s| severity_rank(s.severity)).min().unwrap_or(0)
}

/// One correlated subquery per row to find "the immediately preceding
/// line_status_history row for this same line_id" (Decision 3's guard --
/// NULL previous_statuses means none exists). This workspace's existing
/// data-volume scale ("single trusted personal instance", per DESIGN.md)
/// doesn't justify a window-function rewrite for this; revisit if line
/// count/history volume ever grows enough to matter.
pub async fn poll_line_candidates(pool: &PgPool, since_id: i64) -> anyhow::Result<Vec<LineCandidate>> {
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
    pub user_id: String,
    pub new_rank: u8,
    pub previous_rank: u8,
}

/// Per Task 3's design notes: trains have exactly one owning user per
/// `tracked_trains` row, so there is no separate table-level-vs-per-user
/// split here -- `train_notification_state` doubles as both. Candidates
/// are found by touching `train_movement_events` for the watermark, but
/// the actual current status/delay come from `train_current_state`
/// (the only place a train's *current* derived state lives -- see this
/// plan's Status note on journey.rs). Returns candidates plus the max
/// event id seen (for cursor advancement even over trains this cycle
/// found no notify-worthy transition for).
pub async fn poll_train_candidates(
    pool: &PgPool,
    since_id: i64,
    delay_threshold_minutes: i32,
) -> anyhow::Result<(Vec<TrainCandidate>, i64)> {
    let touched = sqlx::query("SELECT DISTINCT tracked_train_id FROM train_movement_events WHERE id > $1")
        .bind(since_id)
        .fetch_all(pool)
        .await?;

    if touched.is_empty() {
        return Ok((Vec::new(), since_id));
    }

    let max_id: i64 = sqlx::query_scalar("SELECT MAX(id) FROM train_movement_events WHERE id > $1")
        .bind(since_id)
        .fetch_one(pool)
        .await?;

    let mut candidates = Vec::new();
    for row in &touched {
        let tracked_train_id: i64 = row.try_get("tracked_train_id")?;

        let current = sqlx::query(
            "SELECT t.user_id, s.status, s.delay_minutes \
             FROM tracked_trains t JOIN train_current_state s ON s.tracked_train_id = t.id \
             WHERE t.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_optional(pool)
        .await?;
        let Some(current) = current else { continue }; // no current-state row yet -- nothing to compare

        let user_id: String = current.try_get("user_id")?;
        let status: String = current.try_get("status")?;
        let delay_minutes: Option<i32> = current.try_get("delay_minutes")?;
        let new_rank = train_severity_rank(&status, delay_minutes, delay_threshold_minutes);

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
                let previous_delay: Option<i32> = previous.try_get("last_notified_delay_minutes")?;
                train_severity_rank(&previous_status, previous_delay, delay_threshold_minutes)
            }
        };

        if crate::decision::decide_train_notification(previous_rank, new_rank) == crate::decision::NotifyDecision::NotifyNow {
            candidates.push(TrainCandidate { tracked_train_id, user_id, new_rank, previous_rank });
        }
    }
    Ok((candidates, max_id))
}

pub async fn pinned_users_for_line(pool: &PgPool, line_id: &str) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query_scalar::<_, String>("SELECT user_id FROM pinned_lines WHERE line_id = $1")
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
pub struct PushSubscriptionRow {
    pub id: i64,
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
}

pub async fn push_subscriptions_for_user(pool: &PgPool, user_id: &str) -> anyhow::Result<Vec<PushSubscriptionRow>> {
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
    sqlx::query("DELETE FROM push_subscriptions WHERE id = $1").bind(id).execute(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn connect() -> PgPool {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new().connect(&database_url).await.expect("connect to postgres")
    }

    async fn seed_user(pool: &PgPool, user_id: &str) {
        sqlx::query("INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING")
            .bind(user_id)
            .bind(format!("{user_id}@example.com"))
            .bind(user_id)
            .execute(pool)
            .await
            .expect("seed fixture user");
    }

    async fn cleanup_line_history(pool: &PgPool, line_id: &str) {
        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1").bind(line_id).execute(pool).await.expect("cleanup history");
        sqlx::query("DELETE FROM notifier_cursor WHERE name = 'line_status_history'").execute(pool).await.expect("cleanup cursor");
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
            validity: common::ValidityPeriod { from_date: chrono::Utc::now(), to_date: None, is_now: true },
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
        let first_pass = poll_line_candidates(&pool, start).await.expect("first poll");
        let candidate = first_pass.iter().find(|c| c.line_id == line_id).expect("the transition must be a candidate");
        assert_eq!(candidate.previous_rank, 0);
        assert!(candidate.new_rank > 0);

        let max_id = first_pass.iter().map(|c| c.id).max().unwrap_or(start);
        advance_cursor(&pool, cursor_name, max_id).await.expect("advance");

        let second_pass = poll_line_candidates(&pool, max_id).await.expect("second poll");
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

        let subscriptions = push_subscriptions_for_user(&pool, "TEST-NOTIFIER-CLEANUP-USER").await.expect("list");
        let seeded = subscriptions
            .iter()
            .find(|s| s.endpoint == "https://push.example/ep-cleanup-test")
            .expect("seeded row must be listed");

        delete_push_subscription(&pool, seeded.id).await.expect("delete");

        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM push_subscriptions WHERE endpoint = $1")
            .bind("https://push.example/ep-cleanup-test")
            .fetch_one(&pool)
            .await
            .expect("count after delete");
        assert_eq!(remaining, 0, "delete_push_subscription must actually remove the row");

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind("TEST-NOTIFIER-CLEANUP-USER")
            .execute(&pool)
            .await
            .expect("cleanup fixture user");
    }
}
