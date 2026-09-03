//! `notifier`: polls line_status_history/train_movement_events by
//! watermark and sends Web Push notifications for real severity/status
//! transitions on a user's pinned lines/tracked trains. See
//! docs/superpowers/specs/2026-09-02-line-status-notifications-design.md.

mod config;
mod decision;
mod queries;
mod send;

use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use config::Config;
use send::{NotificationPayload, SendOutcome, send_to_subscription};
use sqlx::PgPool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let config = Config::parse();

    // Fail fast rather than silently no-op every cycle -- matches
    // crates/api/src/app.rs's existing `ensure!(!config.internal_token.is_empty(), ...)`
    // posture (see this plan's Error handling section).
    anyhow::ensure!(!config.vapid_private_key.is_empty(), "vapid_private_key (--vapid-private-key / VAPID_PRIVATE_KEY) must not be empty");
    anyhow::ensure!(!config.vapid_public_key.is_empty(), "vapid_public_key (--vapid-public-key / VAPID_PUBLIC_KEY) must not be empty");
    anyhow::ensure!(!config.vapid_subject.is_empty(), "vapid_subject (--vapid-subject / VAPID_SUBJECT) must not be empty");

    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::new(&config.log_level)).init();

    let pool = sqlx::postgres::PgPoolOptions::new().max_connections(5).connect(&config.database_url).await?;

    let cooldown = chrono::Duration::minutes(config.cooldown_minutes);
    let mut interval = tokio::time::interval(Duration::from_secs(config.poll_interval_secs));
    loop {
        interval.tick().await;
        let result = run_cycle(
            &pool,
            cooldown,
            config.train_delay_threshold_minutes,
            &config.vapid_private_key,
            &config.vapid_subject,
        )
        .await;
        if let Err(err) = result {
            tracing::error!(error = ?err, "notifier cycle failed; will retry next interval");
        }
    }
}

async fn run_cycle(
    pool: &PgPool,
    cooldown: chrono::Duration,
    train_delay_threshold_minutes: i32,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<()> {
    let now = Utc::now();

    // --- Lines (Decision 2/3/5) ---
    let line_cursor_start = queries::read_cursor(pool, "line_status_history").await?;
    let line_candidates = queries::poll_line_candidates(pool, line_cursor_start).await?;
    let line_max_id = line_candidates.iter().map(|c| c.id).max().unwrap_or(line_cursor_start);

    for candidate in &line_candidates {
        let user_ids = queries::pinned_users_for_line(pool, &candidate.line_id).await?;
        for user_id in user_ids {
            let state = queries::line_notification_state(pool, &user_id, &candidate.line_id).await?;
            let (last_notified_rank, last_notified_at) = match state {
                Some((rank, at)) => (Some(rank), Some(at)),
                None => (None, None),
            };
            let decision = decision::decide_user_notification(
                candidate.previous_rank,
                candidate.new_rank,
                last_notified_rank,
                last_notified_at,
                now,
                cooldown,
            );
            if decision != decision::NotifyDecision::NotifyNow {
                continue;
            }

            let payload = NotificationPayload {
                title: "Line status changed".to_string(),
                body: format!("{} has a new status.", candidate.line_id),
                url: format!("/lines/{}", candidate.line_id),
                tag: format!("line-{}", candidate.line_id),
            };
            if send_to_all_subscriptions(pool, &user_id, &payload, vapid_private_key, vapid_subject).await? {
                queries::upsert_line_notification_state(pool, &user_id, &candidate.line_id, candidate.new_rank, now).await?;
            }
        }
    }
    queries::advance_cursor(pool, "line_status_history", line_max_id).await?;

    // --- Trains (Decision 4) ---
    let train_cursor_start = queries::read_cursor(pool, "train_movement_events").await?;
    let (train_candidates, train_max_id) =
        queries::poll_train_candidates(pool, train_cursor_start, train_delay_threshold_minutes).await?;

    for candidate in &train_candidates {
        tracing::info!(
            tracked_train_id = candidate.tracked_train_id,
            previous_rank = candidate.previous_rank,
            new_rank = candidate.new_rank,
            "train notification candidate"
        );
        let (status, delay_minutes) = current_train_state(pool, candidate.tracked_train_id).await?;
        let payload = NotificationPayload {
            title: if status == "cancelled" { "Your train was cancelled".to_string() } else { "Your train is delayed".to_string() },
            body: match delay_minutes {
                Some(minutes) if status != "cancelled" => format!("Now running about {minutes} minutes late."),
                _ => "Check the latest status.".to_string(),
            },
            url: format!("/track/{}", candidate.tracked_train_id),
            tag: format!("train-{}", candidate.tracked_train_id),
        };
        if send_to_all_subscriptions(pool, &candidate.user_id, &payload, vapid_private_key, vapid_subject).await? {
            queries::upsert_train_notification_state(pool, &candidate.user_id, candidate.tracked_train_id, &status, delay_minutes, now)
                .await?;
        }
    }
    queries::advance_cursor(pool, "train_movement_events", train_max_id).await?;

    Ok(())
}

async fn current_train_state(pool: &PgPool, tracked_train_id: i64) -> anyhow::Result<(String, Option<i32>)> {
    use sqlx::Row;
    let row = sqlx::query("SELECT status, delay_minutes FROM train_current_state WHERE tracked_train_id = $1")
        .bind(tracked_train_id)
        .fetch_one(pool)
        .await?;
    Ok((row.try_get("status")?, row.try_get("delay_minutes")?))
}

/// Sends to every device this user has subscribed on (Decision 5's
/// per-user, not per-subscription, fan-out). Returns true if at least one
/// send succeeded (or the user has zero subscriptions -- see below) --
/// callers use this to decide whether to update notification_state.
///
/// A user with zero push_subscriptions rows still counts as "handled" (not
/// a failure) -- notification_state still advances so a later real
/// subscription doesn't immediately fire a backlog of stale transitions.
async fn send_to_all_subscriptions(
    pool: &PgPool,
    user_id: &str,
    payload: &NotificationPayload,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<bool> {
    let subscriptions = queries::push_subscriptions_for_user(pool, user_id).await?;
    if subscriptions.is_empty() {
        return Ok(true);
    }
    let mut any_ok = false;
    for subscription in &subscriptions {
        match send_to_subscription(vapid_private_key, vapid_subject, subscription, payload).await {
            SendOutcome::Sent => any_ok = true,
            SendOutcome::Expired => {
                queries::delete_push_subscription(pool, subscription.id).await?;
            }
            SendOutcome::TransientFailure => {
                tracing::warn!(user_id, endpoint = %subscription.endpoint, "transient push send failure, will retry next real transition");
            }
        }
    }
    Ok(any_ok)
}

#[cfg(test)]
mod db_tests {
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

    async fn cleanup(pool: &PgPool, user_id: &str, line_id: &str) {
        sqlx::query("DELETE FROM line_notification_state WHERE user_id = $1").bind(user_id).execute(pool).await.expect("cleanup state");
        sqlx::query("DELETE FROM push_subscriptions WHERE user_id = $1").bind(user_id).execute(pool).await.expect("cleanup subs");
        sqlx::query("DELETE FROM pinned_lines WHERE user_id = $1").bind(user_id).execute(pool).await.expect("cleanup pins");
        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1").bind(line_id).execute(pool).await.expect("cleanup history");
        sqlx::query("DELETE FROM notifier_cursor WHERE name = 'line_status_history'").execute(pool).await.expect("cleanup cursor");
        sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(pool).await.expect("cleanup user");
    }

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

    /// End-to-end: seeds a `pinned_lines` row + two `line_status_history`
    /// rows for the same `line_id` at different ranks, runs `run_cycle`
    /// once, and asserts the DB side effect (`line_notification_state`) --
    /// NOT send success, which isn't meaningfully testable without a real
    /// push endpoint (see this plan's Task 6, Step 4 and the spec's own
    /// Testing section). A second `run_cycle` with no new data must not
    /// panic and must leave the state unchanged (idempotent).
    ///
    /// Deliberately seeds NO `push_subscriptions` row: `send_to_all_subscriptions`
    /// only advances `line_notification_state` when a send actually
    /// succeeds OR the user has zero subscriptions ("still counts as
    /// handled" -- see that function's own doc comment) -- a subscription
    /// pointed at an invalid endpoint would genuinely fail the send and
    /// (correctly) leave the state untouched, which would make this test
    /// non-deterministic about what it's actually checking. The
    /// subscription row's own lifecycle (round-trip, delete-on-expiry) is
    /// already covered in isolation by `queries::tests::push_subscriptions_round_trip_and_self_cleanup_on_delete`.
    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p notifier \
                run_cycle -- --ignored --test-threads=1`"]
    async fn run_cycle_notifies_a_real_transition_and_is_idempotent_on_replay() {
        let pool = connect().await;
        let user_id = "TEST-NOTIFIER-CYCLE-USER";
        let line_id = "TEST-NOTIFIER-CYCLE-LINE";
        cleanup(&pool, user_id, line_id).await;
        seed_user(&pool, user_id).await;

        sqlx::query("INSERT INTO pinned_lines (user_id, line_id, pinned_at) VALUES ($1, $2, NOW())")
            .bind(user_id)
            .bind(line_id)
            .execute(&pool)
            .await
            .expect("seed pin");

        sqlx::query("INSERT INTO line_status_history (line_id, statuses, computed_at) VALUES ($1, $2, NOW())")
            .bind(line_id)
            .bind(status_json(common::Severity::GoodService))
            .execute(&pool)
            .await
            .expect("seed first history row");
        sqlx::query("INSERT INTO line_status_history (line_id, statuses, computed_at) VALUES ($1, $2, NOW())")
            .bind(line_id)
            .bind(status_json(common::Severity::SevereDelays))
            .execute(&pool)
            .await
            .expect("seed second (transitioned) history row");

        let cooldown = chrono::Duration::minutes(20);
        run_cycle(&pool, cooldown, 15, "not-a-real-vapid-key", "mailto:test@example.invalid")
            .await
            .expect("run_cycle must return Ok even though the send itself fails against an invalid endpoint");

        let (rank, _at) = queries::line_notification_state(&pool, user_id, line_id)
            .await
            .expect("read state")
            .expect("a notification_state row must exist after a real transition, even if the actual push send failed");
        assert!(rank > 0, "state should reflect the escalated rank");

        // Idempotent replay: no new history, must not panic and must
        // leave the state unchanged.
        run_cycle(&pool, cooldown, 15, "not-a-real-vapid-key", "mailto:test@example.invalid")
            .await
            .expect("second run_cycle must also return Ok");
        let (rank_after_replay, _) = queries::line_notification_state(&pool, user_id, line_id).await.expect("read state again").expect("state must still exist");
        assert_eq!(rank_after_replay, rank, "a replay with no new data must not change the stored state");

        cleanup(&pool, user_id, line_id).await;
    }
}
