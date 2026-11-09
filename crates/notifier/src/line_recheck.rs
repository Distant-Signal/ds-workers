//! Lines: a de-escalation that landed inside a user's cooldown is decided
//! again once the cooldown has passed (DS-425).
//!
//! `run_cycle` decides each `line_status_history` transition once, from the
//! row alone, and then moves its cursor past it.
//! [`crate::decision::decide_user_notification`] skips a de-escalation while
//! the user's last push for the line is under `cooldown` old, so a line that
//! went Severe (pushed), then Minor 5 minutes later was never told about the
//! recovery: the history row was consumed, and nothing looked at the user's
//! stored state again. A user's `line_notification_state` is the memory this
//! needs: it names the rank they were last told and when, so a stored rank
//! above the line's current one, past the cooldown, is exactly a dropped
//! de-escalation.
//!
//! Each cycle [`recheck_deferred_deescalations`] looks, for the users who
//! pin the line with alerts on, at stored states pushed between `cooldown`
//! and [`RECHECK_WINDOW`] ago whose line is now at a lower rank, and decides
//! them with [`crate::decision::decide_deferred_deescalation`]. It never
//! notifies twice for one rank (the push records the rank it names, so the
//! state then matches), never for an escalation (that is `run_cycle`'s, at
//! once), and the ranks are `common::severity_rank`'s, never `Severity`'s
//! `Ord`. Trains and skipped stops stay escalation-only.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;

use crate::decision::{NotifyDecision, decide_deferred_deescalation};
use crate::push_queue::{Delivered, PgBackend, PushJob, PushQueue};
use crate::send::NotificationPayload;

/// How long after its last push a user's line state is looked at again. A
/// dropped de-escalation is due when the cooldown ends, so this only has to
/// cover a few cycles' lateness; past it the change is old news and the
/// line's own next transition speaks for it.
pub(crate) const RECHECK_WINDOW: Duration = Duration::minutes(60);

/// One user's stored line state with the line's newest statuses.
#[derive(Debug, sqlx::FromRow)]
struct StateRow {
    user_id: String,
    line_id: String,
    last_notified_severity_rank: i16,
    last_notified_at: DateTime<Utc>,
    statuses: serde_json::Value,
}

/// A de-escalation to push now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeferredDeescalation {
    pub user_id: String,
    pub line_id: String,
    pub rank: u8,
}

/// The dropped de-escalations due at `now`: for each pinned line (alerts on,
/// line pushes not turned off) whose user's last push was between `cooldown`
/// and [`RECHECK_WINDOW`] ago, the line's newest status ranked, kept when
/// lower than what the user was last told and decided
/// [`decide_deferred_deescalation`].
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "severity ranks are 0..=3"
)]
pub(crate) async fn deferred_deescalations(
    pool: &PgPool,
    now: DateTime<Utc>,
    cooldown: Duration,
) -> anyhow::Result<Vec<DeferredDeescalation>> {
    let rows = sqlx::query_as::<_, StateRow>(
        "SELECT s.user_id, s.line_id, s.last_notified_severity_rank, s.last_notified_at, \
                h.statuses \
         FROM line_notification_state s \
         JOIN pinned_lines pl ON pl.user_id = s.user_id AND pl.line_id = s.line_id AND pl.notify \
         LEFT JOIN notification_preferences np ON np.user_id = s.user_id \
         JOIN LATERAL ( \
             SELECT statuses FROM line_status_history \
             WHERE line_id = s.line_id ORDER BY id DESC LIMIT 1 \
         ) h ON TRUE \
         WHERE COALESCE(np.line_status, TRUE) \
           AND s.last_notified_at <= $1 - $2::float8 * INTERVAL '1 second' \
           AND s.last_notified_at > $1 - $3::float8 * INTERVAL '1 second' \
         ORDER BY s.last_notified_at, s.user_id, s.line_id",
    )
    .bind(now)
    .bind(cooldown.num_seconds())
    .bind(RECHECK_WINDOW.num_seconds())
    .fetch_all(pool)
    .await?;
    let mut due = Vec::new();
    for row in rows {
        let Some(current) = crate::queries::rank_of_statuses_json(&row.line_id, row.statuses)
        else {
            continue;
        };
        let last_rank = row.last_notified_severity_rank as u8;
        if decide_deferred_deescalation(last_rank, current, row.last_notified_at, now, cooldown)
            == NotifyDecision::NotifyNow
        {
            due.push(DeferredDeescalation {
                user_id: row.user_id,
                line_id: row.line_id,
                rank: current,
            });
        }
    }
    Ok(due)
}

/// Queues the push for each due de-escalation, the same as `run_cycle`'s
/// line push (so its delivery records the rank and the time, and a later
/// pass finds the state matching).
pub(crate) async fn recheck_deferred_deescalations(
    pool: &PgPool,
    queue: &PushQueue<PgBackend>,
    now: DateTime<Utc>,
    cooldown: Duration,
) -> anyhow::Result<()> {
    for due in deferred_deescalations(pool, now, cooldown).await? {
        tracing::info!(
            user_id = %due.user_id,
            line_id = %due.line_id,
            rank = due.rank,
            "line de-escalation held by the cooldown, sending now"
        );
        crate::hold::dispatch(
            pool,
            queue,
            PushJob {
                user_id: due.user_id.clone(),
                payload: NotificationPayload {
                    title: "Line status changed".to_string(),
                    body: format!("{} has a new status.", due.line_id),
                    url: crate::line_url(&due.line_id),
                    tag: format!("line-{}", due.line_id),
                    renotify: false,
                },
                delivered: Delivered::Line {
                    line_id: due.line_id,
                    rank: due.rank,
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
    use super::*;

    const USER: &str = "TEST-LINE-RECHECK-USER";
    const LINE: &str = "TEST-LINE-RECHECK-LINE";

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        sqlx::postgres::PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    async fn cleanup(pool: &PgPool) {
        for sql in [
            "DELETE FROM line_notification_state WHERE user_id = $1",
            "DELETE FROM pinned_lines WHERE user_id = $1",
            "DELETE FROM users WHERE id = $1",
        ] {
            sqlx::query(sql).bind(USER).execute(pool).await.unwrap();
        }
        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1")
            .bind(LINE)
            .execute(pool)
            .await
            .unwrap();
    }

    fn statuses(severity: common::Severity) -> serde_json::Value {
        let status = common::LineStatus {
            severity,
            reason: String::new(),
            validity: common::ValidityPeriod {
                from_date: Utc::now(),
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
        serde_json::to_value(vec![status]).unwrap()
    }

    async fn set_history(pool: &PgPool, severity: common::Severity) {
        sqlx::query(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) VALUES ($1, $2, now())",
        )
        .bind(LINE)
        .bind(statuses(severity))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn set_state(pool: &PgPool, rank: i16, at: DateTime<Utc>) {
        sqlx::query(
            "INSERT INTO line_notification_state (user_id, line_id, last_notified_severity_rank, \
                                                  last_notified_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (user_id, line_id) DO UPDATE SET \
                 last_notified_severity_rank = EXCLUDED.last_notified_severity_rank, \
                 last_notified_at = EXCLUDED.last_notified_at",
        )
        .bind(USER)
        .bind(LINE)
        .bind(rank)
        .bind(at)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn due(pool: &PgPool, now: DateTime<Utc>) -> Vec<DeferredDeescalation> {
        deferred_deescalations(pool, now, Duration::minutes(20))
            .await
            .unwrap()
            .into_iter()
            .filter(|d| d.user_id == USER)
            .collect()
    }

    /// Severe pushed, the line recovers inside the cooldown (dropped by
    /// `run_cycle`), and the recheck sends the recovery once the cooldown
    /// has passed, once.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p notifier \
                line_recheck -- --ignored --test-threads=1"]
    async fn a_dropped_deescalation_is_sent_once_the_cooldown_has_passed() {
        let pool = connect().await;
        cleanup(&pool).await;
        sqlx::query("INSERT INTO users (id, name) VALUES ($1, $1)")
            .bind(USER)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO pinned_lines (user_id, line_id, pinned_at) VALUES ($1, $2, now())",
        )
        .bind(USER)
        .bind(LINE)
        .execute(&pool)
        .await
        .unwrap();
        let now = Utc::now();
        let severe = i16::from(common::severity_rank(common::Severity::SevereDelays));
        let good = common::severity_rank(common::Severity::GoodService);

        // Told "severe" 5 minutes ago; the line is now good: still in the
        // cooldown, nothing yet.
        set_history(&pool, common::Severity::SevereDelays).await;
        set_history(&pool, common::Severity::GoodService).await;
        set_state(&pool, severe, now - Duration::minutes(5)).await;
        assert!(due(&pool, now).await.is_empty(), "inside the cooldown");

        // 25 minutes on: due, as the new rank.
        set_state(&pool, severe, now - Duration::minutes(25)).await;
        assert_eq!(
            due(&pool, now).await,
            vec![DeferredDeescalation {
                user_id: USER.to_string(),
                line_id: LINE.to_string(),
                rank: good,
            }]
        );

        // Delivery records the rank it named: never twice for that rank.
        set_state(&pool, i16::from(good), now).await;
        assert!(
            due(&pool, now + Duration::minutes(30)).await.is_empty(),
            "a rank already delivered is not decided again"
        );

        // An escalation is never this pass's: the line got worse than the
        // stored rank.
        set_history(&pool, common::Severity::SevereDelays).await;
        set_state(&pool, i16::from(good), now - Duration::minutes(25)).await;
        assert!(
            due(&pool, now).await.is_empty(),
            "escalation is run_cycle's"
        );

        // Too old: past the window the change is old news.
        set_history(&pool, common::Severity::GoodService).await;
        set_state(&pool, severe, now - Duration::minutes(90)).await;
        assert!(due(&pool, now).await.is_empty(), "past the window");

        // Alerts off for the pin: nothing.
        set_state(&pool, severe, now - Duration::minutes(25)).await;
        sqlx::query("UPDATE pinned_lines SET notify = FALSE WHERE user_id = $1")
            .bind(USER)
            .execute(&pool)
            .await
            .unwrap();
        assert!(due(&pool, now).await.is_empty(), "alerts off");
        cleanup(&pool).await;
    }
}
