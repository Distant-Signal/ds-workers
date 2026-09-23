//! `notifier`: polls line_status_history/train_movement_events by
//! watermark and sends Web Push notifications for real severity/status
//! transitions on a user's pinned lines/tracked trains. See
//! docs/superpowers/specs/2026-09-02-line-status-notifications-design.md.

mod config;
mod decision;
mod queries;
mod send;
mod skip_check;

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
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
    anyhow::ensure!(
        !config.vapid_private_key.is_empty(),
        "vapid_private_key (--vapid-private-key / VAPID_PRIVATE_KEY) must not be empty"
    );
    anyhow::ensure!(
        !config.vapid_public_key.is_empty(),
        "vapid_public_key (--vapid-public-key / VAPID_PUBLIC_KEY) must not be empty"
    );
    anyhow::ensure!(
        !config.vapid_subject.is_empty(),
        "vapid_subject (--vapid-subject / VAPID_SUBJECT) must not be empty"
    );

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&config.log_level))
        .init();

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await?;

    let cooldown = chrono::Duration::minutes(config.cooldown_minutes);
    let mut interval = tokio::time::interval(Duration::from_secs(config.poll_interval_secs));
    let mut forward_interval =
        tokio::time::interval(Duration::from_secs(config.forward_queue_poll_interval_secs));
    let mut skip_check_interval =
        tokio::time::interval(Duration::from_secs(config.skip_check_poll_interval_secs));
    let mut template_sweep_interval = tokio::time::interval(Duration::from_secs(
        config.template_sweep_poll_interval_secs,
    ));
    loop {
        tokio::select! {
            _ = interval.tick() => {
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
            _ = forward_interval.tick() => {
                let result = run_forward_queue_cycle(
                    &pool,
                    config.train_delay_threshold_minutes,
                    &config.vapid_private_key,
                    &config.vapid_subject,
                )
                .await;
                if let Err(err) = result {
                    tracing::error!(error = ?err, "notifier forward-queue cycle failed; will retry next interval");
                }
            }
            _ = skip_check_interval.tick() => {
                let result = run_skip_check_cycle(
                    &pool,
                    &config.vapid_private_key,
                    &config.vapid_subject,
                )
                .await;
                if let Err(err) = result {
                    tracing::error!(error = ?err, "notifier skip-check cycle failed; will retry next interval");
                }
            }
            _ = template_sweep_interval.tick() => {
                let result = run_template_sweep_cycle(
                    &pool,
                    config.auto_commit_lead_minutes,
                    &config.vapid_private_key,
                    &config.vapid_subject,
                )
                .await;
                if let Err(err) = result {
                    tracing::error!(error = ?err, "notifier template-sweep cycle failed; will retry next interval");
                }
            }
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
    let line_max_id = line_candidates
        .iter()
        .map(|c| c.id)
        .max()
        .unwrap_or(line_cursor_start);

    for candidate in &line_candidates {
        let user_ids = queries::pinned_users_for_line(pool, &candidate.line_id).await?;
        for user_id in user_ids {
            let state =
                queries::line_notification_state(pool, &user_id, &candidate.line_id).await?;
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
            if send_to_all_subscriptions(pool, &user_id, &payload, vapid_private_key, vapid_subject)
                .await?
            {
                queries::upsert_line_notification_state(
                    pool,
                    &user_id,
                    &candidate.line_id,
                    candidate.new_rank,
                    now,
                )
                .await?;
            }
        }
    }
    queries::advance_cursor(pool, "line_status_history", line_max_id).await?;

    // --- Trains (Decision 4) ---
    let train_cursor_start = queries::read_cursor(pool, "train_movement_events").await?;
    let (train_candidates, train_max_id) =
        queries::poll_train_candidates(pool, train_cursor_start, train_delay_threshold_minutes)
            .await?;
    notify_train_candidates(
        pool,
        &train_candidates,
        vapid_private_key,
        vapid_subject,
        now,
    )
    .await?;
    queries::advance_cursor(pool, "train_movement_events", train_max_id).await?;

    Ok(())
}

/// The train-notification-sending body shared by `run_cycle`'s own
/// `train_movement_events` poll AND `run_forward_queue_cycle`'s faster
/// `notifier_forward_queue` poll (Task 18) -- ONE decision path fed by two
/// inputs, never a second, divergent copy of the cooldown/escalation send
/// logic (per the design spec's own §6 non-goal on redesigning escalation
/// logic).
async fn notify_train_candidates(
    pool: &PgPool,
    candidates: &[queries::TrainCandidate],
    vapid_private_key: &str,
    vapid_subject: &str,
    now: chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    for candidate in candidates {
        tracing::info!(
            tracked_train_id = candidate.tracked_train_id,
            trains_id = candidate.trains_id,
            previous_rank = candidate.previous_rank,
            new_rank = candidate.new_rank,
            "train notification candidate"
        );
        let (status, delay_minutes) = current_train_state(pool, candidate.trains_id).await?;
        let journey_context =
            queries::journey_leg_for_train_subscription(pool, candidate.tracked_train_id).await?;
        let payload = build_train_notification_payload(
            candidate.tracked_train_id,
            &status,
            delay_minutes,
            journey_context.as_ref(),
        );
        if send_to_all_subscriptions(
            pool,
            &candidate.user_id,
            &payload,
            vapid_private_key,
            vapid_subject,
        )
        .await?
        {
            queries::upsert_train_notification_state(
                pool,
                &candidate.user_id,
                candidate.tracked_train_id,
                &status,
                delay_minutes,
                now,
            )
            .await?;
        }
    }
    Ok(())
}

/// Builds the delay/cancellation `NotificationPayload` -- journey/leg-aware
/// when `journey_context` names a genuine multi-leg journey (`total_legs >
/// 1`), otherwise byte-for-byte today's plain copy (Judgment Call 5). CRS
/// codes, not resolved station names, in the multi-leg body (Judgment
/// Call 6). Extracted out of `notify_train_candidates` as its own pure
/// function so this copy-building logic (the ONLY thing §5.1 changes,
/// design spec's own framing) is independently testable without a database.
fn build_train_notification_payload(
    tracked_train_id: i64,
    status: &str,
    delay_minutes: Option<i32>,
    journey_context: Option<&queries::JourneyLegContext>,
) -> NotificationPayload {
    let is_cancelled = status == "cancelled";

    let multi_leg = journey_context.filter(|ctx| ctx.total_legs > 1);
    let Some(ctx) = multi_leg else {
        // Fallback: no journey_legs row, or a trivial one-leg journey --
        // today's exact copy/URL, unchanged.
        return NotificationPayload {
            title: if is_cancelled {
                "Your train was cancelled".to_string()
            } else {
                "Your train is delayed".to_string()
            },
            body: match delay_minutes {
                Some(minutes) if !is_cancelled => {
                    format!("Now running about {minutes} minutes late.")
                }
                _ => "Check the latest status.".to_string(),
            },
            url: format!("/track/{tracked_train_id}"),
            tag: format!("train-{tracked_train_id}"),
        };
    };

    let journey_label = match &ctx.journey_name {
        Some(name) => format!("'{name}'"),
        None => "your journey".to_string(),
    };
    let route = match (&ctx.origin_crs, &ctx.destination_crs) {
        (Some(origin), Some(destination)) => Some(format!("{origin} to {destination}")),
        _ => None,
    };

    let title = if is_cancelled {
        format!("Leg {} of {journey_label} was cancelled", ctx.leg_order)
    } else {
        format!("Leg {} of {journey_label} is delayed", ctx.leg_order)
    };
    let body = match (is_cancelled, delay_minutes, &route) {
        (true, _, Some(route)) => format!("The {route} service was cancelled."),
        (true, _, None) => "This service was cancelled.".to_string(),
        (false, Some(minutes), Some(route)) => {
            format!("{route}, now running about {minutes} minutes late.")
        }
        (false, Some(minutes), None) => format!("Now running about {minutes} minutes late."),
        (false, None, _) => "Check the latest status.".to_string(),
    };

    NotificationPayload {
        title,
        body,
        url: format!("/journeys/{}", ctx.journey_id),
        tag: format!("train-{tracked_train_id}"), // unchanged -- still the same underlying tracked-train row
    }
}

/// The forward queue's own, faster-cadence cycle (Task 17/18) -- a second
/// INPUT into `notify_train_candidates`'s same cooldown/escalation logic,
/// never a second decision path. Advances its own `notifier_cursor` row
/// (name `"notifier_forward_queue"`), independent of `run_cycle`'s own
/// `"train_movement_events"` cursor.
async fn run_forward_queue_cycle(
    pool: &PgPool,
    train_delay_threshold_minutes: i32,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let cursor_start = queries::read_cursor(pool, "notifier_forward_queue").await?;
    let (touched_trains_ids, max_id) = queries::poll_forward_queue(pool, cursor_start).await?;
    for trains_id in touched_trains_ids {
        let candidates =
            queries::candidates_for_trains_id(pool, trains_id, train_delay_threshold_minutes)
                .await?;
        notify_train_candidates(pool, &candidates, vapid_private_key, vapid_subject, now).await?;
    }
    queries::advance_cursor(pool, "notifier_forward_queue", max_id).await?;
    Ok(())
}

/// The station-skip check's own cycle (Task 9, §5.2) -- a full poll of
/// today's committed journey legs every `skip_check_poll_interval_secs`,
/// not cursor/watermark-based (see `config.rs`'s own doc comment on why).
/// Each leg is judged independently against its own
/// `journey_leg_notification_state` row -- `decide_skip_notification`'s
/// escalation-only shape, same discipline as every other notification path
/// in this crate: state is written only after a successful send.
async fn run_skip_check_cycle(
    pool: &PgPool,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let today = now.date_naive();
    let legs = queries::list_committed_legs_for_today(pool, today).await?;

    for leg in &legs {
        let is_skipped = skip_check::leg_is_skipped(pool, leg).await?;
        let was_skipped = queries::skip_notification_state(pool, &leg.user_id, leg.journey_leg_id)
            .await?
            .unwrap_or(false);

        // Mirrors notify_train_candidates's own tracing::info! on its
        // candidates -- also the only production (non-test) read of
        // CommittedLeg::trains_id, which leg_is_skipped itself never needs
        // (it matches purely by CRS code, not by the shared physical-train
        // id), so this line is what keeps that field genuinely wired up
        // rather than dead outside of queries.rs's own tests.
        tracing::debug!(
            journey_leg_id = leg.journey_leg_id,
            trains_id = leg.trains_id,
            is_skipped,
            was_skipped,
            "skip-check leg evaluated"
        );

        if decision::decide_skip_notification(was_skipped, is_skipped)
            != decision::NotifyDecision::NotifyNow
        {
            continue;
        }

        let payload = NotificationPayload {
            title: "A stop on your journey is being skipped".to_string(),
            body: format!(
                "Your service between {} and {} is no longer calling at one of those stops today.",
                leg.origin_crs, leg.destination_crs
            ),
            url: format!("/journeys/{}", leg.journey_id),
            tag: format!("journey-leg-skip-{}", leg.journey_leg_id),
        };

        if send_to_all_subscriptions(
            pool,
            &leg.user_id,
            &payload,
            vapid_private_key,
            vapid_subject,
        )
        .await?
        {
            queries::upsert_skip_notification_state(
                pool,
                &leg.user_id,
                leg.journey_leg_id,
                true,
                now,
            )
            .await?;
        }
    }

    Ok(())
}

/// The recurring-journey materialization sweep's own cycle (Task 4/5,
/// spec §3.1-3.2). "Today" is a plain Europe/London calendar date (see
/// this plan's Architecture section for why not a rail day) -- computed
/// once per tick and used for both stages.
async fn run_template_sweep_cycle(
    pool: &PgPool,
    auto_commit_lead_minutes: i64,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let today = now.with_timezone(&chrono_tz::Europe::London).date_naive();

    // --- Stage 1: mint due occurrences ---
    for template in queries::due_templates_for(pool, today).await? {
        match queries::materialize_due_template_occurrence(
            pool,
            template.id,
            &template.user_id,
            template.custom_name.as_deref(),
            today,
        )
        .await
        {
            Ok(Some(journey_id)) => {
                tracing::info!(
                    template_id = template.id,
                    journey_id,
                    "materialized today's occurrence"
                );
            }
            Ok(None) => {} // already materialized this cycle or a prior one today
            Err(err) => {
                tracing::error!(error = ?err, template_id = template.id, "failed to materialize template occurrence; will retry next cycle");
            }
        }
    }

    // --- Stage 2: commit-check due unmatched auto legs ---
    for leg in queries::unmatched_auto_legs_for_commit_check(pool, today).await? {
        let Some(earliest_bound) = leg.depart_after.or(leg.arrive_after) else {
            continue; // guarded by the query's own WHERE, defensive only
        };
        let Some(earliest_bound_utc) = london_to_utc(leg.service_date.and_time(earliest_bound))
        else {
            continue; // nonexistent local time (spring-forward gap) -- best-effort, skip this tick
        };
        if !decision::is_due_for_commit_check(now, earliest_bound_utc, auto_commit_lead_minutes) {
            continue;
        }

        let candidates = queries::schedule_candidates_for_leg(
            pool,
            &leg.origin_crs,
            &leg.destination_crs,
            leg.service_date,
            leg.depart_after,
            leg.depart_before,
            leg.arrive_after,
            leg.arrive_before,
        )
        .await?;

        if candidates.is_empty() {
            let already_notified =
                queries::unmatched_notification_state(pool, &leg.user_id, leg.journey_leg_id)
                    .await?
                    .unwrap_or(false);
            if decision::decide_unmatched_notification(already_notified)
                != decision::NotifyDecision::NotifyNow
            {
                continue;
            }
            let payload = NotificationPayload {
                title: "Your recurring journey needs attention".to_string(),
                body: format!(
                    "No {} to {} service was found for today within your usual window.",
                    leg.origin_crs, leg.destination_crs
                ),
                url: format!("/journeys/{}", leg.journey_id),
                tag: format!("journey-leg-unmatched-{}", leg.journey_leg_id),
            };
            if send_to_all_subscriptions(
                pool,
                &leg.user_id,
                &payload,
                vapid_private_key,
                vapid_subject,
            )
            .await?
            {
                queries::upsert_unmatched_notification_state(
                    pool,
                    &leg.user_id,
                    leg.journey_leg_id,
                    now,
                )
                .await?;
            }
            continue;
        }

        let now_local = now.with_timezone(&chrono_tz::Europe::London).time();
        let scheduled_times: Vec<chrono::NaiveTime> = candidates.iter().map(|(_, t)| *t).collect();
        let Some(winner_idx) = decision::pick_nearest_to_now_candidate(&scheduled_times, now_local)
        else {
            continue; // unreachable given the is_empty() check above, defensive only
        };
        let (train_uid, _) = &candidates[winner_idx];

        let trains_id = queries::find_or_create_train(pool, train_uid, leg.service_date).await?;
        let tracking_id =
            queries::create_subscription_for_train(pool, trains_id, &leg.user_id).await?;
        if !queries::commit_leg_to_train(pool, leg.journey_leg_id, tracking_id).await? {
            tracing::warn!(
                journey_leg_id = leg.journey_leg_id,
                "leg was committed by a concurrent tick before this one finished; skipping"
            );
        } else {
            tracing::info!(
                journey_leg_id = leg.journey_leg_id,
                train_uid,
                "auto-committed leg to nearest-to-now candidate"
            );
        }
    }

    Ok(())
}

/// Resolves a service_date + local wall-clock TIME to the UTC instant it
/// names -- same `LocalResult` handling as
/// `crates/api::data::eta_blend::london_to_utc` (duplicated, per this
/// crate's crate-boundary constraint; that one is `pub(crate)` and
/// unreachable from here anyway).
fn london_to_utc(naive: chrono::NaiveDateTime) -> Option<DateTime<Utc>> {
    match chrono_tz::Europe::London.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
        chrono::LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
        chrono::LocalResult::None => None,
    }
}

async fn current_train_state(
    pool: &PgPool,
    trains_id: i64,
) -> anyhow::Result<(String, Option<i32>)> {
    use sqlx::Row;
    let row =
        sqlx::query("SELECT status, delay_minutes FROM train_current_state WHERE trains_id = $1")
            .bind(trains_id)
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
mod copy_tests {
    use super::*;

    fn ctx(total_legs: i64, journey_name: Option<&str>) -> queries::JourneyLegContext {
        queries::JourneyLegContext {
            journey_id: 42,
            journey_name: journey_name.map(str::to_string),
            leg_order: 2,
            total_legs,
            origin_crs: Some("WAV".to_string()),
            destination_crs: Some("KGX".to_string()),
        }
    }

    #[test]
    fn no_journey_context_falls_back_to_todays_exact_copy() {
        let payload = build_train_notification_payload(7, "en_route", Some(18), None);
        assert_eq!(payload.title, "Your train is delayed");
        assert_eq!(payload.body, "Now running about 18 minutes late.");
        assert_eq!(payload.url, "/track/7");
        assert_eq!(payload.tag, "train-7");
    }

    #[test]
    fn a_one_leg_journey_also_falls_back_to_todays_exact_copy() {
        let context = ctx(1, Some("Weekend in Edinburgh"));
        let payload = build_train_notification_payload(7, "cancelled", None, Some(&context));
        assert_eq!(payload.title, "Your train was cancelled");
        assert_eq!(payload.url, "/track/7");
    }

    #[test]
    fn a_multi_leg_journey_names_the_leg_and_journey() {
        let context = ctx(3, Some("Weekend in Edinburgh"));
        let payload = build_train_notification_payload(7, "en_route", Some(18), Some(&context));
        assert_eq!(payload.title, "Leg 2 of 'Weekend in Edinburgh' is delayed");
        assert_eq!(
            payload.body,
            "WAV to KGX, now running about 18 minutes late."
        );
        assert_eq!(payload.url, "/journeys/42");
        assert_eq!(payload.tag, "train-7");
    }

    #[test]
    fn an_unnamed_multi_leg_journey_falls_back_to_a_generic_journey_label() {
        let context = ctx(2, None);
        let payload = build_train_notification_payload(7, "en_route", Some(5), Some(&context));
        assert_eq!(payload.title, "Leg 2 of your journey is delayed");
    }

    #[test]
    fn a_multi_leg_cancellation_names_the_route_when_known() {
        let context = ctx(2, Some("Weekend in Edinburgh"));
        let payload = build_train_notification_payload(7, "cancelled", None, Some(&context));
        assert_eq!(
            payload.title,
            "Leg 2 of 'Weekend in Edinburgh' was cancelled"
        );
        assert_eq!(payload.body, "The WAV to KGX service was cancelled.");
    }

    #[test]
    fn a_multi_leg_journey_with_no_leg_origin_destination_omits_the_route() {
        let mut context = ctx(2, Some("Weekend in Edinburgh"));
        context.origin_crs = None;
        context.destination_crs = None;
        let payload = build_train_notification_payload(7, "en_route", Some(5), Some(&context));
        assert_eq!(payload.body, "Now running about 5 minutes late.");
    }
}

#[cfg(test)]
mod db_tests {
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

    async fn cleanup(pool: &PgPool, user_id: &str, line_id: &str) {
        sqlx::query("DELETE FROM line_notification_state WHERE user_id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .expect("cleanup state");
        sqlx::query("DELETE FROM push_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .expect("cleanup subs");
        sqlx::query("DELETE FROM pinned_lines WHERE user_id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .expect("cleanup pins");
        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1")
            .bind(line_id)
            .execute(pool)
            .await
            .expect("cleanup history");
        sqlx::query("DELETE FROM notifier_cursor WHERE name = 'line_status_history'")
            .execute(pool)
            .await
            .expect("cleanup cursor");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .expect("cleanup user");
    }

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

        sqlx::query(
            "INSERT INTO pinned_lines (user_id, line_id, pinned_at) VALUES ($1, $2, NOW())",
        )
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
        run_cycle(
            &pool,
            cooldown,
            15,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("second run_cycle must also return Ok");
        let (rank_after_replay, _) = queries::line_notification_state(&pool, user_id, line_id)
            .await
            .expect("read state again")
            .expect("state must still exist");
        assert_eq!(
            rank_after_replay, rank,
            "a replay with no new data must not change the stored state"
        );

        cleanup(&pool, user_id, line_id).await;
    }
}

#[cfg(test)]
mod sweep_cycle_tests {
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
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
    }

    /// Today's own Europe/London calendar date -- must match exactly what
    /// `run_template_sweep_cycle` itself computes (see that function's own
    /// doc comment), so a seeded template's `days_of_week` bit and a seeded
    /// leg's/schedule row's `service_date` actually line up with what the
    /// cycle looks for as "today" when these tests run for real.
    fn today_london() -> chrono::NaiveDate {
        Utc::now()
            .with_timezone(&chrono_tz::Europe::London)
            .date_naive()
    }

    /// End-to-end: seeds one `'auto'`-mode template + one template leg + a
    /// published `schedule_destination_departures` row inside the (very
    /// generous, so this test is not time-of-day-dependent) lead window,
    /// runs `run_template_sweep_cycle` once, and asserts BOTH stages fired
    /// in that same tick -- a `journeys` row was minted (stage 1) AND its
    /// leg was auto-committed to the seeded train (stage 2). A second run
    /// must mint no second journey and must not re-commit the leg to a
    /// different `train_subscription_id`.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                run_template_sweep_cycle_mints_and_auto_commits_then_is_idempotent \
                -- --ignored --test-threads=1`"]
    async fn run_template_sweep_cycle_mints_and_auto_commits_then_is_idempotent() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-CYCLE-COMMIT-USER";
        let train_uid = "TEST-SWEEP-CYCLE-COMMIT-UID";
        seed_user(&pool, user_id).await;
        let today = today_london();

        let template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, default_match_mode, days_of_week, active) \
             VALUES ($1, 'E2E Auto Template', 'auto', $2, TRUE) RETURNING id",
        )
        .bind(user_id)
        .bind(decision::weekday_bit(today))
        .fetch_one(&pool)
        .await
        .expect("seed template");

        sqlx::query(
            "INSERT INTO journey_template_legs \
                (template_id, leg_order, origin_crs, destination_crs, depart_after) \
             VALUES ($1, 1, 'RDG', 'WOK', '00:00:00')",
        )
        .bind(template_id)
        .execute(&pool)
        .await
        .expect("seed template leg");

        sqlx::query(
            "INSERT INTO schedule_destination_departures \
                (service_date, destination_crs, scheduled, train_uid, origin_crs) \
             VALUES ($1, 'WOK', '09:05:00', $2, 'RDG')",
        )
        .bind(today)
        .bind(train_uid)
        .execute(&pool)
        .await
        .expect("seed published schedule row");

        run_template_sweep_cycle(
            &pool,
            1440, // generous lead window -- a seeded 00:00:00 depart_after is always "due" by the time this test runs
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("first run_template_sweep_cycle must succeed");

        let journey_id: i64 =
            sqlx::query_scalar("SELECT id FROM journeys WHERE source_template_id = $1")
                .bind(template_id)
                .fetch_one(&pool)
                .await
                .expect("a journey must have been minted for this template in the first run");

        let (match_mode, train_subscription_id): (String, Option<i64>) = sqlx::query_as(
            "SELECT match_mode, train_subscription_id FROM journey_legs WHERE journey_id = $1",
        )
        .bind(journey_id)
        .fetch_one(&pool)
        .await
        .expect("the minted leg must exist");
        assert_eq!(
            match_mode, "auto",
            "the leg must have been auto-committed in the SAME tick as its own minting"
        );
        let train_subscription_id =
            train_subscription_id.expect("a train_subscription_id must be set on auto-commit");

        run_template_sweep_cycle(
            &pool,
            1440,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("second run_template_sweep_cycle must also succeed");

        let journeys_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM journeys WHERE source_template_id = $1")
                .bind(template_id)
                .fetch_one(&pool)
                .await
                .expect("count journeys after replay");
        assert_eq!(
            journeys_count, 1,
            "a replay with the occurrence already materialized must not mint a second journey"
        );

        let (match_mode_after, train_subscription_id_after): (String, Option<i64>) =
            sqlx::query_as(
                "SELECT match_mode, train_subscription_id FROM journey_legs WHERE journey_id = $1",
            )
            .bind(journey_id)
            .fetch_one(&pool)
            .await
            .expect("the leg must still exist after replay");
        assert_eq!(match_mode_after, "auto");
        assert_eq!(
            train_subscription_id_after,
            Some(train_subscription_id),
            "a replay of an already-committed leg must not re-commit it to a different train_subscription_id"
        );

        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok(); // cascades journey_legs + journey_leg_notification_state
        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(train_subscription_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM schedule_destination_departures WHERE train_uid = $1")
            .bind(train_uid)
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

    /// End-to-end: seeds one `'auto'`-mode template + one template leg, and
    /// deliberately publishes ZERO `schedule_destination_departures` rows
    /// for that route/date. Runs `run_template_sweep_cycle` once, asserts
    /// the "needs attention" notification's DB side effect
    /// (`journey_leg_notification_state.last_notified_unmatched`) was
    /// written exactly once, then runs it again and asserts
    /// `last_notified_unmatched_at` is byte-for-byte unchanged -- the
    /// escalation-only, no-re-fire behavior `decision::decide_unmatched_notification`
    /// implements.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                run_template_sweep_cycle_notifies_once_on_zero_candidates_and_does_not_renotify \
                -- --ignored --test-threads=1`"]
    async fn run_template_sweep_cycle_notifies_once_on_zero_candidates_and_does_not_renotify() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-CYCLE-UNMATCHED-USER";
        seed_user(&pool, user_id).await;
        let today = today_london();

        let template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, default_match_mode, days_of_week, active) \
             VALUES ($1, 'E2E Unmatched Template', 'auto', $2, TRUE) RETURNING id",
        )
        .bind(user_id)
        .bind(decision::weekday_bit(today))
        .fetch_one(&pool)
        .await
        .expect("seed template");

        sqlx::query(
            "INSERT INTO journey_template_legs \
                (template_id, leg_order, origin_crs, destination_crs, depart_after) \
             VALUES ($1, 1, 'RDG', 'WOK', '00:00:00')",
        )
        .bind(template_id)
        .execute(&pool)
        .await
        .expect("seed template leg");

        // Deliberately zero schedule_destination_departures rows for this
        // route/date -- this is the "genuinely nothing published" case
        // this test exercises.

        run_template_sweep_cycle(
            &pool,
            1440,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("first run_template_sweep_cycle must succeed");

        let journey_id: i64 =
            sqlx::query_scalar("SELECT id FROM journeys WHERE source_template_id = $1")
                .bind(template_id)
                .fetch_one(&pool)
                .await
                .expect("a journey must have been minted for this template in the first run");
        let journey_leg_id: i64 =
            sqlx::query_scalar("SELECT id FROM journey_legs WHERE journey_id = $1")
                .bind(journey_id)
                .fetch_one(&pool)
                .await
                .expect("the minted leg must exist");

        let (last_notified_unmatched, last_notified_unmatched_at): (
            Option<bool>,
            Option<DateTime<Utc>>,
        ) = sqlx::query_as(
            "SELECT last_notified_unmatched, last_notified_unmatched_at \
             FROM journey_leg_notification_state WHERE user_id = $1 AND journey_leg_id = $2",
        )
        .bind(user_id)
        .bind(journey_leg_id)
        .fetch_one(&pool)
        .await
        .expect("a notification_state row must exist after the first run's zero-candidate branch");
        assert_eq!(last_notified_unmatched, Some(true));
        let first_at = last_notified_unmatched_at.expect("last_notified_unmatched_at must be set");

        run_template_sweep_cycle(
            &pool,
            1440,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("second run_template_sweep_cycle must also succeed");

        let (_, last_notified_unmatched_at_after): (Option<bool>, Option<DateTime<Utc>>) =
            sqlx::query_as(
                "SELECT last_notified_unmatched, last_notified_unmatched_at \
                 FROM journey_leg_notification_state WHERE user_id = $1 AND journey_leg_id = $2",
            )
            .bind(user_id)
            .bind(journey_leg_id)
            .fetch_one(&pool)
            .await
            .expect("the notification_state row must still exist after replay");
        assert_eq!(
            last_notified_unmatched_at_after,
            Some(first_at),
            "a second run within the same window must not re-write last_notified_unmatched_at"
        );

        sqlx::query("DELETE FROM journeys WHERE id = $1")
            .bind(journey_id)
            .execute(&pool)
            .await
            .ok(); // cascades journey_legs + journey_leg_notification_state
        sqlx::query("DELETE FROM journey_templates WHERE id = $1")
            .bind(template_id)
            .execute(&pool)
            .await
            .ok(); // cascades journey_template_legs
        cleanup_user(&pool, user_id).await;
    }
}
