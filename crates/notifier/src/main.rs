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

/// Builds one of the four poll-cycle `tokio::time::Interval`s ticked in
/// `main`'s own `select!`, ticking every `interval_secs` -- with
/// `MissedTickBehavior::Delay` rather than the default `Burst`.
///
/// `Burst` fires every missed tick back-to-back with zero gap once a cycle
/// overruns its own interval (a slow DB query, a slow Web Push send) --
/// exactly when the database or push service is already struggling, it
/// would pile up a burst of immediate follow-up cycles instead of settling
/// back into its normal cadence. `Delay` instead waits a fresh
/// `interval_secs` from whenever the overrun tick actually completes, so a
/// slow cycle degrades to a slower cadence, never a thundering-herd burst.
/// Each of the four intervals below is its own independent `Interval`
/// value (they tick on different cadences inside one `select!`), so each
/// needs this set individually -- there is no single shared `Interval` to
/// configure once. Same fix, same rationale, as `common::poller_loop`'s
/// own `poll_interval_with_delay_on_overrun`/`aggregator`'s own
/// `cycle_interval`/`enricher`'s own `ticking_interval`. Split into its own
/// function so the configuration is directly assertable in a unit test via
/// `Interval::missed_tick_behavior()`, since the missed-tick BEHAVIOR
/// itself (skipping ticks under a real overrun) isn't practically
/// observable without a slow, flaky, real-time test.
fn poll_interval(interval_secs: u64) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let config = Config::parse();

    // Fail fast on a zero poll interval -- `tokio::time::interval` below
    // panics outright on a zero `Duration` with no context at all; see
    // `Config::validate`'s own doc comment.
    config.validate()?;

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
    let cursor_grace = chrono::Duration::seconds(config.cursor_grace_seconds);
    let mut interval = poll_interval(config.poll_interval_secs);
    let mut forward_interval = poll_interval(config.forward_queue_poll_interval_secs);
    let mut skip_check_interval = poll_interval(config.skip_check_poll_interval_secs);
    let mut template_sweep_interval = poll_interval(config.template_sweep_poll_interval_secs);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let result = run_cycle(
                    &pool,
                    Utc::now(),
                    cooldown,
                    config.train_delay_threshold_minutes,
                    cursor_grace,
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
                    Utc::now(),
                    config.train_delay_threshold_minutes,
                    cursor_grace,
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
                    Utc::now(),
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
                    Utc::now(),
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
    now: DateTime<Utc>,
    cooldown: chrono::Duration,
    train_delay_threshold_minutes: i32,
    cursor_grace: chrono::Duration,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<()> {
    // --- Lines (Decision 2/3/5) ---
    let line_cursor = queries::read_cursor(pool, "line_status_history").await?;
    let (line_candidates, line_observed_max_id) =
        queries::poll_line_candidates(pool, line_cursor.last_processed_id).await?;

    for candidate in &line_candidates {
        // Mirrors `notify_train_candidates`'s own per-candidate log, and is
        // the only production read of `LineCandidate::id` now that the
        // watermark advances over every row POLLED rather than over the
        // candidate ids alone (see `queries::poll_line_candidates`) -- worth
        // keeping precisely because "which history row did this push come
        // from" is the first question asked when a notification looks wrong.
        tracing::debug!(
            line_status_history_id = candidate.id,
            line_id = %candidate.line_id,
            previous_rank = candidate.previous_rank,
            new_rank = candidate.new_rank,
            "line notification candidate"
        );
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
    queries::advance_cursor_with_grace(
        pool,
        "line_status_history",
        &line_cursor,
        line_observed_max_id,
        now,
        cursor_grace,
    )
    .await?;

    // --- Trains (Decision 4) ---
    let train_cursor = queries::read_cursor(pool, "train_movement_events").await?;
    let (train_candidates, train_max_id) = queries::poll_train_candidates(
        pool,
        train_cursor.last_processed_id,
        train_delay_threshold_minutes,
    )
    .await?;
    notify_train_candidates(
        pool,
        &train_candidates,
        vapid_private_key,
        vapid_subject,
        now,
    )
    .await?;
    queries::advance_cursor_with_grace(
        pool,
        "train_movement_events",
        &train_cursor,
        train_max_id,
        now,
        cursor_grace,
    )
    .await?;

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
    now: DateTime<Utc>,
    train_delay_threshold_minutes: i32,
    cursor_grace: chrono::Duration,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<()> {
    let cursor = queries::read_cursor(pool, "notifier_forward_queue").await?;
    let (touched_trains_ids, max_id) =
        queries::poll_forward_queue(pool, cursor.last_processed_id).await?;
    for trains_id in touched_trains_ids {
        let candidates =
            queries::candidates_for_trains_id(pool, trains_id, train_delay_threshold_minutes)
                .await?;
        notify_train_candidates(pool, &candidates, vapid_private_key, vapid_subject, now).await?;
    }
    queries::advance_cursor_with_grace(
        pool,
        "notifier_forward_queue",
        &cursor,
        max_id,
        now,
        cursor_grace,
    )
    .await?;
    Ok(())
}

/// The station-skip check's own cycle (Task 9, §5.2) -- a full poll of
/// today's committed journey legs every `skip_check_poll_interval_secs`,
/// not cursor/watermark-based (see `config.rs`'s own doc comment on why).
/// Each leg is judged independently against its own
/// `journey_leg_notification_state` row -- `decide_skip_notification`'s
/// escalation-only shape, same discipline as every other notification path
/// in this crate: state is written only after a successful send.
///
/// `now` is INJECTED, not read from the clock inside here -- same
/// convention `run_template_sweep_cycle` already establishes (see its own
/// doc comment), needed here for the same reason: which day's committed
/// legs this cycle checks is now a direct function of `now`'s London-local
/// date (see below), so that has to be controllable from a test.
async fn run_skip_check_cycle(
    pool: &PgPool,
    now: DateTime<Utc>,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<()> {
    // London-local calendar date, NOT `now.date_naive()` (bare UTC) --
    // `journey_legs.service_date` is always a London-local calendar date
    // (same convention `run_template_sweep_cycle` already establishes for
    // `today` above), so during the UTC/London date-boundary gap (BST,
    // UTC+1: 00:00-01:00 London is still "yesterday" in UTC) a bare-UTC
    // `today` checked the WRONG day's legs for up to an hour after London
    // midnight -- this cycle's own 90-second-default poll would then find
    // zero committed legs for the correct (London) day and silently skip
    // every skip-check for that hour, exactly the same recurring
    // UTC-vs-London-date bug class already fixed for
    // `routes::trains`'s own `london_now` split.
    let today = now.with_timezone(&chrono_tz::Europe::London).date_naive();
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
/// once per tick from `now` and used for both stages.
///
/// `now` is INJECTED, not read from the clock inside here -- the same
/// convention `api::data::reconciliation::retry_schedule_enrichment_for_nr_primary_trains`
/// and `train_tracking::validate_pin(pin, now)` already establish in this
/// workspace. Which candidate this sweep commits a leg to is now a direct
/// function of the time of day it runs at (see
/// `decision::commit_check_window`), so that time of day has to be
/// controllable from a test rather than being whatever the clock happened to
/// read while the suite ran.
async fn run_template_sweep_cycle(
    pool: &PgPool,
    now: DateTime<Utc>,
    auto_commit_lead_minutes: i64,
    vapid_private_key: &str,
    vapid_subject: &str,
) -> anyhow::Result<()> {
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
        // A template leg is allowed to carry no time window at all (see
        // `api::data::journey_templates::validate_template_leg`'s own doc
        // comment) -- `unmatched_auto_legs_for_commit_check` does not filter
        // such a leg out, so any subset of the four bounds can be `None`
        // here. `decision::commit_check_window` classifies that subset;
        // read its doc comment for the "due from midnight, so committed to
        // an overnight train at the first tick after midnight" bug that
        // replaced `unwrap_or(NaiveTime::MIN)` with this.
        let window = decision::commit_check_window(
            leg.depart_after,
            leg.depart_before,
            leg.arrive_after,
            leg.arrive_before,
        );
        let due_bound_utc = match window.due_check_bound() {
            Some(bound) => {
                let Some(bound_utc) = london_to_utc(leg.service_date.and_time(bound)) else {
                    continue; // nonexistent local time (spring-forward gap) -- best-effort, skip this tick
                };
                bound_utc
            }
            // Fully open: there is no stated time to wait for, so this leg
            // is due whenever the sweep looks -- and `now`, not midnight, is
            // what "nearest/next" is then measured against below.
            None => now,
        };
        if !decision::is_due_for_commit_check(now, due_bound_utc, auto_commit_lead_minutes) {
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
            // "No service was found" is only honest if we actually HAVE
            // today's timetable. Zero candidates against an unpublished day
            // (a fresh environment, a schedule-reference outage, a late CIF
            // delivery) is indistinguishable at this point from zero
            // candidates against a real, complete timetable -- and this
            // notification fires AT MOST ONCE per leg, ever
            // (`decide_unmatched_notification`), so a false one sent now
            // permanently silences the genuine one a real problem would
            // warrant later this morning. Same "an empty result set can't
            // tell you which of the two it is, so probe the day" reasoning
            // `api::data::queries::schedule_destination_departures_published_for`
            // already encodes for its own 404-versus-`200 []` split.
            if !queries::schedule_published_for(pool, leg.service_date).await? {
                tracing::info!(
                    journey_leg_id = leg.journey_leg_id,
                    service_date = %leg.service_date,
                    "no schedule published for this leg's service date yet; deferring the \
                     no-service-found determination to a later tick rather than notifying"
                );
                continue;
            }
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
        // `(day_offset, scheduled)` per candidate, not bare `scheduled` --
        // `now_local` is always day_offset 0 here (this leg's
        // `service_date` is `today`, by the `unmatched_auto_legs_for_commit_check`
        // query's own scoping), so a genuine overnight candidate whose
        // `day_offset` regressed past midnight before reaching this leg's
        // origin must still compare correctly against it. See
        // `decision::pick_nearest_to_now_candidate`'s own doc comment.
        let day_offset_times: Vec<(u8, chrono::NaiveTime)> = candidates
            .iter()
            .map(|(_, day_offset, t)| (*day_offset, *t))
            .collect();
        // A leg that names its own earliest time keeps nearest-to-now (an
        // already-departed candidate is a legitimate answer there -- the
        // sweep may just be running behind the window the user chose). A leg
        // with NO lower bound instead takes the next candidate still
        // upcoming, so "any train" can never resolve to one that has already
        // left. See `decision::commit_check_window`.
        let winner_idx = if window.names_an_earliest_time() {
            decision::pick_nearest_to_now_candidate(&day_offset_times, now_local)
        } else {
            decision::pick_next_upcoming_candidate(&day_offset_times, now_local)
        };
        let Some(winner_idx) = winner_idx else {
            // Only reachable for an open-ended leg whose every candidate has
            // already departed (nearest-to-now is `None` for an empty slice
            // alone, already excluded above). Deliberately silent: candidates
            // DO exist for this route today, so this is not the
            // "no service found" case either -- there is simply nothing left
            // to board today, and nothing worth pushing about.
            tracing::debug!(
                journey_leg_id = leg.journey_leg_id,
                candidates = candidates.len(),
                "every candidate for this open-ended leg has already departed; leaving it \
                 unmatched rather than committing it to a train that has left"
            );
            continue;
        };
        let (train_uid, _, _) = &candidates[winner_idx];

        // Enriching find-or-create, NOT the bare one: a bare `trains` row
        // leaves `origin_crs`/`destination_crs`/`scheduled_departure` NULL,
        // which `create_subscription_for_train` then copies (as NULLs) into
        // the new subscription's own `pin_*` columns -- and with
        // `pin_destination_crs` NULL, `skip_check::leg_is_skipped` has no
        // Darwin departure to match against, so station-skip detection could
        // never fire for an auto-committed leg at all. This is the
        // auto-commit path's counterpart to the enrichment the MANUAL pick
        // route already runs (`api::routes::journeys::post_leg_train` ->
        // `routes::train::enrich_shared_train`); see
        // `queries::find_or_create_train_with_cif_schedule` for why the
        // notifier does the CIF half itself rather than reaching into that
        // (unreachable, api-crate) function.
        let trains_id =
            queries::find_or_create_train_with_cif_schedule(pool, train_uid, leg.service_date)
                .await?;
        // Subscribe-and-commit as ONE transaction: on the no-op path (the
        // user picked a train by hand at the same moment) the subscription
        // this would otherwise have left behind is rolled back with it,
        // instead of lingering and pushing notifications for a train they
        // never chose. See `queries::auto_commit_leg_to_train`.
        match queries::auto_commit_leg_to_train(pool, leg.journey_leg_id, trains_id, &leg.user_id)
            .await?
        {
            Some(tracking_id) => tracing::info!(
                journey_leg_id = leg.journey_leg_id,
                tracking_id,
                train_uid,
                "auto-committed leg to its chosen candidate"
            ),
            None => tracing::warn!(
                journey_leg_id = leg.journey_leg_id,
                "leg was committed by a concurrent actor before this tick finished; rolled back \
                 the subscription created for it rather than orphaning it"
            ),
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
mod poll_interval_tests {
    use super::poll_interval;

    /// Regression for the "L1 -- MissedTickBehavior::Burst still default"
    /// finding: `poll_interval` backs all four of `main`'s independent
    /// `select!` intervals (`interval`/`forward_interval`/
    /// `skip_check_interval`/`template_sweep_interval`), so asserting it
    /// here covers all four -- each must opt into `Delay`, not leave
    /// `Burst` as the default, so an overrun cycle doesn't fire a burst of
    /// back-to-back catch-up cycles.
    #[tokio::test]
    async fn poll_interval_defaults_to_delay_not_burst_on_a_missed_tick() {
        let interval = poll_interval(60);
        assert_eq!(
            interval.missed_tick_behavior(),
            tokio::time::MissedTickBehavior::Delay
        );
    }
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
        // `Duration::zero()` grace: this test asserts the line-notification
        // side effect of a single cycle, and a real grace window would hold
        // the watermark back for the whole of it -- the grace window's own
        // behavior is asserted directly by
        // `queries::tests::advance_cursor_with_grace_*` instead.
        let grace = chrono::Duration::zero();
        run_cycle(&pool, Utc::now(), cooldown, 15, grace, "not-a-real-vapid-key", "mailto:test@example.invalid")
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
            Utc::now(),
            cooldown,
            15,
            grace,
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

    /// A fixed Europe/London wall-clock instant on `date`, as the UTC
    /// `now` `run_template_sweep_cycle` takes. The whole point of injecting
    /// `now` is that WHICH candidate an open-window leg commits to is a
    /// function of the time of day the sweep runs at, so these tests pin
    /// that time instead of inheriting whatever the clock reads.
    fn london_now(date: chrono::NaiveDate, hour: u32, minute: u32) -> DateTime<Utc> {
        london_to_utc(
            date.and_hms_opt(hour, minute, 0)
                .expect("valid wall-clock time"),
        )
        .expect("a real (non-spring-forward-gap) London local time")
    }

    /// Seeds one `'auto'`-mode, active, due-today template with ONE leg
    /// carrying exactly the four window bounds given (all `None` = the
    /// fully-open leg today's time-flexible-templates feature added support
    /// for). Returns the template id.
    async fn seed_auto_template(
        pool: &PgPool,
        user_id: &str,
        name: &str,
        today: chrono::NaiveDate,
        depart_after: Option<chrono::NaiveTime>,
        depart_before: Option<chrono::NaiveTime>,
    ) -> i64 {
        let template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates \
                (user_id, custom_name, default_match_mode, days_of_week, active) \
             VALUES ($1, $2, 'auto', $3, TRUE) RETURNING id",
        )
        .bind(user_id)
        .bind(name)
        .bind(decision::weekday_bit(today))
        .fetch_one(pool)
        .await
        .expect("seed template");

        sqlx::query(
            "INSERT INTO journey_template_legs \
                (template_id, leg_order, origin_crs, destination_crs, depart_after, depart_before) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, $3)",
        )
        .bind(template_id)
        .bind(depart_after)
        .bind(depart_before)
        .execute(pool)
        .await
        .expect("seed template leg");

        template_id
    }

    /// One published RDG -> WOK departure for `today` at `(hour, minute)`,
    /// `day_offset` 0 -- the shape `queries::schedule_candidates_for_leg`
    /// matches (`main.destination_crs = 'WOK'` satisfies its reachability
    /// arm directly).
    async fn seed_departure(
        pool: &PgPool,
        today: chrono::NaiveDate,
        train_uid: &str,
        hour: u32,
        minute: u32,
    ) {
        sqlx::query(
            "INSERT INTO schedule_destination_departures \
                (service_date, destination_crs, scheduled, day_offset, train_uid, origin_crs, \
                 true_origin_crs, destination_arrival, destination_arrival_day_offset) \
             VALUES ($1, 'WOK', $2, 0, $3, 'RDG', 'RDG', $2, 0)",
        )
        .bind(today)
        .bind(chrono::NaiveTime::from_hms_opt(hour, minute, 0).expect("valid scheduled departure"))
        .bind(train_uid)
        .execute(pool)
        .await
        .expect("seed published schedule row");
    }

    /// Reads back the `train_uid` an auto-committed leg actually ended up
    /// bound to, following `journey_legs -> train_subscriptions -> trains`.
    async fn committed_train_uid(pool: &PgPool, template_id: i64) -> Option<String> {
        sqlx::query_scalar(
            "SELECT t.train_uid FROM journeys j \
             JOIN journey_legs jl ON jl.journey_id = j.id \
             JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id \
             JOIN trains t ON t.id = ts.trains_id \
             WHERE j.source_template_id = $1",
        )
        .bind(template_id)
        .fetch_optional(pool)
        .await
        .expect("read the committed train's own uid")
    }

    /// Full teardown for one template-driven fixture: the minted journey (and
    /// its legs/notification-state, by cascade), the subscription and
    /// `trains` rows the auto-commit created, every seeded schedule row, the
    /// template, its tombstones, and the user.
    async fn cleanup_template_fixture(pool: &PgPool, user_id: &str, template_id: i64) {
        sqlx::query(
            "DELETE FROM train_subscriptions WHERE user_id = $1 AND trains_id IN ( \
                 SELECT id FROM trains WHERE train_uid LIKE 'TEST-SWEEP-%' \
             )",
        )
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
        sqlx::query("DELETE FROM journeys WHERE source_template_id = $1")
            .bind(template_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid LIKE 'TEST-SWEEP-%'")
            .execute(pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM schedule_destination_departures WHERE train_uid LIKE 'TEST-SWEEP-%'",
        )
        .execute(pool)
        .await
        .ok();
        sqlx::query("DELETE FROM journey_template_skipped_dates WHERE template_id = $1")
            .bind(template_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM journey_templates WHERE id = $1")
            .bind(template_id)
            .execute(pool)
            .await
            .ok();
        cleanup_user(pool, user_id).await;
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
            Utc::now(),
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
            Utc::now(),
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

    /// End-to-end regression test for a template leg with NO time window
    /// at all (`api::data::journey_templates::validate_template_leg`'s own
    /// doc comment confirms this is a deliberately allowed state -- a
    /// template leg is never itself "matched," so the "at least one
    /// window bound" rule an ordinary journey leg needs doesn't apply
    /// here). Seeds a template leg with `depart_after`/`depart_before`/
    /// `arrive_after`/`arrive_before` all omitted (NULL), and publishes
    /// TWO candidate departures rather than one -- both to prove
    /// `schedule_candidates_for_leg` returns every departure for the
    /// route/date when every bound is NULL (not zero, not the whole
    /// network), and so `pick_nearest_to_now_candidate`'s "closest to now"
    /// choice between the two candidates is a real assertion, not
    /// vacuously true with a single candidate.
    ///
    /// Before this fix: `unmatched_auto_legs_for_commit_check`'s own WHERE
    /// clause required `depart_after IS NOT NULL OR arrive_after IS NOT
    /// NULL`, so this leg was never even selected for a commit-check, and
    /// `main.rs`'s own `let Some(earliest_bound) = ... else { continue }`
    /// independently skipped it even if it had been -- either way, this
    /// leg would stay `'unmatched'` forever. This test fails on either the
    /// pre-fix query or the pre-fix `main.rs` guard alone, so it is a real
    /// regression test for both halves of that fix together.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                run_template_sweep_cycle_mints_and_auto_commits_a_leg_with_no_time_window \
                -- --ignored --test-threads=1`"]
    async fn run_template_sweep_cycle_mints_and_auto_commits_a_leg_with_no_time_window() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-CYCLE-OPEN-WINDOW-USER";
        let nearer_uid = "TEST-SWEEP-CYCLE-OPEN-WINDOW-NEARER-UID";
        let farther_uid = "TEST-SWEEP-CYCLE-OPEN-WINDOW-FARTHER-UID";
        seed_user(&pool, user_id).await;
        let today = today_london();
        let now_local = Utc::now().with_timezone(&chrono_tz::Europe::London).time();

        let template_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, custom_name, default_match_mode, days_of_week, active) \
             VALUES ($1, 'E2E Open Window Auto Template', 'auto', $2, TRUE) RETURNING id",
        )
        .bind(user_id)
        .bind(decision::weekday_bit(today))
        .fetch_one(&pool)
        .await
        .expect("seed template");

        // No depart_after/depart_before/arrive_after/arrive_before column
        // at all in this INSERT -- every one of the four stays NULL.
        sqlx::query(
            "INSERT INTO journey_template_legs (template_id, leg_order, origin_crs, destination_crs) \
             VALUES ($1, 1, 'RDG', 'WOK')",
        )
        .bind(template_id)
        .execute(&pool)
        .await
        .expect("seed fully-open-window template leg");

        // Two candidates: one an hour from now, one twelve hours from
        // now -- both real, in-range departures given no window bound at
        // all, but only the nearer one should win the auto-commit.
        // `day_offset`-aware, same "seconds past midnight on
        // `service_date`" arithmetic `decision::pick_nearest_to_now_candidate`
        // itself uses -- this test must stay correct even when it happens
        // to run within a few hours of real local midnight, where a naive
        // `now_local + Duration::hours(12)` would wrap the clock time back
        // toward "now" without also advancing `day_offset`, silently
        // reintroducing the exact bug that function's own regression test
        // (`nearest_to_now_candidate_is_day_offset_aware_across_a_midnight_boundary`)
        // already covers for the pure-decision layer.
        fn offset_from_now(
            now_local: chrono::NaiveTime,
            add_hours: i64,
        ) -> (i16, chrono::NaiveTime) {
            use chrono::Timelike;
            let total_secs = i64::from(now_local.num_seconds_from_midnight()) + add_hours * 3600;
            let day_offset =
                i16::try_from(total_secs.div_euclid(86_400)).expect("small day offset");
            let secs_in_day =
                u32::try_from(total_secs.rem_euclid(86_400)).expect("secs in day fits u32");
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs_in_day, 0)
                .expect("valid wall-clock time");
            (day_offset, time)
        }
        let (nearer_day_offset, nearer_time) = offset_from_now(now_local, 1);
        let (farther_day_offset, farther_time) = offset_from_now(now_local, 12);
        sqlx::query(
            "INSERT INTO schedule_destination_departures \
                (service_date, destination_crs, scheduled, day_offset, train_uid, origin_crs) \
             VALUES ($1, 'WOK', $2, $3, $4, 'RDG'), ($1, 'WOK', $5, $6, $7, 'RDG')",
        )
        .bind(today)
        .bind(nearer_time)
        .bind(nearer_day_offset)
        .bind(nearer_uid)
        .bind(farther_time)
        .bind(farther_day_offset)
        .bind(farther_uid)
        .execute(&pool)
        .await
        .expect("seed published schedule rows");

        run_template_sweep_cycle(
            &pool,
            Utc::now(),
            120, // the spec's own suggested default -- irrelevant here since a fully-open leg is always due
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("run_template_sweep_cycle must succeed for a fully-open-window leg");

        let journey_id: i64 =
            sqlx::query_scalar("SELECT id FROM journeys WHERE source_template_id = $1")
                .bind(template_id)
                .fetch_one(&pool)
                .await
                .expect("a journey must have been minted for this template");

        let (match_mode, train_subscription_id): (String, Option<i64>) = sqlx::query_as(
            "SELECT match_mode, train_subscription_id FROM journey_legs WHERE journey_id = $1",
        )
        .bind(journey_id)
        .fetch_one(&pool)
        .await
        .expect("the minted leg must exist");
        assert_eq!(
            match_mode, "auto",
            "a fully-open-window leg under an 'auto'-mode template must still get auto-committed"
        );
        let train_subscription_id =
            train_subscription_id.expect("a train_subscription_id must be set on auto-commit");

        let committed_train_uid: String = sqlx::query_scalar(
            "SELECT t.train_uid FROM train_subscriptions ts \
             JOIN trains t ON t.id = ts.trains_id \
             WHERE ts.id = $1",
        )
        .bind(train_subscription_id)
        .fetch_one(&pool)
        .await
        .expect("read the committed train's own uid");
        assert_eq!(
            committed_train_uid, nearer_uid,
            "with no window at all, the sweep must still pick the candidate genuinely nearest \
             to now, not the farther one and not an arbitrary one"
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
        sqlx::query("DELETE FROM trains WHERE train_uid IN ($1, $2)")
            .bind(nearer_uid)
            .bind(farther_uid)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM schedule_destination_departures WHERE train_uid IN ($1, $2)")
            .bind(nearer_uid)
            .bind(farther_uid)
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
        // ROUTE -- but one row for an unrelated route on the same date, so
        // the day itself IS published. That distinction is load-bearing as of
        // the `schedule_published_for` guard added for review finding 7:
        // "zero candidates for this leg's route on a day we genuinely have
        // the timetable for" is the real no-service-found case this test
        // exercises, and it is now deliberately different from "zero
        // candidates because today's CIF delivery hasn't landed" (covered by
        // `no_service_found_is_not_notified_while_the_days_schedule_is_unpublished`,
        // which asserts the opposite outcome).
        sqlx::query(
            "INSERT INTO schedule_destination_departures \
                (service_date, destination_crs, scheduled, train_uid, origin_crs) \
             VALUES ($1, 'PAD', '11:00:00', 'TEST-SWEEP-UNMATCHED-OTHER-ROUTE-UID', 'SLO')",
        )
        .bind(today)
        .execute(&pool)
        .await
        .expect("seed an unrelated published row so the DAY counts as published");

        run_template_sweep_cycle(
            &pool,
            Utc::now(),
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
            Utc::now(),
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
        sqlx::query(
            "DELETE FROM schedule_destination_departures \
             WHERE train_uid = 'TEST-SWEEP-UNMATCHED-OTHER-ROUTE-UID'",
        )
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

    /// Finding 1's regression test, at the real reported hour: the FIRST
    /// sweep tick after midnight, against a template leg with no time window
    /// at all ("RDG to PAD, no specific time" -- the shape today's
    /// time-flexible-templates feature made auto-committable).
    ///
    /// Before this fix, `main.rs` derived the leg's "earliest bound" as
    /// `depart_after.or(arrive_after).unwrap_or(NaiveTime::MIN)`, i.e. LONDON
    /// MIDNIGHT, so the leg was due from the very first tick of the day and
    /// `pick_nearest_to_now_candidate` was asked which candidate was nearest
    /// to 00:37. That is the 00:34 overnight service -- which has ALREADY
    /// DEPARTED, 3 minutes ago, and beats the 07:15 commuter service 6.5
    /// hours ahead on pure absolute distance. The leg was then flagged
    /// `'auto'` and never re-evaluated, so the user got delay/cancellation/
    /// skip pushes all day for a train they were never on.
    ///
    /// Asserts the sensible near-future service wins instead. Fails on the
    /// pre-fix code (which commits `...-OVERNIGHT-UID`), and the assertion is
    /// a real one rather than vacuous: there are two genuine candidates and
    /// the "wrong" one is the one absolute nearness prefers.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                the_first_tick_after_midnight_does_not_bind_an_open_window_leg_to_a_departed_overnight_service \
                -- --ignored --test-threads=1`"]
    async fn the_first_tick_after_midnight_does_not_bind_an_open_window_leg_to_a_departed_overnight_service()
     {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-MIDNIGHT-USER";
        let overnight_uid = "TEST-SWEEP-MIDNIGHT-OVERNIGHT-UID";
        let commuter_uid = "TEST-SWEEP-MIDNIGHT-COMMUTER-UID";
        let today = today_london();
        seed_user(&pool, user_id).await;
        let template_id = seed_auto_template(
            &pool,
            user_id,
            "E2E Midnight Open Window",
            today,
            None, // no depart_after
            None, // no depart_before either -- fully open, every bound NULL
        )
        .await;
        seed_departure(&pool, today, overnight_uid, 0, 34).await;
        seed_departure(&pool, today, commuter_uid, 7, 15).await;

        // 00:37 -- an hourly sweep's first tick after midnight, exactly the
        // reported scenario.
        run_template_sweep_cycle(
            &pool,
            london_now(today, 0, 37),
            120,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("run_template_sweep_cycle must succeed");

        assert_eq!(
            committed_train_uid(&pool, template_id).await.as_deref(),
            Some(commuter_uid),
            "an open-window leg must be committed to the next UPCOMING service (07:15), never to \
             the 00:34 overnight one that had already departed 3 minutes before this tick -- \
             being nearest in absolute clock distance is not being the right train"
        );

        cleanup_template_fixture(&pool, user_id, template_id).await;
    }

    /// Finding 1's regression test at a mid-morning tick, with THREE
    /// candidates -- the "realistic multi-candidate scenario" half of the
    /// same fix, and the one that proves the rule is "next upcoming", not
    /// merely "not an overnight train":
    ///
    /// * 00:34 -- last night's overnight service (what the pre-fix code
    ///   committed to at the first tick after midnight),
    /// * 09:10 -- departed 20 minutes ago, and therefore the NEAREST
    ///   candidate to a 09:30 `now` by absolute distance,
    /// * 10:15 -- the next service that has not yet left, 45 minutes out.
    ///
    /// Pre-fix selection picks 09:10 (20 < 45) and binds the user to a train
    /// that has already gone; post-fix picks 10:15.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                a_mid_morning_sweep_commits_an_open_window_leg_to_the_next_upcoming_service \
                -- --ignored --test-threads=1`"]
    async fn a_mid_morning_sweep_commits_an_open_window_leg_to_the_next_upcoming_service() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-MIDMORNING-USER";
        let overnight_uid = "TEST-SWEEP-MIDMORNING-OVERNIGHT-UID";
        let departed_uid = "TEST-SWEEP-MIDMORNING-DEPARTED-UID";
        let upcoming_uid = "TEST-SWEEP-MIDMORNING-UPCOMING-UID";
        let today = today_london();
        seed_user(&pool, user_id).await;
        let template_id = seed_auto_template(
            &pool,
            user_id,
            "E2E Mid-morning Open Window",
            today,
            None,
            None,
        )
        .await;
        seed_departure(&pool, today, overnight_uid, 0, 34).await;
        seed_departure(&pool, today, departed_uid, 9, 10).await;
        seed_departure(&pool, today, upcoming_uid, 10, 15).await;

        run_template_sweep_cycle(
            &pool,
            london_now(today, 9, 30),
            120,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("run_template_sweep_cycle must succeed");

        assert_eq!(
            committed_train_uid(&pool, template_id).await.as_deref(),
            Some(upcoming_uid),
            "with no window bound at all, the sweep must pick the next service still to depart \
             (10:15) -- not the 09:10 that left 20 minutes ago (nearest by absolute distance), \
             and not last night's 00:34"
        );

        cleanup_template_fixture(&pool, user_id, template_id).await;
    }

    /// Finding 1's other half: a leg that names ONLY an upper bound
    /// (`depart_before = 09:00` -- "any train, as long as it leaves before
    /// nine") was also swallowed by the `unwrap_or(NaiveTime::MIN)` fallback,
    /// so it too was due from midnight rather than from `lead_minutes` before
    /// the user's own latest acceptable time.
    ///
    /// Runs the sweep at 04:00 with a 120-minute lead window: the leg's real
    /// anchor is 09:00, so 04:00 is 5 hours early and NOTHING may be
    /// committed yet. Pre-fix, the leg was due at midnight and this tick
    /// would have bound it to the 05:50 -- a 5am train for someone who said
    /// "before 9".
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                an_upper_bound_only_leg_is_not_due_until_its_own_bound_is_within_the_lead_window \
                -- --ignored --test-threads=1`"]
    async fn an_upper_bound_only_leg_is_not_due_until_its_own_bound_is_within_the_lead_window() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-UPPERBOUND-USER";
        let early_uid = "TEST-SWEEP-UPPERBOUND-EARLY-UID";
        let commuter_uid = "TEST-SWEEP-UPPERBOUND-COMMUTER-UID";
        let today = today_london();
        seed_user(&pool, user_id).await;
        let template_id = seed_auto_template(
            &pool,
            user_id,
            "E2E Upper Bound Only",
            today,
            None,                                     // no depart_after
            chrono::NaiveTime::from_hms_opt(9, 0, 0), // "before 09:00"
        )
        .await;
        seed_departure(&pool, today, early_uid, 5, 50).await;
        seed_departure(&pool, today, commuter_uid, 8, 20).await;

        run_template_sweep_cycle(
            &pool,
            london_now(today, 4, 0),
            120,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("run_template_sweep_cycle must succeed");

        assert_eq!(
            committed_train_uid(&pool, template_id).await,
            None,
            "a leg whose only bound is 'before 09:00' must not be committed at 04:00 -- its \
             commit-check is anchored on 09:00 minus the lead window, not on midnight"
        );

        // Now inside the lead window (07:30 is within 120 minutes of 09:00):
        // the same leg commits, and to the 08:20 rather than the 05:50 that
        // has already gone.
        run_template_sweep_cycle(
            &pool,
            london_now(today, 7, 30),
            120,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("second run_template_sweep_cycle must succeed");

        assert_eq!(
            committed_train_uid(&pool, template_id).await.as_deref(),
            Some(commuter_uid),
            "once inside the lead window the leg commits to the next upcoming service still \
             satisfying its own 'before 09:00' bound"
        );

        cleanup_template_fixture(&pool, user_id, template_id).await;
    }

    /// Finding 5: an auto-committed leg's shared `trains` row must carry
    /// CIF's own schedule, and the subscription minted for it must carry the
    /// matching `pin_*` values -- `pin_destination_crs` above all, since
    /// `skip_check::leg_is_skipped` matches a live Darwin departure board by
    /// exactly that column and could never fire while it was NULL.
    ///
    /// Before this fix the auto-commit called the BARE `find_or_create_train`
    /// (the manual "pick a train" route runs `enrich_shared_train` instead),
    /// so every one of these assertions read back NULL.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                an_auto_committed_leg_gets_an_enriched_trains_row_and_a_pinned_subscription \
                -- --ignored --test-threads=1`"]
    async fn an_auto_committed_leg_gets_an_enriched_trains_row_and_a_pinned_subscription() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-ENRICH-USER";
        let train_uid = "TEST-SWEEP-ENRICH-UID";
        let today = today_london();
        seed_user(&pool, user_id).await;
        let template_id =
            seed_auto_template(&pool, user_id, "E2E Enrichment", today, None, None).await;
        seed_departure(&pool, today, train_uid, 23, 30).await;

        run_template_sweep_cycle(
            &pool,
            london_now(today, 9, 30),
            120,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("run_template_sweep_cycle must succeed");

        let (origin_crs, destination_crs, scheduled_departure): (
            Option<String>,
            Option<String>,
            Option<DateTime<Utc>>,
        ) = sqlx::query_as(
            "SELECT origin_crs, destination_crs, scheduled_departure FROM trains \
             WHERE train_uid = $1 AND service_date = $2",
        )
        .bind(train_uid)
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("the auto-commit must have created a trains row");
        assert_eq!(
            origin_crs.as_deref(),
            Some("RDG"),
            "the shared trains row must carry CIF's own true origin"
        );
        assert_eq!(
            destination_crs.as_deref(),
            Some("WOK"),
            "...and CIF's own terminus -- this is what create_subscription_for_train copies into \
             pin_destination_crs, which station-skip detection matches Darwin against"
        );
        assert_eq!(
            scheduled_departure,
            Some(london_now(today, 23, 30)),
            "...and the booked departure, as a real UTC instant"
        );

        let (pin_origin_crs, pin_destination_crs): (Option<String>, Option<String>) =
            sqlx::query_as(
                "SELECT ts.pin_origin_crs, ts.pin_destination_crs FROM journeys j \
                 JOIN journey_legs jl ON jl.journey_id = j.id \
                 JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id \
                 WHERE j.source_template_id = $1",
            )
            .bind(template_id)
            .fetch_one(&pool)
            .await
            .expect("the auto-committed leg's subscription must exist");
        assert_eq!(pin_origin_crs.as_deref(), Some("RDG"));
        assert_eq!(
            pin_destination_crs.as_deref(),
            Some("WOK"),
            "the pin columns are copied from the trains row AT SUBSCRIPTION-CREATION TIME, so \
             enrichment has to happen before it -- with this NULL, skip detection is dead"
        );

        cleanup_template_fixture(&pool, user_id, template_id).await;
    }

    /// Finding 7: the zero-candidates branch must not claim "no service was
    /// found" on a day whose schedule has not published yet.
    ///
    /// Seeds an `'auto'` template whose route has no departures at all AND
    /// leaves the whole `service_date` unpublished (every
    /// `schedule_destination_departures` row for today is deleted first), then
    /// asserts NO `journey_leg_notification_state` row was written. That
    /// notification fires at most once per leg for ever
    /// (`decide_unmatched_notification`), so a false one sent at 00:37 -- before
    /// the day's CIF delivery has even landed -- permanently silences the
    /// genuine one a real problem would warrant later the same morning.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                no_service_found_is_not_notified_while_the_days_schedule_is_unpublished \
                -- --ignored --test-threads=1`"]
    async fn no_service_found_is_not_notified_while_the_days_schedule_is_unpublished() {
        let pool = connect().await;
        let user_id = "TEST-SWEEP-UNPUBLISHED-USER";
        let today = today_london();
        seed_user(&pool, user_id).await;
        let template_id =
            seed_auto_template(&pool, user_id, "E2E Unpublished Day", today, None, None).await;

        // The day is genuinely unpublished: no rows at all for this
        // service_date, which is what a fresh environment, a
        // schedule-reference outage or a late CIF delivery looks like.
        // Clears this suite's OWN leftovers first (a previous run that
        // panicked before its cleanup), never anything it didn't seed.
        sqlx::query(
            "DELETE FROM schedule_destination_departures \
             WHERE service_date = $1 AND train_uid LIKE 'TEST-%'",
        )
        .bind(today)
        .execute(&pool)
        .await
        .ok();
        let published_before: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM schedule_destination_departures WHERE service_date = $1",
        )
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("count today's published rows");
        assert_eq!(
            published_before, 0,
            "this test needs an unpublished day; another fixture has left rows for today behind"
        );

        run_template_sweep_cycle(
            &pool,
            london_now(today, 0, 37),
            120,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("run_template_sweep_cycle must succeed");

        let notified: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM journey_leg_notification_state s \
             JOIN journey_legs jl ON jl.id = s.journey_leg_id \
             JOIN journeys j ON j.id = jl.journey_id \
             WHERE j.source_template_id = $1 AND s.last_notified_unmatched",
        )
        .bind(template_id)
        .fetch_one(&pool)
        .await
        .expect("count unmatched notifications");
        assert_eq!(
            notified, 0,
            "'No RDG to WOK service was found for today' must not be sent while we simply do not \
             have today's timetable yet -- it can never be taken back"
        );

        cleanup_template_fixture(&pool, user_id, template_id).await;
    }

    /// Finding 2 (Low-severity): `run_skip_check_cycle` must scope
    /// "today's committed legs" by the London-local calendar date, not
    /// `now.date_naive()` (bare UTC) -- the same UTC/London date-boundary
    /// gap already fixed elsewhere in this codebase (`routes::trains`'s own
    /// `london_now` split, and this file's own `run_template_sweep_cycle`).
    ///
    /// 2026-07-15 is deep in BST (UTC+1): `23:30` UTC on that date is
    /// `00:30` London on `2026-07-16`. The committed leg's `service_date`
    /// is `2026-07-16` (the correct LONDON day) -- pre-fix, `now.date_naive()`
    /// would read `2026-07-15` (the UTC day) for this exact `now`, so
    /// `list_committed_legs_for_today` would look up the wrong day and find
    /// nothing, silently skipping every skip-check for this leg for the
    /// whole BST gap hour.
    ///
    /// No `push_subscriptions` row is seeded for this user, so
    /// `send_to_all_subscriptions` takes its own documented "zero
    /// subscriptions still counts as handled" branch and
    /// `journey_leg_notification_state` gets written regardless of whether
    /// a real push was ever attempted -- letting this test observe "the leg
    /// was found and judged skipped" without needing a live push endpoint.
    #[tokio::test]
    #[ignore = "requires a live database with this plan's Task 3 migration already applied; \
                run with `DATABASE_URL=... cargo test -p notifier \
                skip_check_cycle_uses_londons_calendar_date_not_bare_utc_during_the_bst_gap \
                -- --ignored --test-threads=1`"]
    async fn skip_check_cycle_uses_londons_calendar_date_not_bare_utc_during_the_bst_gap() {
        let pool = connect().await;
        let user_id = "TEST-SKIP-CYCLE-BST-USER";
        let train_uid = "TEST-SKIP-CYCLE-BST-UID";
        let utc_date: chrono::NaiveDate = "2026-07-15".parse().unwrap();
        let london_service_date: chrono::NaiveDate = "2026-07-16".parse().unwrap();
        let now = chrono::DateTime::parse_from_rfc3339("2026-07-15T23:30:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            now.date_naive(),
            utc_date,
            "sanity check: this `now` is still 2026-07-15 in bare UTC"
        );
        assert_eq!(
            now.with_timezone(&chrono_tz::Europe::London).date_naive(),
            london_service_date,
            "sanity check: this `now` is already 2026-07-16 in Europe/London (BST, UTC+1)"
        );

        seed_user(&pool, user_id).await;

        let trains_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date, origin_crs) \
             VALUES ($1, $2, 'RDG') RETURNING id",
        )
        .bind(train_uid)
        .bind(london_service_date)
        .fetch_one(&pool)
        .await
        .expect("seed trains row");

        let train_subscription_id: i64 = sqlx::query_scalar(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_destination_crs, \
                 pin_scheduled_departure, trains_id, resolution_status) \
             VALUES ($1, $2, 'RDG', 'PAD', $3, $4, 'resolved') RETURNING id",
        )
        .bind(user_id)
        .bind(london_service_date)
        .bind(london_service_date.and_hms_opt(9, 0, 0).unwrap().and_utc())
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed train_subscriptions row");

        let journey_id: i64 = sqlx::query_scalar(
            "INSERT INTO journeys (user_id, custom_name) VALUES ($1, NULL) RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed journeys row");

        let journey_leg_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs \
                (journey_id, leg_order, origin_crs, destination_crs, service_date, \
                 train_subscription_id, match_mode) \
             VALUES ($1, 1, 'RDG', 'WOK', $2, $3, 'manual') RETURNING id",
        )
        .bind(journey_id)
        .bind(london_service_date)
        .bind(train_subscription_id)
        .fetch_one(&pool)
        .await
        .expect("seed journey_legs row");

        // A live Darwin sample at the leg's own origin (RDG), on a through
        // service to PAD (the subscription's `pin_destination_crs`) that
        // skips WOK -- the leg's own destination -- today. Same fixture
        // shape as `skip_check::tests::seed_station_sample`.
        let departures = serde_json::json!([{
            "service_id": "test-skip-cycle-bst-service",
            "operator": "GW",
            "destination_crs": "PAD",
            "scheduled": "23:45",
            "estimated": "On time",
            "is_cancelled": false,
            "delay_minutes": 0,
            "skipped_stations": ["WOK"],
        }]);
        sqlx::query(
            "INSERT INTO station_samples (crs, polled_at, departures) VALUES ('RDG', NOW(), $1::jsonb) \
             ON CONFLICT (crs) DO UPDATE SET polled_at = EXCLUDED.polled_at, departures = EXCLUDED.departures",
        )
        .bind(departures)
        .execute(&pool)
        .await
        .expect("seed station_samples row");

        run_skip_check_cycle(
            &pool,
            now,
            "not-a-real-vapid-key",
            "mailto:test@example.invalid",
        )
        .await
        .expect("run_skip_check_cycle must succeed");

        let last_notified_skipped: Option<bool> = sqlx::query_scalar(
            "SELECT last_notified_skipped FROM journey_leg_notification_state \
             WHERE user_id = $1 AND journey_leg_id = $2",
        )
        .bind(user_id)
        .bind(journey_leg_id)
        .fetch_optional(&pool)
        .await
        .expect("read journey_leg_notification_state");
        assert_eq!(
            last_notified_skipped,
            Some(true),
            "the leg's service_date is 2026-07-16 (London-local, matching this now's London \
             date) -- if the cycle scoped its lookup by bare UTC date (2026-07-15) instead, this \
             leg would never be found and no notification_state row would exist at all"
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
        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(train_subscription_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM station_samples WHERE crs = 'RDG'")
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }
}
