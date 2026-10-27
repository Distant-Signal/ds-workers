//! Every notification setting's gate against a live database
//! (`DATABASE_URL`): a train with its pushes off, the per-type switches,
//! the per-user delay threshold, a pinned line's alert switch, quiet hours
//! (held, then released as the latest state; cancellations go through)
//! and snooze. Each test owns its `TEST-NOTIFSET-*` users and its
//! far-future fixture day's `TEST-NOTIFSET-*` trains.

use chrono::NaiveDate;
use sqlx::PgPool;

use crate::queries;

async fn connect() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .expect("connect to postgres")
}

/// The fixture day: far in the future, so nothing real is on it.
fn day() -> NaiveDate {
    let day: NaiveDate = "2099-07-01".parse().expect("date");
    ds_store::test_support::assert_synthetic_date(day);
    day
}

async fn exec(pool: &PgPool, sql: &str) {
    sqlx::query(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
}

/// Deletes `user`'s rows (most cascade from `users`) and `uid`'s train,
/// now and again when the test ends.
async fn cleanup(pool: &PgPool, user: &str, uid: &str) -> ds_store::test_support::FixtureCleanup {
    ds_store::test_support::FixtureCleanup::new(
        pool,
        [
            format!("DELETE FROM journeys WHERE user_id = '{user}'"),
            format!("DELETE FROM users WHERE id = '{user}'"),
            format!(
                "DELETE FROM trains WHERE train_uid = '{uid}' AND service_date = '{}'",
                day()
            ),
        ],
    )
    .await
}

/// One user with a one-leg journey on a train of their own.
struct Fixture {
    train: i64,
    subscription: i64,
    leg: i64,
}

async fn seed(pool: &PgPool, user: &str, uid: &str) -> Fixture {
    exec(
        pool,
        &format!("INSERT INTO users (id, name) VALUES ('{user}', '{user}') ON CONFLICT DO NOTHING"),
    )
    .await;
    let trains_id: i64 = sqlx::query_scalar(
        "INSERT INTO trains (train_uid, service_date, origin_crs, destination_crs) \
         VALUES ($1, $2, 'PAD', 'RDG') RETURNING id",
    )
    .bind(uid)
    .bind(day())
    .fetch_one(pool)
    .await
    .expect("seed train");
    let subscription_id: i64 = sqlx::query_scalar(
        "INSERT INTO train_subscriptions \
             (user_id, service_date, pin_origin_crs, pin_destination_crs, trains_id, \
              resolution_status) \
         VALUES ($1, $2, 'PAD', 'RDG', $3, 'resolved') RETURNING id",
    )
    .bind(user)
    .bind(day())
    .bind(trains_id)
    .fetch_one(pool)
    .await
    .expect("seed subscription");
    let journey_id: i64 =
        sqlx::query_scalar("INSERT INTO journeys (user_id) VALUES ($1) RETURNING id")
            .bind(user)
            .fetch_one(pool)
            .await
            .expect("seed journey");
    let journey_leg_id: i64 = sqlx::query_scalar(
        "INSERT INTO journey_legs \
             (journey_id, leg_order, origin_crs, destination_crs, service_date, \
              train_subscription_id, match_mode) \
         VALUES ($1, 1, 'PAD', 'RDG', $2, $3, 'manual') RETURNING id",
    )
    .bind(journey_id)
    .bind(day())
    .bind(subscription_id)
    .fetch_one(pool)
    .await
    .expect("seed leg");
    Fixture {
        train: trains_id,
        subscription: subscription_id,
        leg: journey_leg_id,
    }
}

async fn skip_checked_legs(pool: &PgPool) -> Vec<i64> {
    queries::list_committed_legs_for_today(pool, day())
        .await
        .expect("list_committed_legs_for_today")
        .into_iter()
        .map(|leg| leg.journey_leg_id)
        .collect()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_train_with_its_pushes_off_gets_no_skipped_stop_check() {
    const USER: &str = "TEST-NOTIFSET-SKIP-TRAIN-OFF";
    const UID: &str = "TEST-NOTIFSET-SKIP1";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;
    assert!(skip_checked_legs(&pool).await.contains(&fixture.leg));

    exec(
        &pool,
        &format!(
            "UPDATE train_subscriptions SET notifications_enabled = FALSE WHERE id = {}",
            fixture.subscription
        ),
    )
    .await;
    assert!(!skip_checked_legs(&pool).await.contains(&fixture.leg));
    assert!(fixture.train > 0);
}

/// Replaces `user`'s `notification_preferences` row with `columns` =
/// `values` (SQL literals), every other column at its default.
async fn set_prefs(pool: &PgPool, user: &str, columns: &str, values: &str) {
    exec(
        pool,
        &format!("DELETE FROM notification_preferences WHERE user_id = '{user}'"),
    )
    .await;
    exec(
        pool,
        &format!(
            "INSERT INTO notification_preferences (user_id, {columns}) VALUES ('{user}', {values})"
        ),
    )
    .await;
}

async fn set_train_state(pool: &PgPool, trains_id: i64, status: &str, delay: i32) {
    exec(
        pool,
        &format!("DELETE FROM train_current_state WHERE trains_id = {trains_id}"),
    )
    .await;
    exec(
        pool,
        &format!(
            "INSERT INTO train_current_state (trains_id, status, delay_minutes) \
             VALUES ({trains_id}, '{status}', {delay})"
        ),
    )
    .await;
}

async fn train_candidate_ranks(pool: &PgPool, trains_id: i64) -> Vec<u8> {
    queries::candidates_for_trains_id(pool, trains_id, 15)
        .await
        .expect("candidates_for_trains_id")
        .into_iter()
        .map(|candidate| candidate.new_rank)
        .collect()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn the_delay_and_cancel_switches_and_the_users_threshold_gate_train_pushes() {
    const USER: &str = "TEST-NOTIFSET-TRAIN-TYPES";
    const UID: &str = "TEST-NOTIFSET-TYPES1";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;

    // 20 minutes late, no settings row: today's behaviour, a delay push.
    set_train_state(&pool, fixture.train, "en_route", 20).await;
    assert_eq!(train_candidate_ranks(&pool, fixture.train).await, [1]);

    // Delays off.
    set_prefs(&pool, USER, "train_delay", "FALSE").await;
    assert!(train_candidate_ranks(&pool, fixture.train).await.is_empty());

    // The user's own threshold replaces the default: 30 holds a 20-minute
    // delay back, 10 lets a 12-minute one through that 15 would not.
    set_prefs(&pool, USER, "delay_threshold_minutes", "30").await;
    assert!(train_candidate_ranks(&pool, fixture.train).await.is_empty());
    set_train_state(&pool, fixture.train, "en_route", 12).await;
    assert!(train_candidate_ranks(&pool, fixture.train).await.is_empty());
    set_prefs(&pool, USER, "delay_threshold_minutes", "10").await;
    assert_eq!(train_candidate_ranks(&pool, fixture.train).await, [1]);

    // Cancellations off: no cancel push, whatever the delay switch says.
    set_train_state(&pool, fixture.train, "cancelled", 0).await;
    set_prefs(&pool, USER, "train_cancel", "FALSE").await;
    assert!(train_candidate_ranks(&pool, fixture.train).await.is_empty());
    set_prefs(&pool, USER, "train_delay, train_cancel", "FALSE, TRUE").await;
    assert_eq!(train_candidate_ranks(&pool, fixture.train).await, [2]);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn skipped_stop_pushes_respect_the_station_skip_switch() {
    const USER: &str = "TEST-NOTIFSET-SKIP-TYPE";
    const UID: &str = "TEST-NOTIFSET-SKIP2";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;
    set_prefs(&pool, USER, "train_delay", "FALSE").await;
    assert!(
        skip_checked_legs(&pool).await.contains(&fixture.leg),
        "another switch off leaves skipped stops on"
    );
    set_prefs(&pool, USER, "station_skip", "FALSE").await;
    assert!(!skip_checked_legs(&pool).await.contains(&fixture.leg));
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn line_pushes_respect_the_line_status_switch() {
    const USER: &str = "TEST-NOTIFSET-LINE-TYPE";
    const UID: &str = "TEST-NOTIFSET-LINE1";
    const LINE: &str = "test-notifset-line";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    seed(&pool, USER, UID).await;
    exec(
        &pool,
        &format!("INSERT INTO pinned_lines (user_id, line_id) VALUES ('{USER}', '{LINE}')"),
    )
    .await;
    let users = queries::pinned_users_for_line(&pool, LINE)
        .await
        .expect("users");
    assert_eq!(users, [USER]);
    set_prefs(&pool, USER, "line_status", "FALSE").await;
    let users = queries::pinned_users_for_line(&pool, LINE)
        .await
        .expect("users");
    assert!(users.is_empty());
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_pinned_line_with_its_alerts_off_sends_no_line_push() {
    const USER: &str = "TEST-NOTIFSET-LINE-NOTIFY";
    const UID: &str = "TEST-NOTIFSET-LINE2";
    const LINE: &str = "test-notifset-line-notify";
    const OTHER_LINE: &str = "test-notifset-line-notify-2";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    seed(&pool, USER, UID).await;
    // The column's default is the backfill: a pin made without it alerts.
    exec(
        &pool,
        &format!(
            "INSERT INTO pinned_lines (user_id, line_id) VALUES ('{USER}', '{LINE}'), \
             ('{USER}', '{OTHER_LINE}')"
        ),
    )
    .await;
    assert_eq!(
        queries::pinned_users_for_line(&pool, LINE)
            .await
            .expect("users"),
        [USER]
    );
    exec(
        &pool,
        &format!(
            "UPDATE pinned_lines SET notify = FALSE WHERE user_id = '{USER}' AND line_id = '{LINE}'"
        ),
    )
    .await;
    assert!(
        queries::pinned_users_for_line(&pool, LINE)
            .await
            .expect("users")
            .is_empty()
    );
    assert_eq!(
        queries::pinned_users_for_line(&pool, OTHER_LINE)
            .await
            .expect("users"),
        [USER],
        "only that line is silenced"
    );
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn the_needs_attention_switch_reaches_the_template_sweep() {
    const USER: &str = "TEST-NOTIFSET-TEMPLATE";
    const UID: &str = "TEST-NOTIFSET-TPL1";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    seed(&pool, USER, UID).await;
    let template_id: i64 = sqlx::query_scalar(
        "INSERT INTO journey_templates (user_id, custom_name, default_match_mode, active) \
         VALUES ($1, 'Notifset template', 'auto', TRUE) RETURNING id",
    )
    .bind(USER)
    .fetch_one(&pool)
    .await
    .expect("seed template");
    let journey_id: i64 = sqlx::query_scalar(
        "INSERT INTO journeys (user_id, source_template_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(USER)
    .bind(template_id)
    .fetch_one(&pool)
    .await
    .expect("seed journey");
    let leg_id: i64 = sqlx::query_scalar(
        "INSERT INTO journey_legs (journey_id, leg_order, origin_crs, destination_crs, \
             service_date, match_mode) \
         VALUES ($1, 1, 'PAD', 'RDG', $2, 'unmatched') RETURNING id",
    )
    .bind(journey_id)
    .bind(day())
    .fetch_one(&pool)
    .await
    .expect("seed leg");
    let notify = async || {
        queries::unmatched_auto_legs_for_commit_check(&pool, day())
            .await
            .expect("legs")
            .into_iter()
            .find(|leg| leg.journey_leg_id == leg_id)
            .expect("the leg is still commit-checked")
            .notify_attention
    };
    assert!(notify().await);
    set_prefs(&pool, USER, "template_attention", "FALSE").await;
    assert!(
        !notify().await,
        "the leg is still committed, only not pushed"
    );
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn predicted_late_starts_respect_their_switch_and_the_users_threshold() {
    const USER: &str = "TEST-NOTIFSET-PREDICTED";
    const UID: &str = "TSTNP01";
    const INCOMING: &str = "TSTNP00";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let _incoming = ds_store::test_support::FixtureCleanup::new(
        &pool,
        [
            format!(
                "DELETE FROM trains WHERE train_uid = '{INCOMING}' AND service_date = '{}'",
                day()
            ),
            format!(
                "DELETE FROM schedule_turnarounds WHERE uid = '{UID}' AND service_date = '{}'",
                day()
            ),
        ],
    )
    .await;
    let fixture = seed(&pool, USER, UID).await;
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO schedule_turnarounds (service_date, uid, prev_service_date, prev_uid, \
         booked_turnaround_minutes, link, origin_tiploc, platform, departs_at) \
         VALUES ($1, $2, $1, $3, 9, 'inferred', 'YORK', '3', $4)",
    )
    .bind(day())
    .bind(UID)
    .bind(INCOMING)
    .bind(now + chrono::Duration::minutes(30))
    .execute(&pool)
    .await
    .expect("turnaround");
    let incoming: i64 = sqlx::query_scalar(
        "INSERT INTO trains (train_uid, service_date) VALUES ($1, $2) RETURNING id",
    )
    .bind(INCOMING)
    .bind(day())
    .fetch_one(&pool)
    .await
    .expect("incoming train");
    // 20 late on a 9-minute turnaround: predicted 15 late, high confidence.
    sqlx::query(
        "INSERT INTO train_current_state (trains_id, status, delay_minutes, event_time) \
         VALUES ($1, 'en_route', 20, $2)",
    )
    .bind(incoming)
    .bind(now)
    .execute(&pool)
    .await
    .expect("incoming state");
    let candidates = async || {
        queries::predicted_start_candidates(&pool, 15, now)
            .await
            .expect("predicted_start_candidates")
            .into_iter()
            .filter(|candidate| candidate.tracked_train_id == fixture.subscription)
            .count()
    };
    assert_eq!(candidates().await, 1);
    set_prefs(&pool, USER, "predicted_start", "FALSE").await;
    assert_eq!(candidates().await, 0);
    set_prefs(&pool, USER, "delay_threshold_minutes", "30").await;
    assert_eq!(candidates().await, 0, "15 late is under the user's 30");
    set_prefs(&pool, USER, "delay_threshold_minutes", "10").await;
    assert_eq!(candidates().await, 1);
}

/// Quiet hours from an hour before `now` to an hour after it, in UTC.
async fn quiet_around(pool: &PgPool, user: &str, now: chrono::DateTime<chrono::Utc>) {
    let start = (now - chrono::Duration::hours(1)).format("%H:%M:%S");
    let end = (now + chrono::Duration::hours(1)).format("%H:%M:%S");
    set_prefs(
        pool,
        user,
        "quiet_hours_start, quiet_hours_end, quiet_hours_timezone",
        &format!("'{start}', '{end}', 'UTC'"),
    )
    .await;
}

/// The holds `user` has: (target, train status, delay).
async fn holds(pool: &PgPool, user: &str) -> Vec<(String, Option<String>, Option<i32>)> {
    sqlx::query_as(
        "SELECT target, train_status, train_delay_minutes FROM notification_holds \
         WHERE user_id = $1 ORDER BY target",
    )
    .bind(user)
    .fetch_all(pool)
    .await
    .expect("read holds")
}

/// What the last delivered train push recorded: (status, delay).
async fn delivered_train_state(pool: &PgPool, user: &str) -> Option<(String, Option<i32>)> {
    sqlx::query_as(
        "SELECT last_notified_status, last_notified_delay_minutes \
         FROM train_notification_state WHERE user_id = $1",
    )
    .bind(user)
    .fetch_optional(pool)
    .await
    .expect("read train_notification_state")
}

/// Decides `trains_id`'s train pushes at `now` as the cycles do, and waits
/// for whatever reached the queue (no push subscription: delivered).
async fn decide_train(pool: &PgPool, trains_id: i64, now: chrono::DateTime<chrono::Utc>) {
    let queue = crate::drain_support::test_queue(pool);
    let candidates = queries::candidates_for_trains_id(pool, trains_id, 15)
        .await
        .expect("candidates");
    crate::notify_train_candidates(pool, &queue, &candidates, now)
        .await
        .expect("notify_train_candidates");
    crate::drain_support::drain(queue).await;
}

async fn release(pool: &PgPool, now: chrono::DateTime<chrono::Utc>) {
    let queue = crate::drain_support::test_queue(pool);
    crate::hold::run_release_cycle(pool, &queue, now, 15)
        .await
        .expect("run_release_cycle");
    crate::drain_support::drain(queue).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn quiet_hours_hold_a_delay_and_release_only_its_latest_state() {
    const USER: &str = "TEST-NOTIFSET-QUIET-HOLD";
    const UID: &str = "TEST-NOTIFSET-QUIET1";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;
    let now = chrono::Utc::now();
    quiet_around(&pool, USER, now).await;
    let target = format!("train:{}", fixture.subscription);

    // Late twice during quiet hours: one hold, with the later delay.
    set_train_state(&pool, fixture.train, "en_route", 20).await;
    decide_train(&pool, fixture.train, now).await;
    set_train_state(&pool, fixture.train, "en_route", 25).await;
    decide_train(&pool, fixture.train, now).await;
    assert_eq!(
        holds(&pool, USER).await,
        [(target.clone(), Some("en_route".to_string()), Some(25))]
    );
    assert_eq!(
        delivered_train_state(&pool, USER).await,
        None,
        "nothing sent yet"
    );

    // Still quiet: kept.
    release(&pool, now + chrono::Duration::minutes(30)).await;
    assert_eq!(holds(&pool, USER).await.len(), 1);
    assert_eq!(delivered_train_state(&pool, USER).await, None);

    // Quiet hours over: the latest state goes, once.
    release(&pool, now + chrono::Duration::hours(2)).await;
    assert!(holds(&pool, USER).await.is_empty());
    assert_eq!(
        delivered_train_state(&pool, USER).await,
        Some(("en_route".to_string(), Some(25)))
    );
    // And the escalation rule still holds afterwards: the same lateness
    // decides nothing new.
    decide_train(&pool, fixture.train, now + chrono::Duration::hours(2)).await;
    assert!(holds(&pool, USER).await.is_empty());
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_cancellation_goes_through_quiet_hours_and_outranks_the_held_delay() {
    const USER: &str = "TEST-NOTIFSET-QUIET-CANCEL";
    const UID: &str = "TEST-NOTIFSET-QUIET2";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;
    let now = chrono::Utc::now();
    quiet_around(&pool, USER, now).await;

    set_train_state(&pool, fixture.train, "en_route", 20).await;
    decide_train(&pool, fixture.train, now).await;
    assert_eq!(holds(&pool, USER).await.len(), 1);

    set_train_state(&pool, fixture.train, "cancelled", 20).await;
    decide_train(&pool, fixture.train, now).await;
    assert_eq!(
        delivered_train_state(&pool, USER).await,
        Some(("cancelled".to_string(), Some(20))),
        "the cancellation is sent in quiet hours"
    );
    assert!(
        holds(&pool, USER).await.is_empty(),
        "and replaces the held delay"
    );
    release(&pool, now + chrono::Duration::hours(2)).await;
    assert_eq!(
        delivered_train_state(&pool, USER).await,
        Some(("cancelled".to_string(), Some(20)))
    );
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_held_push_that_is_no_longer_news_is_dropped_unsent() {
    const USER: &str = "TEST-NOTIFSET-QUIET-STALE";
    const UID: &str = "TEST-NOTIFSET-QUIET3";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;
    let now = chrono::Utc::now();
    quiet_around(&pool, USER, now).await;

    set_train_state(&pool, fixture.train, "en_route", 20).await;
    decide_train(&pool, fixture.train, now).await;
    assert_eq!(holds(&pool, USER).await.len(), 1);
    // The train finished before quiet hours ended.
    set_train_state(&pool, fixture.train, "completed", 20).await;
    release(&pool, now + chrono::Duration::hours(2)).await;
    assert!(holds(&pool, USER).await.is_empty());
    assert_eq!(delivered_train_state(&pool, USER).await, None);

    // Held again, then the user turns the train's pushes off.
    set_train_state(&pool, fixture.train, "en_route", 30).await;
    decide_train(&pool, fixture.train, now).await;
    assert_eq!(holds(&pool, USER).await.len(), 1);
    exec(
        &pool,
        &format!(
            "UPDATE train_subscriptions SET notifications_enabled = FALSE WHERE id = {}",
            fixture.subscription
        ),
    )
    .await;
    release(&pool, now + chrono::Duration::hours(2)).await;
    assert!(holds(&pool, USER).await.is_empty());
    assert_eq!(delivered_train_state(&pool, USER).await, None);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn quiet_hours_hold_a_line_push_and_skip_full_poll_pushes_until_they_end() {
    use crate::push_queue::{Delivered, PushJob};
    use crate::send::NotificationPayload;

    const USER: &str = "TEST-NOTIFSET-QUIET-LINE";
    const UID: &str = "TEST-NOTIFSET-QUIET4";
    const LINE: &str = "test-notifset-quiet-line";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;
    exec(
        &pool,
        &format!("INSERT INTO pinned_lines (user_id, line_id) VALUES ('{USER}', '{LINE}')"),
    )
    .await;
    let now = chrono::Utc::now();
    quiet_around(&pool, USER, now).await;
    let payload = || NotificationPayload {
        title: "t".to_string(),
        body: "b".to_string(),
        url: "/u".to_string(),
        tag: "x".to_string(),
    };

    let queue = crate::drain_support::test_queue(&pool);
    for delivered in [
        Delivered::Line {
            line_id: LINE.to_string(),
            rank: 2,
        },
        Delivered::SkippedStop {
            journey_leg_id: fixture.leg,
        },
    ] {
        crate::hold::dispatch(
            &pool,
            &queue,
            PushJob {
                user_id: USER.to_string(),
                payload: payload(),
                delivered,
                decided_at: now,
            },
        )
        .await
        .expect("dispatch");
    }
    crate::drain_support::drain(queue).await;
    assert_eq!(
        holds(&pool, USER).await,
        [(format!("line:{LINE}"), None, None)],
        "the line push is held; the skipped stop is not kept (its cycle decides it again)"
    );
    let skip_state: Option<bool> = sqlx::query_scalar(
        "SELECT last_notified_skipped FROM journey_leg_notification_state WHERE user_id = $1",
    )
    .bind(USER)
    .fetch_optional(&pool)
    .await
    .expect("read skip state");
    assert_eq!(skip_state, None, "nothing was sent for the skipped stop");

    release(&pool, now + chrono::Duration::hours(2)).await;
    let line_rank: Option<i16> = sqlx::query_scalar(
        "SELECT last_notified_severity_rank FROM line_notification_state \
         WHERE user_id = $1 AND line_id = $2",
    )
    .bind(USER)
    .bind(LINE)
    .fetch_optional(&pool)
    .await
    .expect("read line state");
    assert_eq!(line_rank, Some(2));
    assert!(holds(&pool, USER).await.is_empty());
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_snooze_holds_pushes_until_it_ends_and_lets_cancellations_through() {
    const USER: &str = "TEST-NOTIFSET-SNOOZE";
    const UID: &str = "TEST-NOTIFSET-SNOOZE1";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;
    let now = chrono::Utc::now();
    let until = now + chrono::Duration::hours(1);
    set_prefs(
        &pool,
        USER,
        "snoozed_until",
        &format!("'{}'", until.to_rfc3339()),
    )
    .await;

    set_train_state(&pool, fixture.train, "en_route", 20).await;
    decide_train(&pool, fixture.train, now).await;
    assert_eq!(holds(&pool, USER).await.len(), 1, "held while snoozed");
    release(&pool, now + chrono::Duration::minutes(30)).await;
    assert_eq!(holds(&pool, USER).await.len(), 1, "still snoozed");
    assert_eq!(delivered_train_state(&pool, USER).await, None);
    release(&pool, until + chrono::Duration::seconds(1)).await;
    assert!(holds(&pool, USER).await.is_empty());
    assert_eq!(
        delivered_train_state(&pool, USER).await,
        Some(("en_route".to_string(), Some(20))),
        "sent when the snooze ends"
    );

    // A cancellation is not held by a snooze either.
    set_train_state(&pool, fixture.train, "cancelled", 20).await;
    decide_train(&pool, fixture.train, now).await;
    assert!(holds(&pool, USER).await.is_empty());
    assert_eq!(
        delivered_train_state(&pool, USER).await,
        Some(("cancelled".to_string(), Some(20)))
    );
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn the_auto_commit_reuses_a_trains_subscription_without_turning_it_back_on() {
    const USER: &str = "TEST-NOTIFSET-REPIN";
    const UID: &str = "TEST-NOTIFSET-REPIN1";
    let pool = connect().await;
    let _cleanup = cleanup(&pool, USER, UID).await;
    let fixture = seed(&pool, USER, UID).await;
    exec(
        &pool,
        &format!(
            "UPDATE train_subscriptions SET notifications_enabled = FALSE WHERE id = {}",
            fixture.subscription
        ),
    )
    .await;
    // Another leg matched onto the same train: the same row, still off.
    let reused = queries::create_subscription_for_train(&pool, fixture.train, USER)
        .await
        .expect("create_subscription_for_train");
    assert_eq!(reused, fixture.subscription);
    let enabled: bool =
        sqlx::query_scalar("SELECT notifications_enabled FROM train_subscriptions WHERE id = $1")
            .bind(reused)
            .fetch_one(&pool)
            .await
            .expect("read notifications_enabled");
    assert!(!enabled, "the user's off sticks");
}
