//! Station-skip detection for a journey leg's own origin/destination,
//! notifier-side -- §5.2 of docs/superpowers/specs/2026-09-22-journey-tracking-design.md.
//! Cannot import `crates/api/src/data/station_skip.rs` (this crate does
//! not depend on `crates/api` -- see this plan's Architecture section for
//! why), so this async wrapper is written fresh against this crate's own
//! `queries.rs`; the actual matching/skip-membership logic is still shared
//! via `common::match_darwin_departure`/`departure_skips_station` so the
//! two independent implementations (this one, and
//! `crates/api/src/data/station_skip.rs::find_leg_skip`) can never define
//! "skipped" two different ways.

use common::{departure_skips_station, match_darwin_departure};
use sqlx::PgPool;

use crate::queries::{self, CommittedLeg};

/// Whether either end of `leg`'s own travel intent is among today's
/// skipped calling points, per the live Darwin sample(s) currently on
/// file. `Ok(false)` (never an error) when there's simply no sample yet to
/// check against -- a genuine "don't know" degrades to "assume not
/// skipped," matching `station_skip::leg_skip_status`'s own best-effort
/// posture on the API side. A real DB connectivity failure still
/// propagates via `?`, same as every other query in this crate -- the
/// caller (`run_skip_check_cycle`) lets that fail the whole cycle, retried
/// next interval, same as `run_cycle`/`run_forward_queue_cycle` already do
/// for their own DB errors.
pub async fn leg_is_skipped(pool: &PgPool, leg: &CommittedLeg) -> anyhow::Result<bool> {
    let match_target = leg
        .pin_destination_crs
        .as_deref()
        .or(leg.next_calling_point.as_deref());

    let Some(origin_sample) = queries::station_sample_for_crs(pool, &leg.origin_crs).await? else {
        return Ok(false);
    };
    let destination_skipped = match_darwin_departure(&origin_sample.departures, match_target)
        .is_some_and(|matched| departure_skips_station(matched, &leg.destination_crs));

    let origin_skipped = match leg.train_origin_crs.as_deref() {
        Some(train_origin) if !train_origin.eq_ignore_ascii_case(&leg.origin_crs) => {
            match queries::station_sample_for_crs(pool, train_origin).await? {
                Some(sample) => match_darwin_departure(&sample.departures, match_target)
                    .is_some_and(|matched| departure_skips_station(matched, &leg.origin_crs)),
                None => false,
            }
        }
        _ => false,
    };

    Ok(destination_skipped || origin_skipped)
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    /// Same connect-via-`DATABASE_URL` pattern as `queries.rs`'s own test
    /// module (`crates/notifier/src/queries.rs::tests::connect`) -- kept as
    /// a small local copy rather than reached across the module boundary,
    /// same posture that module's own helpers already take for
    /// `seed_user`/`cleanup_line_history`/etc.
    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    /// Seeds one `station_samples` row (`crs CHAR(3) PRIMARY KEY`, so a
    /// real-looking but unused test code -- `ZQK`, following the same
    /// `ZQ*`-prefixed convention `crates/api/src/routes/train.rs`'s own
    /// `seed_station_sample` and its siblings in `departures.rs`/
    /// `station_stats.rs` already use for a fake-but-valid CRS) with one
    /// non-cancelled departure. The JSON keys below are plain snake_case,
    /// NOT camelCase: `common::StationDeparture` has no
    /// `#[serde(rename_all = ...)]` at all, so its `Deserialize` impl reads
    /// these keys verbatim as its own field names -- confirmed against
    /// `crates/api/src/routes/train.rs`'s own `seed_station_sample`, which
    /// seeds the exact same table the exact same way.
    async fn seed_station_sample(
        pool: &PgPool,
        crs: &str,
        destination_crs: &str,
        skipped: &[&str],
    ) {
        let departures = serde_json::json!([{
            "service_id": "test-service",
            "operator": "GR",
            "destination_crs": destination_crs,
            "scheduled": "11:55",
            "estimated": "On time",
            "is_cancelled": false,
            "delay_minutes": 0,
            "skipped_stations": skipped,
        }]);
        sqlx::query(
            "INSERT INTO station_samples (crs, polled_at, departures) VALUES ($1, NOW(), $2::jsonb) \
             ON CONFLICT (crs) DO UPDATE SET polled_at = EXCLUDED.polled_at, departures = EXCLUDED.departures",
        )
        .bind(crs)
        .bind(departures)
        .execute(pool)
        .await
        .expect("seed fixture station_samples row");
    }

    async fn cleanup_station_sample(pool: &PgPool, crs: &str) {
        sqlx::query("DELETE FROM station_samples WHERE crs = $1")
            .bind(crs)
            .execute(pool)
            .await
            .expect("cleanup fixture station_samples row");
    }

    /// A `CommittedLeg` built directly in Rust -- `leg_is_skipped` takes
    /// `&CommittedLeg`, not a `journey_leg_id`, so there's no need to seed
    /// `journeys`/`journey_legs`/`train_subscriptions` rows at all just to
    /// exercise this function. `journey_leg_id`/`journey_id`/`trains_id`
    /// are never read by `leg_is_skipped` (only the CRS/match-target fields
    /// are), so `-1` sentinels are fine here. `train_origin_crs: None` keeps
    /// this fixture on the `destination_skipped` branch only -- the
    /// `origin_skipped` branch (a train whose OWN origin differs from this
    /// leg's origin) is exercised by `station_skip.rs::find_leg_skip`'s own
    /// unit tests on the API side, which share the exact same
    /// `departure_skips_station` predicate this function calls.
    fn leg(origin_crs: &str, destination_crs: &str, match_target: &str) -> CommittedLeg {
        CommittedLeg {
            journey_leg_id: -1,
            journey_id: -1,
            user_id: "TEST-SKIP-CHECK".to_string(),
            origin_crs: origin_crs.to_string(),
            destination_crs: destination_crs.to_string(),
            trains_id: -1,
            pin_destination_crs: Some(match_target.to_string()),
            next_calling_point: None,
            train_origin_crs: None,
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p notifier \
                leg_is_skipped -- --ignored --test-threads=1"]
    async fn leg_is_skipped_matches_the_destination_against_the_matched_departures_skipped_stations()
     {
        let pool = connect().await;
        // One live Darwin sample at "ZQK", matched via `pin_destination_crs`
        // = "RDG", reporting "WOK" among today's skipped calling points --
        // the exact shape `find_leg_skip`'s own doc comment describes.
        seed_station_sample(&pool, "ZQK", "RDG", &["WOK"]).await;

        let skipped = leg("ZQK", "WOK", "RDG");
        assert!(
            leg_is_skipped(&pool, &skipped)
                .await
                .expect("leg_is_skipped should not error"),
            "WOK is in the seeded departure's skippedStations, so this leg's own destination \
             should be reported skipped"
        );

        let not_skipped = leg("ZQK", "AAA", "RDG");
        assert!(
            !leg_is_skipped(&pool, &not_skipped)
                .await
                .expect("leg_is_skipped should not error"),
            "AAA is not in the seeded departure's skippedStations, so this leg should not be \
             reported skipped"
        );

        cleanup_station_sample(&pool, "ZQK").await;
    }
}
