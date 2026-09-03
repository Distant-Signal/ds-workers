//! Deduplicates LDBWS sample departures by Darwin's `serviceID`
//! (`common::StationDeparture::service_id`) so a rate/average computed over
//! a period (e.g. a day) counts each real train once, not once per poll
//! cycle it happened to still be sitting in a station's departure-board
//! window.
//!
//! ## Why this exists
//!
//! `aggregation::compute_sample_stats` (called every cycle,
//! `poll_interval_secs` apart, default 60s) counts every departure
//! currently visible in the configured `sample_stations`' LDBWS windows.
//! Darwin's `GetDepBoardWithDetails` response is a rolling window of
//! upcoming services at a station -- the same physical train is very
//! likely to still be in that window on the next several polls until it
//! actually departs (or is cancelled/skipped and drops out). A service
//! dwelling in the window for e.g. 20 minutes at the default 60s cadence is
//! counted roughly 20 times by `compute_sample_stats`, not once. That is
//! *correct* for `compute_sample_stats`'s actual job -- live severity
//! classification (`aggregation::infer_from_samples`/
//! `escalate_from_sample_stats`) wants "what does the window look like
//! right now," and re-seeing a still-delayed train every cycle is the right
//! behaviour there -- but it is wrong as an input to any rate/average
//! computed *across* cycles (a day, a week): summing raw per-cycle counts
//! weights each train by how long it dwelt in the window, not by "one
//! train, one count." See
//! `docs/superpowers/specs/2026-08-31-line-history-graphics-design.md`
//! Correction 5 / Decision 2 for the original write-up of this gap.
//!
//! This module is the fix. `SeenServiceLedger` tracks, in memory, which
//! Darwin `service_id`s have already contributed to a given `(line_id,
//! period)` pair, and `dedup_new_sample_stats` returns stats built ONLY
//! from departures not already counted this period. Summing its output
//! across a period's cycles (e.g. accumulating into a future daily-rollup
//! row, one call per cycle) answers "how many distinct trains ran," which
//! raw `compute_sample_stats` sums cannot -- without needing any change to
//! `compute_sample_stats`/`infer_from_samples` themselves, since live
//! severity classification is a genuinely different question from "how
//! many distinct trains" and must keep using undeduped, every-cycle counts.
//!
//! This module deliberately stops at "reusable dedup capability" -- it does
//! not write to any table, and no `line_status_daily_stats`-shaped rollup
//! exists yet. `dedup_new_sample_stats` is the exact shape the
//! line-history-graphics daily-rollup work is expected to call once per
//! line per cycle, feeding its result into an accumulating upsert the same
//! way that design's own sketch of `record_daily_stats` already assumed
//! per-cycle `SampleStats`-shaped input -- the only change needed there is
//! swapping which function produces that input.
//!
//! ## In-memory, not persisted -- and why that's judged sufficient
//!
//! The line-history-graphics design doc flagged that "true" per-service
//! dedup would need a persisted, restart-surviving "seen today" ledger, on
//! the grounds that an in-memory `HashSet` reset at process start would
//! "silently undercount after every restart/deploy." Re-examined against
//! how Darwin's LDBWS feed actually behaves, that's the wrong frame: a
//! reset ledger doesn't cause an undercount at all -- it causes a bounded,
//! one-time OVER-count of whichever services happen to still be inside the
//! (small, rolling) departure-board window at the exact moment of restart,
//! since those services' `service_id`s get treated as "new" again post
//! restart. Concretely, in-memory-only is judged sufficient because:
//!
//! - The RDM `GetDepBoardWithDetails` window is a short rolling list of
//!   upcoming services per station, not "every train running today" -- so
//!   the set of `service_id`s a restart can possibly cause to be
//!   re-counted is bounded by however many services are concurrently
//!   visible across a line's `sample_stations` at that instant (typically a
//!   handful), not the line's whole day of traffic.
//! - Aggregator restarts (deploys, crashes) are rare relative to the
//!   `poll_interval_secs` cadence (default 60s) this ledger operates at --
//!   the exposure per restart is one cycle's worth of in-flight services, a
//!   one-off blip, not a growing or systemic skew.
//! - Compare against the status quo, which has NO dedup at all: every
//!   single cycle, all day, every day, currently recounts every dwelling
//!   service (a ~20-40x overcount at the default cadence, per the design
//!   doc's own estimate) -- not a rare edge case, a permanent one. An
//!   in-memory ledger fixes that endemic problem essentially completely,
//!   and only degrades -- to a small, one-off overcount, nowhere near the
//!   pre-existing ~20-40x one -- around the comparatively rare event of a
//!   restart.
//! - A persisted ledger would need a DB write (or existence check) per
//!   newly-seen service, every single cycle, to guard against a
//!   restart-frequency problem, not a per-cycle one -- real, ongoing I/O
//!   cost for a small edge-case improvement given the above.
//!
//! If this residual exposure is later judged unacceptable (e.g. once real
//! restart frequency / traffic data is measured against a shipped rollup),
//! `SeenServiceLedger`'s `mark_seen` could be backed by a
//! `(line_id, day, service_id)` table (an existence-check-then-insert
//! instead of a `HashSet` lookup-then-insert) without changing
//! `dedup_new_sample_stats`'s signature or any call site -- the interface
//! was kept narrow specifically so that swap would stay local to this
//! module.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use common::{
    Defaults, LineDefinition, SampleStats, StationDeparture, StationSample, thresholds_for,
};

use crate::aggregation::{relevant_departures, stats_from_departures};

/// In-memory record of which Darwin `service_id`s have already been counted
/// for a given `(line_id, period)` pair. See module docs for why in-memory
/// (not persisted) is judged sufficient, and how it could be swapped for a
/// persisted backing later without touching `dedup_new_sample_stats`.
#[derive(Default)]
pub struct SeenServiceLedger {
    seen: HashMap<(String, NaiveDate), HashSet<String>>,
}

impl SeenServiceLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `service_id` as seen for `line_id`/`period`. Returns `true`
    /// the first time a given `service_id` is marked for that
    /// `(line_id, period)` pair -- a genuinely new train this period --
    /// `false` on every subsequent call for the same triple: a re-poll of a
    /// train still dwelling in the LDBWS window.
    fn mark_seen(&mut self, line_id: &str, period: NaiveDate, service_id: &str) -> bool {
        self.seen
            .entry((line_id.to_string(), period))
            .or_default()
            .insert(service_id.to_string())
    }

    /// Drops every tracked `(line_id, period)` entry whose `period` is
    /// strictly before `retain_from`, bounding the ledger's memory growth
    /// across the aggregator process's lifetime (otherwise every day's
    /// worth of `service_id`s for every line would accumulate forever).
    /// Intended to be called once per cycle with the current period -- a
    /// cheap no-op once no stale periods remain.
    pub fn prune_before(&mut self, retain_from: NaiveDate) {
        self.seen.retain(|(_, period), _| *period >= retain_from);
    }

    #[cfg(test)]
    fn tracked_period_count(&self) -> usize {
        self.seen.len()
    }
}

/// This cycle's genuinely NEW sample stats for `line`/`period`: among the
/// departures currently visible at `line`'s `sample_stations`, only those
/// whose `service_id` has not already been recorded in `ledger` for this
/// `(line_id, period)` contribute. Marks every newly-seen `service_id` as
/// seen as a side effect, so calling this again this cycle or on a later
/// cycle for the same period never double-counts the same train.
///
/// `line_id` is threaded separately from `line: &LineDefinition` (rather
/// than reading `line.id`) so a caller iterating a `HashMap<String,
/// LineDefinition>` can pass the map key directly without an extra clone --
/// mirrors how `aggregation::aggregate` already keys its own
/// per-line loop.
///
/// Returns `None` when there is nothing new to report -- either no relevant
/// departures at all, or every one of them was already counted on an
/// earlier cycle this period. That is the expected, common case once a
/// line's currently-dwelling services have all been counted once: most
/// cycles legitimately contribute zero new trains.
///
/// Deliberately does NOT apply `Defaults::min_sample_size` the way
/// `aggregation::compute_sample_stats` does: that gate exists to decide
/// whether a single cycle's raw snapshot is trustworthy enough to drive
/// live severity classification. A deduped count answers a different
/// question -- "how many distinct trains have now been observed this
/// period" -- where a single newly-seen train is still one real,
/// worth-counting observation, even on a cycle where the whole window
/// happened to be sparse.
pub fn dedup_new_sample_stats(
    ledger: &mut SeenServiceLedger,
    line_id: &str,
    period: NaiveDate,
    line: &LineDefinition,
    samples: &HashMap<String, StationSample>,
    defaults: &Defaults,
) -> Option<SampleStats> {
    let thresholds = thresholds_for(defaults, &line.severity_overrides);
    let relevant = relevant_departures(line, samples);

    let new_departures: Vec<&StationDeparture> = relevant
        .into_iter()
        .filter(|dep| ledger.mark_seen(line_id, period, &dep.service_id))
        .collect();

    if new_departures.is_empty() {
        return None;
    }

    Some(stats_from_departures(&new_departures, line, &thresholds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use common::Station;

    fn line(id: &str, sample_stations: &[&str]) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "national-rail".to_string(),
            category: "test".to_string(),
            operators: vec!["SW".to_string()],
            stations: vec![
                Station {
                    crs: "AON".to_string(),
                    tiploc: None,
                    role: "terminus".to_string(),
                    segment: None,
                },
                Station {
                    crs: "WOK".to_string(),
                    tiploc: None,
                    role: "waypoint".to_string(),
                    segment: None,
                },
            ],
            sample_stations: sample_stations.iter().map(|s| s.to_string()).collect(),
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: HashMap::new(),
            exclusive_segments: vec![],
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
        }
    }

    fn departure(service_id: &str, delay_minutes: i32, is_cancelled: bool) -> StationDeparture {
        StationDeparture {
            service_id: service_id.to_string(),
            operator: "SW".to_string(),
            destination_crs: "AON".to_string(),
            scheduled: "10:00".to_string(),
            estimated: "10:00".to_string(),
            is_cancelled,
            delay_minutes,
            cancel_reason: None,
            delay_reason: None,
            headcode: None,
            skipped_stations: vec![],
        }
    }

    fn samples_with(
        crs: &str,
        departures: Vec<StationDeparture>,
    ) -> HashMap<String, StationSample> {
        let mut samples = HashMap::new();
        samples.insert(
            crs.to_string(),
            StationSample {
                crs: crs.to_string(),
                polled_at: Utc::now(),
                departures,
            },
        );
        samples
    }

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap().date_naive()
    }

    #[test]
    fn first_sighting_of_a_service_counts_as_new() {
        let l = line("alton", &["AHT"]);
        let defaults = Defaults::default();
        let mut ledger = SeenServiceLedger::new();
        let samples = samples_with("AHT", vec![departure("svc-1", 0, false)]);

        let stats = dedup_new_sample_stats(
            &mut ledger,
            "alton",
            day(2026, 8, 31),
            &l,
            &samples,
            &defaults,
        )
        .expect("first sighting should be new");
        assert_eq!(stats.total, 1);
    }

    #[test]
    fn repeated_polls_of_the_same_dwelling_service_are_not_recounted() {
        // The exact scenario the design doc flagged: the same service_id
        // shows up in consecutive polls' windows while it dwells.
        let l = line("alton", &["AHT"]);
        let defaults = Defaults::default();
        let mut ledger = SeenServiceLedger::new();
        let period = day(2026, 8, 31);

        let cycle1 = samples_with("AHT", vec![departure("svc-1", 0, false)]);
        let first = dedup_new_sample_stats(&mut ledger, "alton", period, &l, &cycle1, &defaults);
        assert_eq!(first.expect("first cycle sees a new service").total, 1);

        // Same service still visible on the next several polls -- none of
        // these should contribute anything new.
        for _ in 0..20 {
            let cycle = samples_with("AHT", vec![departure("svc-1", 0, false)]);
            let result =
                dedup_new_sample_stats(&mut ledger, "alton", period, &l, &cycle, &defaults);
            assert!(
                result.is_none(),
                "a still-dwelling service must not be recounted"
            );
        }
    }

    #[test]
    fn a_new_service_arriving_alongside_an_already_seen_one_only_counts_the_new_one() {
        let l = line("alton", &["AHT"]);
        let defaults = Defaults::default();
        let mut ledger = SeenServiceLedger::new();
        let period = day(2026, 8, 31);

        let cycle1 = samples_with("AHT", vec![departure("svc-1", 0, false)]);
        dedup_new_sample_stats(&mut ledger, "alton", period, &l, &cycle1, &defaults);

        // svc-1 still dwelling, svc-2 newly appeared in the window.
        let cycle2 = samples_with(
            "AHT",
            vec![departure("svc-1", 5, false), departure("svc-2", 0, false)],
        );
        let stats = dedup_new_sample_stats(&mut ledger, "alton", period, &l, &cycle2, &defaults)
            .expect("svc-2 is genuinely new");
        assert_eq!(
            stats.total, 1,
            "only the new service should be counted, not the still-dwelling one"
        );
    }

    #[test]
    fn dedup_is_scoped_per_line_not_global() {
        // Two lines sharing a sample station (e.g. a shared trunk) each
        // independently see the same physical service the first time they
        // observe it -- dedup must not suppress the second line's count
        // just because the first line already saw that service_id.
        let alton = line("alton", &["WOK"]);
        let main = line("main", &["WOK"]);
        let defaults = Defaults::default();
        let mut ledger = SeenServiceLedger::new();
        let period = day(2026, 8, 31);
        let samples = samples_with("WOK", vec![departure("shared-svc", 0, false)]);

        let alton_stats =
            dedup_new_sample_stats(&mut ledger, "alton", period, &alton, &samples, &defaults);
        let main_stats =
            dedup_new_sample_stats(&mut ledger, "main", period, &main, &samples, &defaults);
        assert_eq!(alton_stats.expect("alton's first sighting").total, 1);
        assert_eq!(
            main_stats
                .expect("main's first sighting, independent ledger key")
                .total,
            1
        );
    }

    #[test]
    fn dedup_is_scoped_per_period_not_global() {
        // The same service_id reappearing on a later day (a different
        // service that happens to hash to the same Darwin ID, or -- more
        // relevantly -- the ledger correctly starting a fresh count once
        // `period` advances) must count again.
        let l = line("alton", &["AHT"]);
        let defaults = Defaults::default();
        let mut ledger = SeenServiceLedger::new();
        let samples = samples_with("AHT", vec![departure("svc-1", 0, false)]);

        let day1 = dedup_new_sample_stats(
            &mut ledger,
            "alton",
            day(2026, 8, 31),
            &l,
            &samples,
            &defaults,
        );
        let day2 = dedup_new_sample_stats(
            &mut ledger,
            "alton",
            day(2026, 9, 1),
            &l,
            &samples,
            &defaults,
        );
        assert_eq!(day1.expect("day 1 sighting").total, 1);
        assert_eq!(
            day2.expect("day 2 is a fresh period, must count again")
                .total,
            1
        );
    }

    #[test]
    fn no_relevant_departures_yields_none() {
        let l = line("alton", &["AHT"]);
        let defaults = Defaults::default();
        let mut ledger = SeenServiceLedger::new();
        let samples: HashMap<String, StationSample> = HashMap::new();
        assert!(
            dedup_new_sample_stats(
                &mut ledger,
                "alton",
                day(2026, 8, 31),
                &l,
                &samples,
                &defaults
            )
            .is_none()
        );
    }

    #[test]
    fn min_sample_size_does_not_gate_deduped_stats() {
        // Only one departure total -- would fail `compute_sample_stats`'s
        // default min_sample_size of 3, but a single genuinely-new train is
        // still a real observation worth counting for a rollup.
        let l = line("alton", &["AHT"]);
        let defaults = Defaults::default();
        assert!(
            defaults.min_sample_size > 1,
            "test assumes the default gate exceeds 1"
        );
        let mut ledger = SeenServiceLedger::new();
        let samples = samples_with("AHT", vec![departure("svc-1", 0, false)]);

        let stats = dedup_new_sample_stats(
            &mut ledger,
            "alton",
            day(2026, 8, 31),
            &l,
            &samples,
            &defaults,
        );
        assert_eq!(stats.expect("single new train should still count").total, 1);
    }

    #[test]
    fn deduped_stats_classify_delay_cancellation_and_skip_like_raw_stats_do() {
        let l = line("alton", &["AHT"]);
        let defaults = Defaults::default();
        let mut ledger = SeenServiceLedger::new();
        let skipping = StationDeparture {
            skipped_stations: vec!["WOK".to_string()],
            ..departure("svc-3", 0, false)
        };
        let samples = samples_with(
            "AHT",
            vec![
                departure("svc-1", 10, false), // delayed (>= default threshold)
                departure("svc-2", 0, true),   // cancelled
                skipping,                      // skips a station on the line
            ],
        );

        let stats = dedup_new_sample_stats(
            &mut ledger,
            "alton",
            day(2026, 8, 31),
            &l,
            &samples,
            &defaults,
        )
        .expect("three new services");
        assert_eq!(stats.total, 3);
        assert_eq!(stats.cancelled, 1);
        assert_eq!(stats.delayed, 1);
        assert_eq!(stats.skipped, 1);
    }

    #[test]
    fn prune_before_drops_only_stale_periods() {
        let l = line("alton", &["AHT"]);
        let defaults = Defaults::default();
        let mut ledger = SeenServiceLedger::new();
        let samples = samples_with("AHT", vec![departure("svc-1", 0, false)]);

        dedup_new_sample_stats(
            &mut ledger,
            "alton",
            day(2026, 8, 30),
            &l,
            &samples,
            &defaults,
        );
        dedup_new_sample_stats(
            &mut ledger,
            "alton",
            day(2026, 8, 31),
            &l,
            &samples,
            &defaults,
        );
        assert_eq!(ledger.tracked_period_count(), 2);

        ledger.prune_before(day(2026, 8, 31));
        assert_eq!(
            ledger.tracked_period_count(),
            1,
            "only the stale (2026-08-30) period should be dropped"
        );

        // Pruned period's memory is gone, so the same service_id on that
        // now-forgotten day would count as new again if it recurred -- an
        // accepted tradeoff of bounding memory, not exercised further here.
    }
}
