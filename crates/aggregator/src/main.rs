//! `aggregator`: periodically recomputes every line's status from
//! incidents + LDBWS samples and writes it to `line_status`/
//! `line_status_history`. See
//! `docs/superpowers/specs/2026-07-06-aggregator-read-api-design.md` for
//! the original design, and
//! `docs/superpowers/specs/2026-07-09-custom-lines-and-blended-stats-design.md`
//! for the custom-lines addition.

mod aggregation;
mod config;
mod dedup;
mod matcher;
mod queries;
mod segments;

use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use common::{Defaults, LineDefinition, LineStatus, LineStatusReport};
use config::Config;
use dedup::SeenServiceLedger;
use segments::SegmentRegistry;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    if config.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;

    let static_lines: HashMap<String, LineDefinition> = config
        .lines
        .iter()
        .map(|l| (l.id.clone(), l.clone()))
        .collect();
    tracing::info!(count = static_lines.len(), "loaded static line catalogue");

    let defaults = Defaults::default();

    // Lives for the whole process, threaded into every cycle -- this is
    // exactly what makes the dedup ledger "in-memory, restart-scoped"
    // rather than per-cycle: a service seen on cycle N must still be
    // recognized as already-counted on cycle N+1. See `dedup`'s module
    // docs for why a process-lifetime, non-persisted ledger is judged
    // sufficient.
    let mut dedup_ledger = SeenServiceLedger::new();

    let mut interval = tokio::time::interval(Duration::from_secs(config.poll_interval_secs));

    loop {
        interval.tick().await;

        let cycle_start = std::time::Instant::now();
        let result = run_cycle(
            &pool,
            &static_lines,
            &defaults,
            config.history_retention_days,
            config.daily_stats_retention_days,
            config.half_hourly_stats_retention_hours,
            &mut dedup_ledger,
        )
        .await;
        metrics::histogram!(common::metrics::metric_name(
            "aggregator_cycle_duration_seconds"
        ))
        .record(cycle_start.elapsed().as_secs_f64());

        if let Err(err) = result {
            tracing::error!(error = ?err, "aggregation cycle failed; will retry next interval");
        }
    }
}

/// Cap on how many lines' writes share a single `sqlx::Transaction` in
/// `run_cycle`, below. Mirrors `crates/api/src/data/queries.rs`'s
/// `UPSERT_CHUNK_SIZE` (50) -- same rationale, same codebase precedent
/// (`upsert_incidents`'s doc comment there spells out the tradeoff in
/// detail): batch enough per transaction to collapse most of the
/// per-statement WAL-fsync cost this mitigation targets, but cap it so a
/// mid-batch failure only rolls back one chunk's worth of otherwise-good
/// writes, not the whole cycle, and so no single transaction holds row
/// locks across an unbounded number of lines for an unbounded time.
///
/// # Why chunked transactions, not one whole-cycle transaction
///
/// See docs/superpowers/specs/2026-09-02-slow-query-warnings-research.md,
/// Recommendation #3. Before this change, `run_cycle` issued one
/// autocommitted statement per line per write (~2-3 for
/// `write_line_status`, 2 more for the daily/half-hourly stats pair) --
/// up to ~300-400 independent WAL-fsync-gated commits per 60s cycle
/// across up to ~110 lines, which the research doc ranks as the most
/// likely driver of production `sqlx::query: slow statement` warnings
/// (many backends committing into the same WAL at once, not a per-query
/// inefficiency).
///
/// A single transaction wrapping the *entire* cycle was considered and
/// rejected: `run_cycle`'s caller (`main`, above) already treats any `Err`
/// from a cycle as "log it, retry the whole computation next interval"
/// (60s later) -- there is no partial-credit handling today, so the
/// *existing* per-statement-commit code already tolerates "a failure
/// partway through drops the rest of this cycle's writes" as its error
/// model. But collapsing all ~110 lines' worth of `write_line_status` (and
/// separately, all lines' worth of daily/half-hourly stats) into ONE
/// transaction each would mean a single bad statement -- realistically a
/// transient error on one specific line, not the common case -- rolls back
/// every other, unrelated line's already-computed, already-queued write
/// too, discarding good work for lines that had nothing wrong with them.
/// That is a real regression versus today's "each line's write succeeds or
/// fails independently" behavior, not just a hypothetical concern, since a
/// connection-level failure (which would already lose everything in
/// flight regardless of batching) is far from the only way a single
/// statement can fail.
///
/// Chunking splits the difference precisely: within a chunk, an early
/// line's failure does roll back that chunk's other lines (an accepted,
/// bounded regression -- at most `WRITE_CHUNK_SIZE` lines' worth, and the
/// whole cycle retries in 60s anyway, mirroring `upsert_incidents`'s own
/// "the poller resends the full state every round" reasoning), while
/// still collapsing WAL-fsync count from one-per-statement down to
/// roughly `lines / WRITE_CHUNK_SIZE` transactions for each pass. At the
/// pasted log's own `lines=110`, that's 3 transactions instead of up to
/// ~330 individual commits for the `write_line_status` pass alone.
///
/// The daily/half-hourly stats pass (below) is chunked separately from the
/// `write_line_status` pass, in its own set of transactions -- the two
/// passes touch different tables for a different purpose and have no
/// atomicity requirement *between* them (a line's status can legitimately
/// update in one cycle while its stats contribution lands, or doesn't, in
/// another -- they were never coupled even before this change, since they
/// were always separate autocommitted statements). This also matches the
/// research doc's own suggestion of "a separate one for the daily/
/// half-hourly stats pass."
///
/// One caveat worth recording rather than hiding: `dedup::SeenServiceLedger`
/// (see `dedup.rs`) is mutated in-memory, synchronously, *before* the
/// corresponding `record_daily_stats`/`record_half_hourly_stats` calls in
/// the loop below run. If a chunk's transaction later fails and rolls
/// back, every line already processed earlier in that same chunk has its
/// dedup "seen" marks stay consumed in memory even though their DB writes
/// for this cycle just got undone -- a pre-existing risk (it already
/// existed per-line, pre-batching, whenever a single write_line_status/
/// record_*_stats call failed) that chunking widens from "1 line" to "up
/// to WRITE_CHUNK_SIZE lines" in the rare case a chunk transaction has to
/// roll back. `record_daily_stats`/`record_half_hourly_stats` do simple
/// parameterized `INSERT ... ON CONFLICT` against tables with real PKs on
/// entirely local, already-valid data, so this is expected to be
/// vanishingly rare in practice (the realistic failure mode is a
/// connection-level error, which loses in-flight work regardless of
/// batching); a smaller `WRITE_CHUNK_SIZE` trades this exposure directly
/// against transaction count/WAL-fsync savings if it ever needs revisiting.
const WRITE_CHUNK_SIZE: usize = 50;

async fn run_cycle(
    pool: &sqlx::PgPool,
    static_lines: &HashMap<String, LineDefinition>,
    defaults: &Defaults,
    retention_days: i64,
    daily_stats_retention_days: i64,
    half_hourly_stats_retention_hours: i64,
    dedup_ledger: &mut SeenServiceLedger,
) -> anyhow::Result<()> {
    let custom_lines = queries::load_custom_lines(pool).await?;
    let lines = aggregation::merge_custom_lines(static_lines, custom_lines);
    let registry = SegmentRegistry::new(&lines);

    let incidents = queries::load_incidents(pool).await?;
    let samples = queries::load_station_samples(pool).await?;

    let mut reports = aggregation::aggregate(&lines, &incidents, &samples, &registry, defaults);
    // Layer 3 (Decision 3 scaffolding): merges a per-line materialized
    // full-coverage signal onto the reports Layer 1/2 already built. The
    // `&HashMap::new()` below is a placeholder -- no dedicated
    // TRUST-vs-schedule consumer ("Option B") exists yet to populate it;
    // building one is a separate, later, not-yet-planned task. This call
    // site is where that future consumer's own per-line materialized rows
    // would be handed in once it exists. See
    // docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md
    // Decision 3 and `aggregation::merge_full_coverage`'s own doc comment.
    aggregation::merge_full_coverage(&mut reports, &lines, &HashMap::new(), defaults);

    // Batched into `WRITE_CHUNK_SIZE`-sized transactions rather than one
    // autocommitted statement per line -- see `WRITE_CHUNK_SIZE`'s doc
    // comment for the full reasoning.
    let report_list: Vec<&LineStatusReport> = reports.values().collect();
    for chunk in report_list.chunks(WRITE_CHUNK_SIZE) {
        let mut tx = pool.begin().await?;
        for report in chunk.iter().copied() {
            queries::write_line_status(&mut tx, report).await?;
        }
        tx.commit().await?;
    }

    let current_line_ids: Vec<String> = lines.keys().cloned().collect();
    let removed = queries::prune_removed_lines(pool, &current_line_ids).await?;

    let pruned = queries::prune_history(pool, retention_days).await?;

    // Per-service dedup pass, folded together with the daily-stats write:
    // `dedup::dedup_new_sample_stats` is STATEFUL (it mutates `dedup_ledger`
    // via `mark_seen`), so it must be called AT MOST ONCE per line per
    // cycle -- calling it twice for the same line would make the second
    // call see everything as already-seen and silently under-report. The
    // gate for whether a line gets a `record_daily_stats` write at all
    // (independent of whether dedup finds anything NEW) is computed once
    // per line via `lines_with_sample_coverage`, not once per status --
    // see that function's doc for why iterating `report.statuses` would
    // double-count.
    let cycle_now = chrono::Utc::now();
    let today = queries::london_calendar_day(cycle_now);
    let half_hour_start = queries::utc_half_hour_start(cycle_now);
    let mut new_services_this_cycle: u64 = 0;
    let mut daily_stats_recorded = 0u64;
    let mut half_hourly_stats_recorded = 0u64;
    // Batched into WRITE_CHUNK_SIZE-sized transactions, same rationale (and
    // caveat re: dedup_ledger mutation ordering) as the write_line_status
    // pass above -- see `WRITE_CHUNK_SIZE`'s doc comment. A separate set of
    // transactions from that pass, not a shared one: different tables,
    // different purpose, no atomicity requirement between the two passes.
    let coverage = lines_with_sample_coverage(&reports, &lines);
    for chunk in coverage.chunks(WRITE_CHUNK_SIZE) {
        let mut tx = pool.begin().await?;
        for &(line_id, line) in chunk {
            let deduped = dedup::dedup_new_sample_stats(
                dedup_ledger,
                line_id,
                today,
                line,
                &samples,
                defaults,
            );
            if let Some(ref stats) = deduped {
                new_services_this_cycle += stats.total as u64;
            }
            // Both calls below are fed the SAME `deduped` value -- this is
            // Decision 2's whole point (see that function's own doc comment
            // and the half_hourly_and_daily_stats_reconcile_for_a_single_line_and_period
            // test in queries.rs): a day's 48 half-hourly rows must sum back to
            // that day's daily row, which only holds if both writes see an
            // identical per-cycle contribution, not two independently
            // computed ones. Sharing the same chunk transaction doesn't
            // change this invariant -- it held (and was verified by that
            // test) back when both calls were separately autocommitted too.
            queries::record_daily_stats(&mut *tx, line_id, today, deduped.as_ref()).await?;
            queries::record_half_hourly_stats(&mut *tx, line_id, half_hour_start, deduped.as_ref())
                .await?;
            daily_stats_recorded += 1;
            half_hourly_stats_recorded += 1;
        }
        tx.commit().await?;
    }
    dedup_ledger.prune_before(today);

    let daily_stats_pruned = queries::prune_daily_stats(pool, daily_stats_retention_days).await?;
    let half_hourly_stats_pruned = queries::prune_half_hourly_stats(pool, half_hourly_stats_retention_hours).await?;

    // Decision 4 scaffolding: the full-coverage sibling of the dedup/
    // daily-stats pass above. Unlike that pass, this one is fed each
    // status's raw `full_coverage_stats` directly, NOT run through a
    // dedup step -- see `queries::record_daily_coverage_stats`'s own
    // module doc comment for why (no defined per-service dedup analog
    // exists yet for a full-coverage producer). Always a no-op today
    // (`lines_with_full_coverage` finds nothing, since `merge_full_coverage`
    // above is always called with an empty signal map) -- this is the
    // write-path half of the same scaffolding `merge_full_coverage`
    // documents.
    let coverage_lines = lines_with_full_coverage(&reports);
    let mut coverage_stats_recorded = 0u64;
    for chunk in coverage_lines.chunks(WRITE_CHUNK_SIZE) {
        let mut tx = pool.begin().await?;
        for &(line_id, status) in chunk {
            queries::record_daily_coverage_stats(
                &mut *tx,
                line_id,
                today,
                status.full_coverage_stats.as_ref(),
            )
            .await?;
            queries::record_half_hourly_coverage_stats(
                &mut *tx,
                line_id,
                half_hour_start,
                status.full_coverage_stats.as_ref(),
            )
            .await?;
            coverage_stats_recorded += 1;
        }
        tx.commit().await?;
    }

    let daily_coverage_stats_pruned =
        queries::prune_daily_coverage_stats(pool, daily_stats_retention_days).await?;
    let half_hourly_coverage_stats_pruned =
        queries::prune_half_hourly_coverage_stats(pool, half_hourly_stats_retention_hours).await?;

    metrics::gauge!(common::metrics::metric_name("aggregator_lines_total"))
        .set(reports.len() as f64);
    metrics::gauge!(common::metrics::metric_name("aggregator_incidents_loaded"))
        .set(incidents.len() as f64);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_history_rows_pruned_total"
    ))
    .increment(pruned);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_deduped_new_services_total"
    ))
    .increment(new_services_this_cycle);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_daily_stats_recorded_total"
    ))
    .increment(daily_stats_recorded);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_daily_stats_pruned_total"
    ))
    .increment(daily_stats_pruned);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_half_hourly_stats_recorded_total"
    ))
    .increment(half_hourly_stats_recorded);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_half_hourly_stats_pruned_total"
    ))
    .increment(half_hourly_stats_pruned);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_coverage_stats_recorded_total"
    ))
    .increment(coverage_stats_recorded);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_coverage_stats_pruned_total"
    ))
    .increment(daily_coverage_stats_pruned + half_hourly_coverage_stats_pruned);

    tracing::info!(
        lines = reports.len(),
        incidents = incidents.len(),
        removed_lines = removed,
        pruned_history_rows = pruned,
        deduped_new_services = new_services_this_cycle,
        daily_stats_recorded = daily_stats_recorded,
        daily_stats_pruned = daily_stats_pruned,
        half_hourly_stats_recorded = half_hourly_stats_recorded,
        half_hourly_stats_pruned = half_hourly_stats_pruned,
        coverage_stats_recorded = coverage_stats_recorded,
        daily_coverage_stats_pruned = daily_coverage_stats_pruned,
        half_hourly_coverage_stats_pruned = half_hourly_coverage_stats_pruned,
        "aggregation cycle complete"
    );

    Ok(())
}

/// Selects the `(line_id, &LineDefinition)` pairs that qualify for a
/// `queries::record_daily_stats` write this cycle: a line qualifies when its
/// `LineStatusReport` carries raw `sample_stats` this cycle, i.e. it had ANY
/// raw live coverage, independent of whether dedup later finds anything NEW
/// to contribute (that's `sample_cycles`'s signal -- see
/// `queries::record_daily_stats`'s doc).
///
/// Every status on a report carries an identical `Option<SampleStats>` clone
/// when `Some` (`aggregation.rs`'s Layer 2 and `infer_from_samples` both set
/// it this way), so checking only `report.statuses.first()` is correct and
/// sufficient -- iterating all of `report.statuses` here would call
/// `record_daily_stats` (and, worse, the stateful `dedup_new_sample_stats`)
/// once per status rather than once per line, silently double- (or
/// N-) counting any line with more than one concurrent incident. Pure and
/// synchronous so it's separately testable without a `PgPool`.
fn lines_with_sample_coverage<'a>(
    reports: &HashMap<String, LineStatusReport>,
    lines: &'a HashMap<String, LineDefinition>,
) -> Vec<(&'a str, &'a LineDefinition)> {
    reports
        .values()
        .filter(|report| {
            report
                .statuses
                .first()
                .and_then(|s| s.sample_stats.as_ref())
                .is_some()
        })
        .filter_map(|report| lines.get_key_value(report.id.as_str()))
        .map(|(id, line)| (id.as_str(), line))
        .collect()
}

/// Decision 4 scaffolding: the full-coverage analog of
/// `lines_with_sample_coverage`, selecting which lines qualify for a
/// `record_daily_coverage_stats`/`record_half_hourly_coverage_stats`
/// write this cycle. Same "check only `report.statuses.first()`" pattern
/// and the same reasoning: `merge_full_coverage_stats`
/// (`crates/aggregator/src/aggregation.rs`) sets an identical
/// `full_coverage_stats` clone on every status of a report it touches, so
/// checking the first status is correct and sufficient, and avoids
/// double-counting a line with more than one concurrent status. Returns
/// the line id paired with that first status (not the `LineDefinition`
/// the sample-coverage sibling returns) -- the write path below needs the
/// status's `full_coverage_stats` value itself, not anything from the
/// line's own TOML definition. Always empty in production today, since
/// `merge_full_coverage`'s only call site passes an empty signal map.
fn lines_with_full_coverage(
    reports: &HashMap<String, LineStatusReport>,
) -> Vec<(&str, &LineStatus)> {
    reports
        .values()
        .filter_map(|report| {
            report
                .statuses
                .first()
                .filter(|s| s.full_coverage_stats.is_some())
                .map(|status| (report.id.as_str(), status))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use common::{
        DataQuality, LineStatus, SampleAvailability, SampleStats, Severity, ValidityPeriod,
    };

    use super::*;

    fn line_def(id: &str) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "tube".to_string(),
            category: "tube".to_string(),
            operators: vec![],
            stations: vec![],
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: Default::default(),
            exclusive_segments: vec![],
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
        }
    }

    fn status_with_stats(stats: Option<SampleStats>) -> LineStatus {
        LineStatus {
            severity: Severity::GoodService,
            reason: "Good Service".to_string(),
            validity: ValidityPeriod {
                from_date: chrono::Utc::now(),
                to_date: None,
                is_now: true,
            },
            disruption: None,
            data_quality: DataQuality::default(),
            sample_stats: stats,
            sample_availability: SampleAvailability::NoCoverage,
            full_coverage_stats: None,
            full_coverage_availability: common::FullCoverageAvailability::NotEnabled,
        }
    }

    fn status_with_full_coverage_stats(stats: Option<SampleStats>) -> LineStatus {
        LineStatus {
            severity: Severity::GoodService,
            reason: "Good Service".to_string(),
            validity: ValidityPeriod {
                from_date: chrono::Utc::now(),
                to_date: None,
                is_now: true,
            },
            disruption: None,
            data_quality: DataQuality::default(),
            sample_stats: None,
            sample_availability: SampleAvailability::NoCoverage,
            full_coverage_availability: match &stats {
                Some(s) => common::FullCoverageAvailability::Available(s.clone()),
                None => common::FullCoverageAvailability::NotEnabled,
            },
            full_coverage_stats: stats,
        }
    }

    fn sample_stats() -> SampleStats {
        SampleStats {
            total: 10,
            delayed: 2,
            cancelled: 1,
            skipped: 0,
            avg_delay_minutes: 3.5,
        }
    }

    #[test]
    fn counts_a_line_with_two_concurrent_incidents_exactly_once() {
        let lines: HashMap<String, LineDefinition> =
            [("central".to_string(), line_def("central"))].into();
        let reports: HashMap<String, LineStatusReport> = [(
            "central".to_string(),
            LineStatusReport {
                id: "central".to_string(),
                name: "Central".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                // Two concurrent incidents on the same line -- both carry
                // the identical `Some(SampleStats)` clone, matching how
                // aggregation.rs actually populates multi-status reports.
                statuses: vec![
                    status_with_stats(Some(sample_stats())),
                    status_with_stats(Some(sample_stats())),
                ],
            },
        )]
        .into();

        let selected = lines_with_sample_coverage(&reports, &lines);

        assert_eq!(
            selected.len(),
            1,
            "expected exactly one entry per line regardless of status count"
        );
        assert_eq!(selected[0].0, "central");
    }

    #[test]
    fn excludes_a_line_with_no_sample_stats_on_any_status() {
        let lines: HashMap<String, LineDefinition> =
            [("victoria".to_string(), line_def("victoria"))].into();
        let reports: HashMap<String, LineStatusReport> = [(
            "victoria".to_string(),
            LineStatusReport {
                id: "victoria".to_string(),
                name: "Victoria".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                statuses: vec![status_with_stats(None), status_with_stats(None)],
            },
        )]
        .into();

        let selected = lines_with_sample_coverage(&reports, &lines);

        assert!(selected.is_empty());
    }

    #[test]
    fn skips_a_report_with_no_matching_line_definition_without_panicking() {
        // Report present but its line_id isn't in `lines` -- shouldn't panic,
        // should just be silently skipped (defensive; reports are normally
        // derived from lines).
        let lines: HashMap<String, LineDefinition> = HashMap::new();
        let reports: HashMap<String, LineStatusReport> = [(
            "jubilee".to_string(),
            LineStatusReport {
                id: "jubilee".to_string(),
                name: "Jubilee".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                statuses: vec![status_with_stats(Some(sample_stats()))],
            },
        )]
        .into();

        let selected = lines_with_sample_coverage(&reports, &lines);

        assert!(selected.is_empty());
    }

    #[test]
    fn handles_empty_reports_without_panicking() {
        let lines: HashMap<String, LineDefinition> =
            [("bakerloo".to_string(), line_def("bakerloo"))].into();
        let reports: HashMap<String, LineStatusReport> = HashMap::new();

        let selected = lines_with_sample_coverage(&reports, &lines);

        assert!(selected.is_empty());
    }

    // --- lines_with_full_coverage (Decision 4 scaffolding) ---

    #[test]
    fn lines_with_full_coverage_counts_a_line_with_two_concurrent_statuses_exactly_once() {
        let reports: HashMap<String, LineStatusReport> = [(
            "central".to_string(),
            LineStatusReport {
                id: "central".to_string(),
                name: "Central".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                statuses: vec![
                    status_with_full_coverage_stats(Some(sample_stats())),
                    status_with_full_coverage_stats(Some(sample_stats())),
                ],
            },
        )]
        .into();

        let selected = lines_with_full_coverage(&reports);

        assert_eq!(
            selected.len(),
            1,
            "expected exactly one entry per line regardless of status count"
        );
        assert_eq!(selected[0].0, "central");
    }

    #[test]
    fn lines_with_full_coverage_excludes_a_line_with_no_full_coverage_stats_on_any_status() {
        let reports: HashMap<String, LineStatusReport> = [(
            "victoria".to_string(),
            LineStatusReport {
                id: "victoria".to_string(),
                name: "Victoria".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                statuses: vec![
                    status_with_full_coverage_stats(None),
                    status_with_full_coverage_stats(None),
                ],
            },
        )]
        .into();

        let selected = lines_with_full_coverage(&reports);

        assert!(selected.is_empty());
    }

    #[test]
    fn lines_with_full_coverage_is_independent_of_sample_stats_presence() {
        // A line can have real sample_stats but no full_coverage_stats (the
        // overwhelming majority case today) -- must not be selected.
        let reports: HashMap<String, LineStatusReport> = [(
            "jubilee".to_string(),
            LineStatusReport {
                id: "jubilee".to_string(),
                name: "Jubilee".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                statuses: vec![status_with_stats(Some(sample_stats()))],
            },
        )]
        .into();

        let selected = lines_with_full_coverage(&reports);

        assert!(selected.is_empty());
    }
}
