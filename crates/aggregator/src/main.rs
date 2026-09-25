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
mod queries;

use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use common::segments::SegmentRegistry;
use common::{Defaults, LineDefinition, LineStatus, LineStatusReport};
use config::Config;
use dedup::SeenServiceLedger;
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

    let mut interval = cycle_interval(Duration::from_secs(config.poll_interval_secs));

    loop {
        interval.tick().await;

        let cycle_start = std::time::Instant::now();
        let result = run_cycle(
            &pool,
            &static_lines,
            &defaults,
            &mut dedup_ledger,
            config.full_coverage_enabled_default,
        )
        .await;

        if let Err(err) = result {
            tracing::error!(error = ?err, "aggregation cycle failed; will retry next interval");
        }

        // Retention runs UNCONDITIONALLY, after the aggregation pass and
        // outside its `?`-chain -- deliberately not gated on `result` being
        // `Ok`. Pruning has no data dependency on the status/stats writes
        // above, and two of its tiers exist to enforce real RDM licensing
        // obligations (`trust_event_backlog`'s 1-day window, the LDBWS-derived
        // stats tables' 1-year ceiling), so a run of failing aggregation
        // cycles must not quietly suspend them. See `run_retention`.
        if let Err(err) = run_retention(
            &pool,
            config.history_retention_days,
            config.daily_stats_retention_days,
            config.half_hourly_stats_retention_hours,
            config.trust_event_backlog_retention_days,
            config.trains_retention_days,
            config.untracked_trains_retention_days,
            config.schedule_destination_departures_retention_days,
            config.schedule_derived_products_retention_days,
        )
        .await
        {
            tracing::error!(error = ?err, "retention pruning failed; will retry next interval");
        }

        // Records the whole iteration -- aggregation AND retention -- which
        // is what this histogram measured before retention was split out of
        // `run_cycle`, so its existing dashboards/alerts keep their meaning.
        metrics::histogram!(common::metrics::metric_name(
            "aggregator_cycle_duration_seconds"
        ))
        .record(cycle_start.elapsed().as_secs_f64());
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

/// Builds `main`'s own top-level cycle `tokio::time::Interval`, with
/// `MissedTickBehavior::Delay` rather than the default `Burst`.
///
/// `Burst` fires every missed tick back-to-back with zero gap once a cycle
/// overruns `poll_interval` (a slow DB write, a stuck retention pass, or a
/// run of failing cycles that each still take real wall-clock time) --
/// exactly when the service is already struggling, it would pile up a
/// burst of immediate follow-up cycles against the database instead of
/// settling back into its normal cadence. `Delay` instead waits a fresh
/// `poll_interval` from whenever the overrun tick actually completes, so a
/// slow or failing cycle degrades to a slower cadence, never a
/// thundering-herd burst. Same fix, same rationale, as
/// `common::poller_loop`'s `poll_interval_with_delay_on_overrun` (shared by
/// every `poller-*` crate) and `enricher`'s own loop -- `aggregator`
/// predates `common::poller_loop` and does not share its loop scaffolding
/// (it has its own retention pass interleaved with aggregation, see
/// `run_retention` below), so this crate needed the identical fix applied
/// locally rather than by adopting that shared helper. Split into its own
/// function, mirroring `poller_loop`'s, so the configuration is directly
/// assertable in a unit test via `Interval::missed_tick_behavior()`,
/// since the missed-tick BEHAVIOR itself (skipping ticks under a real
/// overrun) isn't practically observable without a slow, flaky, real-time
/// test.
fn cycle_interval(poll_interval: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

// This crate's own single-call-site orchestration function. Every retention
// knob it used to thread through now belongs to `run_retention` instead --
// see that function's doc comment for why pruning is no longer part of this
// one's `?`-chain.
async fn run_cycle(
    pool: &sqlx::PgPool,
    static_lines: &HashMap<String, LineDefinition>,
    defaults: &Defaults,
    dedup_ledger: &mut SeenServiceLedger,
    full_coverage_enabled_default: bool,
) -> anyhow::Result<()> {
    // Every custom line loaded here is merged into the global catalogue and
    // then fully re-evaluated below -- matcher pass against every active
    // incident, segment rebuild, `line_status`/stats writes -- on every
    // cycle, for as long as it exists. That per-cycle cost is what
    // `crates/api`'s `custom_lines::MAX_CUSTOM_LINES_PER_USER` bounds at the
    // creation end; there is deliberately no cap enforced here, since
    // silently dropping catalogued lines mid-cycle would be worse than the
    // work of evaluating them.
    let custom_lines = queries::load_custom_lines(pool).await?;
    let lines = aggregation::merge_custom_lines(static_lines, custom_lines);
    let registry = SegmentRegistry::new(&lines);

    let incidents = queries::load_incidents(pool).await?;
    let mut samples = queries::load_station_samples(pool).await?;
    // Freshness gate on the LDBWS snapshot BEFORE anything reads it, so both
    // consumers below -- `aggregation::aggregate`'s severity inference and
    // `dedup::dedup_new_sample_stats`'s daily "distinct trains" rollup -- see
    // the same live-only view. See `aggregation::drop_stale_samples`.
    let stale_samples_dropped = aggregation::drop_stale_samples(&mut samples, chrono::Utc::now());

    let mut reports = aggregation::aggregate(&lines, &incidents, &samples, &registry, defaults);
    // Layer 3 (Decision 3): merges a per-line materialized full-coverage
    // signal onto the reports Layer 1/2 already built. `full-coverage-consumer`
    // (docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md,
    // Task 14) is that dedicated TRUST-vs-schedule consumer -- this reads
    // its output directly via SQL (this crate's own `PgPool`, not HTTP;
    // see that plan's Correction 1). Fails open to an empty map on a
    // query error, matching this crate's other reload passes' fail-open
    // posture (e.g. `stanox_crs`) -- every real line then reads as
    // Pending for this cycle rather than aborting the whole run. See
    // docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md
    // Decision 3 and `aggregation::merge_full_coverage`'s own doc comment.
    // Plain UTC date, deliberately NOT `queries::london_calendar_day` --
    // see `load_full_coverage_line_stats`'s own doc comment for why this
    // read must match its writers' (schedule-reference, full-coverage-consumer)
    // plain-UTC `service_date` key convention.
    let full_coverage_today = chrono::Utc::now().date_naive();
    let full_coverage = queries::load_full_coverage_line_stats(pool, full_coverage_today)
        .await
        .unwrap_or_else(|err| {
            tracing::error!(error = ?err, "failed to load full_coverage_line_stats; treating every enabled line as Pending this cycle");
            HashMap::new()
        });
    aggregation::merge_full_coverage(
        &mut reports,
        &lines,
        &full_coverage,
        defaults,
        full_coverage_enabled_default,
    );

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

    // The full-coverage sibling of the dedup/daily-stats pass above. Unlike
    // that pass, this one is fed each status's raw `full_coverage_stats`
    // directly, NOT run through a dedup step -- see
    // `queries::record_daily_coverage_stats`'s own module doc comment for
    // why (no defined per-service dedup analog exists yet for a
    // full-coverage producer).
    //
    // NOT a no-op any more, and this comment used to claim otherwise ("always
    // a no-op today ... `merge_full_coverage` above is always called with an
    // empty signal map"). That stopped being true on 2026-09-21: the Option B
    // consumer shipped as `crates/full-coverage-consumer`, it writes real
    // `full_coverage_line_stats` rows that `load_full_coverage_line_stats`
    // reads above, and `lines/tfw-conwy-valley.toml` sets
    // `full_coverage_enabled = true`, so that line really does flow through
    // `merge_full_coverage` -> here every cycle. Left uncorrected, the stale
    // comment invited exactly one bad conclusion: that
    // `merge_full_coverage_stats`'s severity-overwrite branch could not
    // affect production (it can -- see that function's own doc comment for
    // the demotion bug that reached live traffic through this path).
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

    metrics::gauge!(common::metrics::metric_name("aggregator_lines_total"))
        .set(reports.len() as f64);
    metrics::gauge!(common::metrics::metric_name("aggregator_incidents_loaded"))
        .set(incidents.len() as f64);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_deduped_new_services_total"
    ))
    .increment(new_services_this_cycle);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_daily_stats_recorded_total"
    ))
    .increment(daily_stats_recorded);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_half_hourly_stats_recorded_total"
    ))
    .increment(half_hourly_stats_recorded);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_coverage_stats_recorded_total"
    ))
    .increment(coverage_stats_recorded);

    tracing::info!(
        lines = reports.len(),
        incidents = incidents.len(),
        stale_sample_stations_dropped = stale_samples_dropped,
        removed_lines = removed,
        deduped_new_services = new_services_this_cycle,
        daily_stats_recorded = daily_stats_recorded,
        half_hourly_stats_recorded = half_hourly_stats_recorded,
        coverage_stats_recorded = coverage_stats_recorded,
        "aggregation cycle complete"
    );

    Ok(())
}

/// Every data-independent retention prune, in its own pass.
///
/// # Why this is NOT part of `run_cycle`'s `?`-chain
///
/// It used to be, interleaved with the status/stats writes, so ANY earlier
/// failure in the cycle -- a malformed row, a transient write error on one
/// line, a single `write_line_status` panic-free `Err` -- returned early and
/// silently skipped every prune queued behind it. One of those prunes is
/// `trust_event_backlog`'s, whose 1-day window exists to enforce an RDM
/// licensing safeguard (see `Config::trust_event_backlog_retention_days`),
/// and another is `line_status_daily_stats`/`line_status_half_hourly_stats`'
/// LDBWS 1-year deletion ceiling. Retention is a compliance obligation with
/// no data dependency on the aggregation it was sharing a `?`-chain with, so
/// `main`'s loop now awaits this separately and logs its own failure --
/// pruning happens on schedule even during a stretch of failing cycles.
///
/// `prune_removed_lines` deliberately stays in `run_cycle`: it needs that
/// cycle's freshly-merged line set, so it genuinely cannot run without a
/// successful load.
///
/// Every argument is a retention knob threaded straight through from
/// `Config` -- same posture (and same `#[allow]`) as
/// `full-coverage-consumer/src/main.rs` and `schedule-ingest/src/main.rs`'s
/// analogous top-level loop functions, which `run_cycle` itself used to
/// carry before this split.
#[allow(clippy::too_many_arguments)]
async fn run_retention(
    pool: &sqlx::PgPool,
    retention_days: i64,
    daily_stats_retention_days: i64,
    half_hourly_stats_retention_hours: i64,
    trust_event_backlog_retention_days: i64,
    trains_retention_days: i64,
    untracked_trains_retention_days: i64,
    schedule_destination_departures_retention_days: i64,
    schedule_derived_products_retention_days: i64,
) -> anyhow::Result<()> {
    let pruned = queries::prune_history(pool, retention_days).await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_history_rows_pruned_total"
    ))
    .increment(pruned);

    // Loud, unmissable, per-cycle (not "first cycle only") retention
    // safeguard for trust_event_backlog -- see
    // Config::trust_event_backlog_retention_days's own doc comment and
    // docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md's own
    // "Scope decision: retention tier and the licensing safeguard"
    // section. Checked every cycle, not gated behind a "first cycle only"
    // flag, so it reappears in logs on every restart too, not just the
    // very first one -- cheap, and survives log rotation.
    if trust_event_backlog_retention_days > 1 {
        tracing::warn!(
            configured_days = trust_event_backlog_retention_days,
            "trust_event_backlog retention is configured above the safe 1-day default -- \
             a human must have already confirmed TRUST's real Train Movements licence terms \
             directly with RDM before this value was set; see \
             docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md's own \
             \"Scope decision: retention tier and the licensing safeguard\" section"
        );
    }
    let trust_event_backlog_pruned =
        queries::prune_trust_event_backlog(pool, trust_event_backlog_retention_days).await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_trust_event_backlog_rows_pruned_total"
    ))
    .increment(trust_event_backlog_pruned);

    // Two-tier retention: a train with at least one train_subscriptions
    // row keeps trains_retention_days (30 by default), an untracked train
    // (no subscription at all) is pruned on the shorter
    // untracked_trains_retention_days (14 by default) instead -- see
    // Config::untracked_trains_retention_days's own doc comment and
    // queries::prune_trains's doc comment for the two-tier query
    // structure.
    let trains_pruned =
        queries::prune_trains(pool, trains_retention_days, untracked_trains_retention_days).await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_trains_rows_pruned_total"
    ))
    .increment(trains_pruned);

    // The CIF-derived destination-search table -- the one published product
    // in this repo that genuinely accrues (~377,000 rows per service date,
    // one row per departure) rather than wholesale-replacing a bounded key
    // space. See queries::prune_schedule_destination_departures' own doc
    // comment.
    let schedule_destination_departures_pruned = queries::prune_schedule_destination_departures(
        pool,
        schedule_destination_departures_retention_days,
    )
    .await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_schedule_destination_departures_rows_pruned_total"
    ))
    .increment(schedule_destination_departures_pruned);

    // The other three CIF-derived published products, which had NO pruning job
    // anywhere in this repo until 2026-09-25. All three grow by a new
    // `service_date` per delivery, forever -- the "bounded key space, trivial
    // steady-state size" reasoning that justified skipping them (quoted in the
    // comment directly above, which called its own table "the one published
    // product in this repo that genuinely accrues") bounded the `crs`/`line_id`
    // dimension and not the date one. See
    // Config::schedule_derived_products_retention_days and each prune
    // function's own doc comment.
    let schedule_calling_points_full_pruned =
        queries::prune_schedule_calling_points_full(pool, schedule_derived_products_retention_days)
            .await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_schedule_calling_points_full_rows_pruned_total"
    ))
    .increment(schedule_calling_points_full_pruned);

    let schedule_network_departures_pruned =
        queries::prune_schedule_network_departures(pool, schedule_derived_products_retention_days)
            .await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_schedule_network_departures_rows_pruned_total"
    ))
    .increment(schedule_network_departures_pruned);

    let schedule_line_population_pruned =
        queries::prune_schedule_line_population(pool, schedule_derived_products_retention_days)
            .await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_schedule_line_population_rows_pruned_total"
    ))
    .increment(schedule_line_population_pruned);

    let daily_stats_pruned = queries::prune_daily_stats(pool, daily_stats_retention_days).await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_daily_stats_pruned_total"
    ))
    .increment(daily_stats_pruned);
    let half_hourly_stats_pruned =
        queries::prune_half_hourly_stats(pool, half_hourly_stats_retention_hours).await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_half_hourly_stats_pruned_total"
    ))
    .increment(half_hourly_stats_pruned);

    let daily_coverage_stats_pruned =
        queries::prune_daily_coverage_stats(pool, daily_stats_retention_days).await?;
    let half_hourly_coverage_stats_pruned =
        queries::prune_half_hourly_coverage_stats(pool, half_hourly_stats_retention_hours).await?;
    metrics::counter!(common::metrics::metric_name(
        "aggregator_coverage_stats_pruned_total"
    ))
    .increment(daily_coverage_stats_pruned + half_hourly_coverage_stats_pruned);

    tracing::info!(
        pruned_history_rows = pruned,
        trust_event_backlog_pruned = trust_event_backlog_pruned,
        trains_pruned = trains_pruned,
        schedule_destination_departures_pruned = schedule_destination_departures_pruned,
        daily_stats_pruned = daily_stats_pruned,
        half_hourly_stats_pruned = half_hourly_stats_pruned,
        daily_coverage_stats_pruned = daily_coverage_stats_pruned,
        half_hourly_coverage_stats_pruned = half_hourly_coverage_stats_pruned,
        "retention pruning complete"
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

    #[tokio::test]
    async fn cycle_interval_defaults_to_delay_not_burst_on_a_missed_tick() {
        let interval = cycle_interval(Duration::from_secs(60));
        assert_eq!(
            interval.missed_tick_behavior(),
            tokio::time::MissedTickBehavior::Delay,
            "a slow or failing cycle must not burst-fire every missed tick back-to-back"
        );
    }

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
