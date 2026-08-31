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
use common::{Defaults, LineDefinition};
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

    let static_lines: HashMap<String, LineDefinition> =
        config.lines.iter().map(|l| (l.id.clone(), l.clone())).collect();
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
        let result =
            run_cycle(&pool, &static_lines, &defaults, config.history_retention_days, &mut dedup_ledger).await;
        metrics::histogram!(common::metrics::metric_name("aggregator_cycle_duration_seconds"))
            .record(cycle_start.elapsed().as_secs_f64());

        if let Err(err) = result {
            tracing::error!(error = ?err, "aggregation cycle failed; will retry next interval");
        }
    }
}

/// The Europe/London calendar day `now` falls on -- the dedup ledger's
/// period boundary. Deliberately the plain calendar day, not the
/// aggregator's own rail-day-02:00 convention (`aggregation::
/// next_rail_day_boundary`): this matches the convention the
/// line-history-graphics design doc's daily rollup already settled on
/// (Decision 1, for consistency with the adjacent Timeline tab's own
/// grouping), so a future rollup consumer can use this same period value
/// directly as its own `day` key with no conversion.
fn london_calendar_day(now: chrono::DateTime<chrono::Utc>) -> chrono::NaiveDate {
    now.with_timezone(&chrono_tz::Europe::London).date_naive()
}

async fn run_cycle(
    pool: &sqlx::PgPool,
    static_lines: &HashMap<String, LineDefinition>,
    defaults: &Defaults,
    retention_days: i64,
    dedup_ledger: &mut SeenServiceLedger,
) -> anyhow::Result<()> {
    let custom_lines = queries::load_custom_lines(pool).await?;
    let lines = aggregation::merge_custom_lines(static_lines, custom_lines);
    let registry = SegmentRegistry::new(&lines);

    let incidents = queries::load_incidents(pool).await?;
    let samples = queries::load_station_samples(pool).await?;

    let reports = aggregation::aggregate(&lines, &incidents, &samples, &registry, defaults);

    for report in reports.values() {
        queries::write_line_status(pool, report).await?;
    }

    let current_line_ids: Vec<String> = lines.keys().cloned().collect();
    let removed = queries::prune_removed_lines(pool, &current_line_ids).await?;

    let pruned = queries::prune_history(pool, retention_days).await?;

    // Per-service dedup pass: computed alongside (not instead of) the raw,
    // per-cycle `sample_stats` already attached to `reports` above -- this
    // does not persist anywhere yet (no `line_status_daily_stats` table
    // exists), it only proves the `dedup` module's ledger stays correct
    // across real consecutive cycles and exposes the "how many genuinely
    // new trains this cycle" signal as a metric, ready for the eventual
    // daily-rollup write path to consume instead of raw per-cycle counts.
    // Bounded to an aggregate count (not per-line) to avoid per-line metric
    // cardinality, matching this loop's existing aggregate-only gauges.
    let period = london_calendar_day(chrono::Utc::now());
    let mut new_services_this_cycle: u64 = 0;
    for line in lines.values() {
        if let Some(stats) = dedup::dedup_new_sample_stats(dedup_ledger, &line.id, period, line, &samples, defaults) {
            new_services_this_cycle += stats.total as u64;
        }
    }
    dedup_ledger.prune_before(period);

    metrics::gauge!(common::metrics::metric_name("aggregator_lines_total")).set(reports.len() as f64);
    metrics::gauge!(common::metrics::metric_name("aggregator_incidents_loaded")).set(incidents.len() as f64);
    metrics::counter!(common::metrics::metric_name("aggregator_history_rows_pruned_total")).increment(pruned);
    metrics::counter!(common::metrics::metric_name("aggregator_deduped_new_services_total"))
        .increment(new_services_this_cycle);

    tracing::info!(
        lines = reports.len(),
        incidents = incidents.len(),
        removed_lines = removed,
        pruned_history_rows = pruned,
        deduped_new_services = new_services_this_cycle,
        "aggregation cycle complete"
    );

    Ok(())
}
