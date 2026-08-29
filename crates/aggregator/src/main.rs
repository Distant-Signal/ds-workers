//! `aggregator`: periodically recomputes every line's status from
//! incidents + LDBWS samples and writes it to `line_status`/
//! `line_status_history`. See
//! `docs/superpowers/specs/2026-07-06-aggregator-read-api-design.md` for
//! the original design, and
//! `docs/superpowers/specs/2026-07-09-custom-lines-and-blended-stats-design.md`
//! for the custom-lines addition.

mod aggregation;
mod config;
mod matcher;
mod queries;
mod segments;

use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use common::{Defaults, LineDefinition};
use config::Config;
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

    let mut interval = tokio::time::interval(Duration::from_secs(config.poll_interval_secs));

    loop {
        interval.tick().await;

        let cycle_start = std::time::Instant::now();
        let result = run_cycle(&pool, &static_lines, &defaults, config.history_retention_days).await;
        metrics::histogram!(common::metrics::metric_name("aggregator_cycle_duration_seconds"))
            .record(cycle_start.elapsed().as_secs_f64());

        if let Err(err) = result {
            tracing::error!(error = ?err, "aggregation cycle failed; will retry next interval");
        }
    }
}

async fn run_cycle(
    pool: &sqlx::PgPool,
    static_lines: &HashMap<String, LineDefinition>,
    defaults: &Defaults,
    retention_days: i64,
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

    metrics::gauge!(common::metrics::metric_name("aggregator_lines_total")).set(reports.len() as f64);
    metrics::gauge!(common::metrics::metric_name("aggregator_incidents_loaded")).set(incidents.len() as f64);
    metrics::counter!(common::metrics::metric_name("aggregator_history_rows_pruned_total")).increment(pruned);

    tracing::info!(
        lines = reports.len(),
        incidents = incidents.len(),
        removed_lines = removed,
        pruned_history_rows = pruned,
        "aggregation cycle complete"
    );

    Ok(())
}
