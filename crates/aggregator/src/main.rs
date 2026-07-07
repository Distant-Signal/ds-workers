//! `aggregator`: periodically recomputes every line's status from
//! incidents + LDBWS samples and writes it to `line_status`/
//! `line_status_history`. See
//! `docs/superpowers/specs/2026-07-06-aggregator-read-api-design.md` for
//! the full design.

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
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;

    let lines: HashMap<String, LineDefinition> =
        config.lines.iter().map(|l| (l.id.clone(), l.clone())).collect();
    tracing::info!(count = lines.len(), "loaded line catalogue");

    let registry = SegmentRegistry::new(&lines);
    let defaults = Defaults::default();

    let mut interval = tokio::time::interval(Duration::from_secs(config.poll_interval_secs));

    loop {
        interval.tick().await;

        if let Err(err) = run_cycle(&pool, &lines, &registry, &defaults, config.history_retention_days).await {
            tracing::error!(error = ?err, "aggregation cycle failed; will retry next interval");
        }
    }
}

async fn run_cycle(
    pool: &sqlx::PgPool,
    lines: &HashMap<String, LineDefinition>,
    registry: &SegmentRegistry,
    defaults: &Defaults,
    retention_days: i64,
) -> anyhow::Result<()> {
    let incidents = queries::load_incidents(pool).await?;
    let samples = queries::load_station_samples(pool).await?;

    let reports = aggregation::aggregate(lines, &incidents, &samples, registry, defaults);

    for report in reports.values() {
        queries::write_line_status(pool, report).await?;
    }

    let pruned = queries::prune_history(pool, retention_days).await?;
    tracing::info!(
        lines = reports.len(),
        incidents = incidents.len(),
        pruned_history_rows = pruned,
        "aggregation cycle complete"
    );

    Ok(())
}
