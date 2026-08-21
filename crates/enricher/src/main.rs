//! `enricher`: extracts structured resolution status, category, schedule
//! window, and ETA from Knowledgebase incident text via an OpenAI-compatible
//! LLM endpoint. See
//! docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md.

mod combine;
mod config;
mod hash;
mod llm;
mod stream;
mod sweep;

use std::time::Duration;

use clap::Parser;
use config::Config;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await?;

    let redis_client = redis::Client::open(config.redis_url.clone())?;
    let mut redis = redis_client.get_connection_manager().await?;
    stream::ensure_group(&mut redis).await?;

    tokio::spawn(sweep_loop(pool.clone(), config.llm_model.clone(), config.sweep_interval_secs));

    loop {
        match stream::read_one(&mut redis).await {
            Ok(Some((entry_id, incident_id))) => {
                tracing::info!(incident_id, "received text-changed event");
                // Task 8 replaces this stub with the real two-pass
                // extraction + DB write.
                if let Err(err) = stream::ack(&mut redis, &entry_id).await {
                    tracing::error!(error = ?err, entry_id, "failed to ack stream entry");
                }
            }
            Ok(None) => {}
            Err(err) => {
                tracing::error!(error = ?err, "error reading from incident-text-changed stream");
            }
        }
    }
}

/// Hourly (by default) backstop that re-checks every uncleared incident's
/// text hash / extraction model version against what's stored, catching
/// anything the Redis Stream consumer loop above missed (publish failure,
/// consumer downtime, etc). Runs independently of that loop.
async fn sweep_loop(pool: PgPool, model_version: String, interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        match sweep::fetch_sweep_rows(&pool).await {
            Ok(rows) => {
                let ids = sweep::incidents_needing_extraction(&rows, &model_version);
                tracing::info!(count = ids.len(), "sweep found incidents needing extraction");
                // Task 8 replaces this log line with actually enqueueing
                // each id through the same processor the stream loop uses.
            }
            Err(err) => tracing::error!(error = ?err, "sweep query failed"),
        }
    }
}
