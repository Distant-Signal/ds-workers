//! `enricher`: extracts structured resolution status, category, schedule
//! window, and ETA from Knowledgebase incident text via an OpenAI-compatible
//! LLM endpoint. See
//! docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md.

mod config;

use std::time::Duration;

use clap::Parser;
use config::Config;
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
    let _redis = redis_client.get_connection_manager().await?;

    tracing::info!("enricher connected to postgres and redis; consumer loop and sweep land in later tasks");

    // Placeholder idle loop -- Task 4 replaces this with the real Redis
    // Streams consumer-group loop, and Task 5 adds the sweep timer
    // alongside it.
    let mut interval = tokio::time::interval(Duration::from_secs(config.sweep_interval_secs));
    loop {
        interval.tick().await;
        tracing::info!("enricher heartbeat");
        let _ = &pool;
    }
}
