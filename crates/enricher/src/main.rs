//! `enricher`: extracts structured resolution status, category, schedule
//! window, and ETA from Knowledgebase incident text via an OpenAI-compatible
//! LLM endpoint. See
//! docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md.

mod config;
mod stream;

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
    let mut redis = redis_client.get_connection_manager().await?;
    stream::ensure_group(&mut redis).await?;

    spawn_sweep_timer(pool.clone(), config.sweep_interval_secs); // implemented in Task 5

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

fn spawn_sweep_timer(_pool: sqlx::PgPool, _interval_secs: u64) {
    // Task 5 fills this in.
}
