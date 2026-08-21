//! `enricher`: extracts structured resolution status, category, schedule
//! window, and ETA from Knowledgebase incident text via an OpenAI-compatible
//! LLM endpoint. See
//! docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md.

mod combine;
mod config;
mod hash;
mod llm;
mod queries;
mod stream;
mod sweep;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use config::Config;
use llm::LlmClient;
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

    let llm = Arc::new(LlmClient::new(config.llm_base_url.clone(), config.llm_api_key.clone(), config.llm_model.clone()));
    let model_version = config.llm_model.clone();

    tokio::spawn(sweep_loop(pool.clone(), Arc::clone(&llm), model_version.clone(), config.sweep_interval_secs));

    loop {
        match stream::read_one(&mut redis).await {
            Ok(Some((entry_id, incident_id))) => {
                process_incident(&pool, &llm, &model_version, &incident_id).await;
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
/// consumer downtime, etc). Runs independently of that loop, processing
/// each incident it finds through the same `process_incident` the stream
/// loop uses.
async fn sweep_loop(pool: PgPool, llm: Arc<LlmClient>, model_version: String, interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        match sweep::fetch_sweep_rows(&pool).await {
            Ok(rows) => {
                let ids = sweep::incidents_needing_extraction(&rows, &model_version);
                tracing::info!(count = ids.len(), "sweep found incidents needing extraction");
                for id in ids {
                    process_incident(&pool, &llm, &model_version, &id).await;
                }
            }
            Err(err) => tracing::error!(error = ?err, "sweep query failed"),
        }
    }
}

/// Runs both extraction passes for one incident and writes the result.
/// Never propagates an error -- a bad response, a timeout, or a schema
/// mismatch leaves the incident's existing columns untouched (or NULL, if
/// this is the first attempt) and simply logs, so the next sweep pass
/// retries it. This is deliberate per the spec: a broken enrichment step
/// must never be able to take displayed status down with it.
async fn process_incident(pool: &PgPool, llm: &LlmClient, model_version: &str, incident_id: &str) {
    let text = match queries::fetch_incident_text(pool, incident_id).await {
        Ok(Some(text)) => text,
        Ok(None) => {
            tracing::warn!(incident_id, "incident vanished before extraction ran");
            return;
        }
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "failed to fetch incident text");
            return;
        }
    };
    let (summary, description) = text;

    let primary = match llm.extract_primary(&summary, &description).await {
        Ok(p) => p,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "primary extraction failed");
            return;
        }
    };

    let adversarial_status = match llm.extract_adversarial(&summary, &description).await {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "adversarial extraction failed");
            return;
        }
    };

    let (resolution_status, confidence) = combine::combine(&primary.resolution_status, &adversarial_status);
    let text_hash = hash::text_hash(&summary, &description);

    if let Err(err) = queries::write_extraction(pool, incident_id, &primary, &resolution_status, &confidence, model_version, &text_hash).await {
        tracing::error!(error = ?err, incident_id, "failed to write extraction result");
        return;
    }

    tracing::info!(incident_id, resolution_status, confidence, "extraction written");
}
