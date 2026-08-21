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

    let reclaim_redis = redis_client.get_connection_manager().await?;
    tokio::spawn(reclaim_loop(
        pool.clone(),
        Arc::clone(&llm),
        model_version.clone(),
        reclaim_redis,
        config.reclaim_interval_secs,
        config.reclaim_min_idle_secs,
    ));

    loop {
        match stream::read_one(&mut redis).await {
            Ok(Some((entry_id, incident_id))) => {
                if process_incident(&pool, &llm, &model_version, &incident_id).await {
                    if let Err(err) = stream::ack(&mut redis, &entry_id).await {
                        tracing::error!(error = ?err, entry_id, "failed to ack stream entry");
                    }
                } else {
                    tracing::warn!(entry_id, incident_id, "extraction did not complete; leaving entry pending for reclaim");
                }
            }
            Ok(None) => {}
            Err(err) => {
                // Redis is deployed WITHOUT persistence on purpose -- it is a
                // disposable trigger queue, not a system of record -- so a pod
                // restart takes the stream and the consumer group with it and
                // every subsequent read fails NOGROUP. Recreating the group
                // here (`ensure_group` is idempotent; BUSYGROUP is swallowed)
                // makes that self-heal within seconds instead of needing a
                // manual enricher restart. The sleep is what stops the same
                // error from becoming a tight CPU-burning retry loop in the
                // meantime; `read_one` only blocks when it gets far enough to
                // block at all, which a NOGROUP read never does.
                tracing::error!(error = ?err, "error reading from incident-text-changed stream; recreating consumer group and backing off");
                if let Err(err) = stream::ensure_group(&mut redis).await {
                    tracing::error!(error = ?err, "failed to recreate the consumer group");
                }
                tokio::time::sleep(STREAM_ERROR_BACKOFF).await;
            }
        }
    }
}

/// How long the stream consumer loop waits after a failed read before trying
/// again. Short enough that a Redis restart is picked back up promptly, long
/// enough that a persistent error (Redis down, network partition) costs a
/// couple of log lines a second rather than a pegged core. Correctness never
/// depends on this: the hourly sweep re-finds anything the stream missed.
const STREAM_ERROR_BACKOFF: Duration = Duration::from_secs(2);

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
/// this is the first attempt) and simply logs. This is deliberate per the
/// spec: a broken enrichment step must never be able to take displayed
/// status down with it.
///
/// Returns `true` when the caller should `ack` the stream entry -- a
/// successful write, or a terminal case with nothing left to retry (the
/// incident no longer exists) -- and `false` for a transient failure (LLM
/// call error/timeout, DB error). On `false` the caller leaves the entry
/// unacked in the consumer group's pending-entries list, so
/// `stream::claim_stale`'s reclaim loop retries it once it's been idle long
/// enough, rather than relying on the hourly sweep alone for a failure mode
/// the sweep wasn't designed to catch quickly (it only re-triggers on a
/// text or model-version change, not a bare processing failure).
async fn process_incident(pool: &PgPool, llm: &LlmClient, model_version: &str, incident_id: &str) -> bool {
    let text = match queries::fetch_incident_text(pool, incident_id).await {
        Ok(Some(text)) => text,
        Ok(None) => {
            tracing::warn!(incident_id, "incident vanished before extraction ran");
            return true;
        }
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "failed to fetch incident text");
            return false;
        }
    };
    let (summary, description) = text;

    let primary = match llm.extract_primary(&summary, &description).await {
        Ok(p) => p,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "primary extraction failed");
            return false;
        }
    };

    let adversarial_status = match llm.extract_adversarial(&summary, &description).await {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "adversarial extraction failed");
            return false;
        }
    };

    let severity_adversarial = match llm.extract_severity_adversarial(&summary, &description).await {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "severity adversarial extraction failed");
            return false;
        }
    };

    let (resolution_status, confidence) = combine::combine(&primary.resolution_status, &adversarial_status);
    let (severity, severity_confidence) = combine::combine_severity(&primary.apparent_severity, &severity_adversarial);
    let text_hash = hash::text_hash(&summary, &description);

    if let Err(err) = queries::write_extraction(
        pool,
        incident_id,
        &primary,
        &resolution_status,
        &confidence,
        &severity,
        &severity_confidence,
        model_version,
        &text_hash,
    )
    .await
    {
        tracing::error!(error = ?err, incident_id, "failed to write extraction result");
        return false;
    }

    tracing::info!(incident_id, resolution_status, confidence, severity, severity_confidence, "extraction written");
    true
}

/// Periodically reclaims stream entries stuck in the pending-entries list
/// (see `stream::claim_stale`) and retries each through `process_incident`,
/// acking on success and leaving a repeat failure pending for the next
/// reclaim pass. Runs independently of the stream consumer loop and the
/// hourly sweep -- this is the debounced retry path for a transient
/// per-incident failure, distinct from both.
async fn reclaim_loop(
    pool: PgPool,
    llm: Arc<LlmClient>,
    model_version: String,
    mut redis: redis::aio::ConnectionManager,
    interval_secs: u64,
    min_idle_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    let min_idle = Duration::from_secs(min_idle_secs);
    loop {
        interval.tick().await;
        match stream::claim_stale(&mut redis, min_idle).await {
            Ok(entries) => {
                if !entries.is_empty() {
                    tracing::info!(count = entries.len(), "reclaimed stale pending entries for retry");
                }
                for (entry_id, incident_id) in entries {
                    if process_incident(&pool, &llm, &model_version, &incident_id).await {
                        if let Err(err) = stream::ack(&mut redis, &entry_id).await {
                            tracing::error!(error = ?err, entry_id, "failed to ack reclaimed stream entry");
                        }
                    } else {
                        tracing::warn!(
                            entry_id,
                            incident_id,
                            "reclaimed extraction failed again; will be reclaimed once more after the idle window"
                        );
                    }
                }
            }
            Err(err) => tracing::error!(error = ?err, "failed to check for stale pending entries"),
        }
    }
}
