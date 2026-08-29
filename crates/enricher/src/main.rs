//! `enricher`: extracts structured resolution status, category, and
//! per-period schedule window/date-range facts from Knowledgebase incident
//! text via an OpenAI-compatible LLM endpoint. See
//! docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md and
//! docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md.

mod combine;
mod config;
mod hash;
mod llm;
mod queries;
mod stream;
mod sweep;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use config::Config;
use llm::LlmClient;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Bare (unprefixed) name of the LLM-call duration histogram, shared by the
/// `install_with_buckets` bucket override in `main` and the `histogram!`
/// call in `record_llm_call_metrics`. Both must name the *same* metric --
/// the override is matched by exact name, so two independently hand-written
/// copies of this string could silently desync, leaving the histogram on
/// the module-wide default buckets with nothing to flag it.
const LLM_DURATION_METRIC: &str = "enricher_llm_call_duration_seconds";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    if config.metrics_enabled {
        common::metrics::install_with_buckets(
            config.metrics_port,
            &[(
                &common::metrics::metric_name(LLM_DURATION_METRIC),
                &[1.0, 5.0, 15.0, 30.0, 60.0, 90.0, 120.0, 180.0, 300.0],
            )],
        )?;
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await?;

    let redis_client = redis::Client::open(config.redis_url.clone())?;
    let mut redis = redis_client.get_connection_manager().await?;
    stream::ensure_group(&mut redis).await?;

    // `config.llm_model` is the ONLY thing ever sent to the endpoint as the
    // literal `model` field of a chat-completion request. `model_version`
    // below is a deliberately DIFFERENT string -- what's written to and
    // compared against the `extraction_model_version` column -- so that
    // bumping the prompt/schema version (this multi-period redesign) forces
    // re-extraction via the sweep's existing mismatch check WITHOUT asking
    // the configured endpoint to serve a model name it doesn't have. See
    // docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md, §5.
    let llm = Arc::new(LlmClient::new(
        config.llm_base_url.clone(),
        config.llm_api_key.clone(),
        config.llm_model.clone(),
        Duration::from_secs(config.llm_request_timeout_secs),
    ));
    let model_version = format!("{}@periods-v1", config.llm_model);

    let mismatch_tracker = Arc::new(MismatchTracker::default());

    tokio::spawn(sweep_loop(
        pool.clone(),
        Arc::clone(&llm),
        model_version.clone(),
        Arc::clone(&mismatch_tracker),
        config.sweep_interval_secs,
    ));

    let reclaim_redis = redis_client.get_connection_manager().await?;
    tokio::spawn(reclaim_loop(
        pool.clone(),
        Arc::clone(&llm),
        model_version.clone(),
        Arc::clone(&mismatch_tracker),
        reclaim_redis,
        config.reclaim_interval_secs,
        config.reclaim_min_idle_secs,
    ));

    loop {
        match stream::read_one(&mut redis).await {
            Ok(Some((entry_id, incident_id))) => {
                if process_incident(&pool, &llm, &model_version, &incident_id, &mismatch_tracker).await {
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

/// Tracks consecutive `CombineError` (length/ordinal-alignment mismatch)
/// failures per `incident_id`, across retries from any of the three call
/// sites (stream loop, sweep, reclaim loop). Exists specifically to satisfy
/// design §7 item 3's operational-visibility requirement: because
/// `chat_completion` sends `temperature: 0.0`, a mismatch against one
/// incident's *current* text is deterministic -- every retry reproduces the
/// identical mismatch, and nothing in the retry paths advances past it (a
/// failed attempt never updates `source_text_hash`). Left unaddressed, that
/// incident silently fails at 3 LLM calls per attempt indefinitely until its
/// text next changes, indistinguishable in the logs from ordinary one-off
/// transient noise. This tracker is what lets an operator tell those two
/// cases apart.
#[derive(Default)]
struct MismatchTracker {
    counts: Mutex<HashMap<String, u32>>,
}

impl MismatchTracker {
    /// Records a combine failure for `incident_id` and returns the new
    /// consecutive-failure count (1 on the first occurrence).
    fn record_failure(&self, incident_id: &str) -> u32 {
        let mut counts = self.counts.lock().expect("mismatch tracker mutex poisoned");
        let count = counts.entry(incident_id.to_string()).or_insert(0);
        *count += 1;
        *count
    }

    /// Clears any tracked failure count for `incident_id` -- called on any
    /// successful combination, since a text change (which resets
    /// `source_text_hash`) or a prompt fix could make a previously-mismatching
    /// incident succeed again.
    fn record_success(&self, incident_id: &str) {
        let mut counts = self.counts.lock().expect("mismatch tracker mutex poisoned");
        counts.remove(incident_id);
    }

    /// Current count of incidents with at least one recorded consecutive
    /// combine-mismatch failure -- exposed as
    /// `nr_status_enricher_mismatch_incidents` (Task 9).
    fn len(&self) -> usize {
        self.counts.lock().expect("mismatch tracker mutex poisoned").len()
    }
}

/// Hourly (by default) backstop that re-checks every uncleared incident's
/// text hash / extraction model version against what's stored, catching
/// anything the Redis Stream consumer loop above missed (publish failure,
/// consumer downtime, etc). Runs independently of that loop, processing
/// each incident it finds through the same `process_incident` the stream
/// loop uses.
async fn sweep_loop(pool: PgPool, llm: Arc<LlmClient>, model_version: String, mismatch_tracker: Arc<MismatchTracker>, interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        match sweep::fetch_sweep_rows(&pool).await {
            Ok(rows) => {
                let ids = sweep::incidents_needing_extraction(&rows, &model_version);
                tracing::info!(count = ids.len(), "sweep found incidents needing extraction");
                for id in ids {
                    process_incident(&pool, &llm, &model_version, &id, &mismatch_tracker).await;
                }
            }
            Err(err) => tracing::error!(error = ?err, "sweep query failed"),
        }
    }
}

/// Records one LLM call's duration and success/error outcome. `call` names
/// the call site (`"primary"`, `"resolution_adversarial"`,
/// `"severity_adversarial"`) as both a log field and the metric's `call`
/// label -- a small, fixed set, not user data, so no cardinality risk.
///
/// `outcome` is `success`/`error` only, not the design doc's illustrative
/// `success`/`error`/`timeout` three-way split -- `LlmClient::extract_*`
/// returns a bare `anyhow::Result`, with no typed distinction between "the
/// request timed out" (`config.llm_request_timeout_secs`, currently 300s)
/// and any other request failure. Distinguishing them would need `llm.rs`
/// to expose a typed error enum, which is out of scope for this plan --
/// this mirrors the same restraint the aggregator/pollers' `result` label
/// already applies (design doc Open Question 5): the histogram's own
/// bucket boundaries (extended past the tuned timeout, see `main`'s
/// `install_with_buckets` call) are what actually serves "is a call about
/// to time out," not the outcome label.
fn record_llm_call_metrics(call: &'static str, elapsed: std::time::Duration, success: bool) {
    metrics::histogram!(
        common::metrics::metric_name(LLM_DURATION_METRIC),
        "call" => call
    )
    .record(elapsed.as_secs_f64());
    metrics::counter!(
        common::metrics::metric_name("enricher_llm_call_total"),
        "call" => call,
        "outcome" => if success { "success" } else { "error" }
    )
    .increment(1);
}

/// Runs all three extraction passes for one incident and writes the result.
/// Never propagates an error -- a bad response, a timeout, or a schema
/// mismatch leaves the incident's existing columns untouched (or NULL, if
/// this is the first attempt) and simply logs. This is deliberate per the
/// spec: a broken enrichment step must never be able to take displayed
/// status down with it.
///
/// Returns `true` when the caller should `ack` the stream entry -- a
/// successful write, or a terminal case with nothing left to retry (the
/// incident no longer exists) -- and `false` for a transient failure (LLM
/// call error/timeout, DB error, or a length/ordinal-alignment mismatch
/// between the primary and adversarial period arrays). On `false` the caller
/// leaves the entry unacked in the consumer group's pending-entries list, so
/// `stream::claim_stale`'s reclaim loop retries it once it's been idle long
/// enough, rather than relying on the hourly sweep alone for a failure mode
/// the sweep wasn't designed to catch quickly (it only re-triggers on a
/// text or model-version change, not a bare processing failure).
async fn process_incident(pool: &PgPool, llm: &LlmClient, model_version: &str, incident_id: &str, mismatch_tracker: &MismatchTracker) -> bool {
    let state = match queries::fetch_incident_state(pool, incident_id).await {
        Ok(Some(state)) => state,
        Ok(None) => {
            tracing::warn!(incident_id, "incident vanished before extraction ran");
            return true;
        }
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "failed to fetch incident text");
            return false;
        }
    };
    let (summary, description, first_seen_at) = (state.summary, state.description, state.first_seen_at);
    let text_hash = hash::text_hash(&summary, &description);

    // Guards every caller (stream loop, sweep, reclaim) against running the
    // LLM again over text it already successfully extracted -- e.g. a
    // successful write whose subsequent XACK failed gets redelivered by
    // the reclaim loop even though nothing needs re-doing. Each caller
    // already tries not to enqueue unchanged content (upsert_incidents'
    // text_changed check, sweep's own hash comparison), but this is the
    // one place all three paths funnel through, so it's the actual
    // guarantee rather than three separate best-effort ones.
    if state.source_text_hash.as_deref() == Some(text_hash.as_str())
        && state.extraction_model_version.as_deref() == Some(model_version)
    {
        tracing::info!(incident_id, "text unchanged since last successful extraction; skipping");
        return true;
    }

    let primary_start = std::time::Instant::now();
    let primary_result = llm.extract_primary(&summary, &description, first_seen_at).await;
    record_llm_call_metrics("primary", primary_start.elapsed(), primary_result.is_ok());
    let primary = match primary_result {
        Ok(p) => p,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "primary extraction failed");
            return false;
        }
    };

    let resolution_adversarial_start = std::time::Instant::now();
    let resolution_adversarial_result = llm.extract_adversarial(&summary, &description, &primary.periods).await;
    record_llm_call_metrics("resolution_adversarial", resolution_adversarial_start.elapsed(), resolution_adversarial_result.is_ok());
    let resolution_adversarial = match resolution_adversarial_result {
        Ok(v) => v,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "adversarial extraction failed");
            return false;
        }
    };

    let severity_adversarial_start = std::time::Instant::now();
    let severity_adversarial_result = llm.extract_severity_adversarial(&summary, &description, &primary.periods).await;
    record_llm_call_metrics("severity_adversarial", severity_adversarial_start.elapsed(), severity_adversarial_result.is_ok());
    let severity_adversarial = match severity_adversarial_result {
        Ok(v) => v,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "severity adversarial extraction failed");
            return false;
        }
    };

    let periods = match combine::combine_periods(&primary.periods, &resolution_adversarial, &severity_adversarial) {
        Ok(periods) => {
            mismatch_tracker.record_success(incident_id);
            periods
        }
        Err(err) => {
            let consecutive = mismatch_tracker.record_failure(incident_id);
            if consecutive > 1 {
                // Distinguishable from the generic error path below on
                // purpose -- design §7 item 3 wants this recognizable as
                // "this one incident has been silently failing for a while"
                // rather than folded into ordinary transient-failure noise.
                tracing::error!(
                    incident_id,
                    consecutive_failures = consecutive,
                    error = %err,
                    "persistent length mismatch, likely needs prompt tuning"
                );
            } else {
                tracing::error!(error = %err, incident_id, "period combination failed (length or ordinal-alignment mismatch)");
            }
            return false;
        }
    };

    if let Err(err) = queries::write_extraction(pool, incident_id, &primary.category, &periods, model_version, &text_hash).await {
        tracing::error!(error = ?err, incident_id, "failed to write extraction result");
        return false;
    }

    tracing::info!(incident_id, period_count = periods.len(), "extraction written");
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
    mismatch_tracker: Arc<MismatchTracker>,
    mut redis: redis::aio::ConnectionManager,
    interval_secs: u64,
    min_idle_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    let min_idle = Duration::from_secs(min_idle_secs);
    loop {
        interval.tick().await;

        match stream::group_lag(&mut redis).await {
            Ok(Some(lag)) => {
                metrics::gauge!(common::metrics::metric_name("enricher_stream_lag")).set(lag as f64);
            }
            Ok(None) => {}
            Err(err) => tracing::warn!(error = ?err, "failed to sample stream consumer-group lag"),
        }
        metrics::gauge!(common::metrics::metric_name("enricher_mismatch_incidents")).set(mismatch_tracker.len() as f64);

        match stream::claim_stale(&mut redis, min_idle).await {
            Ok(entries) => {
                if !entries.is_empty() {
                    tracing::info!(count = entries.len(), "reclaimed stale pending entries for retry");
                }
                for (entry_id, incident_id) in entries {
                    if process_incident(&pool, &llm, &model_version, &incident_id, &mismatch_tracker).await {
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
