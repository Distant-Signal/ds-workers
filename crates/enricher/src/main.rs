//! `enricher`: extracts structured resolution status, category, and
//! per-period schedule window/date-range facts from Knowledgebase incident
//! text via an OpenAI-compatible LLM endpoint. See
//! docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md and
//! docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md.

mod churn;
mod combine;
mod config;
mod llm;
mod queries;
#[cfg(test)]
mod replay_eval;
mod retry_backoff;
mod stream;
mod sweep;
mod text_delta;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use config::Config;
use llm::LlmClient;
use retry_backoff::RetryBackoff;
use sqlx::PgPool;

/// Bare (unprefixed) name of the LLM-call duration histogram, shared by the
/// `install_with_buckets` bucket override in `main` and the `histogram!`
/// call in `record_llm_call_metrics`. Both must name the *same* metric --
/// the override is matched by exact name, so two independently hand-written
/// copies of this string could silently desync, leaving the histogram on
/// the module-wide default buckets with nothing to flag it.
const LLM_DURATION_METRIC: &str = "enricher_llm_call_duration_seconds";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    common::logging::init("enricher");

    let config = Config::parse();
    if config.metrics_enabled {
        let buckets = llm_duration_buckets(config.llm_request_timeout_secs);
        common::metrics::install_with_buckets(
            config.metrics_port,
            &[(&common::metrics::metric_name(LLM_DURATION_METRIC), &buckets)],
        )?;
    }

    let (ready, progress) = health_http::spawn_worker(&config.health);
    // INF-5: wait for Postgres and Redis (Postgres in crash recovery, Redis
    // still loading its AOF after a node reboot) instead of exiting into
    // CrashLoopBackOff. `/healthz` stays 503 until all three steps below
    // are done; `/livez` stays 200 while they retry.
    common::startup::retry_until_ready(
        "Postgres",
        common::startup::CONNECT_BACKOFF,
        Some(&progress),
        || async {
            use sqlx::Connection;
            sqlx::PgConnection::connect(config.database_url.expose())
                .await?
                .close()
                .await
        },
    )
    .await;
    // application_name, statement/idle-in-transaction timeouts and a short
    // acquire_timeout; see `common::pg`.
    let pool = common::pg::PoolSettings::from_env("distant-signal-enricher", 5)?
        .connect(config.database_url.expose())
        .await?;

    let redis_url = common::redis_auth::redis_url_with_password(
        config.redis_url.expose(),
        config.redis_password.as_ref(),
    )?;
    let redis_client = redis::Client::open(redis_url.expose())?;
    // Bounded connections (`common::redis_conn`): no redis-rs-internal
    // retries (minutes, unlogged, with the defaults), a connect timeout and a
    // per-command response timeout. A Redis outage later on fails
    // `read_one` within seconds, and the loop below logs, backs off and
    // retries -- beating progress every iteration.
    let mut redis = common::redis_conn::connect_until_ready(
        "Redis",
        &redis_client,
        common::startup::CONNECT_BACKOFF,
        Some(&progress),
    )
    .await;
    common::startup::retry_until_ready(
        "Redis consumer group",
        common::startup::CONNECT_BACKOFF,
        Some(&progress),
        || {
            let mut conn = redis.clone();
            async move { stream::ensure_group(&mut conn).await }
        },
    )
    .await;
    // The group's position, kept for `stream::recreate_group`.
    let mut last_delivered = stream::group_last_delivered_id(&mut redis).await;
    ready.store(true, std::sync::atomic::Ordering::Relaxed);

    // `config.llm_model` is the ONLY thing ever sent to the endpoint as the
    // literal `model` field of a chat-completion request. `model_version`
    // below is a deliberately DIFFERENT string -- what's written to and
    // compared against the `extraction_model_version` column -- so that
    // bumping the prompt/schema version (this multi-period redesign) forces
    // re-extraction via the sweep's existing mismatch check WITHOUT asking
    // the configured endpoint to serve a model name it doesn't have. See
    // docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md, §5.
    let llm = LlmClient::new(
        config.llm_base_url.clone(),
        config
            .llm_api_key
            .as_ref()
            .map(|key| key.expose().to_string()),
        config.llm_model.clone(),
        Duration::from_secs(config.llm_request_timeout_secs),
    )
    // Every provider-policy knob defaults to "off" (see `ProviderPolicy`).
    .with_provider_policy(config.provider.policy());
    let enricher = Arc::new(Enricher {
        pool,
        llm,
        model_version: format!("{}@periods-v2", config.llm_model),
        mismatch_tracker: MismatchTracker::default(),
        retry_backoff: RetryBackoff::default(),
        in_flight: InFlight::default(),
        carry_forward_noops: config.carry_forward_semantic_noops,
    });
    if enricher.carry_forward_noops {
        tracing::info!(
            "CARRY_FORWARD_SEMANTIC_NOOPS is on: semantic no-op text changes skip the LLM"
        );
    }

    tokio::spawn(sweep_loop(
        Arc::clone(&enricher),
        config.sweep_interval_secs,
    ));

    let reclaim_redis = common::redis_conn::connect_until_ready(
        "Redis (reclaim connection)",
        &redis_client,
        common::startup::CONNECT_BACKOFF,
        Some(&progress),
    )
    .await;
    tokio::spawn(reclaim_loop(
        Arc::clone(&enricher),
        reclaim_redis,
        config.reclaim_interval_secs,
        config.reclaim_min_idle_secs,
    ));

    loop {
        // One iteration: a blocking read of at most 5s, then (maybe) one
        // incident's extraction -- up to three LLM calls, each bounded by
        // llm_request_timeout_secs. PROGRESS_STALL_SECS must exceed that.
        progress.beat();
        match stream::read_one(&mut redis).await {
            Ok(Some((entry_id, incident_id))) => {
                last_delivered = Some(entry_id.clone());
                if process_stream_entry(&enricher, &entry_id, &incident_id).await
                    && let Err(err) = stream::ack(&mut redis, &entry_id).await
                {
                    tracing::error!(error = ?err, entry_id, "failed to ack stream entry");
                }
            }
            Ok(None) => {}
            Err(err) => {
                // Redis is deployed WITHOUT persistence on purpose -- it is a
                // disposable trigger queue, not a system of record -- so a pod
                // restart takes the stream and the consumer group with it and
                // every subsequent read fails NOGROUP. Recreating the group
                // here (a no-op BUSYGROUP while it exists) makes that
                // self-heal within seconds instead of needing a manual
                // enricher restart. It is recreated after the last entry
                // read, not at the tail, so entries `api` published in
                // between are not skipped (see `stream::recreate_group`). The sleep is what stops the same
                // error from becoming a tight CPU-burning retry loop in the
                // meantime; `read_one` only blocks when it gets far enough to
                // block at all, which a NOGROUP read never does.
                tracing::error!(error = ?err, "error reading from incident-text-changed stream; recreating consumer group and backing off");
                if let Err(err) = stream::recreate_group(&mut redis, &mut last_delivered).await {
                    tracing::error!(error = ?err, "failed to recreate the consumer group");
                }
                tokio::time::sleep(STREAM_ERROR_BACKOFF).await;
            }
        }
    }
}

/// Bucket boundaries for the LLM-call duration histogram: the fixed set,
/// extended past the configured per-request timeout. A call (in-call
/// retries included) can legitimately run to a few multiples of it, and a
/// timeout above the old 300 s top bucket (e.g. 320 s behind a 302 s
/// gateway) would otherwise land every slow call in `+Inf`.
fn llm_duration_buckets(request_timeout_secs: u64) -> Vec<f64> {
    let timeout = request_timeout_secs as f64;
    let mut buckets = vec![1.0, 5.0, 15.0, 30.0, 60.0, 90.0, 120.0, 180.0, 300.0];
    for extra in [timeout, 2.0 * timeout, 4.0 * timeout] {
        if extra > 0.0 && !buckets.contains(&extra) {
            buckets.push(extra);
        }
    }
    buckets.sort_by(f64::total_cmp);
    buckets
}

/// Everything the three loops (stream consumer, sweep, reclaim) share.
struct Enricher {
    pool: PgPool,
    llm: LlmClient,
    model_version: String,
    mismatch_tracker: MismatchTracker,
    retry_backoff: RetryBackoff,
    in_flight: InFlight,
    /// `CARRY_FORWARD_SEMANTIC_NOOPS` (default off).
    carry_forward_noops: bool,
}

impl Enricher {
    /// Runs `process_incident` unless another loop is already processing
    /// the same incident, in which case it returns `None` without touching
    /// the DB or the LLM. The caller decides what "busy" means for it --
    /// see `InFlight`.
    async fn process_exclusive(&self, incident_id: &str) -> Option<bool> {
        let _claim = self.in_flight.try_claim(incident_id)?;
        Some(process_incident(self, incident_id).await)
    }
}

/// The incidents some loop is currently running `process_incident` for.
///
/// The three loops share one process (the chart runs `replicas: 1`, one
/// consumer name), but each runs serially on its own, so without this they
/// overlap on the same incident whenever an extraction is slow:
///
/// - The **sweep** re-selects every incident whose stored hash doesn't match
///   yet, including the one the stream loop is extracting right now. It
///   skips in-flight ids: the holder is already doing that work, and a
///   failure there leaves a pending stream entry for reclaim.
/// - **Reclaim** `XAUTOCLAIM`s any entry idle past `RECLAIM_MIN_IDLE_SECS`,
///   and an entry the stream loop is still processing is exactly that once
///   an attempt (retries and 429 waits included) outlasts the idle window.
///   It skips in-flight ids and leaves the entry pending: the holder acks it
///   (same consumer group) on success, or a later reclaim pass retries it.
///   So min-idle is a retry delay, not a correctness bound.
/// - The **stream loop** leaves its entry pending when the sweep or reclaim
///   holds the incident, rather than waiting and stalling every other
///   incident behind one slow call. A later reclaim pass then either finds
///   the text already extracted (acked with no LLM call) or extracts it.
#[derive(Default)]
struct InFlight {
    ids: Mutex<HashSet<String>>,
}

impl InFlight {
    fn try_claim(&self, incident_id: &str) -> Option<InFlightClaim<'_>> {
        let mut ids = self.ids.lock().expect("in-flight set mutex poisoned");
        ids.insert(incident_id.to_string()).then(|| InFlightClaim {
            set: self,
            incident_id: incident_id.to_string(),
        })
    }
}

/// Releases its incident on drop -- including when the owning future is
/// dropped mid-extraction.
struct InFlightClaim<'a> {
    set: &'a InFlight,
    incident_id: String,
}

impl Drop for InFlightClaim<'_> {
    fn drop(&mut self) {
        // Never panic in drop: a poisoned mutex still holds valid data.
        let mut ids = match self.set.ids.lock() {
            Ok(ids) => ids,
            Err(poisoned) => poisoned.into_inner(),
        };
        ids.remove(&self.incident_id);
    }
}

fn record_in_flight_skip(caller: &'static str, incident_id: &str) {
    tracing::info!(
        incident_id,
        caller,
        "incident is already being processed by another loop; skipping"
    );
    metrics::counter!(
        common::metrics::metric_name("enricher_in_flight_skips_total"),
        "caller" => caller
    )
    .increment(1);
}

/// One entry from the stream consumer loop. Returns whether to ack it.
async fn process_stream_entry(enricher: &Enricher, entry_id: &str, incident_id: &str) -> bool {
    match enricher.process_exclusive(incident_id).await {
        Some(true) => true,
        Some(false) => {
            tracing::warn!(
                entry_id,
                incident_id,
                "extraction did not complete; leaving entry pending for reclaim"
            );
            false
        }
        None => {
            record_in_flight_skip("stream", incident_id);
            false
        }
    }
}

/// How long the stream consumer loop waits after a failed read before trying
/// again. Short enough that a Redis restart is picked back up promptly, long
/// enough that a persistent error (Redis down, network partition) costs a
/// couple of log lines a second rather than a pegged core. Correctness never
/// depends on this: the hourly sweep re-finds anything the stream missed.
const STREAM_ERROR_BACKOFF: Duration = Duration::from_secs(2);

/// Builds a `tokio::time::Interval` firing every `interval_secs`, with
/// `MissedTickBehavior::Delay` rather than the default `Burst`. Shared by
/// `sweep_loop` and `reclaim_loop` (both otherwise built their own inline
/// `tokio::time::interval` with the default behavior).
///
/// `Burst` fires every missed tick back-to-back with zero gap once a cycle
/// overruns its own interval (a slow LLM endpoint, a DB hiccup) -- for
/// `sweep_loop` that means a pile of immediate back-to-back sweeps the
/// moment things recover, each re-running `fetch_sweep_rows` and
/// re-queuing whatever it finds; for `reclaim_loop`, a pile of immediate
/// back-to-back `XAUTOCLAIM` scans. `Delay` instead waits a fresh
/// `interval_secs` from whenever the overrun tick actually completes, so a
/// slow cycle never causes a burst of immediate follow-up work. Same fix
/// already applied to `crates/common::poller_loop` and (separately)
/// `crates/aggregator`'s own loop -- see their doc comments for the fuller
/// "why" this repo keeps hitting the same default-`Burst` footgun. Split
/// into its own function (mirroring `common::poller_loop`'s own
/// `poll_interval_with_delay_on_overrun`) so the configuration is directly
/// assertable in a unit test, since the missed-tick BEHAVIOR itself (skipping
/// ticks under a real overrun) isn't practically testable without a slow,
/// timing-flaky test.
fn ticking_interval(interval_secs: u64) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

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
    /// `distant_signal_enricher_mismatch_incidents` (Task 9).
    fn len(&self) -> usize {
        self.counts
            .lock()
            .expect("mismatch tracker mutex poisoned")
            .len()
    }
}

/// Hourly (by default) backstop that re-checks every uncleared incident's
/// text hash / extraction model version against what's stored, catching
/// anything the Redis Stream consumer loop above missed (publish failure,
/// consumer downtime, etc). Runs independently of that loop, processing
/// each incident it finds through the same `process_incident` the stream
/// loop uses -- skipping any another loop is processing right now (see
/// `InFlight`).
async fn sweep_loop(enricher: Arc<Enricher>, interval_secs: u64) {
    let mut interval = ticking_interval(interval_secs);
    loop {
        interval.tick().await;
        match sweep::fetch_sweep_rows(&enricher.pool).await {
            Ok(rows) => {
                let ids = sweep::incidents_needing_extraction(&rows, &enricher.model_version);
                tracing::info!(
                    count = ids.len(),
                    "sweep found incidents needing extraction"
                );
                let skipped = sweep_ids(&enricher, &ids).await;
                if skipped > 0 {
                    tracing::info!(skipped, "sweep skipped incidents already in flight");
                }
            }
            Err(err) => tracing::error!(error = ?err, "sweep query failed"),
        }
    }
}

/// Processes one sweep's worth of incident ids; returns how many were
/// skipped because another loop was already processing them.
async fn sweep_ids(enricher: &Enricher, ids: &[String]) -> usize {
    let mut skipped = 0;
    for id in ids {
        if enricher.process_exclusive(id).await.is_none() {
            record_in_flight_skip("sweep", id);
            skipped += 1;
        }
    }
    skipped
}

/// Records one LLM call's duration and outcome. `call` names the call site
/// (`"primary"`, `"resolution_adversarial"`, `"severity_adversarial"`) as
/// both a log field and the metric's `call` label -- a small, fixed set,
/// not user data, so no cardinality risk. `outcome` is `llm_outcome`'s
/// fixed label set. The duration covers the whole call, in-call retries and
/// 429 waits included.
fn record_llm_call_metrics(
    call: &'static str,
    elapsed: std::time::Duration,
    outcome: &'static str,
) {
    metrics::histogram!(
        common::metrics::metric_name(LLM_DURATION_METRIC),
        "call" => call
    )
    .record(elapsed.as_secs_f64());
    metrics::counter!(
        common::metrics::metric_name("enricher_llm_call_total"),
        "call" => call,
        "outcome" => outcome
    )
    .increment(1);
}

/// `success` plus the typed `llm::LlmCallError` labels (`rate_limited`,
/// `gateway_error`, `timeout`, `http_error`, `empty_content`), falling back
/// to `error` (malformed JSON, connection refused, ...).
fn llm_outcome<T>(result: &anyhow::Result<T>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(err) => err
            .downcast_ref::<llm::LlmCallError>()
            .map_or("error", llm::LlmCallError::outcome_label),
    }
}

/// A provider-side transient (see `LlmClient::is_provider_transient`) says
/// nothing about this incident's text, so it must not push the text into
/// `RetryBackoff`'s 30 min -> 24 h per-text backoff; the stream entry stays
/// pending and reclaim retries it at its normal cadence.
fn record_extraction_failure(
    enricher: &Enricher,
    incident_id: &str,
    text_hash: &str,
    err: &anyhow::Error,
) {
    if !enricher.llm.is_provider_transient(err) {
        enricher
            .retry_backoff
            .record_failure(incident_id, text_hash);
    }
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
///
/// `retry_backoff` may also make this a no-op that immediately returns
/// `false` -- see `retry_backoff::RetryBackoff`'s own doc for why a second
/// consecutive failure against the same text is backed off rather than
/// retried at full LLM cost on every call.
///
/// Callers go through `Enricher::process_exclusive`, never straight here,
/// so two loops never run this for the same incident at once.
async fn process_incident(enricher: &Enricher, incident_id: &str) -> bool {
    let Enricher {
        pool,
        llm,
        model_version,
        mismatch_tracker,
        retry_backoff,
        carry_forward_noops,
        in_flight: _,
    } = enricher;
    let (model_version, carry_forward_noops) = (model_version.as_str(), *carry_forward_noops);
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
    let text_hash = common::text_hash::text_hash(&state.summary, &state.description);
    // Churn measurement baseline (instrumentation only -- see `churn`'s
    // module doc): `Some` only for a text-change re-run under the current
    // model version. Captured now, before `write_extraction` overwrites it.
    let churn_baseline =
        churn::baseline_for_text_change_rerun(incident_id, &state, &text_hash, model_version);
    let (summary, description, first_seen_at) =
        (state.summary, state.description, state.first_seen_at);

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
        tracing::info!(
            incident_id,
            "text unchanged since last successful extraction; skipping"
        );
        return true;
    }

    // Classify how the text moved since the stored extraction was computed -- only for a same-model
    // text-change re-run (`churn_baseline` is `Some` exactly then). Used to
    // label the churn metric and, behind `carry_forward_noops`, to skip the
    // LLM for a semantic no-op. Any failure here just means "no class":
    // measurement and the skip must never be able to block a full extraction.
    let edit_class = match (&churn_baseline, state.source_text_hash.as_deref()) {
        (Some(_), Some(old_hash)) => {
            match queries::fetch_extracted_source_text(pool, incident_id, old_hash).await {
                Ok(Some((old_summary, old_description))) => Some(text_delta::classify(
                    &old_summary,
                    &old_description,
                    &summary,
                    &description,
                )),
                Ok(None) => None,
                Err(err) => {
                    tracing::warn!(error = ?err, incident_id, "could not fetch the previously extracted text; classifying as unknown");
                    None
                }
            }
        }
        _ => None,
    };
    if carry_forward_noops
        && edit_class == Some(text_delta::EditClass::SemanticNoop)
        && let Some(old_hash) = state.source_text_hash.as_deref()
    {
        match queries::carry_forward_extraction(
            pool,
            incident_id,
            &text_hash,
            old_hash,
            &summary,
            &description,
            model_version,
        )
        .await
        {
            Ok(true) => {
                metrics::counter!(common::metrics::metric_name(
                    "enricher_extraction_carried_forward_total"
                ))
                .increment(1);
                tracing::info!(
                    incident_id,
                    "semantic no-op text change; carried the previous extraction forward without an LLM call"
                );
                return true;
            }
            Ok(false) => tracing::info!(
                incident_id,
                "carry-forward guard rejected (text or extraction moved); falling back to a full extraction"
            ),
            Err(err) => tracing::warn!(
                error = ?err,
                incident_id,
                "carry-forward write failed; falling back to a full extraction"
            ),
        }
    }

    if retry_backoff.should_skip(incident_id, &text_hash) {
        // This exact text already failed extraction at least once before
        // and hasn't waited out its backoff window yet -- see
        // `retry_backoff::RetryBackoff` for why. Skip without spending an
        // LLM call; the entry stays unacked (`false`) so the caller's own
        // normal retry path picks it up again once the backoff elapses.
        tracing::info!(
            incident_id,
            "backing off a recently-failing extraction; skipping this attempt"
        );
        return false;
    }

    let primary_start = std::time::Instant::now();
    let primary_result = llm
        .extract_primary(&summary, &description, first_seen_at)
        .await;
    record_llm_call_metrics(
        "primary",
        primary_start.elapsed(),
        llm_outcome(&primary_result),
    );
    let primary = match primary_result {
        Ok(p) => p,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "primary extraction failed");
            record_extraction_failure(enricher, incident_id, &text_hash, &err);
            return false;
        }
    };

    // Decision 3 of docs/superpowers/specs/2026-09-01-enricher-period-cap-remediation-design.md:
    // a truncated primary extraction is NOT an error -- it already
    // succeeded, and the pipeline below continues completely unaware
    // anything unusual happened (extract_adversarial/
    // extract_severity_adversarial/combine::combine_periods/
    // write_extraction all just see an already-in-bounds `periods` list).
    // This is purely operator-facing visibility: a counter for an alert
    // rule to fire on, and a human-readable log line alongside it -- the
    // same split MismatchTracker already uses (gauge for the alertable
    // signal there, tracing::error! for the human-readable why), except a
    // counter (not a gauge, no "currently outstanding" set to track) and
    // tracing::warn! (not tracing::error!, since this run still succeeds
    // and writes normally, unlike a persistent combine mismatch).
    if primary.dropped_period_count > 0 {
        tracing::warn!(
            incident_id,
            original_count = primary.periods.len() + primary.dropped_period_count,
            kept_count = primary.periods.len(),
            "primary extraction exceeded the period cap; truncated to the N most severe/soonest periods"
        );
        metrics::counter!(common::metrics::metric_name(
            "enricher_period_truncations_total"
        ))
        .increment(1);
    }

    let resolution_adversarial_start = std::time::Instant::now();
    let resolution_adversarial_result = llm
        .extract_adversarial(&summary, &description, &primary.periods)
        .await;
    record_llm_call_metrics(
        "resolution_adversarial",
        resolution_adversarial_start.elapsed(),
        llm_outcome(&resolution_adversarial_result),
    );
    let resolution_adversarial = match resolution_adversarial_result {
        Ok(v) => v,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "adversarial extraction failed");
            record_extraction_failure(enricher, incident_id, &text_hash, &err);
            return false;
        }
    };

    let severity_adversarial_start = std::time::Instant::now();
    let severity_adversarial_result = llm
        .extract_severity_adversarial(&summary, &description, &primary.periods)
        .await;
    record_llm_call_metrics(
        "severity_adversarial",
        severity_adversarial_start.elapsed(),
        llm_outcome(&severity_adversarial_result),
    );
    let severity_adversarial = match severity_adversarial_result {
        Ok(v) => v,
        Err(err) => {
            tracing::error!(error = ?err, incident_id, "severity adversarial extraction failed");
            record_extraction_failure(enricher, incident_id, &text_hash, &err);
            return false;
        }
    };

    let periods = match combine::combine_periods(
        &primary.periods,
        &resolution_adversarial,
        &severity_adversarial,
    ) {
        Ok(periods) => {
            mismatch_tracker.record_success(incident_id);
            // The extraction pipeline itself (all three LLM calls plus
            // combination) demonstrably worked against this exact text --
            // clear any backoff now rather than waiting for the write below
            // to also succeed, since a subsequent write failure (DB error,
            // or the stale-text race handled below) is not a reason to keep
            // treating this text as one that fails extraction.
            retry_backoff.record_success(incident_id);
            periods
        }
        Err(err) => {
            let consecutive = mismatch_tracker.record_failure(incident_id);
            retry_backoff.record_failure(incident_id, &text_hash);
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

    // Compared before the write (cheap, pure, panic-free) but only
    // reported once the write has actually landed, so a stale-result
    // discard below is never counted as a re-run.
    let churn_report = churn_baseline.map(|baseline| {
        churn::compare(
            baseline.category.as_deref(),
            &baseline.periods,
            &primary.category,
            &periods,
        )
    });

    // Retried locally (DB2-30): by now three LLM calls (up to 3 x 300 s)
    // have succeeded, and giving up here would leave the entry pending for
    // the reclaim loop to re-run all three, un-backed-off since
    // `record_success` already ran above.
    let write = retry_locally(WRITE_EXTRACTION_ATTEMPTS, WRITE_EXTRACTION_BACKOFF, || {
        queries::write_extraction(
            pool,
            incident_id,
            &primary.category,
            &periods,
            model_version,
            &text_hash,
            &summary,
            &description,
        )
    })
    .await;
    match write {
        Ok(true) => {}
        Ok(false) => {
            // The incident's text moved between `fetch_incident_state`
            // above and this write -- see `queries::write_extraction`'s
            // doc. This attempt's result is computed from text that's no
            // longer current, so it must be discarded rather than written
            // over whatever a fresher concurrent extraction (or the text
            // change itself) already produced/queued. The text change that
            // caused this race is itself what published a fresh
            // `incident-text-changed` stream entry for this same
            // `incident_id` (see `crates/api/src/data/queries.rs`'s
            // publish path), and failing that, the hourly sweep will still
            // re-select this incident on its next tick since
            // `source_text_hash` was never advanced to match the current
            // text -- so there is nothing left for *this* stream entry to
            // do. Returning `true` acks it rather than leaving it pending
            // for the reclaim loop to retry a result that would just be
            // discarded again.
            tracing::warn!(
                incident_id,
                "incident text changed since extraction started; discarding stale result \
                 instead of overwriting a possibly fresher one (a fresh extraction for the \
                 new text has already been triggered)"
            );
            return true;
        }
        Err(err) => {
            tracing::error!(
                error = ?err,
                incident_id,
                attempts = WRITE_EXTRACTION_ATTEMPTS,
                "failed to write extraction result"
            );
            return false;
        }
    }

    tracing::info!(
        incident_id,
        period_count = periods.len(),
        "extraction written"
    );
    if let Some(report) = &churn_report {
        churn::record(
            incident_id,
            report,
            edit_class.map_or("unknown", text_delta::EditClass::label),
        );
    }
    true
}

/// Attempts at the final `write_extraction` UPDATE (DB2-30).
const WRITE_EXTRACTION_ATTEMPTS: u32 = 3;
/// First retry delay; doubled for each later retry (0.5 s, then 1 s).
const WRITE_EXTRACTION_BACKOFF: Duration = Duration::from_millis(500);

/// Runs `op` up to `attempts` times, sleeping `backoff`, then twice that,
/// and so on between failures. Returns the first success or the last error.
/// For a single idempotent DB write whose inputs were expensive to compute.
async fn retry_locally<T, F, Fut>(attempts: u32, backoff: Duration, mut op: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut delay = backoff;
    let mut attempt = 1;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt >= attempts.max(1) => return Err(err),
            Err(err) => {
                tracing::warn!(error = ?err, attempt, "write failed; retrying");
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
                attempt += 1;
            }
        }
    }
}

/// Periodically reclaims stream entries stuck in the pending-entries list
/// (see `stream::claim_stale`) and retries each through `process_incident`,
/// acking on success and leaving a repeat failure pending for the next
/// reclaim pass. Runs independently of the stream consumer loop and the
/// hourly sweep -- this is the debounced retry path for a transient
/// per-incident failure, distinct from both. Entries whose incident another
/// loop is still processing are skipped (see `InFlight`).
async fn reclaim_loop(
    enricher: Arc<Enricher>,
    mut redis: common::redis_conn::RedisConn,
    interval_secs: u64,
    min_idle_secs: u64,
) {
    let mut interval = ticking_interval(interval_secs);
    let min_idle = Duration::from_secs(min_idle_secs);
    loop {
        interval.tick().await;

        match stream::group_lag(&mut redis).await {
            Ok(Some(lag)) => {
                metrics::gauge!(common::metrics::metric_name("enricher_stream_lag"))
                    .set(lag as f64);
            }
            Ok(None) => {}
            Err(err) => tracing::warn!(error = ?err, "failed to sample stream consumer-group lag"),
        }
        metrics::gauge!(common::metrics::metric_name("enricher_mismatch_incidents"))
            .set(enricher.mismatch_tracker.len() as f64);

        match stream::claim_stale(&mut redis, min_idle).await {
            Ok(entries) => {
                if !entries.is_empty() {
                    tracing::info!(
                        count = entries.len(),
                        "reclaimed stale pending entries for retry"
                    );
                }
                for entry_id in process_reclaimed(&enricher, entries).await {
                    if let Err(err) = stream::ack(&mut redis, &entry_id).await {
                        tracing::error!(error = ?err, entry_id, "failed to ack reclaimed stream entry");
                    }
                }
            }
            Err(err) => tracing::error!(error = ?err, "failed to check for stale pending entries"),
        }
    }
}

/// Retries reclaimed `(entry_id, incident_id)` entries; returns the entry
/// ids to ack. An entry whose incident is in flight elsewhere is neither
/// processed nor acked: re-running it concurrently is exactly the double
/// extraction `InFlight` exists to prevent.
async fn process_reclaimed(enricher: &Enricher, entries: Vec<(String, String)>) -> Vec<String> {
    let mut to_ack = Vec::new();
    for (entry_id, incident_id) in entries {
        match enricher.process_exclusive(&incident_id).await {
            Some(true) => to_ack.push(entry_id),
            Some(false) => tracing::warn!(
                entry_id,
                incident_id,
                "reclaimed extraction failed again; will be reclaimed once more after the idle window"
            ),
            None => record_in_flight_skip("reclaim", &incident_id),
        }
    }
    to_ack
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// DB2-30: a transient write failure after the three LLM calls is
    /// retried locally, and gives up only after the last attempt.
    #[tokio::test]
    async fn retry_locally_retries_a_failed_write_then_gives_up() {
        let calls = std::sync::atomic::AtomicU32::new(0);
        let result = retry_locally(3, Duration::from_millis(1), || async {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if n < 3 {
                anyhow::bail!("transient failure {n}")
            }
            Ok(n)
        })
        .await;
        assert_eq!(result.unwrap(), 3, "succeeds on the third attempt");

        let calls = std::sync::atomic::AtomicU32::new(0);
        let result: anyhow::Result<()> = retry_locally(3, Duration::from_millis(1), || async {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            anyhow::bail!("still down")
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "exactly `attempts` tries, no more"
        );
    }

    /// Finding regression: `sweep_loop`/`reclaim_loop`'s own interval must
    /// be configured with `MissedTickBehavior::Delay`, not the
    /// default `Burst` -- see `ticking_interval`'s own doc for why.
    #[tokio::test]
    async fn ticking_interval_defaults_to_delay_not_burst_on_a_missed_tick() {
        let interval = ticking_interval(60);
        assert_eq!(
            interval.missed_tick_behavior(),
            tokio::time::MissedTickBehavior::Delay,
            "an overrun sweep/reclaim cycle must not burst-fire every missed tick back-to-back"
        );
    }

    async fn test_pool() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    fn test_enricher(
        pool: PgPool,
        server: &MockServer,
        model_version: &str,
        carry_forward_noops: bool,
    ) -> Enricher {
        Enricher {
            pool,
            llm: LlmClient::new(
                server.uri(),
                None,
                "test-model".to_string(),
                Duration::from_secs(30),
            ),
            model_version: model_version.to_string(),
            mismatch_tracker: MismatchTracker::default(),
            retry_backoff: RetryBackoff::default(),
            in_flight: InFlight::default(),
            carry_forward_noops,
        }
    }

    /// The full crux of this plan: a primary extraction that exceeds
    /// MAX_PERIODS must now (a) still write successfully, and (b) leave
    /// `source_text_hash`/`extraction_model_version` matching the current
    /// text/version -- proving `sweep::incidents_needing_extraction` will
    /// NOT re-select this incident on its next tick (it re-selects only on
    /// a hash or version mismatch, `sweep.rs:27-35`). Before Decision 3,
    /// this incident would fail at `extract_primary` and neither column
    /// would ever be written, reproducing the retry-forever bug this test
    /// exists to close. Mocks all three LLM calls against one wiremock
    /// server, distinguished by each request's `response_format.json_schema.name`
    /// (`"incident_extraction"` / `"adversarial_resolution_check"` /
    /// `"adversarial_severity_check"`, matching PRIMARY_SCHEMA_NAME/
    /// ADVERSARIAL_SCHEMA_NAME/SEVERITY_ADVERSARIAL_SCHEMA_NAME in llm.rs)
    /// so the primary call can return more than MAX_PERIODS periods while
    /// the two adversarial calls return exactly MAX_PERIODS verdicts each
    /// -- matching what extract_primary's own truncation guarantees
    /// process_incident will actually send them.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher process_incident -- --ignored --test-threads=1`"]
    async fn process_incident_writes_successfully_and_advances_hash_and_version_when_primary_extraction_is_truncated()
     {
        let pool = test_pool().await;
        let incident_id = "TEST-ENRICHER-TRUNCATION-1";
        let summary = "Test incident exceeding the period cap";
        let description = "Thirteen distinct facts reported across this incident's lifetime.";

        sqlx::query(
            "INSERT INTO incidents (incident_id, summary, description, operators, affected_stations, priority) \
             VALUES ($1, $2, $3, '{}', '{}', 3) \
             ON CONFLICT (incident_id) DO UPDATE SET summary = EXCLUDED.summary, description = EXCLUDED.description, \
                 source_text_hash = NULL, extraction_model_version = NULL, extracted_periods = NULL",
        )
        .bind(incident_id)
        .bind(summary)
        .bind(description)
        .execute(&pool)
        .await
        .expect("seed fixture incident row");

        let server = MockServer::start().await;
        let over_cap_periods: Vec<serde_json::Value> = (0..(MAX_PERIODS_FOR_TEST + 3))
            .map(|i| {
                serde_json::json!({
                    "scope_description": format!("p{i}"),
                    "date_range": null,
                    "schedule_window": null,
                    "resolution_status": "ongoing",
                    "apparent_severity": "moderate_disruption",
                    "impact_type": null
                })
            })
            .collect();
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("incident_extraction"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{ "message": { "content": serde_json::json!({ "category": "signal_failure", "periods": over_cap_periods }).to_string() } }]
            })))
            .mount(&server)
            .await;
        let kept_verdicts: Vec<serde_json::Value> = (0..MAX_PERIODS_FOR_TEST)
            .map(|i| serde_json::json!({ "period_index": i, "scope_description": format!("p{i}"), "resolution_status": "ongoing" }))
            .collect();
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("adversarial_resolution_check"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{ "message": { "content": serde_json::json!({ "periods": kept_verdicts }).to_string() } }]
            })))
            .mount(&server)
            .await;
        let kept_severity_verdicts: Vec<serde_json::Value> = (0..MAX_PERIODS_FOR_TEST)
            .map(|i| serde_json::json!({ "period_index": i, "scope_description": format!("p{i}"), "apparent_severity": "moderate_disruption" }))
            .collect();
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("adversarial_severity_check"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{ "message": { "content": serde_json::json!({ "periods": kept_severity_verdicts }).to_string() } }]
            })))
            .mount(&server)
            .await;

        let model_version = "test-model@periods-v1";
        let enricher = test_enricher(pool.clone(), &server, model_version, false);

        let ok = process_incident(&enricher, incident_id).await;
        assert!(
            ok,
            "a truncated-but-successful extraction must return true (ack the entry), not false"
        );

        let row: (Option<String>, Option<String>, Option<serde_json::Value>) = sqlx::query_as(
            "SELECT source_text_hash, extraction_model_version, extracted_periods FROM incidents WHERE incident_id = $1",
        )
        .bind(incident_id)
        .fetch_one(&pool)
        .await
        .expect("fetch written row");
        let expected_hash = common::text_hash::text_hash(summary, description);
        assert_eq!(
            row.0.as_deref(),
            Some(expected_hash.as_str()),
            "source_text_hash must advance even though the extraction was truncated"
        );
        assert_eq!(
            row.1.as_deref(),
            Some(model_version),
            "extraction_model_version must advance even though the extraction was truncated"
        );
        let periods = row.2.expect("extracted_periods must be written");
        assert_eq!(
            periods.as_array().expect("periods is an array").len(),
            MAX_PERIODS_FOR_TEST,
            "the written periods must be the truncated (in-cap) set, not the original over-cap one"
        );

        // The actual retry-forever-loop-is-closed assertion: re-running
        // sweep::incidents_needing_extraction's own comparison against
        // what was just written must NOT re-select this incident.
        let current_hash = common::text_hash::text_hash(summary, description);
        assert!(
            row.0.as_deref() == Some(current_hash.as_str())
                && row.1.as_deref() == Some(model_version),
            "this incident must no longer match sweep::incidents_needing_extraction's re-select condition (sweep.rs:27-35)"
        );

        sqlx::query("DELETE FROM incidents WHERE incident_id = $1")
            .bind(incident_id)
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    // `MAX_PERIODS` itself is private to `llm.rs`; this local alias avoids
    // either making it pub(crate) just for a test fixture or hardcoding
    // the literal `8` twice in a way that would silently desync if
    // Task 5's Axis 2 process ever changes the real constant. Update this
    // alongside `llm::MAX_PERIODS` if that ever happens.
    const MAX_PERIODS_FOR_TEST: usize = 8;

    /// Finding regression: a deterministically-failing incident (here, an
    /// endpoint that always returns malformed content for the primary
    /// pass -- indistinguishable, from `process_incident`'s point of view,
    /// from a model that can never parse this particular text) must stop
    /// costing an LLM call on every single attempt. The first two attempts
    /// against unchanged text (the original call, plus one retry) still
    /// reach the endpoint -- `RetryBackoff` never delays the first failure,
    /// so a genuinely transient blip keeps retrying at the caller's normal
    /// cadence -- but the THIRD attempt, still against the same
    /// unchanged text, must be skipped locally without another request,
    /// proving the backoff actually suppresses the wasted call rather than
    /// merely logging about it.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher process_incident -- --ignored --test-threads=1`"]
    async fn process_incident_backs_off_a_second_consecutive_deterministic_failure_without_another_llm_call()
     {
        let pool = test_pool().await;
        let incident_id = "TEST-ENRICHER-BACKOFF-1";
        let summary = "Test incident whose extraction can never succeed";
        let description = "Deliberately triggers a malformed primary-pass response every time.";

        sqlx::query(
            "INSERT INTO incidents (incident_id, summary, description, operators, affected_stations, priority) \
             VALUES ($1, $2, $3, '{}', '{}', 3) \
             ON CONFLICT (incident_id) DO UPDATE SET summary = EXCLUDED.summary, description = EXCLUDED.description, \
                 source_text_hash = NULL, extraction_model_version = NULL, extracted_periods = NULL",
        )
        .bind(incident_id)
        .bind(summary)
        .bind(description)
        .execute(&pool)
        .await
        .expect("seed fixture incident row");

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{ "message": { "content": "not valid json" } }]
            })))
            .mount(&server)
            .await;

        let model_version = "test-model@periods-v1";
        let enricher = test_enricher(pool.clone(), &server, model_version, false);

        for attempt in 1..=2 {
            let ok = process_incident(&enricher, incident_id).await;
            assert!(!ok, "a malformed response must never be treated as success");
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                attempt,
                "attempt {attempt} against unchanged text must still reach the endpoint -- only \
                 a SECOND consecutive failure starts backing off later attempts, not this one"
            );
        }

        // Third attempt, same unchanged text: `RetryBackoff` must now skip
        // it locally -- the request count must stay at 2, not become 3.
        let ok = process_incident(&enricher, incident_id).await;
        assert!(!ok, "a backed-off attempt still has nothing to ack");
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            2,
            "a third attempt against the same still-failing text must be backed off locally, \
             not spend another LLM call reproducing the identical deterministic failure"
        );

        sqlx::query("DELETE FROM incidents WHERE incident_id = $1")
            .bind(incident_id)
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    // -- In-flight dedupe across the stream, sweep and reclaim loops --

    /// A pool that never connects (nothing listens on port 1). Anything
    /// that reaches the DB fails fast, so a test using it proves the
    /// in-flight skip happens before any DB (or LLM) work.
    fn unreachable_pool() -> PgPool {
        PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(500))
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .expect("a lazy pool never connects at construction")
    }

    #[test]
    fn in_flight_claim_is_exclusive_and_released_on_drop() {
        let in_flight = InFlight::default();
        let claim = in_flight.try_claim("A").expect("first claim");
        assert!(in_flight.try_claim("A").is_none(), "A is already claimed");
        let other = in_flight.try_claim("B").expect("other ids are independent");
        drop(claim);
        assert!(in_flight.try_claim("A").is_some(), "released on drop");
        drop(other);
        assert!(in_flight.ids.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn in_flight_claim_is_released_when_the_owning_future_is_dropped() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
            .mount(&server)
            .await;
        let enricher = test_enricher(unreachable_pool(), &server, "m@v", false);
        let claimed = async {
            let _claim = enricher.in_flight.try_claim("A").unwrap();
            std::future::pending::<()>().await;
        };
        // Cancelled mid-"extraction" (e.g. the task is aborted).
        let _ = tokio::time::timeout(Duration::from_millis(10), claimed).await;
        assert!(enricher.in_flight.try_claim("A").is_some());
    }

    /// Overlap fix: the sweep must not re-run an incident another loop is
    /// already processing. With "A" claimed, the sweep skips it (no DB, no
    /// LLM) and still attempts "B".
    #[tokio::test]
    async fn sweep_skips_incidents_in_flight_in_another_loop() {
        let server = MockServer::start().await;
        let enricher = test_enricher(unreachable_pool(), &server, "m@v", false);
        let claim = enricher.in_flight.try_claim("A").unwrap();

        let skipped = sweep_ids(&enricher, &["A".to_string(), "B".to_string()]).await;
        assert_eq!(skipped, 1, "only the in-flight incident is skipped");
        assert!(server.received_requests().await.unwrap().is_empty());
        assert!(
            enricher.in_flight.try_claim("B").is_some(),
            "the sweep's own claim on B was released after its attempt"
        );

        drop(claim);
        assert_eq!(sweep_ids(&enricher, &["A".to_string()]).await, 0);
    }

    /// Overlap fix: reclaim hands back an entry the stream loop is still
    /// processing once the attempt outlasts RECLAIM_MIN_IDLE_SECS. It must
    /// neither re-run it nor ack it (the holder acks on success; otherwise a
    /// later reclaim pass retries it).
    #[tokio::test]
    async fn reclaim_neither_processes_nor_acks_an_in_flight_incident() {
        let server = MockServer::start().await;
        let enricher = test_enricher(unreachable_pool(), &server, "m@v", false);
        let _claim = enricher.in_flight.try_claim("A").unwrap();

        let to_ack = process_reclaimed(
            &enricher,
            vec![
                ("1-0".to_string(), "A".to_string()),
                ("2-0".to_string(), "B".to_string()),
            ],
        )
        .await;
        // "B" was attempted (and failed on the unreachable DB), "A" skipped:
        // neither is acked.
        assert!(to_ack.is_empty(), "{to_ack:?}");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn stream_entry_is_left_pending_when_its_incident_is_in_flight() {
        let server = MockServer::start().await;
        let enricher = test_enricher(unreachable_pool(), &server, "m@v", false);
        let _claim = enricher.in_flight.try_claim("A").unwrap();
        assert!(!process_stream_entry(&enricher, "1-0", "A").await);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[test]
    fn llm_duration_buckets_extend_past_the_configured_timeout() {
        let default = llm_duration_buckets(300);
        assert_eq!(default.last(), Some(&1200.0));
        assert!(default.contains(&600.0));
        let gateway = llm_duration_buckets(320);
        assert!(gateway.contains(&320.0) && gateway.contains(&640.0) && gateway.contains(&1280.0));
        assert!(gateway.windows(2).all(|w| w[0] < w[1]), "{gateway:?}");
    }

    // -- DB-gated: reclaim/in-flight and carry-forward end to end --

    async fn seed_incident(pool: &PgPool, incident_id: &str, summary: &str, description: &str) {
        sqlx::query("DELETE FROM incident_history WHERE incident_id = $1")
            .bind(incident_id)
            .execute(pool)
            .await
            .expect("clear history");
        sqlx::query(
            "INSERT INTO incidents (incident_id, summary, description, operators, affected_stations, priority) \
             VALUES ($1, $2, $3, '{}', '{}', 3) \
             ON CONFLICT (incident_id) DO UPDATE SET summary = EXCLUDED.summary, description = EXCLUDED.description, \
                 source_text_hash = NULL, extraction_model_version = NULL, extracted_periods = NULL, \
                 extracted_category = NULL",
        )
        .bind(incident_id)
        .bind(summary)
        .bind(description)
        .execute(pool)
        .await
        .expect("seed fixture incident row");
        sqlx::query(
            "INSERT INTO incident_history (incident_id, summary, description, operators, affected_stations, is_planned, priority) \
             VALUES ($1, $2, $3, '{}', '{}', false, 3)",
        )
        .bind(incident_id)
        .bind(summary)
        .bind(description)
        .execute(pool)
        .await
        .expect("seed history row");
    }

    async fn cleanup_incident(pool: &PgPool, incident_id: &str) {
        for table in ["incident_history", "incidents"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE incident_id = $1"))
                .bind(incident_id)
                .execute(pool)
                .await
                .expect("cleanup");
        }
    }

    /// Mounts a one-period answer for all three passes.
    async fn mount_flat_extraction(server: &MockServer) {
        let answers = [
            (
                "incident_extraction",
                serde_json::json!({ "category": "signal_failure", "periods": [{
                    "scope_description": null, "date_range": null, "schedule_window": null,
                    "resolution_status": "ongoing", "apparent_severity": "moderate_disruption",
                    "impact_type": null }] }),
            ),
            (
                "adversarial_resolution_check",
                serde_json::json!({ "periods": [{ "period_index": 0, "scope_description": null,
                    "resolution_status": "ongoing" }] }),
            ),
            (
                "adversarial_severity_check",
                serde_json::json!({ "periods": [{ "period_index": 0, "scope_description": null,
                    "apparent_severity": "moderate_disruption" }] }),
            ),
        ];
        for (schema, content) in answers {
            Mock::given(method("POST"))
                .and(path("/chat/completions"))
                .and(body_string_contains(schema))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "choices": [{ "message": { "content": content.to_string() } }]
                })))
                .mount(server)
                .await;
        }
    }

    /// With a real DB: an already-extracted incident's reclaimed entry is
    /// acked with no LLM call, while an in-flight incident's entry is left
    /// pending and never extracted a second time.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher reclaim -- --ignored --test-threads=1`"]
    async fn reclaim_acks_finished_entries_and_skips_in_flight_ones() {
        let pool = test_pool().await;
        let (done, busy) = ("TEST-ENRICHER-RECLAIM-DONE", "TEST-ENRICHER-RECLAIM-BUSY");
        let server = MockServer::start().await;
        mount_flat_extraction(&server).await;
        let model_version = "test-model@periods-v2";
        let enricher = test_enricher(pool.clone(), &server, model_version, false);
        seed_incident(&pool, done, "Signal failure", "Lines closed.").await;
        seed_incident(&pool, busy, "Points failure", "Lines closed.").await;
        assert!(process_incident(&enricher, done).await, "first extraction");
        assert_eq!(server.received_requests().await.unwrap().len(), 3);

        let claim = enricher.in_flight.try_claim(busy).unwrap();
        let to_ack = process_reclaimed(
            &enricher,
            vec![
                ("1-0".to_string(), done.to_string()),
                ("2-0".to_string(), busy.to_string()),
            ],
        )
        .await;
        assert_eq!(to_ack, ["1-0"], "done is acked, busy is left pending");
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            3,
            "neither entry cost another LLM call"
        );

        drop(claim);
        let to_ack =
            process_reclaimed(&enricher, vec![("2-0".to_string(), busy.to_string())]).await;
        assert_eq!(
            to_ack,
            ["2-0"],
            "once released, a later pass processes and acks it"
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 6);

        cleanup_incident(&pool, done).await;
        cleanup_incident(&pool, busy).await;
    }

    /// Carry-forward end to end: a semantic no-op text change makes 0 LLM
    /// requests and advances `source_text_hash` with the flag on, and runs
    /// the full 3-call extraction with it off (the default).
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher carry_forward -- --ignored --test-threads=1`"]
    async fn carry_forward_skips_the_llm_for_a_semantic_noop_only_when_enabled() {
        let pool = test_pool().await;
        let incident_id = "TEST-ENRICHER-CARRY-FORWARD-1";
        let summary = "Signal failure at Crewe";
        let (old_description, new_description) =
            ("<p>Lines are closed.</p>", "<h4>Lines   are closed</h4>");
        let model_version = "test-model@periods-v2";

        for carry_forward in [true, false] {
            let server = MockServer::start().await;
            mount_flat_extraction(&server).await;
            let enricher = test_enricher(pool.clone(), &server, model_version, carry_forward);
            seed_incident(&pool, incident_id, summary, old_description).await;
            assert!(
                process_incident(&enricher, incident_id).await,
                "initial extraction"
            );
            assert_eq!(server.received_requests().await.unwrap().len(), 3);
            let before: (Option<String>, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
                "SELECT extracted_category, extracted_at FROM incidents WHERE incident_id = $1",
            )
            .bind(incident_id)
            .fetch_one(&pool)
            .await
            .unwrap();

            // The feed re-renders the same text with different markup.
            sqlx::query("UPDATE incidents SET description = $2 WHERE incident_id = $1")
                .bind(incident_id)
                .bind(new_description)
                .execute(&pool)
                .await
                .unwrap();
            assert!(process_incident(&enricher, incident_id).await);

            let expected_calls = if carry_forward { 3 } else { 6 };
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                expected_calls,
                "carry_forward={carry_forward}"
            );
            let after: (
                Option<String>,
                Option<String>,
                Option<chrono::DateTime<chrono::Utc>>,
            ) = sqlx::query_as(
                "SELECT source_text_hash, extracted_category, extracted_at FROM incidents \
                     WHERE incident_id = $1",
            )
            .bind(incident_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                after.0.as_deref(),
                Some(common::text_hash::text_hash(summary, new_description).as_str()),
                "the hash advances either way"
            );
            assert_eq!(after.1, before.0);
            if carry_forward {
                assert_eq!(
                    after.2, before.1,
                    "extracted_at still says when the LLM last ran"
                );
            }
        }
        cleanup_incident(&pool, incident_id).await;
    }
}
