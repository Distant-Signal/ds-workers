//! Batches: extractions sent through the provider's batch API (the Claude
//! Message Batches API or the `OpenAI` Batch API, `llm::batch_api`), at
//! half the synchronous price, instead of one incident at a time.
//! docs/enricher-anthropic.md, "LLM modes" and "Batch mode", and
//! docs/enricher-openai.md, "Batch mode".
//!
//! **Where it applies.** Three ways in (`config::BatchConfig`):
//!
//! - `LLM_MODE=normal` with `LLM_SWEEP_MODE=batch`: only the hourly sweep.
//!   It is the bulk path: after a model or prompt change (`model_version`)
//!   it re-extracts every live incident, and nothing waits on it. The
//!   stream loop (a text change, seconds after it lands) and reclaim (its
//!   retries) stay synchronous: a batch can take up to 24 hours. A sweep
//!   that finds fewer than `LLM_BATCH_MIN_ITEMS` incidents runs them
//!   synchronously.
//! - `LLM_MODE=batch`: the stream loop and reclaim make no LLM call. They
//!   run the preflight (skip unchanged text, carry a semantic no-op
//!   forward) and, where the LLM would be needed, ACK without calling it
//!   (`enricher_deferred_to_batch_total`), leaving the incident stale. A
//!   sweep every `LLM_BATCH_SWEEP_INTERVAL_SECS` (120 s) then batches what
//!   it finds if there are at least `LLM_BATCH_MIN_ITEMS`, and extracts
//!   them synchronously right away if fewer.
//! - `LLM_MODE=batch-only`: as `batch`, but the sweep (every 300 s)
//!   batches whatever it finds, even one incident; nothing ever runs
//!   synchronously.
//!
//! **No double submission.** In `normal`, the sweep skips every incident in
//! an in-flight batch. In `batch` and `batch-only` it skips only those
//! whose *current* text is in one: a text that changed since its batch was
//! submitted is extracted again right away (batched or synchronously), and
//! the old batch's result for it is dropped by `write_extraction`'s
//! stale-text guard when it lands (it no longer matches the incident's
//! text). With the stream loop deferring, waiting for the old batch would
//! otherwise hold a changed text back for up to a day.
//!
//! **Two stages.** One incident is three calls, and the two adversarial
//! passes need the primary pass's periods. So a sweep submits a *primary*
//! batch (one request per incident); when it has ended, the poll loop
//! submits an *adversarial* batch (two requests per incident whose primary
//! output parsed); when that has ended, it combines and writes each
//! incident through the synchronous path's own `finish_extraction`.
//!
//! **Persistence.** Each in-flight batch is a row of `enricher_llm_batches`
//! (batch id, stage, `model_version` and the items: the exact text it was
//! built from, plus the primary output in the adversarial stage). The poll
//! loop works from the table, so a restart resumes polling instead of
//! submitting again, and the sweep skips every incident in a row. Moving
//! to the adversarial stage inserts the new row and deletes the old one in
//! one transaction, after the new batch was created. A crash between
//! creating a batch and recording it orphans that batch (it runs and is
//! billed, but nobody reads it; the incidents go into a later batch).
//!
//! **Memory.** A batch is up to `LLM_BATCH_MAX_ITEMS` (2,000) incidents;
//! its requests and results are tens of MB, so nothing holds a whole batch
//! more than once. The requests are held as serialised text and written
//! into the upload body without a `Value` copy, and the results file is
//! parsed line by line as it downloads (`llm::batch_api`, "Memory"). The
//! poll loop reads only each row's id, stage and model version every tick;
//! a row's `items` are loaded once its batch has ended, decoded straight
//! into [`BatchItem`]s, and moved (never cloned) through the stage. An
//! adversarial stage is combined and written [`WRITE_CHUNK`] incidents at
//! a time, each result dropped once used. Afterwards
//! `common::heap::release_freed_memory` hands the freed heap back to the kernel.
//!
//! **Results.** Matched by `custom_id` (`p-<n>`, `r-<n>`, `s-<n>`, `n` the
//! item's index), never by position. `errored`, `canceled`, `expired`,
//! `auth_expired` and missing entries drop that incident from the batch;
//! the next sweep finds it again (its stored hash still doesn't match).
//! `auth_expired` (`OpenAI` `token_expired`: the creator's token had
//! expired when `OpenAI` authorised the batch, after the create call) is
//! also counted once per batch (`event="auth_expired"`); the next sweep's
//! batch is created with a freshly minted token
//! (`LlmAuth::submission_credential`). An unusable success (a
//! refusal, a truncated or unparseable output) also feeds the per-text
//! backoff, exactly as on the synchronous path. A batch whose
//! `model_version` is no longer the service's is canceled and dropped, and
//! so is one submitted to another provider (`provider` column).
//!
//! **Provider files.** An `OpenAI` batch has an input, an output and an
//! error file; once its results are processed they are deleted (a failure
//! is logged and counted, `enricher_llm_batch_files_total`, and the files
//! expire within two days anyway). A batch that failed input validation
//! (`OpenAI` `failed`) is dropped (`validation_failed`, or `auth_expired`
//! when the failure is `token_expired`) and its input file deleted.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::config::LlmMode;
use crate::llm::batch_api::{
    BatchMeta, BatchOutcome, BatchPoll, BatchRequest, BatchResults, CreatedBatch,
};
use crate::llm::{self, LlmCall, LlmCallError, TokenUsage};
use crate::{Enricher, Preflight, Prepared};

/// Ceiling on `LLM_BATCH_MAX_ITEMS`. Claude takes up to 100,000 requests
/// or 256 MB per batch, `OpenAI` 50,000 requests or a 200 MB input file;
/// each request carries a system prompt of up to ~11 KB, and the
/// adversarial batch has two per incident, so 5,000 incidents stay well
/// under both (10,000 requests, ~130 MB).
pub(crate) const MAX_BATCH_INCIDENTS: usize = 5_000;

/// `enricher_llm_batches_total{provider, stage, event}`: `submitted`,
/// `submit_failed`, `ended`, `poll_failed`, `abandoned`,
/// `validation_failed` (`OpenAI`: the input file failed validation, so
/// nothing ran), `auth_expired` (`OpenAI`: the batch failed, or some of its
/// requests did, with `token_expired`, because the creator's token had
/// expired when `OpenAI` authorised it; once per batch).
const BATCHES_METRIC: &str = "enricher_llm_batches_total";
/// `enricher_llm_batch_requests_total{provider, call, result}`: one per
/// request of an
/// ended batch -- `succeeded`, `invalid` (a success the enricher could not
/// use: a refusal, truncated or unparseable output), `errored`,
/// `canceled`, `expired`, `auth_expired` (`OpenAI` `token_expired`: the
/// creator's token had expired when the request was authorised), `missing`
/// (no result line).
const REQUESTS_METRIC: &str = "enricher_llm_batch_requests_total";
/// `enricher_llm_batches_in_flight`: rows of `enricher_llm_batches`, as of
/// the last poll.
const IN_FLIGHT_METRIC: &str = "enricher_llm_batches_in_flight";
/// `enricher_llm_batch_oldest_age_seconds`: how long ago the oldest
/// in-flight batch row was submitted (0 with none), as of the last poll. A
/// batch ends within 24 h (it expires then), and its adversarial stage is a
/// new row, so a value well past a day means the enricher is not finishing
/// it (`DistantSignalEnricherBatchStuck`).
const OLDEST_AGE_METRIC: &str = "enricher_llm_batch_oldest_age_seconds";

/// The batch-mode settings (`config::BatchConfig`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BatchSettings {
    pub min_items: usize,
    pub max_items: usize,
    pub poll_interval: Duration,
    /// `LLM_MODE`: `normal` here means `LLM_SWEEP_MODE=batch`.
    pub mode: LlmMode,
}

impl BatchSettings {
    /// `LLM_MODE=batch`/`batch-only`: the stream loop and reclaim leave
    /// extraction to the sweep.
    pub(crate) fn defers_stream(&self) -> bool {
        self.mode != LlmMode::Normal
    }

    /// `LLM_MODE=batch-only`: no synchronous extraction anywhere.
    pub(crate) fn batch_only(&self) -> bool {
        self.mode == LlmMode::BatchOnly
    }
}

/// A batch's stage (the `stage` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    Primary,
    Adversarial,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Adversarial => "adversarial",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "primary" => Some(Self::Primary),
            "adversarial" => Some(Self::Adversarial),
            _ => None,
        }
    }
}

/// One incident of a batch, as stored in `items`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BatchItem {
    /// Unique within the batch; the `<n>` of its custom ids.
    pub index: usize,
    pub incident_id: String,
    pub text_hash: String,
    pub summary: String,
    pub description: String,
    pub reference_date: DateTime<Utc>,
    /// `Prepared::text_seen_at`, for the latency metric. Absent from rows
    /// written before it existed, which then record no latency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_seen_at: Option<DateTime<Utc>>,
    /// The primary pass's raw output (adversarial stage only), re-parsed
    /// with `llm::parse_primary` when the adversarial results arrive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_content: Option<String>,
}

impl BatchItem {
    fn from_prepared(index: usize, prepared: &Prepared) -> Self {
        Self {
            index,
            incident_id: prepared.incident_id.clone(),
            text_hash: prepared.text_hash.clone(),
            summary: prepared.summary.clone(),
            description: prepared.description.clone(),
            reference_date: prepared.reference_date,
            text_seen_at: prepared.text_seen_at,
            primary_content: None,
        }
    }

    fn prepared(&self) -> Prepared {
        Prepared {
            incident_id: self.incident_id.clone(),
            text_hash: self.text_hash.clone(),
            summary: self.summary.clone(),
            description: self.description.clone(),
            reference_date: self.reference_date,
            churn_baseline: None,
            edit_class: None,
            text_seen_at: self.text_seen_at,
        }
    }
}

/// The `custom_id` of item `index`'s request for `call`.
pub(crate) fn custom_id(call: LlmCall, index: usize) -> String {
    let prefix = match call {
        LlmCall::Primary => "p",
        LlmCall::ResolutionAdversarial => "r",
        LlmCall::SeverityAdversarial => "s",
    };
    format!("{prefix}-{index}")
}

/// Every `event` of [`BATCHES_METRIC`].
const BATCH_EVENTS: [&str; 7] = [
    "submitted",
    "submit_failed",
    "ended",
    "poll_failed",
    "abandoned",
    "validation_failed",
    "auth_expired",
];

fn record_batch_event(provider: &'static str, stage: Stage, event: &'static str) {
    metrics::counter!(
        common::metrics::metric_name(BATCHES_METRIC),
        "provider" => provider,
        "stage" => stage.as_str(),
        "event" => event
    )
    .increment(1);
}

/// Registers the batch metrics' series at 0 (batch mode only), labelled
/// with the configured `provider`.
pub(crate) fn register_metrics(provider: &'static str) {
    llm::register_token_series(llm::BATCH_TOKENS_METRIC);
    for stage in [Stage::Primary, Stage::Adversarial] {
        for event in BATCH_EVENTS {
            metrics::counter!(
                common::metrics::metric_name(BATCHES_METRIC),
                "provider" => provider,
                "stage" => stage.as_str(),
                "event" => event
            )
            .increment(0);
        }
    }
    for call in LlmCall::ALL {
        for result in [
            "succeeded",
            "invalid",
            "errored",
            "canceled",
            "expired",
            "auth_expired",
            "missing",
        ] {
            metrics::counter!(
                common::metrics::metric_name(REQUESTS_METRIC),
                "provider" => provider,
                "call" => call.label(),
                "result" => result
            )
            .increment(0);
        }
    }
    if provider == "openai" {
        for event in ["uploaded", "deleted", "delete_failed"] {
            metrics::counter!(
                common::metrics::metric_name(llm::batch_api::FILES_METRIC),
                "provider" => provider,
                "event" => event
            )
            .increment(0);
        }
    }
    metrics::gauge!(common::metrics::metric_name(IN_FLIGHT_METRIC)).set(0.0);
    metrics::gauge!(common::metrics::metric_name(OLDEST_AGE_METRIC)).set(0.0);
}

// ---------------------------------------------------------------------------
// Result planning: pure, so the matching rules are testable without a
// database or a server.
// ---------------------------------------------------------------------------

/// One batch request's fate, for the metrics.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RequestRecord {
    pub call: LlmCall,
    pub result: &'static str,
    /// Billed tokens (a success's `usage`).
    pub usage: Option<TokenUsage>,
}

/// What to do with an ended batch's results.
#[derive(Debug, Default)]
pub(crate) struct StagePlan {
    /// Primary stage: the items to submit as the adversarial batch, each
    /// with its `primary_content`.
    pub next: Vec<BatchItem>,
    /// Adversarial stage: the items to combine and write.
    pub ready: Vec<ReadyItem>,
    pub records: Vec<RequestRecord>,
    /// `(incident_id, text_hash, error)` of every unusable success: the
    /// model's answer to that text, so it feeds the per-text backoff.
    pub text_failures: Vec<(String, String, String)>,
}

/// An incident whose three passes all succeeded in the batches.
#[derive(Debug)]
pub(crate) struct ReadyItem {
    pub item: BatchItem,
    pub primary: llm::PrimaryExtraction,
    pub resolution: Vec<llm::AdversarialPeriodVerdict>,
    pub severity: Vec<llm::SeverityAdversarialPeriodVerdict>,
}

/// A request's usable output: `Ok(text)`, or `Err(None)` for a request
/// that produced nothing billable or usable (errored, canceled, expired,
/// missing; already recorded), or `Err(Some(reason))` for an unusable
/// success (recorded as `invalid`).
fn take_output(
    results: &mut BatchResults,
    call: LlmCall,
    index: usize,
    records: &mut Vec<RequestRecord>,
) -> Result<String, Option<String>> {
    let Some(outcome) = results
        .outcomes
        .remove(&custom_id(call, index))
        .or_else(|| results.unanswered.clone())
    else {
        records.push(RequestRecord {
            call,
            result: "missing",
            usage: None,
        });
        return Err(None);
    };
    let BatchOutcome::Succeeded { usage, content, .. } = outcome else {
        if let BatchOutcome::Errored { kind, message } = &outcome {
            tracing::warn!(
                call = call.label(),
                error_type = kind.as_deref(),
                error = message.as_deref(),
                "batch request errored"
            );
        }
        records.push(RequestRecord {
            call,
            result: outcome.label(),
            usage: None,
        });
        return Err(None);
    };
    match content {
        Ok(text) => {
            records.push(RequestRecord {
                call,
                result: "succeeded",
                usage,
            });
            Ok(text)
        }
        Err(err) => {
            records.push(RequestRecord {
                call,
                result: "invalid",
                usage,
            });
            Err(Some(err))
        }
    }
}

/// Plans an ended primary batch: parse each item's output; the parsed ones
/// go on to the adversarial stage.
pub(crate) fn plan_primary(items: Vec<BatchItem>, mut outcomes: BatchResults) -> StagePlan {
    let mut plan = StagePlan::default();
    for mut item in items {
        let parsed = take_output(
            &mut outcomes,
            LlmCall::Primary,
            item.index,
            &mut plan.records,
        )
        .and_then(|text| match llm::parse_primary(&text) {
            Ok(_) => Ok(text),
            Err(err) => {
                // A success whose JSON is unusable: re-label it.
                if let Some(last) = plan.records.last_mut() {
                    last.result = "invalid";
                }
                Err(Some(err.to_string()))
            }
        });
        match parsed {
            Ok(text) => {
                item.primary_content = Some(text);
                plan.next.push(item);
            }
            Err(Some(reason)) => {
                plan.text_failures
                    .push((item.incident_id, item.text_hash, reason));
            }
            Err(None) => {}
        }
    }
    plan
}

/// Plans an ended adversarial batch: the items whose two verdict lists
/// both parsed are ready to combine and write.
pub(crate) fn plan_adversarial(items: Vec<BatchItem>, outcomes: &mut BatchResults) -> StagePlan {
    let mut plan = StagePlan::default();
    for item in items {
        let Some(primary) = item
            .primary_content
            .as_deref()
            .and_then(|content| llm::parse_primary(content).ok())
        else {
            tracing::error!(
                incident_id = item.incident_id,
                "adversarial batch item has no parseable primary output; dropping it"
            );
            continue;
        };
        let resolution = take_output(
            outcomes,
            LlmCall::ResolutionAdversarial,
            item.index,
            &mut plan.records,
        )
        .and_then(|text| {
            llm::parse_adversarial(&text).map_err(|err| {
                if let Some(last) = plan.records.last_mut() {
                    last.result = "invalid";
                }
                Some(err.to_string())
            })
        });
        let severity = take_output(
            outcomes,
            LlmCall::SeverityAdversarial,
            item.index,
            &mut plan.records,
        )
        .and_then(|text| {
            llm::parse_severity_adversarial(&text).map_err(|err| {
                if let Some(last) = plan.records.last_mut() {
                    last.result = "invalid";
                }
                Some(err.to_string())
            })
        });
        match (resolution, severity) {
            (Ok(resolution), Ok(severity)) => plan.ready.push(ReadyItem {
                item,
                primary,
                resolution,
                severity,
            }),
            (Err(Some(reason)), _) | (_, Err(Some(reason))) => {
                plan.text_failures
                    .push((item.incident_id, item.text_hash, reason));
            }
            _ => {}
        }
    }
    plan
}

/// Counts a plan's records and backs off its text failures.
fn apply_records(enricher: &Enricher, plan: &StagePlan) {
    for record in &plan.records {
        metrics::counter!(
            common::metrics::metric_name(REQUESTS_METRIC),
            "provider" => enricher.llm.provider_label(),
            "call" => record.call.label(),
            "result" => record.result
        )
        .increment(1);
        if let Some(usage) = &record.usage {
            llm::record_token_usage_to(llm::BATCH_TOKENS_METRIC, record.call, usage);
        }
    }
    for (incident_id, text_hash, reason) in &plan.text_failures {
        tracing::warn!(
            incident_id,
            error = %reason,
            "batch extraction output unusable; backing this text off"
        );
        enricher
            .retry_backoff
            .record_failure(incident_id, text_hash);
    }
}

// ---------------------------------------------------------------------------
// Database: `enricher_llm_batches`.
// ---------------------------------------------------------------------------

/// One row of `enricher_llm_batches`.
#[derive(Debug, Clone)]
pub(crate) struct BatchRow {
    pub batch_id: String,
    /// The provider it was submitted to (`LlmClient::provider_label`).
    pub provider: String,
    pub stage: Stage,
    pub model_version: String,
    pub submitted_at: DateTime<Utc>,
}

/// Seconds since the oldest row's submission (0 with none, or a clock
/// behind the database's).
pub(crate) fn oldest_age_secs(rows: &[BatchRow], now: DateTime<Utc>) -> u32 {
    rows.iter()
        .map(|row| (now - row.submitted_at).num_seconds())
        .max()
        .map_or(0, |secs| u32::try_from(secs.max(0)).unwrap_or(u32::MAX))
}

type BatchRecord = (String, String, String, String, DateTime<Utc>);

/// Every row, without its `items` (read once the batch has ended,
/// [`load_items`]): a tick that finds every batch still running reads a
/// few bytes per row, not every incident's text.
async fn load_batches(pool: &PgPool) -> anyhow::Result<Vec<BatchRow>> {
    let rows: Vec<BatchRecord> = sqlx::query_as(
        "SELECT batch_id, provider, stage, model_version, submitted_at \
         FROM enricher_llm_batches \
         ORDER BY submitted_at",
    )
    .fetch_all(pool)
    .await?;
    let mut batches = Vec::with_capacity(rows.len());
    for (batch_id, provider, stage, model_version, submitted_at) in rows {
        let Some(stage) = Stage::parse(&stage) else {
            tracing::error!(batch_id, stage, "unknown batch stage; skipping the row");
            continue;
        };
        batches.push(BatchRow {
            batch_id,
            provider,
            stage,
            model_version,
            submitted_at,
        });
    }
    Ok(batches)
}

/// An ended batch's `items`, decoded straight from the column's JSON (no
/// `Value` tree in between). `None` if the row is gone. Unreadable items
/// are an error: the row stays, and `enricher_llm_batch_oldest_age_seconds`
/// shows it stuck.
async fn load_items(pool: &PgPool, batch_id: &str) -> anyhow::Result<Option<Vec<BatchItem>>> {
    let row: Option<(sqlx::types::Json<Vec<BatchItem>>,)> =
        sqlx::query_as("SELECT items FROM enricher_llm_batches WHERE batch_id = $1")
            .bind(batch_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| anyhow::anyhow!("reading batch {batch_id}'s items: {err}"))?;
    Ok(row.map(|(items,)| items.0))
}

/// `(incident_id, text_hash)` of every item of every in-flight batch: the
/// sweep skips them (`normal`: by id; `batch`/`batch-only`: by id and
/// text).
async fn in_flight_items(pool: &PgPool) -> anyhow::Result<InFlightItems> {
    let pairs: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT item->>'incident_id', item->>'text_hash' FROM enricher_llm_batches, \
         jsonb_array_elements(items) AS item",
    )
    .fetch_all(pool)
    .await?;
    Ok(InFlightItems::new(pairs))
}

/// What is already in an in-flight batch.
#[derive(Debug, Default)]
pub(crate) struct InFlightItems {
    ids: HashSet<String>,
    texts: HashSet<(String, String)>,
}

impl InFlightItems {
    pub(crate) fn new(pairs: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut items = Self::default();
        for (id, hash) in pairs {
            items.ids.insert(id.clone());
            items.texts.insert((id, hash));
        }
        items
    }

    /// Some batch holds this incident (whatever its text).
    pub(crate) fn has_incident(&self, incident_id: &str) -> bool {
        self.ids.contains(incident_id)
    }

    /// Some batch holds this incident with exactly this text.
    pub(crate) fn has_text(&self, incident_id: &str, text_hash: &str) -> bool {
        self.texts
            .contains(&(incident_id.to_string(), text_hash.to_string()))
    }
}

/// `batch`/`batch-only`: the prepared incidents to extract -- all but those
/// whose current text is already in a batch. A changed text is extracted
/// again (see the module doc). Pure, for the tests.
pub(crate) fn not_in_a_batch(prepared: Vec<Prepared>, in_batch: &InFlightItems) -> Vec<Prepared> {
    prepared
        .into_iter()
        .filter(|p| !in_batch.has_text(&p.incident_id, &p.text_hash))
        .collect()
}

async fn insert_batch(
    executor: impl sqlx::PgExecutor<'_>,
    batch_id: &str,
    provider: &str,
    stage: Stage,
    model_version: &str,
    items: &[BatchItem],
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO enricher_llm_batches (batch_id, provider, stage, model_version, items) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(batch_id)
    .bind(provider)
    .bind(stage.as_str())
    .bind(model_version)
    .bind(sqlx::types::Json(items))
    .execute(executor)
    .await?;
    Ok(())
}

async fn delete_batch(executor: impl sqlx::PgExecutor<'_>, batch_id: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM enricher_llm_batches WHERE batch_id = $1")
        .bind(batch_id)
        .execute(executor)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The sweep side: submitting primary batches.
// ---------------------------------------------------------------------------

/// What the sweep's batch step decided.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SweepSplit {
    /// Submit these as primary batches.
    Batch(Vec<String>),
    /// Extract these synchronously, now.
    Sync(Vec<String>),
}

/// The threshold rule, pure for the tests: `count` incidents left after
/// the in-flight skip go to a batch when there are at least `min_items`
/// (always, in `batch-only`), else to the synchronous path.
pub(crate) fn split_for_sweep(settings: &BatchSettings, ids: Vec<String>) -> SweepSplit {
    if settings.batch_only() || ids.len() >= settings.min_items {
        SweepSplit::Batch(ids)
    } else {
        SweepSplit::Sync(ids)
    }
}

/// The sweep's batch step. Skips what is already in a batch (`normal`: by
/// id, up front; `batch`/`batch-only`: by id and current text, after the
/// preflight); with at least `min_items` left (any, in `batch-only`), runs
/// each through `preflight` and submits the ones that need the LLM as
/// primary batches of up to `max_items`. Returns the ids left for the
/// synchronous path: all of them below the threshold, none otherwise (and
/// none if the in-flight list can't be read, so an outage never
/// double-submits). `batch-only` never returns any.
pub(crate) async fn sweep_with_batches(
    enricher: &Enricher,
    settings: &BatchSettings,
    ids: Vec<String>,
) -> Vec<String> {
    let in_batch = match in_flight_items(&enricher.pool).await {
        Ok(items) => items,
        Err(err) => {
            tracing::error!(error = ?err, "could not read in-flight batches; skipping this sweep");
            return Vec::new();
        }
    };
    let per_text = settings.defers_stream();
    let ids: Vec<String> = if per_text {
        ids
    } else {
        ids.into_iter()
            .filter(|id| !in_batch.has_incident(id))
            .collect()
    };
    // `normal` only: below the threshold before the preflight already (the
    // per-text skip needs the preflight's hash first).
    if !per_text && let SweepSplit::Sync(ids) = split_for_sweep(settings, ids.clone()) {
        return ids;
    }
    let mut prepared = Vec::new();
    for id in &ids {
        // Skip one another loop is extracting right now, as the sync sweep
        // does; the claim is held only for the preflight.
        let Some(_claim) = enricher.in_flight.try_claim(id) else {
            crate::record_in_flight_skip("sweep", id);
            continue;
        };
        if let Preflight::Extract(ready) =
            crate::preflight(enricher, id, crate::latency::Path::Sweep).await
        {
            prepared.push(ready);
        }
    }
    if per_text {
        prepared = not_in_a_batch(prepared, &in_batch);
    }
    let ids = prepared.iter().map(|p| p.incident_id.clone()).collect();
    if let SweepSplit::Sync(ids) = split_for_sweep(settings, ids) {
        return ids;
    }
    for chunk in prepared.chunks(settings.max_items) {
        if let Err(err) = submit_primary(enricher, chunk).await {
            tracing::error!(error = ?err, count = chunk.len(), "could not submit a primary batch; the next sweep retries");
        }
    }
    drop(prepared);
    common::heap::release_freed_memory();
    Vec::new()
}

async fn submit_primary(enricher: &Enricher, prepared: &[Prepared]) -> anyhow::Result<()> {
    let provider = enricher.llm.provider_label();
    let items: Vec<BatchItem> = prepared
        .iter()
        .enumerate()
        .map(|(index, p)| BatchItem::from_prepared(index, p))
        .collect();
    let requests = items
        .iter()
        .map(|item| {
            let spec = llm::primary_spec(
                enricher.llm.prompts(),
                &item.summary,
                &item.description,
                item.reference_date,
            );
            Ok(BatchRequest {
                custom_id: custom_id(LlmCall::Primary, item.index),
                params: enricher.llm.batch_params(spec)?,
            })
        })
        .collect::<Result<Vec<_>, LlmCallError>>()?;
    let meta = BatchMeta {
        stage: Stage::Primary.as_str(),
        model_version: &enricher.model_version,
    };
    let batch = match enricher.llm.create_batch(&requests, meta).await {
        Ok(batch) => batch,
        Err(err) => {
            record_batch_event(provider, Stage::Primary, "submit_failed");
            return Err(err.into());
        }
    };
    if let Err(err) = insert_batch(
        &enricher.pool,
        &batch.id,
        provider,
        Stage::Primary,
        &enricher.model_version,
        &items,
    )
    .await
    {
        // Unrecorded, nobody would read it: cancel it and drop its files
        // (best effort).
        discard_quietly(enricher, &batch).await;
        record_batch_event(provider, Stage::Primary, "submit_failed");
        return Err(err);
    }
    record_batch_event(provider, Stage::Primary, "submitted");
    tracing::info!(
        batch_id = batch.id,
        provider,
        count = items.len(),
        "submitted a primary extraction batch"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The poll loop.
// ---------------------------------------------------------------------------

/// Polls every in-flight batch every `poll_interval`, from startup on (so
/// a restart picks up where the last process left off).
pub(crate) async fn poll_loop(enricher: Arc<Enricher>, settings: BatchSettings) {
    let mut interval = crate::ticking_interval(settings.poll_interval.as_secs());
    loop {
        interval.tick().await;
        poll_once(&enricher, settings.max_items).await;
    }
}

/// One tick of [`poll_loop`]: every row of `enricher_llm_batches`.
pub(crate) async fn poll_once(enricher: &Enricher, max_items: usize) {
    match load_batches(&enricher.pool).await {
        Ok(rows) => {
            let count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
            metrics::gauge!(common::metrics::metric_name(IN_FLIGHT_METRIC)).set(f64::from(count));
            metrics::gauge!(common::metrics::metric_name(OLDEST_AGE_METRIC))
                .set(f64::from(oldest_age_secs(&rows, Utc::now())));
            for row in &rows {
                if let Err(err) = poll_batch(enricher, max_items, row).await {
                    record_batch_event(enricher.llm.provider_label(), row.stage, "poll_failed");
                    tracing::warn!(error = ?err, batch_id = row.batch_id, "polling a batch failed; retrying next tick");
                }
            }
        }
        Err(err) => tracing::error!(error = ?err, "could not read in-flight batches"),
    }
}

/// Cancels a batch whose results nobody will read; a failure is only
/// logged (the batch then runs to its end, billed but unread).
async fn cancel_quietly(enricher: &Enricher, batch_id: &str) {
    if let Err(err) = enricher.llm.cancel_batch(batch_id).await {
        tracing::warn!(error = %err, batch_id, "could not cancel a batch");
    }
}

/// Cancels a just-created batch nobody will read and deletes its files.
async fn discard_quietly(enricher: &Enricher, batch: &CreatedBatch) {
    cancel_quietly(enricher, &batch.id).await;
    enricher.llm.delete_batch_files(&batch.files).await;
}

async fn abandon(enricher: &Enricher, row: &BatchRow, reason: &str) -> anyhow::Result<()> {
    tracing::warn!(
        batch_id = row.batch_id,
        stage = row.stage.as_str(),
        reason,
        "abandoning a batch; its incidents go into a later sweep"
    );
    delete_batch(&enricher.pool, &row.batch_id).await?;
    record_batch_event(enricher.llm.provider_label(), row.stage, "abandoned");
    Ok(())
}

/// An `OpenAI` batch the creator's token had expired for (whole, or
/// `requests` of it). Its incidents are dropped like any failed request's
/// and go into the next sweep's batch, which is created with a freshly
/// minted token; they are not backed off (nothing is wrong with the text),
/// and nothing is resubmitted before that sweep, so it cannot spin.
fn warn_auth_expired(row: &BatchRow, provider: &str, requests: Option<usize>, reason: &str) {
    tracing::error!(
        batch_id = row.batch_id,
        stage = row.stage.as_str(),
        provider,
        requests,
        reason,
        "batch_auth_expired: OpenAI authorised the batch after its creator's token expired; \
         those requests did not run and go into the next sweep"
    );
}

async fn poll_batch(enricher: &Enricher, max_items: usize, row: &BatchRow) -> anyhow::Result<()> {
    let provider = enricher.llm.provider_label();
    if row.provider != provider {
        // This client can't reach it; it runs out on its own.
        return abandon(enricher, row, "submitted to another provider").await;
    }
    if row.model_version != enricher.model_version {
        cancel_quietly(enricher, &row.batch_id).await;
        return abandon(enricher, row, "submitted under another model version").await;
    }
    let ended = match enricher.llm.poll_batch(&row.batch_id).await? {
        BatchPoll::Pending => return Ok(()),
        BatchPoll::Gone => {
            return abandon(enricher, row, "the API no longer knows the batch").await;
        }
        BatchPoll::ResultsGone => {
            return abandon(enricher, row, "its results can no longer be downloaded").await;
        }
        BatchPoll::ValidationFailed { reason, files } => {
            tracing::error!(
                batch_id = row.batch_id,
                stage = row.stage.as_str(),
                provider,
                reason,
                "batch failed input validation; nothing in it ran, its incidents go into a later sweep"
            );
            delete_batch(&enricher.pool, &row.batch_id).await?;
            record_batch_event(provider, row.stage, "validation_failed");
            enricher.llm.delete_batch_files(&files).await;
            return Ok(());
        }
        BatchPoll::AuthExpired { reason, files } => {
            warn_auth_expired(row, provider, None, &reason);
            delete_batch(&enricher.pool, &row.batch_id).await?;
            record_batch_event(provider, row.stage, "auth_expired");
            enricher.llm.delete_batch_files(&files).await;
            return Ok(());
        }
        BatchPoll::Ended(ended) => ended,
    };
    let Some(items) = load_items(&enricher.pool, &row.batch_id).await? else {
        tracing::warn!(
            batch_id = row.batch_id,
            "batch row went away before its results were processed; skipping it"
        );
        return Ok(());
    };
    let auth_expired = ended
        .results
        .outcomes
        .values()
        .filter(|o| **o == BatchOutcome::AuthExpired)
        .count();
    record_batch_event(provider, row.stage, "ended");
    tracing::info!(
        batch_id = row.batch_id,
        stage = row.stage.as_str(),
        provider,
        status = ended.summary,
        "batch ended"
    );
    let finished = match row.stage {
        Stage::Primary => {
            finish_primary_stage(enricher, max_items, row, items, ended.results).await
        }
        Stage::Adversarial => finish_adversarial_stage(enricher, row, items, ended.results).await,
    };
    // A stage's items, results and request bodies are freed by now,
    // whether it succeeded or not.
    common::heap::release_freed_memory();
    finished?;
    // Counted once the row is processed, so a stage retried next tick is
    // not counted twice.
    if auth_expired > 0 {
        warn_auth_expired(
            row,
            provider,
            Some(auth_expired),
            "token_expired result lines",
        );
        record_batch_event(provider, row.stage, "auth_expired");
    }
    // Processed and its row gone: its files have served their purpose.
    enricher.llm.delete_batch_files(&ended.files).await;
    Ok(())
}

/// Submits the adversarial batch(es) for an ended primary batch, then
/// swaps the rows in one transaction. A failed submission leaves the
/// primary row for the next tick (its results stay downloadable: 29 days
/// on Claude, `FILE_TTL_SECS` on `OpenAI`), and nothing is counted until the
/// swap succeeds.
///
/// `items` and `results` are moved through: each chunk's items go into its
/// row as they are (no copy), and only one chunk's requests exist at a time.
async fn finish_primary_stage(
    enricher: &Enricher,
    max_items: usize,
    row: &BatchRow,
    items: Vec<BatchItem>,
    results: BatchResults,
) -> anyhow::Result<()> {
    let provider = enricher.llm.provider_label();
    let of = items.len();
    let mut plan = plan_primary(items, results);
    let next = plan.next.len();
    let meta = BatchMeta {
        stage: Stage::Adversarial.as_str(),
        model_version: &row.model_version,
    };
    let mut submitted = Vec::new();
    for chunk in into_chunks(std::mem::take(&mut plan.next), max_items) {
        let requests = adversarial_requests(enricher, &chunk)?;
        match enricher.llm.create_batch(&requests, meta).await {
            Ok(batch) => submitted.push((batch, chunk)),
            Err(err) => {
                record_batch_event(provider, Stage::Adversarial, "submit_failed");
                for (batch, _) in &submitted {
                    discard_quietly(enricher, batch).await;
                }
                return Err(err.into());
            }
        }
    }
    let mut tx = enricher.pool.begin().await?;
    for (batch, items) in &submitted {
        insert_batch(
            &mut *tx,
            &batch.id,
            provider,
            Stage::Adversarial,
            &row.model_version,
            items,
        )
        .await?;
    }
    delete_batch(&mut *tx, &row.batch_id).await?;
    tx.commit().await?;
    for _ in &submitted {
        record_batch_event(provider, Stage::Adversarial, "submitted");
    }
    apply_records(enricher, &plan);
    tracing::info!(
        batch_id = row.batch_id,
        next,
        of,
        "primary batch done; adversarial batch submitted"
    );
    Ok(())
}

/// `items` in consecutive chunks of at most `size` (at least 1), moved
/// rather than copied.
pub(crate) fn into_chunks(items: Vec<BatchItem>, size: usize) -> Vec<Vec<BatchItem>> {
    let size = size.max(1);
    let mut chunks = Vec::with_capacity(items.len().div_ceil(size));
    let mut items = items.into_iter();
    loop {
        let chunk: Vec<BatchItem> = items.by_ref().take(size).collect();
        if chunk.is_empty() {
            return chunks;
        }
        chunks.push(chunk);
    }
}

/// The two adversarial requests of every item of `chunk`.
fn adversarial_requests(
    enricher: &Enricher,
    chunk: &[BatchItem],
) -> anyhow::Result<Vec<BatchRequest>> {
    let mut requests = Vec::with_capacity(chunk.len() * 2);
    for item in chunk {
        let primary = llm::parse_primary(item.primary_content.as_deref().unwrap_or_default())?;
        for spec in [
            llm::adversarial_spec(
                enricher.llm.prompts(),
                &item.summary,
                &item.description,
                &primary.periods,
            )?,
            llm::severity_adversarial_spec(
                enricher.llm.prompts(),
                &item.summary,
                &item.description,
                &primary.periods,
            )?,
        ] {
            requests.push(BatchRequest {
                custom_id: custom_id(spec.call, item.index),
                params: enricher.llm.batch_params(spec)?,
            });
        }
    }
    Ok(requests)
}

/// How many incidents of an adversarial batch are combined and written at
/// a time: their parsed outputs are all that is held besides the
/// remaining results.
pub(crate) const WRITE_CHUNK: usize = 100;

/// Combines and writes every incident of an ended adversarial batch whose
/// passes all succeeded, [`WRITE_CHUNK`] at a time (each result is dropped
/// once used), then deletes the row. As before, each incident's write
/// stands on its own; the row is deleted, and the requests counted, only
/// once every incident has been through.
async fn finish_adversarial_stage(
    enricher: &Enricher,
    row: &BatchRow,
    items: Vec<BatchItem>,
    mut results: BatchResults,
) -> anyhow::Result<()> {
    let of = items.len();
    let mut done = StagePlan::default();
    let mut written = 0_usize;
    for chunk in into_chunks(items, WRITE_CHUNK) {
        let plan = plan_adversarial(chunk, &mut results);
        for ready in plan.ready {
            let prepared = ready.item.prepared();
            // Another loop extracting it right now wins; its own write is fresh.
            let Some(_claim) = enricher.in_flight.try_claim(&prepared.incident_id) else {
                crate::record_in_flight_skip("batch", &prepared.incident_id);
                continue;
            };
            crate::note_truncation(&prepared.incident_id, &ready.primary);
            if crate::finish_extraction(
                enricher,
                &prepared,
                &ready.primary,
                &ready.resolution,
                &ready.severity,
                crate::latency::Path::Batch,
            )
            .await
            {
                written += 1;
            }
        }
        done.records.extend(plan.records);
        done.text_failures.extend(plan.text_failures);
    }
    drop(results);
    delete_batch(&enricher.pool, &row.batch_id).await?;
    apply_records(enricher, &done);
    tracing::info!(
        batch_id = row.batch_id,
        written,
        of,
        "adversarial batch done; extractions written"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::anthropic::parse_results_jsonl;

    fn item(index: usize, id: &str) -> BatchItem {
        BatchItem {
            index,
            incident_id: id.to_string(),
            text_hash: format!("hash-{id}"),
            summary: "Signal failure".to_string(),
            description: "Lines blocked".to_string(),
            reference_date: "2026-10-01T00:00:00Z".parse().unwrap(),
            text_seen_at: None,
            primary_content: None,
        }
    }

    /// The latency start survives the `items` JSON round trip, and a row
    /// written before the field existed still parses (recording nothing).
    #[test]
    fn text_seen_at_round_trips_and_defaults_for_old_rows() {
        let seen: DateTime<Utc> = "2026-10-01T00:00:00Z".parse().unwrap();
        let with = BatchItem {
            text_seen_at: Some(seen),
            ..item(0, "A")
        };
        let json = serde_json::to_value(&with).unwrap();
        let back: BatchItem = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(back.prepared().text_seen_at, Some(seen));

        let mut old = json;
        old.as_object_mut().unwrap().remove("text_seen_at");
        let back: BatchItem = serde_json::from_value(old).unwrap();
        assert_eq!(back.prepared().text_seen_at, None);
    }

    fn primary_json() -> String {
        serde_json::json!({
            "category": "signal_failure",
            "periods": [{
                "scope_description": null,
                "date_range": null,
                "schedule_window": null,
                "resolution_status": "ongoing",
                "apparent_severity": "severe_disruption",
                "impact_type": null
            }]
        })
        .to_string()
    }

    fn message(text: &str, usage: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "text", "text": text }],
            "stop_reason": "end_turn",
            "usage": usage
        })
    }

    fn succeeded(custom_id: &str, text: &str) -> String {
        serde_json::json!({
            "custom_id": custom_id,
            "result": {
                "type": "succeeded",
                "message": message(text, &serde_json::json!({
                    "input_tokens": 120,
                    "cache_read_input_tokens": 2900,
                    "cache_creation_input_tokens": 0,
                    "output_tokens": 210
                }))
            }
        })
        .to_string()
    }

    #[test]
    fn custom_ids_are_short_valid_and_distinct_per_call() {
        let ids: Vec<String> = LlmCall::ALL.iter().map(|c| custom_id(*c, 4999)).collect();
        assert_eq!(ids, ["p-4999", "r-4999", "s-4999"]);
        let valid = |id: &str| {
            !id.is_empty()
                && id.len() <= 64
                && id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        };
        assert!(ids.iter().all(|id| valid(id)));
    }

    /// Results arrive in any order and are matched by `custom_id`; every
    /// non-success kind drops its item without blaming the text, an
    /// unusable success does blame it, and a missing line is `missing`.
    #[test]
    fn primary_results_are_matched_by_custom_id_and_classified() {
        let jsonl = [
            // Out of order on purpose.
            succeeded("p-2", &primary_json()),
            serde_json::json!({"custom_id": "p-1", "result": {"type": "errored",
                "error": {"type": "error", "error": {"type": "invalid_request_error", "message": "bad"}}}})
            .to_string(),
            succeeded("p-0", "{\"not\": \"the schema\"}"),
            serde_json::json!({"custom_id": "p-3", "result": {"type": "expired"}}).to_string(),
            serde_json::json!({"custom_id": "p-4", "result": {"type": "canceled"}}).to_string(),
            "this line is garbage".to_string(),
        ]
        .join("\n");
        let outcomes = parse_results_jsonl(&jsonl);
        assert_eq!(outcomes.len(), 5);
        let items = (0..6).map(|i| item(i, &format!("I{i}"))).collect();
        let plan = plan_primary(items, outcomes.into());

        assert_eq!(plan.next.len(), 1);
        assert_eq!(plan.next[0].incident_id, "I2");
        assert_eq!(
            plan.next[0].primary_content.as_deref(),
            Some(primary_json().as_str())
        );
        assert_eq!(plan.text_failures.len(), 1);
        assert_eq!(plan.text_failures[0].0, "I0");
        let results: Vec<&str> = plan.records.iter().map(|r| r.result).collect();
        assert_eq!(
            results,
            [
                "invalid",
                "errored",
                "succeeded",
                "expired",
                "canceled",
                "missing"
            ]
        );
        let usage = plan.records[2].usage.clone().unwrap();
        assert_eq!(usage.prompt_tokens, Some(3020));
        assert_eq!(usage.cached_tokens, Some(2900));
        assert_eq!(usage.completion_tokens, Some(210));
    }

    #[test]
    fn adversarial_results_need_both_verdicts() {
        let verdicts = |field: &str, value: &str| {
            serde_json::json!({"periods": [{
                "period_index": 0, "scope_description": null, field: value
            }]})
            .to_string()
        };
        let resolution = verdicts("resolution_status", "ongoing");
        let severity = verdicts("apparent_severity", "moderate_disruption");
        let jsonl = [
            succeeded("s-0", &severity),
            succeeded("r-0", &resolution),
            succeeded("r-1", &resolution),
            serde_json::json!({"custom_id": "s-1", "result": {"type": "expired"}}).to_string(),
            succeeded("r-2", "not json"),
            succeeded("s-2", &severity),
        ]
        .join("\n");
        let items = (0..3)
            .map(|i| BatchItem {
                primary_content: Some(primary_json()),
                ..item(i, &format!("I{i}"))
            })
            .collect();
        let plan = plan_adversarial(items, &mut parse_results_jsonl(&jsonl).into());
        assert_eq!(plan.ready.len(), 1);
        let ready = &plan.ready[0];
        assert_eq!(ready.item.incident_id, "I0");
        assert_eq!(ready.resolution[0].resolution_status, "ongoing");
        assert_eq!(ready.severity[0].apparent_severity, "moderate_disruption");
        assert_eq!(ready.primary.category, "signal_failure");
        // I1: expired severity -> dropped quietly. I2: bad resolution JSON
        // -> a text failure.
        assert_eq!(plan.text_failures.len(), 1);
        assert_eq!(plan.text_failures[0].0, "I2");
        let results: Vec<(&str, &str)> = plan
            .records
            .iter()
            .map(|r| (r.call.label(), r.result))
            .collect();
        assert_eq!(
            results,
            [
                ("resolution_adversarial", "succeeded"),
                ("severity_adversarial", "succeeded"),
                ("resolution_adversarial", "succeeded"),
                ("severity_adversarial", "expired"),
                ("resolution_adversarial", "invalid"),
                ("severity_adversarial", "succeeded"),
            ]
        );
    }

    /// `finish_adversarial_stage` plans [`WRITE_CHUNK`] items at a time
    /// against one shared set of results: the same plan as all at once,
    /// with every used result taken out of the set as it goes.
    #[test]
    fn planning_in_chunks_matches_planning_at_once_and_drains_the_results() {
        let resolution = serde_json::json!({"periods": [{
            "period_index": 0, "scope_description": null, "resolution_status": "ongoing"
        }]})
        .to_string();
        let severity = serde_json::json!({"periods": [{
            "period_index": 0, "scope_description": null,
            "apparent_severity": "moderate_disruption"
        }]})
        .to_string();
        let n = 2 * WRITE_CHUNK + 7;
        let jsonl: Vec<String> = (0..n)
            .flat_map(|i| {
                // Every fifth item's severity is missing: dropped quietly.
                let severity = (i % 5 != 0).then(|| succeeded(&format!("s-{i}"), &severity));
                [Some(succeeded(&format!("r-{i}"), &resolution)), severity]
            })
            .flatten()
            .collect();
        let jsonl = jsonl.join("\n");
        let items = || -> Vec<BatchItem> {
            (0..n)
                .map(|i| BatchItem {
                    primary_content: Some(primary_json()),
                    ..item(i, &format!("I{i}"))
                })
                .collect()
        };
        let summary = |plan: &StagePlan| {
            (
                plan.ready
                    .iter()
                    .map(|r| r.item.incident_id.clone())
                    .collect::<Vec<_>>(),
                plan.records
                    .iter()
                    .map(|r| (r.call, r.result))
                    .collect::<Vec<_>>(),
            )
        };
        let whole = plan_adversarial(items(), &mut parse_results_jsonl(&jsonl).into());

        let mut results: BatchResults = parse_results_jsonl(&jsonl).into();
        let chunks = into_chunks(items(), WRITE_CHUNK);
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            [WRITE_CHUNK, WRITE_CHUNK, 7]
        );
        let mut chunked = StagePlan::default();
        for chunk in chunks {
            let plan = plan_adversarial(chunk, &mut results);
            chunked.ready.extend(plan.ready);
            chunked.records.extend(plan.records);
        }
        assert_eq!(summary(&chunked), summary(&whole));
        assert_eq!(whole.ready.len(), n - n.div_ceil(5));
        assert!(results.outcomes.is_empty(), "{:?}", results.outcomes.keys());
        assert!(into_chunks(Vec::new(), 0).is_empty());
        assert_eq!(into_chunks(items(), 0).len(), n, "a size of 0 counts as 1");
    }

    #[test]
    fn a_refused_or_truncated_batch_result_is_invalid() {
        let refused = serde_json::json!({"custom_id": "p-0", "result": {"type": "succeeded",
            "message": {"content": [], "stop_reason": "refusal",
                        "stop_details": {"type": "refusal", "category": "cyber", "explanation": "no"},
                        "usage": {"input_tokens": 10, "output_tokens": 0}}}})
        .to_string();
        let truncated = serde_json::json!({"custom_id": "p-1", "result": {"type": "succeeded",
            "message": {"content": [{"type": "text", "text": "{\"category\": \"sig"}],
                        "stop_reason": "max_tokens", "usage": {"input_tokens": 10, "output_tokens": 16000}}}})
        .to_string();
        let plan = plan_primary(
            vec![item(0, "A"), item(1, "B")],
            parse_results_jsonl(&format!("{refused}\n{truncated}")).into(),
        );
        assert!(plan.next.is_empty());
        assert_eq!(plan.text_failures.len(), 2);
        assert!(
            plan.text_failures[0].2.contains("refused"),
            "{:?}",
            plan.text_failures
        );
        assert!(
            plan.text_failures[1].2.contains("max_tokens"),
            "{:?}",
            plan.text_failures
        );
        assert!(plan.records.iter().all(|r| r.result == "invalid"));
        // Both were billed: their usage is still counted.
        assert_eq!(
            plan.records[1].usage.as_ref().unwrap().completion_tokens,
            Some(16000)
        );
    }

    #[test]
    fn oldest_age_is_the_oldest_rows_and_zero_without_rows() {
        let now: DateTime<Utc> = "2026-10-10T12:00:00Z".parse().unwrap();
        let row = |submitted: &str| BatchRow {
            batch_id: "b".into(),
            provider: "anthropic".into(),
            stage: Stage::Primary,
            model_version: "m".into(),
            submitted_at: submitted.parse().unwrap(),
        };
        assert_eq!(oldest_age_secs(&[], now), 0);
        assert_eq!(
            oldest_age_secs(
                &[row("2026-10-10T11:00:00Z"), row("2026-10-09T10:00:00Z")],
                now
            ),
            26 * 3600
        );
        // A row "from the future" (clock skew) is not negative.
        assert_eq!(oldest_age_secs(&[row("2026-10-10T12:05:00Z")], now), 0);
    }

    #[test]
    fn items_round_trip_through_the_stored_json() {
        let mut stored = item(7, "X");
        stored.primary_content = Some(primary_json());
        let value = serde_json::to_value(vec![stored.clone(), item(8, "Y")]).unwrap();
        assert!(value[1].get("primary_content").is_none());
        let back: Vec<BatchItem> = serde_json::from_value(value).unwrap();
        assert_eq!(back[0], stored);
        assert_eq!(back[1].primary_content, None);
        assert_eq!(
            Stage::parse(Stage::Adversarial.as_str()),
            Some(Stage::Adversarial)
        );
        assert_eq!(Stage::parse("other"), None);
    }

    fn prepared(id: &str, hash: &str) -> Prepared {
        Prepared {
            incident_id: id.to_string(),
            text_hash: hash.to_string(),
            summary: "s".to_string(),
            description: "d".to_string(),
            reference_date: "2026-10-01T00:00:00Z".parse().unwrap(),
            churn_baseline: None,
            edit_class: None,
            text_seen_at: None,
        }
    }

    /// A stale incident is kept; one whose current text is already in a
    /// batch is not; one whose text changed since its batch went in is
    /// extracted again.
    #[test]
    fn the_per_text_skip_keeps_every_stale_text_not_already_in_a_batch() {
        let none = InFlightItems::default();
        let single = not_in_a_batch(vec![prepared("A", "h1")], &none);
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].incident_id, "A");

        let in_batch = InFlightItems::new([
            ("A".to_string(), "h1".to_string()),
            ("B".to_string(), "old".to_string()),
        ]);
        let ids: Vec<String> = not_in_a_batch(
            vec![
                prepared("A", "h1"),
                prepared("B", "new"),
                prepared("C", "h3"),
            ],
            &in_batch,
        )
        .into_iter()
        .map(|p| p.incident_id)
        .collect();
        assert_eq!(ids, ["B", "C"]);
        // `batch` mode's coarser check: B is in a batch whatever its text.
        assert!(in_batch.has_incident("B"));
        assert!(!in_batch.has_text("B", "new"));
    }

    fn settings(mode: LlmMode) -> BatchSettings {
        BatchSettings {
            min_items: if mode == LlmMode::BatchOnly { 1 } else { 20 },
            max_items: 2000,
            poll_interval: Duration::from_secs(60),
            mode,
        }
    }

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("I{i}")).collect()
    }

    /// `batch`: a small set runs synchronously within the period, 20 or
    /// more are batched. `batch-only`: a single incident is a batch.
    /// `normal` (`LLM_SWEEP_MODE=batch`): as `batch`.
    #[test]
    fn the_sweep_threshold_per_mode() {
        for mode in [LlmMode::Normal, LlmMode::Batch] {
            let s = settings(mode);
            assert_eq!(split_for_sweep(&s, ids(3)), SweepSplit::Sync(ids(3)));
            assert_eq!(split_for_sweep(&s, ids(19)), SweepSplit::Sync(ids(19)));
            assert_eq!(split_for_sweep(&s, ids(20)), SweepSplit::Batch(ids(20)));
            assert_eq!(split_for_sweep(&s, ids(250)), SweepSplit::Batch(ids(250)));
        }
        let only = settings(LlmMode::BatchOnly);
        assert_eq!(split_for_sweep(&only, ids(1)), SweepSplit::Batch(ids(1)));
        assert_eq!(split_for_sweep(&only, ids(0)), SweepSplit::Batch(ids(0)));
        assert!(only.defers_stream() && only.batch_only());
        assert!(settings(LlmMode::Batch).defers_stream());
        assert!(!settings(LlmMode::Normal).defers_stream());
    }

    /// Restart resume against a real database: a primary `OpenAI` batch row
    /// left by a previous process is polled by a fresh `Enricher` that has
    /// nothing but the row. Its results are read, the adversarial batch is
    /// submitted and swapped in, and the primary batch's files are deleted.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher openai_batch_row_resumes -- --ignored --test-threads=1`"]
    #[expect(
        clippy::too_many_lines,
        reason = "test code: one scenario, mocks then assertions"
    )]
    async fn an_openai_batch_row_resumes_after_a_restart() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let pool = crate::test_db::pool().await;
        sqlx::query("DELETE FROM enricher_llm_batches")
            .execute(&pool)
            .await
            .unwrap();
        insert_batch(
            &pool,
            "batch_resume",
            "openai",
            Stage::Primary,
            "mv-test",
            &[item(0, "TEST-OPENAI-BATCH-RESUME")],
        )
        .await
        .unwrap();

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/batches/batch_resume"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "batch_resume", "status": "completed", "input_file_id": "file-in",
                "output_file_id": "file-out", "error_file_id": null,
                "request_counts": { "total": 1, "completed": 1, "failed": 0 }
            })))
            .mount(&server)
            .await;
        let line = serde_json::json!({
            "custom_id": "p-0", "error": null,
            "response": { "status_code": 200, "body": {
                "choices": [{ "message": { "content": primary_json() }, "finish_reason": "stop" }],
                "usage": { "prompt_tokens": 10, "completion_tokens": 5 } } }
        });
        Mock::given(method("GET"))
            .and(path("/files/file-out/content"))
            .respond_with(ResponseTemplate::new(200).set_body_string(line.to_string()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/files"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "id": "file-adv" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/batches"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "batch_adv", "status": "validating", "input_file_id": "file-adv"
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let enricher = Enricher {
            pool: pool.clone(),
            llm: llm::LlmClient::new(
                server.uri(),
                Some("sk-test".into()),
                "m".into(),
                Duration::from_secs(30),
            ),
            model_version: "mv-test".into(),
            mismatch_tracker: crate::MismatchTracker::default(),
            retry_backoff: crate::RetryBackoff::default(),
            in_flight: crate::InFlight::default(),
            carry_forward_noops: false,
            sweep_concurrency: 1,
            batch: None,
        };
        poll_once(&enricher, 100).await;

        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT batch_id, provider, stage FROM enricher_llm_batches ORDER BY batch_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows,
            [(
                "batch_adv".to_string(),
                "openai".to_string(),
                "adversarial".to_string()
            )]
        );
        let requests = server.received_requests().await.unwrap();
        let deleted: Vec<&str> = requests
            .iter()
            .filter(|r| r.method.as_str() == "DELETE")
            .map(|r| r.url.path())
            .collect();
        assert_eq!(deleted, ["/files/file-in", "/files/file-out"]);
        let upload = requests
            .iter()
            .find(|r| r.method.as_str() == "POST" && r.url.path() == "/files")
            .unwrap();
        let form = String::from_utf8_lossy(&upload.body);
        assert!(form.contains("\"custom_id\":\"r-0\""), "{form}");
        assert!(form.contains("\"custom_id\":\"s-0\""), "{form}");

        sqlx::query("DELETE FROM enricher_llm_batches")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// `LLM_ADVERSARIAL_MODEL` in the service's batch path, against a real
    /// database: the primary batch is the primary model's requests, the
    /// adversarial batch (submitted by the poll loop from the stored
    /// primary output) the adversarial model's, and both rows carry the
    /// split `model_version`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher split_model_batches -- --ignored --test-threads=1`"]
    #[expect(
        clippy::too_many_lines,
        reason = "test code: one scenario, mocks then assertions"
    )]
    async fn split_model_batches_send_each_stage_to_its_own_model() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, Request, ResponseTemplate};

        let pool = crate::test_db::pool().await;
        sqlx::query("DELETE FROM enricher_llm_batches")
            .execute(&pool)
            .await
            .unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/messages/batches"))
            .respond_with(|request: &Request| {
                let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                let primary = body["requests"][0]["custom_id"]
                    .as_str()
                    .unwrap()
                    .starts_with("p-");
                let id = if primary {
                    "msgbatch_split_p"
                } else {
                    "msgbatch_split_a"
                };
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": id, "processing_status": "in_progress",
                    "request_counts": { "processing": 1 }
                }))
            })
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/messages/batches/msgbatch_split_p"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msgbatch_split_p", "processing_status": "ended",
                "results_url": format!("{}/results/msgbatch_split_p", server.uri()),
                "request_counts": { "succeeded": 1 }
            })))
            .mount(&server)
            .await;
        let line = serde_json::json!({ "custom_id": "p-0", "result": { "type": "succeeded",
            "message": { "content": [{ "type": "text", "text": primary_json() }],
                         "stop_reason": "end_turn",
                         "usage": { "input_tokens": 10, "output_tokens": 5 } } } });
        Mock::given(method("GET"))
            .and(path("/results/msgbatch_split_p"))
            .respond_with(ResponseTemplate::new(200).set_body_string(line.to_string()))
            .mount(&server)
            .await;

        let adversarial = crate::profile::resolve_adversarial(
            llm::ProviderKind::Anthropic,
            "claude-sonnet-5-5",
            Some("claude-haiku-5-5"),
            None,
        )
        .unwrap();
        let llm = llm::LlmClient::new(
            server.uri(),
            Some("sk-ant-test".into()),
            "claude-sonnet-5-5".into(),
            Duration::from_secs(30),
        )
        .with_anthropic(llm::anthropic::AnthropicSettings::default())
        .with_adversarial(adversarial.as_ref());
        let model_version = crate::profile::model_version(
            "claude-sonnet-5-5",
            llm.adversarial_model(),
            llm.prompts(),
        );
        assert_eq!(
            model_version,
            "claude-sonnet-5-5@periods-v2+adversarial-claude-haiku-5-5"
        );
        let enricher = Enricher {
            pool: pool.clone(),
            llm,
            model_version: model_version.clone(),
            mismatch_tracker: crate::MismatchTracker::default(),
            retry_backoff: crate::RetryBackoff::default(),
            in_flight: crate::InFlight::default(),
            carry_forward_noops: false,
            sweep_concurrency: 1,
            batch: None,
        };
        submit_primary(&enricher, &[item(0, "TEST-SPLIT-BATCH").prepared()])
            .await
            .unwrap();
        poll_once(&enricher, 100).await;

        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT batch_id, stage, model_version FROM enricher_llm_batches ORDER BY batch_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows,
            [(
                "msgbatch_split_a".to_string(),
                "adversarial".to_string(),
                model_version
            )]
        );
        let creates: Vec<serde_json::Value> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method.as_str() == "POST" && r.url.path() == "/messages/batches")
            .map(|r| serde_json::from_slice(&r.body).unwrap())
            .collect();
        let models = |body: &serde_json::Value| {
            body["requests"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["custom_id"].as_str().unwrap().to_string(),
                        r["params"]["model"].as_str().unwrap().to_string(),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            models(&creates[0]),
            [("p-0".to_string(), "claude-sonnet-5-5".to_string())]
        );
        assert_eq!(
            models(&creates[1]),
            [
                ("r-0".to_string(), "claude-haiku-5-5".to_string()),
                ("s-0".to_string(), "claude-haiku-5-5".to_string())
            ]
        );

        sqlx::query("DELETE FROM enricher_llm_batches")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// `token_expired` against a real database: a batch that `failed` with
    /// it and one whose every request did are both dropped (their
    /// incidents go to the next sweep), nothing is resubmitted from the
    /// poll loop (no spin), and the texts are not backed off.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher auth_expired_batches -- --ignored --test-threads=1`"]
    #[expect(
        clippy::too_many_lines,
        reason = "test code: one scenario, mocks then assertions"
    )]
    async fn auth_expired_batches_are_dropped_without_backoff_or_resubmission() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let pool = crate::test_db::pool().await;
        sqlx::query("DELETE FROM enricher_llm_batches")
            .execute(&pool)
            .await
            .unwrap();
        for (batch_id, incident) in [
            ("batch_tok_failed", "TEST-AUTH-EXPIRED-FAILED"),
            ("batch_tok_lines", "TEST-AUTH-EXPIRED-LINES"),
        ] {
            insert_batch(
                &pool,
                batch_id,
                "openai",
                Stage::Primary,
                "mv-test",
                &[item(0, incident)],
            )
            .await
            .unwrap();
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/batches/batch_tok_failed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "batch_tok_failed", "status": "failed", "input_file_id": "file-in-1",
                "errors": { "data": [{ "code": "token_expired",
                                       "message": "Provided authentication token is expired." }] }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/batches/batch_tok_lines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "batch_tok_lines", "status": "completed", "input_file_id": "file-in-2",
                "output_file_id": null, "error_file_id": "file-err-2",
                "request_counts": { "total": 1, "completed": 0, "failed": 1 }
            })))
            .mount(&server)
            .await;
        let line = serde_json::json!({
            "custom_id": "p-0", "response": null,
            "error": { "code": "token_expired",
                       "message": "Provided authentication token is expired." }
        });
        Mock::given(method("GET"))
            .and(path("/files/file-err-2/content"))
            .respond_with(ResponseTemplate::new(200).set_body_string(line.to_string()))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let enricher = Enricher {
            pool: pool.clone(),
            llm: llm::LlmClient::new(
                server.uri(),
                Some("sk-test".into()),
                "m".into(),
                Duration::from_secs(30),
            ),
            model_version: "mv-test".into(),
            mismatch_tracker: crate::MismatchTracker::default(),
            retry_backoff: crate::RetryBackoff::default(),
            in_flight: crate::InFlight::default(),
            carry_forward_noops: false,
            sweep_concurrency: 1,
            batch: None,
        };
        poll_once(&enricher, 100).await;
        // A second tick finds nothing to do.
        poll_once(&enricher, 100).await;

        let rows: Vec<(String,)> = sqlx::query_as("SELECT batch_id FROM enricher_llm_batches")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert!(rows.is_empty(), "{rows:?}");
        let requests = server.received_requests().await.unwrap();
        assert!(
            !requests.iter().any(|r| r.method.as_str() == "POST"),
            "nothing resubmitted from the poll loop"
        );
        let mut deleted: Vec<&str> = requests
            .iter()
            .filter(|r| r.method.as_str() == "DELETE")
            .map(|r| r.url.path())
            .collect();
        deleted.sort_unstable();
        assert_eq!(
            deleted,
            ["/files/file-err-2", "/files/file-in-1", "/files/file-in-2"]
        );
        // Not backed off: this is each text's first recorded failure.
        for incident in ["TEST-AUTH-EXPIRED-FAILED", "TEST-AUTH-EXPIRED-LINES"] {
            assert_eq!(
                enricher
                    .retry_backoff
                    .record_failure(incident, &format!("hash-{incident}")),
                1
            );
        }
    }
}
