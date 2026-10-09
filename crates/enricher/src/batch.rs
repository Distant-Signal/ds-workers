//! Batch mode (`LLM_SWEEP_MODE=batch`, Claude only): the reconciliation
//! sweep sends its extractions through the Claude Message Batches API, at
//! half the synchronous price, instead of one incident at a time.
//! docs/enricher-anthropic.md, "Batch mode".
//!
//! **Where it applies.** Only the sweep. It is the bulk path: after a model
//! or prompt change (`model_version`) it re-extracts every live incident,
//! and nothing waits on it. The stream loop (a text change, seconds after
//! it lands) and reclaim (its retries) stay synchronous: a batch can take
//! up to 24 hours. A sweep that finds fewer than `LLM_BATCH_MIN_ITEMS`
//! incidents also runs synchronously.
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
//! **Results.** Matched by `custom_id` (`p-<n>`, `r-<n>`, `s-<n>`, `n` the
//! item's index), never by position. `errored`, `canceled`, `expired` and
//! missing entries drop that incident from the batch; the next sweep finds
//! it again (its stored hash still doesn't match). An unusable success (a
//! refusal, a truncated or unparseable output) also feeds the per-text
//! backoff, exactly as on the synchronous path. A batch whose
//! `model_version` is no longer the service's is canceled and dropped.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::llm::anthropic::{BatchOutcome, BatchRequest, completion_from_message};
use crate::llm::{self, LlmCall, LlmCallError, TokenUsage};
use crate::{Enricher, Preflight, Prepared};

/// Ceiling on `LLM_BATCH_MAX_ITEMS`. The API takes up to 100,000 requests
/// or 256 MB per batch; each request carries a system prompt of up to
/// ~11 KB, and the adversarial batch has two per incident, so 5,000
/// incidents stay well under the size limit (~130 MB).
pub(crate) const MAX_BATCH_INCIDENTS: usize = 5_000;

/// `enricher_llm_batches_total{stage, event}`: `submitted`,
/// `submit_failed`, `ended`, `poll_failed`, `abandoned`.
const BATCHES_METRIC: &str = "enricher_llm_batches_total";
/// `enricher_llm_batch_requests_total{call, result}`: one per request of an
/// ended batch -- `succeeded`, `invalid` (a success the enricher could not
/// use: a refusal, truncated or unparseable output), `errored`,
/// `canceled`, `expired`, `missing` (no result line).
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

fn record_batch_event(stage: Stage, event: &'static str) {
    metrics::counter!(
        common::metrics::metric_name(BATCHES_METRIC),
        "stage" => stage.as_str(),
        "event" => event
    )
    .increment(1);
}

/// Registers the batch metrics' series at 0 (batch mode only).
pub(crate) fn register_metrics() {
    llm::register_token_series(llm::BATCH_TOKENS_METRIC);
    for stage in [Stage::Primary, Stage::Adversarial] {
        for event in [
            "submitted",
            "submit_failed",
            "ended",
            "poll_failed",
            "abandoned",
        ] {
            metrics::counter!(
                common::metrics::metric_name(BATCHES_METRIC),
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
            "missing",
        ] {
            metrics::counter!(
                common::metrics::metric_name(REQUESTS_METRIC),
                "call" => call.label(),
                "result" => result
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
    outcomes: &mut HashMap<String, BatchOutcome>,
    call: LlmCall,
    index: usize,
    records: &mut Vec<RequestRecord>,
) -> Result<String, Option<String>> {
    let Some(outcome) = outcomes.remove(&custom_id(call, index)) else {
        records.push(RequestRecord {
            call,
            result: "missing",
            usage: None,
        });
        return Err(None);
    };
    let BatchOutcome::Succeeded(message) = &outcome else {
        if let BatchOutcome::Errored { kind, message } = &outcome {
            tracing::warn!(
                call = call.label(),
                error_type = kind.as_deref(),
                error = message.as_deref(),
                "Message Batch request errored"
            );
        }
        records.push(RequestRecord {
            call,
            result: outcome.label(),
            usage: None,
        });
        return Err(None);
    };
    let (usage, content) = completion_from_message(message);
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
            Err(Some(err.to_string()))
        }
    }
}

/// Plans an ended primary batch: parse each item's output; the parsed ones
/// go on to the adversarial stage.
pub(crate) fn plan_primary(
    items: Vec<BatchItem>,
    mut outcomes: HashMap<String, BatchOutcome>,
) -> StagePlan {
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
                    .push((item.incident_id, item.text_hash, reason))
            }
            Err(None) => {}
        }
    }
    plan
}

/// Plans an ended adversarial batch: the items whose two verdict lists
/// both parsed are ready to combine and write.
pub(crate) fn plan_adversarial(
    items: Vec<BatchItem>,
    mut outcomes: HashMap<String, BatchOutcome>,
) -> StagePlan {
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
            &mut outcomes,
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
            &mut outcomes,
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
    pub stage: Stage,
    pub model_version: String,
    pub items: Vec<BatchItem>,
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

type BatchRecord = (String, String, String, serde_json::Value, DateTime<Utc>);

async fn load_batches(pool: &PgPool) -> anyhow::Result<Vec<BatchRow>> {
    let rows: Vec<BatchRecord> = sqlx::query_as(
        "SELECT batch_id, stage, model_version, items, submitted_at FROM enricher_llm_batches \
         ORDER BY submitted_at",
    )
    .fetch_all(pool)
    .await?;
    let mut batches = Vec::with_capacity(rows.len());
    for (batch_id, stage, model_version, items, submitted_at) in rows {
        let Some(stage) = Stage::parse(&stage) else {
            tracing::error!(batch_id, stage, "unknown batch stage; skipping the row");
            continue;
        };
        match serde_json::from_value(items) {
            Ok(items) => batches.push(BatchRow {
                batch_id,
                stage,
                model_version,
                items,
                submitted_at,
            }),
            Err(err) => {
                tracing::error!(batch_id, error = %err, "unreadable batch items; skipping the row");
            }
        }
    }
    Ok(batches)
}

/// Every incident in an in-flight batch: the sweep skips them.
async fn in_flight_incident_ids(pool: &PgPool) -> anyhow::Result<HashSet<String>> {
    let ids: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT item->>'incident_id' FROM enricher_llm_batches, \
         jsonb_array_elements(items) AS item",
    )
    .fetch_all(pool)
    .await?;
    Ok(ids.into_iter().map(|(id,)| id).collect())
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
    .bind(serde_json::to_value(items)?)
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

/// The sweep's batch step. Drops every id already in a batch; with at
/// least `min_items` left, runs each through `preflight` and submits the
/// ones that need the LLM as primary batches of up to `max_items`. Returns
/// the ids left for the synchronous path: all of them below the threshold,
/// none otherwise (and none if the in-flight list can't be read, so an
/// outage never double-submits).
pub(crate) async fn sweep_with_batches(
    enricher: &Enricher,
    settings: &BatchSettings,
    ids: Vec<String>,
) -> Vec<String> {
    let in_batch = match in_flight_incident_ids(&enricher.pool).await {
        Ok(ids) => ids,
        Err(err) => {
            tracing::error!(error = ?err, "could not read in-flight batches; skipping this sweep");
            return Vec::new();
        }
    };
    let ids: Vec<String> = ids
        .into_iter()
        .filter(|id| !in_batch.contains(id))
        .collect();
    if ids.len() < settings.min_items {
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
        if let Preflight::Extract(ready) = crate::preflight(enricher, id).await {
            prepared.push(ready);
        }
    }
    if prepared.len() < settings.min_items {
        return prepared.into_iter().map(|p| p.incident_id).collect();
    }
    for chunk in prepared.chunks(settings.max_items) {
        if let Err(err) = submit_primary(enricher, chunk).await {
            tracing::error!(error = ?err, count = chunk.len(), "could not submit a primary batch; the next sweep retries");
        }
    }
    Vec::new()
}

async fn submit_primary(enricher: &Enricher, prepared: &[Prepared]) -> anyhow::Result<()> {
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
                params: enricher.llm.batch_params(&spec)?,
            })
        })
        .collect::<Result<Vec<_>, LlmCallError>>()?;
    let batch = match enricher.llm.create_message_batch(&requests).await {
        Ok(batch) => batch,
        Err(err) => {
            record_batch_event(Stage::Primary, "submit_failed");
            return Err(err.into());
        }
    };
    if let Err(err) = insert_batch(
        &enricher.pool,
        &batch.id,
        "anthropic",
        Stage::Primary,
        &enricher.model_version,
        &items,
    )
    .await
    {
        // Unrecorded, nobody would read it: cancel it (best effort).
        cancel_quietly(enricher, &batch.id).await;
        record_batch_event(Stage::Primary, "submit_failed");
        return Err(err);
    }
    record_batch_event(Stage::Primary, "submitted");
    tracing::info!(
        batch_id = batch.id,
        count = items.len(),
        "submitted a primary extraction Message Batch"
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
        match load_batches(&enricher.pool).await {
            Ok(rows) => {
                let count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
                metrics::gauge!(common::metrics::metric_name(IN_FLIGHT_METRIC))
                    .set(f64::from(count));
                metrics::gauge!(common::metrics::metric_name(OLDEST_AGE_METRIC))
                    .set(f64::from(oldest_age_secs(&rows, Utc::now())));
                for row in &rows {
                    if let Err(err) = poll_batch(&enricher, settings.max_items, row).await {
                        record_batch_event(row.stage, "poll_failed");
                        tracing::warn!(error = ?err, batch_id = row.batch_id, "polling a Message Batch failed; retrying next tick");
                    }
                }
            }
            Err(err) => tracing::error!(error = ?err, "could not read in-flight batches"),
        }
    }
}

/// Cancels a batch whose results nobody will read; a failure is only
/// logged (the batch then runs to its end, billed but unread).
async fn cancel_quietly(enricher: &Enricher, batch_id: &str) {
    if let Err(err) = enricher.llm.cancel_message_batch(batch_id).await {
        tracing::warn!(error = %err, batch_id, "could not cancel a Message Batch");
    }
}

async fn abandon(enricher: &Enricher, row: &BatchRow, reason: &str) -> anyhow::Result<()> {
    tracing::warn!(
        batch_id = row.batch_id,
        stage = row.stage.as_str(),
        reason,
        "abandoning a Message Batch; its incidents go into a later sweep"
    );
    delete_batch(&enricher.pool, &row.batch_id).await?;
    record_batch_event(row.stage, "abandoned");
    Ok(())
}

async fn poll_batch(enricher: &Enricher, max_items: usize, row: &BatchRow) -> anyhow::Result<()> {
    if row.model_version != enricher.model_version {
        cancel_quietly(enricher, &row.batch_id).await;
        return abandon(enricher, row, "submitted under another model version").await;
    }
    let batch = match enricher.llm.get_message_batch(&row.batch_id).await {
        Ok(batch) => batch,
        Err(LlmCallError::Status { status: 404 }) => {
            return abandon(enricher, row, "the API no longer knows the batch").await;
        }
        Err(err) => return Err(err.into()),
    };
    if !batch.has_ended() {
        return Ok(());
    }
    let outcomes = match batch.results_url.as_deref() {
        Some(url) => match enricher.llm.message_batch_results(url).await {
            Ok(outcomes) => outcomes,
            Err(LlmCallError::Status { status: 404 }) => {
                return abandon(enricher, row, "its results are gone (over 29 days old)").await;
            }
            Err(err) => return Err(err.into()),
        },
        // Ended with no results file (canceled before anything ran).
        None => HashMap::new(),
    };
    record_batch_event(row.stage, "ended");
    tracing::info!(
        batch_id = row.batch_id,
        stage = row.stage.as_str(),
        counts = ?batch.request_counts,
        "Message Batch ended"
    );
    match row.stage {
        Stage::Primary => finish_primary_stage(enricher, max_items, row, outcomes).await,
        Stage::Adversarial => finish_adversarial_stage(enricher, row, outcomes).await,
    }
}

/// Submits the adversarial batch(es) for an ended primary batch, then
/// swaps the rows in one transaction. A failed submission leaves the
/// primary row for the next tick (its results stay downloadable for 29
/// days), and nothing is counted until the swap succeeds.
async fn finish_primary_stage(
    enricher: &Enricher,
    max_items: usize,
    row: &BatchRow,
    outcomes: HashMap<String, BatchOutcome>,
) -> anyhow::Result<()> {
    let plan = plan_primary(row.items.clone(), outcomes);
    let mut submitted = Vec::new();
    for chunk in plan.next.chunks(max_items) {
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
                    params: enricher.llm.batch_params(&spec)?,
                });
            }
        }
        match enricher.llm.create_message_batch(&requests).await {
            Ok(batch) => submitted.push((batch.id, chunk.to_vec())),
            Err(err) => {
                record_batch_event(Stage::Adversarial, "submit_failed");
                for (batch_id, _) in &submitted {
                    cancel_quietly(enricher, batch_id).await;
                }
                return Err(err.into());
            }
        }
    }
    let mut tx = enricher.pool.begin().await?;
    for (batch_id, items) in &submitted {
        insert_batch(
            &mut *tx,
            batch_id,
            "anthropic",
            Stage::Adversarial,
            &row.model_version,
            items,
        )
        .await?;
    }
    delete_batch(&mut *tx, &row.batch_id).await?;
    tx.commit().await?;
    for _ in &submitted {
        record_batch_event(Stage::Adversarial, "submitted");
    }
    apply_records(enricher, &plan);
    tracing::info!(
        batch_id = row.batch_id,
        next = plan.next.len(),
        of = row.items.len(),
        "primary batch done; adversarial batch submitted"
    );
    Ok(())
}

/// Combines and writes every incident of an ended adversarial batch whose
/// passes all succeeded, then deletes the row.
async fn finish_adversarial_stage(
    enricher: &Enricher,
    row: &BatchRow,
    outcomes: HashMap<String, BatchOutcome>,
) -> anyhow::Result<()> {
    let plan = plan_adversarial(row.items.clone(), outcomes);
    let mut written = 0_usize;
    for ready in &plan.ready {
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
        )
        .await
        {
            written += 1;
        }
    }
    delete_batch(&enricher.pool, &row.batch_id).await?;
    apply_records(enricher, &plan);
    tracing::info!(
        batch_id = row.batch_id,
        written,
        of = row.items.len(),
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
            primary_content: None,
        }
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

    fn message(text: &str, usage: serde_json::Value) -> serde_json::Value {
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
                "message": message(text, serde_json::json!({
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
        let plan = plan_primary(items, outcomes);

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
        let plan = plan_adversarial(items, parse_results_jsonl(&jsonl));
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
            parse_results_jsonl(&format!("{refused}\n{truncated}")),
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
            stage: Stage::Primary,
            model_version: "m".into(),
            items: Vec::new(),
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
}
