//! Batch mode, the eval's default (`mode = "batch"`, `EVAL_MODE`): a run's
//! whole record set goes through the provider's batch API (Claude Message
//! Batches or the `OpenAI` Batch API, `llm::batch_api`) instead of one
//! synchronous call per pass. Half the price, the batch path's own quality
//! and latency, and large runs without rate limits.
//!
//! The flow is the service's (`crate::batch`): every primary request goes
//! into a *primary* stage; once it has ended, the records whose primary
//! output parsed get their two adversarial requests in an *adversarial*
//! stage; results are matched by `custom_id` (`p-<n>`, `r-<n>`, `s-<n>`,
//! `crate::batch::custom_id`, `n` the job's index), never by position.
//! Every request body comes from `LlmClient::batch_params`, the synchronous
//! path's own builder (profile, sampling, schema, prompts, prompt-cache
//! settings). Each stage is split into as many batches as the provider's
//! limits need ([`ChunkLimits`]).
//!
//! The output is the sync path's: one [`PipelineRecord`] per (case,
//! repetition), scored by the same `PipelineRecord::outcome` (the service's
//! parsers and `combine_periods`). A batch call's `elapsed_ms` is its
//! batch's turnaround, and it carries its `batch_id`. A request that
//! errored, expired, was canceled or has no result line is recorded as a
//! failed call (`batch_errored`, `batch_expired`, `batch_canceled`,
//! `batch_auth_expired`, `batch_missing`; a transport failure in the
//! scores), never dropped. `batch_auth_expired` is `OpenAI`'s
//! `token_expired`: the token that created the batch had expired by the
//! time `OpenAI` authorised it (keyless auth's tokens last ~900 s); the
//! report calls it out, and sync mode avoids it.
//! Unlike the sync pipeline, both adversarial passes are always sent
//! together (the service's batch path does the same); the scoring checks
//! the resolution pass first either way, so the outcome is the same.
//!
//! **Resume.** The run's state (batch ids, submit and end times, and the
//! primary stage's calls once it has ended) is a JSON file next to the
//! run's output, `<target>.batch-state.json` ([`BatchState`]), rewritten
//! after every submission and every ended batch. An interrupted run
//! re-run on the same directory (`EVAL_RESUME=<run dir>`) polls the
//! recorded batches instead of submitting again, and submits only what no
//! recorded batch holds. A crash between a batch's creation and the state
//! write orphans that batch (it runs and is billed, nobody reads it), as in
//! the service.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::eval::cost::UsageTotals;
use crate::eval::dataset::Case;
use crate::eval::pipeline::{self, Backend, CallRecord, Pass, PipelineRecord, TargetLabel};
use crate::eval::report::{self, secs};
use crate::llm::batch_api::{BatchMeta, BatchOutcome, BatchPoll, BatchRequest, BatchResults};
use crate::llm::{self, LlmCall, LlmCallError, LlmClient};

/// [`BatchState::version`].
pub(crate) const STATE_VERSION: u32 = 1;

/// Bytes reserved for a batch's envelope around its requests (the
/// Message Batches body's `{"requests": [...]}`).
const ENVELOPE_BYTES: usize = 64;

/// Run options (`EVAL_BATCH_*`).
#[derive(Debug, Clone)]
pub(crate) struct BatchOptions {
    /// Between polls of the in-flight batches (`EVAL_BATCH_POLL_SECS`, 30).
    pub poll_interval: Duration,
    /// A cap on requests per batch below the provider's own
    /// (`EVAL_BATCH_MAX_REQUESTS`).
    pub max_requests: Option<usize>,
    /// Re-run each record a batch failed (errored, expired, canceled,
    /// missing) synchronously at the end (`EVAL_BATCH_RETRY_SYNC=1`).
    pub retry_failed_sync: bool,
    /// The state file ([`BatchState`]).
    pub state_path: PathBuf,
    /// Consecutive failed polls of one batch before the run stops (its
    /// state kept for a resume).
    pub max_poll_failures: u32,
}

impl BatchOptions {
    pub(crate) fn from_env(state_path: PathBuf) -> Self {
        Self {
            poll_interval: Duration::from_secs(crate::eval::env_parse("EVAL_BATCH_POLL_SECS", 30)),
            max_requests: std::env::var("EVAL_BATCH_MAX_REQUESTS")
                .ok()
                .filter(|raw| !raw.trim().is_empty())
                .map(|_| crate::eval::env_parse("EVAL_BATCH_MAX_REQUESTS", 0_usize).max(1)),
            retry_failed_sync: crate::eval::env_flag("EVAL_BATCH_RETRY_SYNC"),
            state_path,
            max_poll_failures: 20,
        }
    }
}

/// `<dir>/<stem>.batch-state.json`.
pub(crate) fn state_path(dir: &Path, stem: &str) -> PathBuf {
    dir.join(format!("{stem}.batch-state.json"))
}

// ---------------------------------------------------------------------------
// Chunking.
// ---------------------------------------------------------------------------

/// One batch's limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkLimits {
    pub max_requests: usize,
    /// Of the uploaded input: `OpenAI`'s JSONL file, Claude's request body.
    pub max_bytes: usize,
}

impl ChunkLimits {
    /// The provider's own limits: `OpenAI` 50,000 requests and a 200 MB
    /// input file; Claude 100,000 requests and 256 MB.
    pub(crate) fn for_provider(provider: &str) -> Self {
        if provider == "openai" {
            Self {
                max_requests: llm::openai_batch::MAX_REQUESTS,
                max_bytes: llm::openai_batch::MAX_INPUT_BYTES,
            }
        } else {
            Self {
                max_requests: llm::anthropic::MAX_BATCH_REQUESTS,
                max_bytes: llm::anthropic::MAX_BATCH_BYTES,
            }
        }
    }

    /// With `max_requests` lowered to `cap` (never raised).
    pub(crate) fn capped(self, cap: Option<usize>) -> Self {
        Self {
            max_requests: cap.map_or(self.max_requests, |cap| cap.clamp(1, self.max_requests)),
            ..self
        }
    }
}

/// Bytes one request adds to its batch's upload: its `OpenAI` JSONL line,
/// or its entry in the Message Batches body (plus a separator).
pub(crate) fn request_bytes(provider: &str, request: &BatchRequest) -> usize {
    let line = if provider == "openai" {
        llm::openai_batch::batch_jsonl(std::slice::from_ref(request)).map(|line| line.len() - 1)
    } else {
        serde_json::to_vec(&llm::batch_api::MessageBatchEntry {
            custom_id: &request.custom_id,
            params: &request.params,
        })
        .map(|entry| entry.len())
        .map_err(|err| LlmCallError::Other(err.into()))
    };
    line.map_or(usize::MAX, |len| len + 1)
}

/// Splits `requests` into batches within `limits`, and one model per batch
/// (`OpenAI` takes one model per input file; a target only ever has one).
pub(crate) fn chunk_requests(
    provider: &str,
    requests: Vec<BatchRequest>,
    limits: ChunkLimits,
) -> anyhow::Result<Vec<Vec<BatchRequest>>> {
    let budget = limits.max_bytes.saturating_sub(ENVELOPE_BYTES);
    let model = BatchRequest::model;
    let mut chunks: Vec<Vec<BatchRequest>> = Vec::new();
    let mut bytes = 0;
    for request in requests {
        let size = request_bytes(provider, &request);
        if size > budget {
            anyhow::bail!(
                "request {} is {size} bytes, over the batch limit of {} bytes",
                request.custom_id,
                limits.max_bytes
            );
        }
        let open = chunks.last_mut().filter(|chunk| {
            chunk.len() < limits.max_requests
                && bytes + size <= budget
                && chunk.first().map(model) == Some(model(&request))
        });
        if let Some(chunk) = open {
            bytes += size;
            chunk.push(request);
        } else {
            bytes = size;
            chunks.push(vec![request]);
        }
    }
    Ok(chunks)
}

// ---------------------------------------------------------------------------
// State.
// ---------------------------------------------------------------------------

/// One (case, repetition) of the run: the `<n>` of its custom ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobKey {
    pub index: usize,
    pub case_id: String,
    pub input_hash: String,
    pub repetition: u32,
}

/// One submitted batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SubmittedBatch {
    pub id: String,
    /// The requests it holds.
    pub custom_ids: Vec<String>,
    /// Provider-side files to delete once processed (`OpenAI`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,
    pub submitted_at: DateTime<Utc>,
    /// When a poll first saw it ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    /// The provider's final status and counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl SubmittedBatch {
    fn turnaround_ms(&self) -> Option<u64> {
        self.ended_at
            .map(|end| millis_between(self.submitted_at, end))
    }
}

fn millis_between(from: DateTime<Utc>, to: DateTime<Utc>) -> u64 {
    u64::try_from((to - from).num_milliseconds().max(0)).unwrap_or(0)
}

/// The state file: everything a resumed run needs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BatchState {
    /// [`STATE_VERSION`].
    pub version: u32,
    pub target: String,
    pub model: String,
    /// The adversarial stage's model when it isn't `model`
    /// (`adversarial_model`); absent in older state files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adversarial_model: Option<String>,
    /// `openai` or `anthropic`.
    pub provider: String,
    pub base_url: String,
    pub prompt_fingerprint: String,
    pub jobs: Vec<JobKey>,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub primary: Vec<SubmittedBatch>,
    /// The primary stage's call per job index, once that stage has ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_calls: Option<BTreeMap<usize, CallRecord>>,
    #[serde(default)]
    pub adversarial: Vec<SubmittedBatch>,
    /// Both stages ended and the records were built.
    #[serde(default)]
    pub done: bool,
}

impl BatchState {
    fn stage(&self, stage: Stage) -> &Vec<SubmittedBatch> {
        match stage {
            Stage::Primary => &self.primary,
            Stage::Adversarial => &self.adversarial,
        }
    }

    fn stage_mut(&mut self, stage: Stage) -> &mut Vec<SubmittedBatch> {
        match stage {
            Stage::Primary => &mut self.primary,
            Stage::Adversarial => &mut self.adversarial,
        }
    }

    /// Whether `self` (read from disk) is a state of the run `fresh`
    /// describes; the first difference otherwise.
    fn mismatch(&self, fresh: &Self) -> Option<&'static str> {
        if self.version != fresh.version {
            return Some("version");
        }
        [
            (self.target == fresh.target, "target"),
            (self.model == fresh.model, "model"),
            (
                self.adversarial_model == fresh.adversarial_model,
                "adversarial model",
            ),
            (self.provider == fresh.provider, "provider"),
            (self.base_url == fresh.base_url, "base_url"),
            (
                self.prompt_fingerprint == fresh.prompt_fingerprint,
                "prompt fingerprint",
            ),
            (self.jobs == fresh.jobs, "cases or repetitions"),
        ]
        .into_iter()
        .find_map(|(same, what)| (!same).then_some(what))
    }

    pub(crate) fn read(path: &Path) -> anyhow::Result<Option<Self>> {
        match std::fs::read_to_string(path) {
            Ok(raw) => serde_json::from_str(&raw)
                .map(Some)
                .map_err(|err| anyhow::anyhow!("{}: {err}", path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(anyhow::anyhow!("reading {}: {err}", path.display())),
        }
    }

    /// Writes the file atomically (a temporary file, then a rename).
    pub(crate) fn save(&self, path: &Path) -> anyhow::Result<()> {
        let tmp = path.with_extension("json.tmp");
        crate::eval::write_json(&tmp, self)?;
        std::fs::rename(&tmp, path)
            .map_err(|err| anyhow::anyhow!("renaming {} into place: {err}", tmp.display()))
    }
}

/// A batch stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Primary,
    Adversarial,
}

impl Stage {
    fn label(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Adversarial => "adversarial",
        }
    }
}

// ---------------------------------------------------------------------------
// Results.
// ---------------------------------------------------------------------------

/// One request's result, from its batch.
#[derive(Debug, Clone)]
struct RequestResult {
    /// `None`: no result line (`batch_missing`).
    outcome: Option<BatchOutcome>,
    batch_id: String,
    elapsed_ms: u64,
}

/// The `CallRecord` of a batch request: a success as a synchronous call
/// would record it (the same outcome labels for an unusable answer), and
/// every other result as a failed call.
fn call_record(pass: Pass, result: &RequestResult) -> CallRecord {
    let failed = |outcome: &str, error: String| (outcome.to_string(), None, Some(error), None);
    let (outcome, content, error, usage) = match &result.outcome {
        Some(BatchOutcome::Succeeded {
            usage,
            content,
            label,
        }) => match content {
            Ok(text) => (
                "success".to_string(),
                Some(text.clone()),
                None,
                usage.clone(),
            ),
            Err(err) => ((*label).to_string(), None, Some(err.clone()), usage.clone()),
        },
        Some(BatchOutcome::Errored { kind, message }) => {
            let kind = kind.as_deref().unwrap_or("error");
            // A batch-level failure (`batch_gone`, ...) keeps its own label.
            let label = if kind.starts_with("batch_") {
                kind
            } else {
                "batch_errored"
            };
            failed(
                label,
                format!(
                    "batch request errored ({kind}): {}",
                    message.as_deref().unwrap_or("no message")
                ),
            )
        }
        Some(BatchOutcome::Expired) => failed(
            "batch_expired",
            "the batch expired (24 h window) before this request ran".to_string(),
        ),
        Some(BatchOutcome::Canceled) => failed(
            "batch_canceled",
            "the batch was canceled before this request ran".to_string(),
        ),
        Some(BatchOutcome::AuthExpired) => failed(
            "batch_auth_expired",
            "token_expired: the token that created the batch had expired when OpenAI authorised \
             it; nothing ran"
                .to_string(),
        ),
        None => failed(
            "batch_missing",
            "the ended batch has no result for this request".to_string(),
        ),
    };
    CallRecord {
        pass,
        elapsed_ms: result.elapsed_ms,
        retries: 0,
        outcome,
        attempts: Vec::new(),
        content,
        error,
        usage,
        batch_id: Some(result.batch_id.clone()),
    }
}

/// A call that failed while building its request: nothing was sent (the
/// sync path's `RawCall::failed_before_sending`).
fn unsent(pass: Pass, err: &anyhow::Error) -> CallRecord {
    CallRecord {
        pass,
        elapsed_ms: 0,
        retries: 0,
        outcome: "error".to_string(),
        attempts: Vec::new(),
        content: None,
        error: Some(format!("{err:#}")),
        usage: None,
        batch_id: None,
    }
}

/// A call the batch couldn't run (not an unusable answer).
fn batch_failed(call: &CallRecord) -> bool {
    call.batch_id.is_some() && call.outcome.starts_with("batch_")
}

/// The submit error, with what to do about it: a missing batch API or
/// permission names the sync opt-in instead of falling back silently.
fn submit_error(
    client: &LlmClient,
    stage: Stage,
    state_path: &Path,
    err: &LlmCallError,
) -> anyhow::Error {
    let provider = client.provider_label();
    let stage = stage.label();
    let sync = "set mode = \"sync\" on this target (or EVAL_MODE=sync for the run) to evaluate \
                it with synchronous calls instead";
    match err {
        LlmCallError::Status {
            status: status @ (404 | 405 | 501),
        } => anyhow::anyhow!(
            "{} has no {provider} batch API (HTTP {status} submitting the {stage} batch). Batch \
             is the eval's default mode; {sync}",
            client.base_url()
        ),
        LlmCallError::Status {
            status: status @ (401 | 403),
        } => {
            let permission = if provider == "openai" {
                "OpenAI batches need the api.batch.write permission on the API key or on the \
                 workload identity's service account and project role (docs/enricher-openai.md, \
                 \"Permissions\")"
            } else {
                "check that the key or workload identity may use the Message Batches API"
            };
            anyhow::anyhow!(
                "the {stage} batch was refused (HTTP {status}): {permission}. Or {sync}"
            )
        }
        LlmCallError::Unauthorized => {
            anyhow::anyhow!("the {stage} batch was refused: {err}. Or {sync}")
        }
        _ => anyhow::anyhow!(
            "submitting the {stage} batch failed: {err}. Batches already submitted are recorded \
             in {}: re-run with EVAL_RESUME=<run dir> to resume, or {sync}",
            state_path.display()
        ),
    }
}

/// Records an ended (or lost) batch's summary and files on `batch`, and
/// returns its results; a lost batch's requests all get one batch-level
/// failure.
fn take_results(batch: &mut SubmittedBatch, poll: BatchPoll) -> BatchResults {
    let lost = |kind: &str, message: String| BatchResults {
        outcomes: HashMap::new(),
        unanswered: Some(BatchOutcome::Errored {
            kind: Some(kind.to_string()),
            message: Some(message),
        }),
    };
    match poll {
        BatchPoll::Ended(ended) => {
            batch.summary = Some(ended.summary);
            for file in ended.files {
                if !batch.files.contains(&file) {
                    batch.files.push(file);
                }
            }
            ended.results
        }
        BatchPoll::ValidationFailed { reason, files } => {
            batch.summary = Some(format!("failed validation: {reason}"));
            batch.files.extend(files);
            lost("batch_validation_failed", reason)
        }
        BatchPoll::AuthExpired { reason, files } => {
            batch.summary = Some(format!("failed, creator's token expired: {reason}"));
            batch.files.extend(files);
            BatchResults {
                outcomes: HashMap::new(),
                unanswered: Some(BatchOutcome::AuthExpired),
            }
        }
        BatchPoll::Gone => {
            batch.summary = Some("gone (404)".to_string());
            lost(
                "batch_gone",
                "the API no longer knows this batch".to_string(),
            )
        }
        BatchPoll::ResultsGone => {
            batch.summary = Some("results gone (404)".to_string());
            lost(
                "batch_results_gone",
                "the batch's results can no longer be downloaded".to_string(),
            )
        }
        // Not ended: nothing to take (the caller polls it again).
        BatchPoll::Pending => BatchResults::default(),
    }
}

/// Submits whatever of `requests` no recorded batch of `stage` holds, then
/// polls every batch of the stage until all have ended. Returns each
/// request's result by `custom_id`.
async fn run_stage(
    client: &LlmClient,
    run: &mut BatchState,
    stage: Stage,
    requests: Vec<BatchRequest>,
    opts: &BatchOptions,
) -> anyhow::Result<HashMap<String, RequestResult>> {
    submit_stage(client, run, stage, requests, opts).await?;
    poll_stage(client, run, stage, opts).await
}

/// Submits whatever of `requests` no recorded batch of `stage` holds, in
/// batches within the provider's limits, recording each in the state file.
async fn submit_stage(
    client: &LlmClient,
    run: &mut BatchState,
    stage: Stage,
    requests: Vec<BatchRequest>,
    opts: &BatchOptions,
) -> anyhow::Result<()> {
    let provider = client.provider_label();
    let target = run.target.clone();
    let submitted: HashSet<String> = run
        .stage(stage)
        .iter()
        .flat_map(|b| b.custom_ids.iter().cloned())
        .collect();
    let pending: Vec<BatchRequest> = requests
        .into_iter()
        .filter(|r| !submitted.contains(&r.custom_id))
        .collect();
    let limits = ChunkLimits::for_provider(provider).capped(opts.max_requests);
    let model_version = format!("eval:{}", run.prompt_fingerprint);
    for chunk in chunk_requests(provider, pending, limits)? {
        let meta = BatchMeta {
            stage: stage.label(),
            model_version: &model_version,
        };
        // One model per batch (`chunk_requests`): the stage's call model.
        let model = chunk
            .first()
            .and_then(BatchRequest::model)
            .unwrap_or_else(|| "-".to_string());
        let created = client
            .create_batch(&chunk, meta)
            .await
            .map_err(|err| submit_error(client, stage, &opts.state_path, &err))?;
        eprintln!(
            "batch: {target} {} batch {} submitted ({} request(s), model {model})",
            stage.label(),
            created.id,
            chunk.len()
        );
        run.stage_mut(stage).push(SubmittedBatch {
            id: created.id,
            custom_ids: chunk.into_iter().map(|r| r.custom_id).collect(),
            files: created.files,
            submitted_at: Utc::now(),
            ended_at: None,
            summary: None,
        });
        run.save(&opts.state_path)?;
    }
    Ok(())
}

/// Polls every batch of `stage` until all have ended; each request's
/// result by `custom_id`.
async fn poll_stage(
    client: &LlmClient,
    run: &mut BatchState,
    stage: Stage,
    opts: &BatchOptions,
) -> anyhow::Result<HashMap<String, RequestResult>> {
    let target = run.target.clone();
    let mut results = HashMap::new();
    let mut open: Vec<usize> = (0..run.stage(stage).len()).collect();
    let mut failures: HashMap<usize, u32> = HashMap::new();
    loop {
        let mut still_open = Vec::new();
        for index in open {
            let id = run.stage(stage)[index].id.clone();
            let poll = match client.poll_batch(&id).await {
                Ok(BatchPoll::Pending) => {
                    still_open.push(index);
                    continue;
                }
                Ok(poll) => poll,
                Err(err) => {
                    let failed = failures.entry(index).or_default();
                    *failed += 1;
                    if *failed >= opts.max_poll_failures {
                        anyhow::bail!(
                            "polling {} batch {id} failed {failed} times in a row ({err}); the run's \
                             state is in {}: re-run with EVAL_RESUME=<run dir> to resume",
                            stage.label(),
                            opts.state_path.display()
                        );
                    }
                    eprintln!("batch: polling {id} failed ({err}); retrying");
                    still_open.push(index);
                    continue;
                }
            };
            let batch = &mut run.stage_mut(stage)[index];
            let ended_at = *batch.ended_at.get_or_insert_with(Utc::now);
            let elapsed_ms = millis_between(batch.submitted_at, ended_at);
            let mut ended = take_results(batch, poll);
            eprintln!(
                "batch: {target} {} batch {id} ended after {} ({})",
                stage.label(),
                secs(Some(elapsed_ms)),
                batch.summary.as_deref().unwrap_or("")
            );
            for custom_id in &batch.custom_ids {
                let outcome = ended
                    .outcomes
                    .remove(custom_id)
                    .or_else(|| ended.unanswered.clone());
                results.insert(
                    custom_id.clone(),
                    RequestResult {
                        outcome,
                        batch_id: id.clone(),
                        elapsed_ms,
                    },
                );
            }
            run.save(&opts.state_path)?;
        }
        open = still_open;
        if open.is_empty() {
            break;
        }
        tokio::time::sleep(opts.poll_interval).await;
    }
    Ok(results)
}

/// Deletes a stage's provider-side files (`OpenAI`; best effort, they
/// expire on their own within 2 days).
async fn delete_files(client: &LlmClient, batches: &[SubmittedBatch]) {
    let files: Vec<String> = batches.iter().flat_map(|b| b.files.clone()).collect();
    if !files.is_empty() {
        client.delete_batch_files(&files).await;
    }
}

/// The jobs, in records order: by case, then repetition.
fn jobs(cases: &[Case], repetitions: u32) -> Vec<(JobKey, usize)> {
    let mut jobs = Vec::new();
    for (case_index, case) in cases.iter().enumerate() {
        for repetition in 0..repetitions {
            jobs.push((
                JobKey {
                    index: jobs.len(),
                    case_id: case.id.clone(),
                    input_hash: case.input_hash(),
                    repetition,
                },
                case_index,
            ));
        }
    }
    jobs
}

/// A batch run's records and metadata.
#[derive(Debug)]
pub(crate) struct BatchRun {
    pub records: Vec<PipelineRecord>,
    pub meta: BatchRunMeta,
}

/// Runs every case `repetitions` times through the provider's batch API
/// (resuming the state file when there is one), and returns the records,
/// ordered by case, then repetition, as the sync path does.
#[expect(
    clippy::too_many_lines,
    reason = "the two stages read top to bottom, like crate::batch's"
)]
pub(crate) async fn run_batch(
    client: &LlmClient,
    label: &TargetLabel,
    cases: &[Case],
    repetitions: u32,
    opts: &BatchOptions,
) -> anyhow::Result<BatchRun> {
    let jobs = jobs(cases, repetitions);
    let fingerprint = Backend::prompt_fingerprint(client);
    let fresh = BatchState {
        version: STATE_VERSION,
        target: label.target.clone(),
        model: label.model.clone(),
        adversarial_model: client.adversarial_model().map(str::to_string),
        provider: client.provider_label().to_string(),
        base_url: client.base_url().to_string(),
        prompt_fingerprint: fingerprint.clone(),
        jobs: jobs.iter().map(|(job, _)| job.clone()).collect(),
        started_at: Utc::now(),
        primary: Vec::new(),
        primary_calls: None,
        adversarial: Vec::new(),
        done: false,
    };
    let (mut state, resumed) = if let Some(saved) = BatchState::read(&opts.state_path)? {
        if let Some(what) = saved.mismatch(&fresh) {
            anyhow::bail!(
                "{} belongs to another run (its {what} differs); delete it, or run into a \
                     new directory (unset EVAL_RESUME)",
                opts.state_path.display()
            );
        }
        if saved.done {
            anyhow::bail!(
                "{} is a finished run, but its records file is missing; delete the state \
                     file to run again",
                opts.state_path.display()
            );
        }
        eprintln!("batch: resuming {}", opts.state_path.display());
        (saved, true)
    } else {
        fresh.save(&opts.state_path)?;
        (fresh, false)
    };
    let prompts = client.prompts();

    // Primary stage.
    let primary_calls = if let Some(calls) = state.primary_calls.clone() {
        calls
    } else {
        let requests = jobs
            .iter()
            .map(|(job, case_index)| {
                let case = &cases[*case_index];
                let spec = llm::primary_spec(
                    prompts,
                    &case.summary,
                    &case.description,
                    case.reference_date,
                );
                Ok(BatchRequest {
                    custom_id: crate::batch::custom_id(LlmCall::Primary, job.index),
                    params: client.batch_params(spec)?,
                })
            })
            .collect::<Result<Vec<_>, LlmCallError>>()?;
        let results = run_stage(client, &mut state, Stage::Primary, requests, opts).await?;
        let calls: BTreeMap<usize, CallRecord> = jobs
            .iter()
            .map(|(job, _)| {
                let id = crate::batch::custom_id(LlmCall::Primary, job.index);
                let result = results.get(&id).cloned().unwrap_or_else(|| RequestResult {
                    outcome: None,
                    batch_id: String::new(),
                    elapsed_ms: 0,
                });
                (job.index, call_record(Pass::Primary, &result))
            })
            .collect();
        state.primary_calls = Some(calls.clone());
        state.save(&opts.state_path)?;
        delete_files(client, &state.primary).await;
        calls
    };

    // Adversarial stage: the jobs whose primary output parsed.
    let mut periods_by_job: BTreeMap<usize, Vec<llm::ExtractionPeriod>> = BTreeMap::new();
    let mut unsent_calls: BTreeMap<usize, Vec<CallRecord>> = BTreeMap::new();
    let mut requests = Vec::new();
    for (job, case_index) in &jobs {
        let Some(periods) = primary_calls
            .get(&job.index)
            .and_then(|call| call.content.as_deref())
            .and_then(|content| llm::parse_primary(content).ok())
            .map(|primary| primary.periods)
        else {
            continue;
        };
        let case = &cases[*case_index];
        for (pass, spec) in [
            (
                Pass::ResolutionAdversarial,
                llm::adversarial_spec(prompts, &case.summary, &case.description, &periods),
            ),
            (
                Pass::SeverityAdversarial,
                llm::severity_adversarial_spec(prompts, &case.summary, &case.description, &periods),
            ),
        ] {
            match spec {
                Ok(spec) => requests.push(BatchRequest {
                    custom_id: crate::batch::custom_id(spec.call, job.index),
                    params: client.batch_params(spec)?,
                }),
                Err(err) => unsent_calls
                    .entry(job.index)
                    .or_default()
                    .push(unsent(pass, &err)),
            }
        }
        periods_by_job.insert(job.index, periods);
    }
    let results = if requests.is_empty() {
        HashMap::new()
    } else {
        run_stage(client, &mut state, Stage::Adversarial, requests, opts).await?
    };

    // Records.
    let mut records: Vec<PipelineRecord> = Vec::with_capacity(jobs.len());
    for (job, _) in &jobs {
        let primary = primary_calls
            .get(&job.index)
            .cloned()
            .unwrap_or_else(|| call_record(Pass::Primary, &missing_result()));
        let mut calls = vec![primary];
        if periods_by_job.contains_key(&job.index) {
            let mut unsent = unsent_calls.remove(&job.index).unwrap_or_default();
            for (pass, call) in [
                (Pass::ResolutionAdversarial, LlmCall::ResolutionAdversarial),
                (Pass::SeverityAdversarial, LlmCall::SeverityAdversarial),
            ] {
                if let Some(at) = unsent.iter().position(|c| c.pass == pass) {
                    calls.push(unsent.remove(at));
                    continue;
                }
                let id = crate::batch::custom_id(call, job.index);
                let result = results.get(&id).cloned().unwrap_or_else(missing_result);
                calls.push(call_record(pass, &result));
            }
        }
        let adversarial_ms = calls[1..].iter().map(|c| c.elapsed_ms).max().unwrap_or(0);
        records.push(PipelineRecord {
            label: label.clone(),
            case_id: job.case_id.clone(),
            input_hash: job.input_hash.clone(),
            prompt_fingerprint: fingerprint.clone(),
            repetition: job.repetition,
            elapsed_ms: calls[0].elapsed_ms.saturating_add(adversarial_ms),
            calls,
        });
    }
    let mut meta = BatchRunMeta::new(&state, &records, &opts.state_path, resumed);

    // Optional: re-run what the batches couldn't, synchronously.
    if opts.retry_failed_sync {
        for (record, (job, case_index)) in records.iter_mut().zip(&jobs) {
            if !record.calls.iter().any(batch_failed) {
                continue;
            }
            let case = &cases[*case_index];
            if batch_failed(&record.calls[0]) {
                *record = pipeline::run_pipeline(client, label, case, record.repetition).await;
            } else if let Some(periods) = periods_by_job.get(&job.index) {
                for call in &mut record.calls {
                    if batch_failed(call) {
                        *call = pipeline::timed_call(client, call.pass, case, periods).await;
                    }
                }
            }
            meta.retried_in_sync += 1;
        }
    }

    // The service's echo retry (`main.rs::realign_adversarial`): an
    // adversarial verdict list that doesn't echo the primary periods is
    // asked again once, synchronously, as the service's batch path does.
    for (record, (_, case_index)) in records.iter_mut().zip(&jobs) {
        let retried = pipeline::retry_echo_mismatches(client, &cases[*case_index], record).await;
        if retried > 0 {
            eprintln!(
                "batch: {}: {}: re-asked {retried} adversarial pass(es) whose verdicts didn't echo the periods",
                label.target, record.case_id
            );
        }
    }

    state.done = true;
    state.save(&opts.state_path)?;
    delete_files(client, &state.adversarial).await;
    Ok(BatchRun { records, meta })
}

fn missing_result() -> RequestResult {
    RequestResult {
        outcome: None,
        batch_id: String::new(),
        elapsed_ms: 0,
    }
}

// ---------------------------------------------------------------------------
// Metadata for the reports.
// ---------------------------------------------------------------------------

/// One batch, for the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct BatchInfo {
    pub id: String,
    pub requests: usize,
    pub submitted_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub turnaround_ms: Option<u64>,
    pub summary: Option<String>,
}

/// One stage, for the report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct StageInfo {
    pub stage: &'static str,
    /// The model the stage's requests went to: the primary model, or the
    /// adversarial calls' own (`adversarial_model`). Its `usage` is that
    /// model's tokens.
    pub model: String,
    pub batches: Vec<BatchInfo>,
    pub requests: usize,
    /// First submission to the last batch seen ended.
    pub turnaround_ms: Option<u64>,
    /// Result label -> count, over the stage's batch calls.
    pub outcomes: BTreeMap<String, usize>,
    pub succeeded: usize,
    /// Answered, but unusable (`refused`, `empty_content`, `error`).
    pub unusable: usize,
    pub errored: usize,
    pub expired: usize,
    pub canceled: usize,
    /// `OpenAI` `token_expired` (`batch_auth_expired`): the creator's token
    /// had expired when the batch was authorised.
    pub auth_expired: usize,
    /// No result line, or a batch-level loss (gone, results gone, failed
    /// validation).
    pub missing: usize,
    pub usage: UsageTotals,
}

/// A batch run's metadata, in the report (`batch`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct BatchRunMeta {
    pub provider: String,
    pub state_file: String,
    /// The run picked up an existing state file.
    pub resumed: bool,
    pub stages: Vec<StageInfo>,
    /// First primary submission to the last batch seen ended.
    pub turnaround_ms: Option<u64>,
    /// Records re-run synchronously after a batch failure
    /// (`EVAL_BATCH_RETRY_SYNC=1`).
    pub retried_in_sync: usize,
    /// Every batch call's usage.
    pub usage: UsageTotals,
}

impl BatchRunMeta {
    /// From the state and the records' batch calls (before any sync retry).
    pub(crate) fn new(
        state: &BatchState,
        records: &[PipelineRecord],
        state_path: &Path,
        resumed: bool,
    ) -> Self {
        let mut usage = UsageTotals::default();
        let mut stages = Vec::new();
        for (stage, batches) in [
            (Stage::Primary, &state.primary),
            (Stage::Adversarial, &state.adversarial),
        ] {
            let model = match stage {
                Stage::Primary => &state.model,
                Stage::Adversarial => state.adversarial_model.as_ref().unwrap_or(&state.model),
            };
            let mut info = StageInfo {
                stage: stage.label(),
                model: model.clone(),
                ..StageInfo::default()
            };
            for batch in batches {
                info.requests += batch.custom_ids.len();
                info.batches.push(BatchInfo {
                    id: batch.id.clone(),
                    requests: batch.custom_ids.len(),
                    submitted_at: batch.submitted_at,
                    ended_at: batch.ended_at,
                    turnaround_ms: batch.turnaround_ms(),
                    summary: batch.summary.clone(),
                });
            }
            let first = batches.iter().map(|b| b.submitted_at).min();
            let last = batches.iter().map(|b| b.ended_at).max().flatten();
            info.turnaround_ms = first
                .zip(last)
                .filter(|_| batches.iter().all(|b| b.ended_at.is_some()))
                .map(|(from, to)| millis_between(from, to));
            let ids: HashSet<&str> = batches.iter().map(|b| b.id.as_str()).collect();
            let calls = records
                .iter()
                .flat_map(|r| &r.calls)
                .filter(|c| c.batch_id.as_deref().is_some_and(|id| ids.contains(id)));
            for call in calls {
                *info.outcomes.entry(call.outcome.clone()).or_default() += 1;
                match call.outcome.as_str() {
                    "success" => info.succeeded += 1,
                    "batch_errored" => info.errored += 1,
                    "batch_expired" => info.expired += 1,
                    "batch_canceled" => info.canceled += 1,
                    "batch_auth_expired" => info.auth_expired += 1,
                    other if other.starts_with("batch_") => info.missing += 1,
                    _ => info.unusable += 1,
                }
                if let Some(call_usage) = &call.usage {
                    info.usage.add(call_usage);
                    usage.add(call_usage);
                }
            }
            if !batches.is_empty() {
                stages.push(info);
            }
        }
        let all: Vec<&SubmittedBatch> = state.primary.iter().chain(&state.adversarial).collect();
        let turnaround_ms = all
            .iter()
            .map(|b| b.submitted_at)
            .min()
            .zip(all.iter().map(|b| b.ended_at).max().flatten())
            .filter(|_| all.iter().all(|b| b.ended_at.is_some()))
            .map(|(from, to)| millis_between(from, to));
        Self {
            provider: state.provider.clone(),
            turnaround_ms,
            state_file: crate::eval::display_path(state_path),
            resumed,
            stages,
            retried_in_sync: 0,
            usage,
        }
    }

    /// The report's "Batch" section.
    pub(crate) fn markdown(&self) -> String {
        let mut md = String::from("\n## Batch\n\n");
        let _ = writeln!(
            md,
            "Sent through the {} batch API{}; state file `{}`.{}\n",
            self.provider,
            if self.resumed { " (resumed)" } else { "" },
            self.state_file,
            if self.retried_in_sync > 0 {
                format!(
                    " {} record(s) a batch failed were re-run synchronously.",
                    self.retried_in_sync
                )
            } else {
                String::new()
            }
        );
        let rows: Vec<Vec<String>> = self
            .stages
            .iter()
            .map(|s| {
                vec![
                    s.stage.to_string(),
                    format!("`{}`", s.model),
                    s.batches
                        .iter()
                        .map(|b| format!("`{}`", b.id))
                        .collect::<Vec<_>>()
                        .join(", "),
                    s.requests.to_string(),
                    secs(s.turnaround_ms),
                    s.succeeded.to_string(),
                    s.unusable.to_string(),
                    s.errored.to_string(),
                    s.expired.to_string(),
                    s.canceled.to_string(),
                    s.auth_expired.to_string(),
                    s.missing.to_string(),
                    format!(
                        "{} ({} cached, {} cache write)",
                        s.usage.prompt_tokens, s.usage.cached_tokens, s.usage.cache_write_tokens
                    ),
                    s.usage.completion_tokens.to_string(),
                ]
            })
            .collect();
        md.push_str(&report::table(
            &[
                "Stage",
                "Model",
                "Batches",
                "Requests",
                "Turnaround",
                "Succeeded",
                "Unusable",
                "Errored",
                "Expired",
                "Canceled",
                "Auth expired",
                "Missing",
                "Prompt tokens",
                "Completion tokens",
            ],
            &rows,
        ));
        md.push_str(
            "\nA batch call's latency is its batch's turnaround (submitted to seen ended, at the \
             poll interval's resolution), not a request's own time. Errored, expired, canceled, \
             auth-expired and missing requests are failed calls (transport failures in the \
             scores).\n",
        );
        let auth_expired: usize = self.stages.iter().map(|s| s.auth_expired).sum();
        if auth_expired > 0 {
            let _ = writeln!(
                md,
                "\n**batch_auth_expired: {auth_expired} request(s) did not run** because the token \
                 that created their batch had expired when {} authorised it (`token_expired`). \
                 Keyless (WIF) tokens last about 900 s and the batch is checked after it is \
                 created, so a long queue can outlive the token. These are not model failures: \
                 re-run the target with `mode = \"sync\"` (or `EVAL_BATCH_RETRY_SYNC=1`), or \
                 resume it later (docs/enricher-model-eval.md, \"Batch mode\").",
                self.provider
            );
        }
        md
    }
}

/// Mocked-HTTP tests of whole batch runs on both providers.
#[cfg(test)]
#[expect(
    clippy::too_many_lines,
    reason = "test code: scenario tests read top to bottom"
)]
mod tests;
