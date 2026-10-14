//! The provider-neutral batch client: one interface over the Claude
//! Message Batches API (`anthropic.rs`) and the `OpenAI` Batch API
//! (`openai_batch.rs`), dispatched on the client's [`Provider`].
//! `batch.rs` (the sweep's batch mode and the poll loop) only sees this
//! module's types, so whatever decides *when* to batch works with either
//! provider.
//!
//! Every request body comes from the synchronous path's own builder
//! (`LlmClient::wire_request`), so a batch request carries exactly the
//! profile, schema, sampling and reasoning effort a synchronous call would.
//!
//! **Memory.** A batch is up to `LLM_BATCH_MAX_ITEMS` (2,000) incidents,
//! and every request carries its whole system prompt (~11 KB primary), so
//! a batch is tens of MB of JSON. Each request is therefore held as its
//! serialised text ([`BatchRequest::params`], a `RawValue`), never as a
//! `serde_json::Value` tree (about three times the text), and the upload
//! body is written straight from those texts into one buffer sized up
//! front ([`message_batches_body`], [`openai_batch::batch_jsonl`]). Results
//! are read off the response as it arrives and parsed line by line
//! ([`read_jsonl_lines`]); the raw file is never held whole.
//!
//! [`openai_batch::batch_jsonl`]: super::openai_batch::batch_jsonl

use std::collections::HashMap;

use serde::Serialize;
use serde_json::value::RawValue;

use super::{CallSpec, LlmCallError, LlmClient, Provider, TokenUsage, WireRequest};

/// `enricher_llm_batch_files_total{provider, event}`: provider-side batch
/// files (`OpenAI` only): `uploaded`, `deleted`, `delete_failed`.
pub(crate) const FILES_METRIC: &str = "enricher_llm_batch_files_total";

/// One request of a batch. `custom_id` must match `^[a-zA-Z0-9_-]{1,64}$`
/// (both APIs); results come back in any order and are matched by it.
#[derive(Debug, Clone)]
pub(crate) struct BatchRequest {
    pub custom_id: String,
    /// The provider's request body ([`LlmClient::batch_params`]), as the
    /// compact JSON text that goes on the wire.
    pub params: Box<RawValue>,
}

impl BatchRequest {
    /// A request whose body is `params` serialised.
    #[cfg(test)]
    pub(crate) fn new(
        custom_id: String,
        params: &impl Serialize,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            custom_id,
            params: serde_json::value::to_raw_value(params)?,
        })
    }

    /// The body as a `Value`, for the tests.
    #[cfg(test)]
    pub(crate) fn params_value(params: &RawValue) -> serde_json::Value {
        serde_json::from_str(params.get()).expect("a RawValue is valid JSON")
    }

    /// The body's `model` (the model-eval harness batches by model).
    pub(crate) fn model(&self) -> Option<String> {
        #[derive(serde::Deserialize)]
        struct Model {
            model: Option<String>,
        }
        serde_json::from_str::<Model>(self.params.get())
            .ok()
            .and_then(|m| m.model)
    }
}

/// One entry of a Message Batches create body.
#[derive(Serialize)]
pub(crate) struct MessageBatchEntry<'a> {
    pub custom_id: &'a str,
    pub params: &'a RawValue,
}

/// `{"requests": [{"custom_id", "params"}, ...]}`, the Message Batches
/// create body, written into one buffer allocated at its final size.
pub(crate) fn message_batches_body(requests: &[BatchRequest]) -> Result<Vec<u8>, LlmCallError> {
    const ENTRY_OVERHEAD: usize = r#"{"custom_id":"","params":},"#.len();
    let capacity = requests
        .iter()
        .map(|r| r.custom_id.len() + r.params.get().len() + ENTRY_OVERHEAD)
        .sum::<usize>()
        + r#"{"requests":[]}"#.len();
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(br#"{"requests":["#);
    for (i, request) in requests.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        let entry = MessageBatchEntry {
            custom_id: &request.custom_id,
            params: &request.params,
        };
        serde_json::to_writer(&mut out, &entry).map_err(|err| LlmCallError::Other(err.into()))?;
    }
    out.extend_from_slice(b"]}");
    Ok(out)
}

/// Splits a byte stream into lines as its chunks arrive, holding only the
/// current partial line between chunks. Pure, for the tests.
#[derive(Debug, Default)]
pub(crate) struct LineSplitter {
    partial: Vec<u8>,
}

impl LineSplitter {
    /// Calls `line` with every line `chunk` completes (without its `\n`).
    pub(crate) fn push(&mut self, chunk: &[u8], line: &mut impl FnMut(&[u8])) {
        let mut rest = chunk;
        while let Some(end) = rest.iter().position(|b| *b == b'\n') {
            if self.partial.is_empty() {
                line(&rest[..end]);
            } else {
                self.partial.extend_from_slice(&rest[..end]);
                line(&self.partial);
                self.partial.clear();
            }
            rest = &rest[end + 1..];
        }
        self.partial.extend_from_slice(rest);
    }

    /// Calls `line` with the last line if the stream did not end in `\n`.
    pub(crate) fn finish(self, line: &mut impl FnMut(&[u8])) {
        if !self.partial.is_empty() {
            line(&self.partial);
        }
    }
}

/// Reads a JSONL response body as it arrives and calls `line` with each
/// non-blank line, so a results file is never held whole: only the chunk
/// in hand and one partial line. A line that is not UTF-8 is passed as
/// `Err` with its first bytes, for the caller to log and skip.
pub(crate) async fn read_jsonl_lines(
    mut response: reqwest::Response,
    mut line: impl FnMut(Result<&str, &str>),
) -> Result<(), LlmCallError> {
    let mut splitter = LineSplitter::default();
    let mut emit = |bytes: &[u8]| {
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return;
        }
        match std::str::from_utf8(bytes) {
            Ok(text) => line(Ok(text)),
            Err(_) => line(Err(&String::from_utf8_lossy(
                &bytes[..bytes.len().min(200)],
            ))),
        }
    };
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|err| LlmCallError::Other(err.into()))?
    {
        splitter.push(&chunk, &mut emit);
    }
    splitter.finish(&mut emit);
    Ok(())
}

/// What `batch.rs` tells the provider about a batch (`OpenAI` metadata).
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchMeta<'a> {
    pub stage: &'static str,
    pub model_version: &'a str,
}

/// A created batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreatedBatch {
    pub id: String,
    /// Provider-side files to delete if the batch is discarded at once
    /// (`OpenAI`: the input file).
    pub files: Vec<String>,
}

/// One batch request's result, already normalised.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BatchOutcome {
    /// The request ran (and was billed): its usage, and its JSON text or
    /// why the text is unusable (a refusal, truncated or empty output), as
    /// the synchronous path's error message.
    Succeeded {
        usage: Option<TokenUsage>,
        content: Result<String, String>,
        /// The synchronous path's `enricher_llm_call_total` outcome label
        /// for the same response: `success`, or the unusable text's
        /// (`refused`, `empty_content`, `error`). The model-eval harness
        /// records it like a synchronous call's.
        label: &'static str,
    },
    /// No completion was produced (an invalid request or a server error).
    /// `kind` is the error's type or code.
    Errored {
        kind: Option<String>,
        message: Option<String>,
    },
    /// The batch was canceled before this request ran.
    Canceled,
    /// The batch hit its 24-hour window before this request ran.
    Expired,
    /// `OpenAI` refused to run the request because the token that created
    /// the batch had expired by the time it authorised it (error code
    /// `token_expired`). Nothing ran and nothing is wrong with the request:
    /// a batch created with a fresh token would run it.
    AuthExpired,
}

impl BatchOutcome {
    /// The `result` label of `enricher_llm_batch_requests_total`.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Succeeded { .. } => "succeeded",
            Self::Errored { .. } => "errored",
            Self::Canceled => "canceled",
            Self::Expired => "expired",
            Self::AuthExpired => "auth_expired",
        }
    }

    /// A success from a provider's `(usage, content)` pair.
    pub(crate) fn succeeded(
        (usage, content): (Option<TokenUsage>, Result<String, LlmCallError>),
    ) -> Self {
        let label = content
            .as_ref()
            .map_or_else(LlmCallError::outcome_label, |_| "success");
        Self::Succeeded {
            usage,
            content: content.map_err(|err| err.to_string()),
            label,
        }
    }
}

/// An ended batch's results.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct BatchResults {
    /// Every result line, by `custom_id`.
    pub outcomes: HashMap<String, BatchOutcome>,
    /// What a request with no result line counts as: `None` is `missing`.
    /// `OpenAI`'s expired or cancelled batch may leave requests without a
    /// line; they never ran.
    pub unanswered: Option<BatchOutcome>,
}

impl From<HashMap<String, BatchOutcome>> for BatchResults {
    fn from(outcomes: HashMap<String, BatchOutcome>) -> Self {
        Self {
            outcomes,
            unanswered: None,
        }
    }
}

/// A finished batch.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EndedBatch {
    pub results: BatchResults,
    /// The provider's final status and request counts, for the log.
    pub summary: String,
    /// Provider-side files to delete once the results are processed
    /// (`OpenAI`: input, output and error files; none for Claude).
    pub files: Vec<String>,
}

/// One poll of a batch.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BatchPoll {
    /// Still running (validating, in progress, finalizing, cancelling).
    Pending,
    Ended(EndedBatch),
    /// `OpenAI` `failed`: the input file failed validation, so nothing ran
    /// and there are no results. `files` is the input file.
    ValidationFailed {
        reason: String,
        files: Vec<String>,
    },
    /// The API no longer knows the batch (404).
    Gone,
    /// Ended, but its results can no longer be downloaded (404).
    ResultsGone,
    /// `OpenAI` `failed` with `token_expired`: the creator's token had
    /// expired when `OpenAI` authorised the batch, so nothing ran. `files`
    /// is the input file.
    AuthExpired {
        reason: String,
        files: Vec<String>,
    },
}

pub(crate) fn record_file_event(provider: &'static str, event: &'static str) {
    metrics::counter!(
        common::metrics::metric_name(FILES_METRIC),
        "provider" => provider,
        "event" => event
    )
    .increment(1);
}

impl LlmClient {
    /// `openai` or `anthropic`: the `provider` column of
    /// `enricher_llm_batches` and the batch metrics' label.
    pub(crate) fn provider_label(&self) -> &'static str {
        match self.provider {
            Provider::OpenAi => "openai",
            Provider::Anthropic(_) => "anthropic",
        }
    }

    /// The provider's request body for `spec`, exactly as the synchronous
    /// path would send it: a Chat Completions body (`OpenAI`) or a Messages
    /// body (Claude), serialised.
    pub(crate) fn batch_params(&self, spec: CallSpec) -> Result<Box<RawValue>, LlmCallError> {
        match self.wire_request(spec) {
            WireRequest::Chat(request) => serde_json::value::to_raw_value(&request),
            WireRequest::Messages { body, .. } => serde_json::value::to_raw_value(&body),
        }
        .map_err(|err| LlmCallError::Other(err.into()))
    }

    /// Submits `requests` as one batch.
    pub(crate) async fn create_batch(
        &self,
        requests: &[BatchRequest],
        meta: BatchMeta<'_>,
    ) -> Result<CreatedBatch, LlmCallError> {
        match &self.provider {
            Provider::Anthropic(_) => {
                let batch = self.create_message_batch(requests).await?;
                Ok(CreatedBatch {
                    id: batch.id,
                    files: Vec::new(),
                })
            }
            Provider::OpenAi => self.create_openai_batch(requests, meta).await,
        }
    }

    /// Polls a batch and, once it has ended, downloads its results.
    pub(crate) async fn poll_batch(&self, id: &str) -> Result<BatchPoll, LlmCallError> {
        match &self.provider {
            Provider::Anthropic(_) => self.poll_message_batch(id).await,
            Provider::OpenAi => self.poll_openai_batch(id).await,
        }
    }

    /// Cancels a batch (best effort; its results would be discarded).
    pub(crate) async fn cancel_batch(&self, id: &str) -> Result<(), LlmCallError> {
        match &self.provider {
            Provider::Anthropic(_) => self.cancel_message_batch(id).await,
            Provider::OpenAi => self.cancel_openai_batch(id).await,
        }
    }

    /// Deletes a batch's provider-side files. A failure is logged and
    /// counted (`delete_failed`), never returned: the files' own short
    /// `expires_after` bounds them anyway. Returns how many failed.
    pub(crate) async fn delete_batch_files(&self, files: &[String]) -> usize {
        let provider = self.provider_label();
        if !matches!(self.provider, Provider::OpenAi) {
            return 0;
        }
        let mut failed = 0;
        for file in files {
            match self.delete_openai_file(file).await {
                // Already gone counts as deleted.
                Ok(()) | Err(LlmCallError::Status { status: 404 }) => {
                    record_file_event(provider, "deleted");
                }
                Err(err) => {
                    failed += 1;
                    record_file_event(provider, "delete_failed");
                    tracing::warn!(
                        error = %err,
                        file_id = file,
                        "could not delete a batch file; it expires on its own"
                    );
                }
            }
        }
        failed
    }
}

/// The batch path's memory bounds, and the line splitter.
#[cfg(test)]
mod tests;
