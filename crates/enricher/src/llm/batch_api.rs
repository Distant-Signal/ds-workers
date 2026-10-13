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

use std::collections::HashMap;

use serde_json::Value;

use super::{CallSpec, LlmCallError, LlmClient, Provider, TokenUsage, WireRequest};

/// `enricher_llm_batch_files_total{provider, event}`: provider-side batch
/// files (`OpenAI` only): `uploaded`, `deleted`, `delete_failed`.
pub(crate) const FILES_METRIC: &str = "enricher_llm_batch_files_total";

/// One request of a batch. `custom_id` must match `^[a-zA-Z0-9_-]{1,64}$`
/// (both APIs); results come back in any order and are matched by it.
#[derive(Debug, Clone)]
pub(crate) struct BatchRequest {
    pub custom_id: String,
    /// The provider's request body ([`LlmClient::batch_params`]).
    pub params: Value,
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
}

impl BatchOutcome {
    /// The `result` label of `enricher_llm_batch_requests_total`.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Succeeded { .. } => "succeeded",
            Self::Errored { .. } => "errored",
            Self::Canceled => "canceled",
            Self::Expired => "expired",
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
    /// body (Claude).
    pub(crate) fn batch_params(&self, spec: CallSpec) -> Result<Value, LlmCallError> {
        match self.wire_request(spec) {
            WireRequest::Chat(request) => {
                serde_json::to_value(&request).map_err(|err| LlmCallError::Other(err.into()))
            }
            WireRequest::Messages { body, .. } => Ok(body),
        }
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
