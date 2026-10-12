//! The `OpenAI` Batch API (`LLM_PROVIDER=openai`, `LLM_SWEEP_MODE=batch`):
//! the Files and Batches routes behind `batch_api`. See
//! docs/enricher-openai.md, "Batch mode".
//!
//! `OpenAI` documentation this relies on (read 2026-10-09):
//!
//! - Batch guide: <https://developers.openai.com/api/docs/guides/batch>
//! - Files: <https://developers.openai.com/api/reference/resources/files>
//! - Your data (batch files are not covered by zero data retention):
//!   <https://developers.openai.com/api/docs/guides/your-data>
//!
//! The flow: the requests become a JSONL file (`{custom_id, method, url,
//! body}` per line, `body` the synchronous path's own Chat Completions
//! body), uploaded with `purpose=batch` and a short `expires_after`; a
//! batch is created on it with `output_expires_after` and metadata; polls
//! map its status; once it has ended, the output file (successes) and
//! error file (everything else, `batch_expired` included) are downloaded
//! and matched by `custom_id` (their lines are in no particular order).
//! After processing, `batch.rs` deletes all three files.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use super::batch_api::{
    BatchMeta, BatchOutcome, BatchPoll, BatchRequest, BatchResults, CreatedBatch, EndedBatch,
    record_file_event,
};
use super::{LlmCallError, LlmClient};

/// The batch's `endpoint` and each line's `url`: the synchronous path's
/// route.
pub(crate) const ENDPOINT: &str = "/v1/chat/completions";

/// `expires_after` of the uploaded input file and `output_expires_after`
/// of the output and error files: 2 days (the API allows 1 hour to 30
/// days). A batch runs within 24 hours, and the enricher deletes every file
/// once it has processed the results; this only bounds a file the
/// enricher lost track of (a crash, a failed delete). Batch files are not
/// covered by zero data retention.
pub(crate) const FILE_TTL_SECS: u64 = 2 * 24 * 3600;

/// The API's per-batch limits: 50,000 requests, a 200 MB input file.
const MAX_REQUESTS: usize = 50_000;
const MAX_INPUT_BYTES: usize = 200 * 1024 * 1024;

/// Longest metadata value the API accepts.
const MAX_METADATA_CHARS: usize = 512;

/// A Batch object (create, retrieve and cancel responses).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct OpenAiBatch {
    pub id: String,
    /// `validating`, `failed`, `in_progress`, `finalizing`, `completed`,
    /// `expired`, `cancelling` or `cancelled`.
    pub status: String,
    #[serde(default)]
    pub input_file_id: Option<String>,
    #[serde(default)]
    pub output_file_id: Option<String>,
    #[serde(default)]
    pub error_file_id: Option<String>,
    /// `failed`'s validation errors: `{"data": [{"code", "message", "line"}]}`.
    #[serde(default)]
    pub errors: Option<Value>,
    #[serde(default)]
    pub request_counts: Option<Value>,
}

#[derive(Deserialize)]
struct FileObject {
    id: String,
}

/// The input file: one line per request.
pub(crate) fn batch_jsonl(requests: &[BatchRequest]) -> Result<String, LlmCallError> {
    if requests.len() > MAX_REQUESTS {
        return Err(LlmCallError::Other(anyhow::anyhow!(
            "{} requests is over the Batch API's {MAX_REQUESTS} per batch",
            requests.len()
        )));
    }
    let mut out = String::new();
    for request in requests {
        let line = serde_json::json!({
            "custom_id": request.custom_id,
            "method": "POST",
            "url": ENDPOINT,
            "body": request.params,
        });
        out.push_str(&line.to_string());
        out.push('\n');
    }
    if out.len() > MAX_INPUT_BYTES {
        return Err(LlmCallError::Other(anyhow::anyhow!(
            "batch input file is {} bytes, over the Batch API's 200 MB; lower LLM_BATCH_MAX_ITEMS",
            out.len()
        )));
    }
    Ok(out)
}

/// The `errors` of a `failed` batch, as one log-friendly line.
fn validation_reason(batch: &OpenAiBatch) -> String {
    let lines: Vec<String> = batch
        .errors
        .as_ref()
        .and_then(|e| e.get("data"))
        .and_then(Value::as_array)
        .map(|data| {
            data.iter()
                .take(5)
                .map(|e| {
                    let text = |key: &str| e.get(key).and_then(Value::as_str).unwrap_or("?");
                    match e.get("line").and_then(Value::as_u64) {
                        Some(line) => format!("line {line}: {} {}", text("code"), text("message")),
                        None => format!("{} {}", text("code"), text("message")),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    if lines.is_empty() {
        "input file failed validation (no errors reported)".to_string()
    } else {
        lines.join("; ")
    }
}

/// One result line of an output or error file.
fn parse_line(value: &Value) -> Option<(String, BatchOutcome)> {
    let custom_id = value.get("custom_id")?.as_str()?.to_string();
    let error = value.get("error").filter(|e| !e.is_null());
    let response = value.get("response").filter(|r| !r.is_null());
    let text = |parent: Option<&Value>, key: &str| {
        parent
            .and_then(|p| p.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    if let Some(error) = error {
        let code = text(Some(error), "code");
        let outcome = match code.as_deref() {
            Some("batch_expired") => BatchOutcome::Expired,
            // The guide names no code for a request a cancel stopped; these
            // are the spellings the API uses for the batch status.
            Some("batch_cancelled" | "batch_canceled") => BatchOutcome::Canceled,
            _ => BatchOutcome::Errored {
                kind: code,
                message: text(Some(error), "message"),
            },
        };
        return Some((custom_id, outcome));
    }
    let response = response?;
    let status = response.get("status_code")?.as_u64()?;
    let body = response.get("body").cloned().unwrap_or(Value::Null);
    if status == 200 {
        return Some((
            custom_id,
            BatchOutcome::succeeded(super::chat_completion_content(body)),
        ));
    }
    let api_error = body.get("error");
    Some((
        custom_id,
        BatchOutcome::Errored {
            kind: text(api_error, "code")
                .or_else(|| text(api_error, "type"))
                .or_else(|| Some(format!("http_{status}"))),
            message: text(api_error, "message"),
        },
    ))
}

/// Parses an output or error file into outcomes by `custom_id`. A line that
/// doesn't parse is logged and skipped: its request then counts as
/// unanswered, never as a crash of the whole batch.
pub(crate) fn parse_results_jsonl(text: &str, into: &mut HashMap<String, BatchOutcome>) {
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let parsed = serde_json::from_str::<Value>(line)
            .ok()
            .as_ref()
            .and_then(parse_line);
        if let Some((custom_id, outcome)) = parsed {
            into.insert(custom_id, outcome);
        } else {
            tracing::warn!(
                line = %line.chars().take(200).collect::<String>(),
                "unparseable OpenAI batch result line; skipping it"
            );
        }
    }
}

impl LlmClient {
    /// Sends one Files/Batches request with the bearer credential (an API
    /// key or a federated token: the same header). A federated token's 401
    /// is retried once with a fresh one, like a synchronous call. A non-2xx
    /// is classified like a chat completion's (a 404 is `Status(404)`).
    async fn openai_api_send(
        &self,
        what: &'static str,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, LlmCallError> {
        let mut refreshed = false;
        loop {
            let mut request = build();
            let credential = self
                .auth
                .credential(crate::auth::StaticKeyHeader::Bearer)
                .await?;
            if let Some(credential) = &credential {
                request = credential.apply(request);
            }
            let response = request.send().await.map_err(|err| {
                if err.is_timeout() {
                    LlmCallError::ClientTimeout
                } else {
                    LlmCallError::Other(err.into())
                }
            })?;
            let status = response.status();
            if status == reqwest::StatusCode::UNAUTHORIZED && self.auth.is_federated() {
                if let Some(credential) = &credential {
                    self.auth.invalidate(credential.secret());
                }
                if refreshed {
                    tracing::warn!(what, "OpenAI Batch API call failed: 401 to a fresh token");
                    return Err(LlmCallError::Unauthorized);
                }
                refreshed = true;
                continue;
            }
            if status.is_success() {
                return Ok(response);
            }
            let retry_after = super::parse_retry_after(response.headers());
            let request_id = super::request_id(response.headers());
            let body = response.text().await.unwrap_or_default();
            let api_error = super::parse_api_error(&body);
            let err =
                super::classify_error_status(status.as_u16(), retry_after, api_error.as_ref());
            let api_error = api_error.unwrap_or_default();
            tracing::warn!(
                what,
                status = status.as_u16(),
                request_id = request_id.as_deref(),
                error_code = api_error.code.as_deref(),
                error_type = api_error.kind.as_deref(),
                outcome = err.outcome_label(),
                "OpenAI Batch API call failed"
            );
            return Err(err);
        }
    }

    async fn openai_json<T: serde::de::DeserializeOwned>(
        &self,
        what: &'static str,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<T, LlmCallError> {
        self.openai_api_send(what, build)
            .await?
            .json()
            .await
            .map_err(|err| LlmCallError::Other(err.into()))
    }

    /// `POST {base}/files` (multipart, `purpose=batch`, expiring after
    /// [`FILE_TTL_SECS`]). Returns the file id.
    pub(crate) async fn upload_batch_file(&self, jsonl: String) -> Result<String, LlmCallError> {
        let bytes = jsonl.into_bytes();
        let url = format!("{}/files", self.base_url);
        let file: FileObject = self
            .openai_json("upload", || {
                let part = reqwest::multipart::Part::bytes(bytes.clone())
                    .file_name("enricher-batch.jsonl")
                    .mime_str("application/jsonl")
                    .unwrap_or_else(|_| reqwest::multipart::Part::bytes(bytes.clone()));
                let form = reqwest::multipart::Form::new()
                    .text("purpose", "batch")
                    .text("expires_after[anchor]", "created_at")
                    .text("expires_after[seconds]", FILE_TTL_SECS.to_string())
                    .part("file", part);
                self.http.post(&url).multipart(form)
            })
            .await?;
        record_file_event("openai", "uploaded");
        Ok(file.id)
    }

    /// Uploads the input file and creates a batch on it. A failed create
    /// deletes the file it uploaded (best effort).
    pub(crate) async fn create_openai_batch(
        &self,
        requests: &[BatchRequest],
        meta: BatchMeta<'_>,
    ) -> Result<CreatedBatch, LlmCallError> {
        let jsonl = batch_jsonl(requests)?;
        let file_id = self.upload_batch_file(jsonl).await?;
        let body = serde_json::json!({
            "input_file_id": file_id,
            "endpoint": ENDPOINT,
            "completion_window": "24h",
            "output_expires_after": { "anchor": "created_at", "seconds": FILE_TTL_SECS },
            "metadata": {
                "source": "distant-signal-enricher",
                "stage": meta.stage,
                "model_version": meta
                    .model_version
                    .chars()
                    .take(MAX_METADATA_CHARS)
                    .collect::<String>(),
            },
        });
        let url = format!("{}/batches", self.base_url);
        match self
            .openai_json::<OpenAiBatch>("create", || self.http.post(&url).json(&body))
            .await
        {
            Ok(batch) => Ok(CreatedBatch {
                id: batch.id,
                files: vec![file_id],
            }),
            Err(err) => {
                // Nothing will ever read the file: don't leave it behind.
                self.delete_batch_files(std::slice::from_ref(&file_id))
                    .await;
                Err(err)
            }
        }
    }

    /// `GET {base}/batches/{id}`.
    pub(crate) async fn get_openai_batch(&self, id: &str) -> Result<OpenAiBatch, LlmCallError> {
        let url = format!("{}/batches/{id}", self.base_url);
        self.openai_json("retrieve", || self.http.get(&url)).await
    }

    /// `POST {base}/batches/{id}/cancel`: the batch goes `cancelling` (up
    /// to 10 minutes), then `cancelled` with whatever had finished.
    pub(crate) async fn cancel_openai_batch(&self, id: &str) -> Result<(), LlmCallError> {
        let url = format!("{}/batches/{id}/cancel", self.base_url);
        self.openai_api_send("cancel", || self.http.post(&url))
            .await
            .map(|_| ())
    }

    /// `GET {base}/files/{id}/content`.
    pub(crate) async fn openai_file_content(&self, id: &str) -> Result<String, LlmCallError> {
        let url = format!("{}/files/{id}/content", self.base_url);
        self.openai_api_send("download", || self.http.get(&url))
            .await?
            .text()
            .await
            .map_err(|err| LlmCallError::Other(err.into()))
    }

    /// `DELETE {base}/files/{id}`.
    pub(crate) async fn delete_openai_file(&self, id: &str) -> Result<(), LlmCallError> {
        let url = format!("{}/files/{id}", self.base_url);
        self.openai_api_send("delete", || self.http.delete(&url))
            .await
            .map(|_| ())
    }

    /// One poll: the status mapped onto [`BatchPoll`], and once ended the
    /// output and error files merged by `custom_id`.
    pub(crate) async fn poll_openai_batch(&self, id: &str) -> Result<BatchPoll, LlmCallError> {
        let batch = match self.get_openai_batch(id).await {
            Ok(batch) => batch,
            Err(LlmCallError::Status { status: 404 }) => return Ok(BatchPoll::Gone),
            Err(err) => return Err(err),
        };
        let unanswered = match batch.status.as_str() {
            "validating" | "in_progress" | "finalizing" | "cancelling" => {
                return Ok(BatchPoll::Pending);
            }
            "failed" => {
                return Ok(BatchPoll::ValidationFailed {
                    reason: validation_reason(&batch),
                    files: batch.input_file_id.into_iter().collect(),
                });
            }
            "completed" => None,
            "expired" => Some(BatchOutcome::Expired),
            "cancelled" => Some(BatchOutcome::Canceled),
            other => {
                tracing::warn!(
                    batch_id = id,
                    status = other,
                    "unknown OpenAI batch status; polling again later"
                );
                return Ok(BatchPoll::Pending);
            }
        };
        let mut outcomes = HashMap::new();
        for file in [&batch.output_file_id, &batch.error_file_id]
            .into_iter()
            .flatten()
        {
            match self.openai_file_content(file).await {
                Ok(text) => parse_results_jsonl(&text, &mut outcomes),
                Err(LlmCallError::Status { status: 404 }) => return Ok(BatchPoll::ResultsGone),
                Err(err) => return Err(err),
            }
        }
        let files = [
            batch.input_file_id,
            batch.output_file_id,
            batch.error_file_id,
        ]
        .into_iter()
        .flatten()
        .collect();
        Ok(BatchPoll::Ended(EndedBatch {
            results: BatchResults {
                outcomes,
                unanswered,
            },
            summary: format!(
                "{}, {}",
                batch.status,
                batch.request_counts.unwrap_or(Value::Null)
            ),
            files,
        }))
    }
}

/// Mocked-HTTP tests of the whole lifecycle.
#[cfg(test)]
#[expect(
    clippy::too_many_lines,
    reason = "test code: scenario tests read top to bottom"
)]
mod tests;
