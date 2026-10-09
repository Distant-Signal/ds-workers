//! The Claude API (`LLM_PROVIDER=anthropic`): the Messages API request and
//! response mapping, and the Message Batches API client. See
//! docs/enricher-anthropic.md.
//!
//! Claude API documentation this relies on (read 2026-10-09):
//!
//! - Messages: <https://platform.claude.com/docs/en/api/messages/create>
//! - Structured outputs (`output_config.format`):
//!   <https://platform.claude.com/docs/en/build-with-claude/structured-outputs>
//! - Prompt caching:
//!   <https://platform.claude.com/docs/en/build-with-claude/prompt-caching>
//! - Message Batches:
//!   <https://platform.claude.com/docs/en/build-with-claude/batch-processing>
//! - Errors: <https://platform.claude.com/docs/en/api/errors>
//!
//! Raw HTTP on the service's own `reqwest` client: there is no official
//! Rust SDK, and the request is three fields plus a schema.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{LlmCallError, LlmClient, ProviderPolicy, TokenUsage};

/// `LLM_BASE_URL` when `LLM_PROVIDER=anthropic` and none is set. Like the
/// `OpenAI` convention it includes `/v1`: requests go to `{base}/messages`
/// and `{base}/messages/batches`.
pub(crate) const DEFAULT_BASE_URL: &str = "https://api.anthropic.com/v1";

/// `LLM_MODEL` when `LLM_PROVIDER=anthropic` and none is set: Claude
/// Haiku 5.5, the current Haiku (released 2026-10-07) and the model the
/// Claude docs position for high-volume extraction, at $0.10/$0.50 per
/// million input/output tokens -- the same price point as the `OpenAI`
/// candidate (`gpt-6-luna`). Claude Sonnet 5.5 (`claude-sonnet-5-5`,
/// $2/$10) is the documented step-up if the quality eval shows misses.
/// See docs/enricher-anthropic.md, "Model".
pub(crate) const DEFAULT_MODEL: &str = "claude-haiku-5-5";

/// The `anthropic-version` header (`LLM_ANTHROPIC_VERSION`).
pub(crate) const DEFAULT_VERSION: &str = "2023-06-01";

/// `max_tokens` when `LLM_MAX_TOKENS` is unset. The Messages API requires
/// the field (`OpenAI`'s may be omitted). Adaptive thinking counts against
/// it, so it is generous: the visible output is a few hundred tokens. It
/// stays under the size where a non-streaming request risks the API's
/// long-request limits.
pub(crate) const DEFAULT_MAX_TOKENS: u32 = 16_000;

/// `LLM_PROMPT_CACHE`: whether, and for how long, the static system prompt
/// is marked for prompt caching (docs/enricher-anthropic.md, "Prompt
/// caching"). `OpenAI` caches automatically, so this only affects the Claude
/// API.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
pub(crate) enum PromptCache {
    /// No `cache_control` marker.
    #[serde(rename = "off")]
    #[value(name = "off")]
    Off,
    /// The default 5-minute TTL (write 1.25x, read 0.1x or less of the
    /// input price).
    #[serde(rename = "5m")]
    #[value(name = "5m")]
    FiveMinutes,
    /// The 1-hour TTL (write 2x): the default, because the enricher's
    /// primary calls are minutes apart (see the doc's analysis).
    #[default]
    #[serde(rename = "1h")]
    #[value(name = "1h")]
    OneHour,
}

impl PromptCache {
    fn marker(self) -> Option<Value> {
        match self {
            Self::Off => None,
            Self::FiveMinutes => Some(serde_json::json!({ "type": "ephemeral" })),
            Self::OneHour => Some(serde_json::json!({ "type": "ephemeral", "ttl": "1h" })),
        }
    }
}

/// The Claude-specific request settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AnthropicSettings {
    /// The `anthropic-version` header.
    pub version: String,
    pub prompt_cache: PromptCache,
    /// Sent as `thinking: {"type": <this>}` when set (`LLM_THINKING`, e.g.
    /// `adaptive`, `disabled` on Haiku 5.5, `between_tools` on Sonnet 5.5).
    /// Unset sends no `thinking`: the model's default (adaptive on every
    /// current model).
    pub thinking: Option<String>,
}

impl Default for AnthropicSettings {
    fn default() -> Self {
        Self {
            version: DEFAULT_VERSION.to_string(),
            prompt_cache: PromptCache::default(),
            thinking: None,
        }
    }
}

/// Keywords Claude's structured outputs reject with a 400 (structured
/// outputs guide, "JSON Schema limitations"). They are moved into the
/// node's `description` instead, as the official SDKs do; the enricher's
/// parsers never relied on them (the one use, `days_of_week`'s 1..=7, is a
/// hint to the model, not a validation the service needs).
const UNSUPPORTED_KEYWORDS: [&str; 8] = [
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "maxItems",
];

/// The enricher's schemas (written in `OpenAI`'s strict subset, see
/// `llm::primary_schema`) rewritten into what Claude's structured outputs
/// accept: a nullable scalar's type union `["string", "null"]` (rejected by
/// Claude) becomes `anyOf: [{"type": "string", ...}, {"type": "null"}]`,
/// with an `enum` split between the branches, and numeric/string
/// constraints become description text. The JSON a model emits is the same
/// shape, so the parsers are unchanged.
pub(crate) fn claude_schema(schema: &Value) -> Value {
    let Value::Object(map) = schema else {
        return match schema {
            Value::Array(items) => Value::Array(items.iter().map(claude_schema).collect()),
            other => other.clone(),
        };
    };
    let mut out = Map::new();
    let mut notes = Vec::new();
    for (key, value) in map {
        if UNSUPPORTED_KEYWORDS.contains(&key.as_str()) {
            notes.push(format!("{key}: {value}"));
            continue;
        }
        let converted = match (key.as_str(), value) {
            // Property NAMES are not keywords: convert each property's schema.
            ("properties", Value::Object(properties)) => Value::Object(
                properties
                    .iter()
                    .map(|(name, property)| (name.clone(), claude_schema(property)))
                    .collect(),
            ),
            _ => claude_schema(value),
        };
        out.insert(key.clone(), converted);
    }
    if !notes.is_empty() {
        let note = notes.join(", ");
        let description = match out.get("description").and_then(Value::as_str) {
            Some(existing) => format!("{existing} ({note})"),
            None => note,
        };
        out.insert("description".to_string(), Value::String(description));
    }
    if let Some(Value::Array(types)) = out.get("type").cloned() {
        out.remove("type");
        let description = out.remove("description");
        let enum_values = out.remove("enum");
        // Everything else (`items`, `properties`, ...) belongs to the
        // non-null branches.
        let rest = std::mem::take(&mut out);
        let branches = types
            .iter()
            .map(|kind| {
                let mut branch = Map::new();
                branch.insert("type".to_string(), kind.clone());
                if kind != "null" {
                    if let Some(Value::Array(values)) = &enum_values {
                        branch.insert(
                            "enum".to_string(),
                            Value::Array(values.iter().filter(|v| !v.is_null()).cloned().collect()),
                        );
                    }
                    branch.extend(rest.clone());
                }
                Value::Object(branch)
            })
            .collect();
        out.insert("anyOf".to_string(), Value::Array(branches));
        if let Some(description) = description {
            out.insert("description".to_string(), description);
        }
    }
    Value::Object(out)
}

/// The Messages API body for one call: the same for a synchronous request
/// and for a Message Batches request's `params`.
///
/// - `system` is one text block, marked `cache_control` per
///   [`PromptCache`]. It is the only static prefix (render order is
///   `tools` -> `system` -> `messages`; the enricher sends no tools), and
///   the per-incident text is entirely in the user message after it.
/// - No `temperature`: the current models reject a non-default one
///   (Sonnet 5.5) or say to omit it (Haiku 5.5, Opus 5.5). Repeat runs on
///   the same text are therefore not guaranteed identical.
/// - `output_config.format` carries the JSON schema ([`claude_schema`]);
///   `output_config.effort` is `LLM_REASONING_EFFORT` when set.
/// - `max_tokens` is `LLM_MAX_TOKENS`, else [`DEFAULT_MAX_TOKENS`].
pub(crate) fn messages_body(
    model: &str,
    settings: &AnthropicSettings,
    policy: &ProviderPolicy,
    sampling: super::Sampling,
    system_prompt: &str,
    user_content: &str,
    schema: &Value,
) -> Value {
    let mut system = serde_json::json!({ "type": "text", "text": system_prompt });
    if let Some(marker) = settings.prompt_cache.marker() {
        system["cache_control"] = marker;
    }
    let mut output_config = serde_json::json!({
        "format": { "type": "json_schema", "schema": claude_schema(schema) }
    });
    if let Some(effort) = &policy.reasoning_effort {
        output_config["effort"] = Value::String(effort.clone());
    }
    let mut body = serde_json::json!({
        "model": model,
        "max_tokens": policy.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        "system": [system],
        "messages": [{ "role": "user", "content": user_content }],
        "output_config": output_config,
    });
    if let Some(thinking) = &settings.thinking {
        body["thinking"] = serde_json::json!({ "type": thinking });
    }
    // Only when a profile sets them (none of the built-in Claude profiles
    // does; `profile::resolve` refuses them for models that reject them).
    if let Some(temperature) = sampling.temperature {
        body["temperature"] = serde_json::json!(temperature);
    }
    if let Some(top_p) = sampling.top_p {
        body["top_p"] = serde_json::json!(top_p);
    }
    body
}

/// Token counts from a Messages response's `usage`, in the enricher's
/// kinds: `prompt` is every input token (`input_tokens`, which excludes
/// cached ones, plus `cache_creation_input_tokens` plus
/// `cache_read_input_tokens`), `cached` the cache reads, `cache_write` the
/// cache writes, `completion` `output_tokens` (thinking included: Claude
/// doesn't report it separately, so `reasoning` stays unset). Tolerant like
/// `TokenUsage::from_response`.
pub(crate) fn usage_from_message(usage: &Value) -> Option<TokenUsage> {
    let count = |key: &str| usage.get(key).and_then(Value::as_u64);
    let input = count("input_tokens");
    let cache_write = count("cache_creation_input_tokens");
    let cache_read = count("cache_read_input_tokens");
    let prompt = (input.is_some() || cache_write.is_some() || cache_read.is_some())
        .then(|| input.unwrap_or(0) + cache_write.unwrap_or(0) + cache_read.unwrap_or(0));
    let parsed = TokenUsage {
        prompt_tokens: prompt,
        completion_tokens: count("output_tokens"),
        reasoning_tokens: None,
        cached_tokens: cache_read,
        cache_write_tokens: cache_write,
    };
    (parsed != TokenUsage::default()).then_some(parsed)
}

/// Longest refusal explanation kept.
const MAX_EXPLANATION_CHARS: usize = 200;

/// A Messages response (a synchronous body, or a batch result's `message`):
/// its usage (`None` if absent), and its JSON text or the typed failure:
///
/// - `stop_reason: "refusal"` is [`LlmCallError::Refused`] (with
///   `stop_details.category`/`explanation` when sent);
/// - `stop_reason: "max_tokens"` is [`LlmCallError::EmptyContent`] with
///   `finish_reason: "max_tokens"`, even with partial text: the JSON is cut
///   off (structured outputs guide);
/// - no text block, or only whitespace, is `EmptyContent` too.
///
/// `thinking` blocks are skipped; the text blocks are joined.
pub(crate) fn completion_from_message(
    message: &Value,
) -> (Option<TokenUsage>, Result<String, LlmCallError>) {
    let usage = message.get("usage").and_then(usage_from_message);
    let stop_reason = message
        .get("stop_reason")
        .and_then(Value::as_str)
        .map(str::to_string);
    if stop_reason.as_deref() == Some("refusal") {
        let details = message.get("stop_details");
        let field = |key: &str| {
            details
                .and_then(|d| d.get(key))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        let explanation: Option<String> = field("explanation")
            .map(|text| text.chars().take(MAX_EXPLANATION_CHARS).collect::<String>());
        let refusal = match (field("category"), explanation) {
            (Some(category), Some(explanation)) => format!("{category}: {explanation}"),
            (Some(category), None) => category,
            (None, Some(explanation)) => explanation,
            (None, None) => "refusal".to_string(),
        };
        return (usage, Err(LlmCallError::Refused { refusal }));
    }
    let Some(blocks) = message.get("content").and_then(Value::as_array) else {
        return (
            usage,
            Err(LlmCallError::Other(anyhow::anyhow!(
                "Messages response had no content array"
            ))),
        );
    };
    let text: String = blocks
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect();
    if stop_reason.as_deref() == Some("max_tokens") || text.trim().is_empty() {
        return (
            usage,
            Err(LlmCallError::EmptyContent {
                finish_reason: stop_reason,
            }),
        );
    }
    (usage, Ok(text))
}

/// Classifies a non-2xx Claude API response (errors guide):
///
/// - 429 `rate_limit_error`: [`LlmCallError::RateLimited`], honouring
///   `retry-after` like `OpenAI`'s. (A usage-tier spend cap is also a 429,
///   with no `retry-after`; it is retried like a rate limit and then fails.)
/// - 402 `billing_error`: [`LlmCallError::QuotaExhausted`], never retried.
/// - 500 `api_error`, 504 `timeout_error`, 529 `overloaded_error` (and any
///   502/503 from a proxy): [`LlmCallError::GatewayUnavailable`], retried
///   under `LLM_GATEWAY_RETRIES` with `retry-after` when sent.
/// - anything else (400, 401, 403, 404, 413): [`LlmCallError::Status`].
pub(crate) fn classify_error_status(
    status: u16,
    retry_after: Option<std::time::Duration>,
) -> LlmCallError {
    match status {
        429 => LlmCallError::RateLimited { retry_after },
        402 => LlmCallError::QuotaExhausted {
            code: "billing_error".to_string(),
        },
        500 | 502..=504 | 529 => LlmCallError::GatewayUnavailable {
            status,
            retry_after,
        },
        _ => LlmCallError::Status { status },
    }
}

// ---------------------------------------------------------------------------
// Message Batches API
// ---------------------------------------------------------------------------

/// One request of a Message Batch. `custom_id` must match
/// `^[a-zA-Z0-9_-]{1,64}$`; results come back in any order and are matched
/// by it.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BatchRequest {
    pub custom_id: String,
    pub params: Value,
}

/// A Message Batch object (create and retrieve responses).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct MessageBatch {
    pub id: String,
    /// `in_progress`, `canceling` or `ended`.
    pub processing_status: String,
    /// Set once the batch has ended.
    #[serde(default)]
    pub results_url: Option<String>,
    #[serde(default)]
    pub request_counts: RequestCounts,
}

impl MessageBatch {
    pub(crate) fn has_ended(&self) -> bool {
        self.processing_status == "ended"
    }
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub(crate) struct RequestCounts {
    #[serde(default)]
    pub processing: u64,
    #[serde(default)]
    pub succeeded: u64,
    #[serde(default)]
    pub errored: u64,
    #[serde(default)]
    pub canceled: u64,
    #[serde(default)]
    pub expired: u64,
}

/// One batch request's result.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BatchOutcome {
    /// The Messages response, to read with [`completion_from_message`].
    Succeeded(Value),
    /// No message was created (an invalid request or a server error); not
    /// billed. `kind` is the error's `type` (e.g. `invalid_request_error`).
    Errored {
        kind: Option<String>,
        message: Option<String>,
    },
    /// The batch was canceled before this request ran; not billed.
    Canceled,
    /// The batch hit its 24-hour expiry before this request ran; not billed.
    Expired,
}

impl BatchOutcome {
    /// The `result` label of `enricher_llm_batch_requests_total`.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Succeeded(_) => "succeeded",
            Self::Errored { .. } => "errored",
            Self::Canceled => "canceled",
            Self::Expired => "expired",
        }
    }
}

/// Parses a results file (`results_url`, JSONL: one
/// `{"custom_id", "result": {"type", ...}}` per line) into outcomes by
/// `custom_id`. A line that doesn't parse is logged and skipped: its
/// request then counts as missing, never as a crash of the whole batch.
pub(crate) fn parse_results_jsonl(text: &str) -> HashMap<String, BatchOutcome> {
    let mut outcomes = HashMap::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let parsed: Option<(String, BatchOutcome)> =
            serde_json::from_str::<Value>(line).ok().and_then(|value| {
                let custom_id = value.get("custom_id")?.as_str()?.to_string();
                let result = value.get("result")?;
                let outcome = match result.get("type")?.as_str()? {
                    "succeeded" => BatchOutcome::Succeeded(result.get("message")?.clone()),
                    "errored" => {
                        // `result.error` is an error response body:
                        // `{"type": "error", "error": {"type", "message"}}`.
                        let error = result.get("error");
                        let inner = error.and_then(|e| e.get("error")).or(error);
                        let text = |key: &str| {
                            inner
                                .and_then(|e| e.get(key))
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        };
                        BatchOutcome::Errored {
                            kind: text("type"),
                            message: text("message"),
                        }
                    }
                    "canceled" => BatchOutcome::Canceled,
                    "expired" => BatchOutcome::Expired,
                    _ => return None,
                };
                Some((custom_id, outcome))
            });
        if let Some((custom_id, outcome)) = parsed {
            outcomes.insert(custom_id, outcome);
        } else {
            tracing::warn!(
                line = %line.chars().take(200).collect::<String>(),
                "unparseable Message Batch result line; skipping it"
            );
        }
    }
    outcomes
}

impl LlmClient {
    /// The Claude settings, or an error naming the misconfiguration (the
    /// batch API is only used with `LLM_PROVIDER=anthropic`, which config
    /// validation enforces).
    fn anthropic(&self) -> Result<&AnthropicSettings, LlmCallError> {
        match &self.provider {
            super::Provider::Anthropic(settings) => Ok(settings),
            super::Provider::OpenAi => Err(LlmCallError::Other(anyhow::anyhow!(
                "the Message Batches API needs LLM_PROVIDER=anthropic"
            ))),
        }
    }

    /// The Messages body for `spec`, as a batch request's `params`.
    pub(crate) fn batch_params(&self, spec: &super::CallSpec) -> Result<Value, LlmCallError> {
        let settings = self.anthropic()?;
        Ok(messages_body(
            &self.model,
            settings,
            &self.policy,
            self.sampling,
            &spec.system_prompt,
            &spec.user_content,
            &spec.schema,
        ))
    }

    /// Sends one batch-API request: the version header and credential, and
    /// a non-2xx classified like a Messages call's (a 404 is
    /// `Status { status: 404 }`).
    async fn batch_api_send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, LlmCallError> {
        let settings = self.anthropic()?;
        let mut request = request.header("anthropic-version", &settings.version);
        if let Some(credential) = self
            .auth
            .credential(crate::auth::StaticKeyHeader::XApiKey)
            .await?
        {
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
        if status.is_success() {
            return Ok(response);
        }
        let retry_after = super::parse_retry_after(response.headers());
        let request_id = super::request_id(response.headers());
        let body = response.text().await.unwrap_or_default();
        let api_error = super::parse_api_error(&body).unwrap_or_default();
        let err = classify_error_status(status.as_u16(), retry_after);
        tracing::warn!(
            status = status.as_u16(),
            request_id = request_id.as_deref(),
            error_type = api_error.kind.as_deref(),
            outcome = err.outcome_label(),
            "Message Batches API call failed"
        );
        Err(err)
    }

    /// `POST {base}/messages/batches`.
    pub(crate) async fn create_message_batch(
        &self,
        requests: &[BatchRequest],
    ) -> Result<MessageBatch, LlmCallError> {
        let request = self
            .http
            .post(format!("{}/messages/batches", self.base_url))
            .json(&serde_json::json!({ "requests": requests }));
        let response = self.batch_api_send(request).await?;
        response
            .json()
            .await
            .map_err(|err| LlmCallError::Other(err.into()))
    }

    /// `GET {base}/messages/batches/{id}`.
    pub(crate) async fn get_message_batch(&self, id: &str) -> Result<MessageBatch, LlmCallError> {
        let request = self
            .http
            .get(format!("{}/messages/batches/{id}", self.base_url));
        let response = self.batch_api_send(request).await?;
        response
            .json()
            .await
            .map_err(|err| LlmCallError::Other(err.into()))
    }

    /// `POST {base}/messages/batches/{id}/cancel`: best effort, for a batch
    /// whose results the enricher would discard anyway.
    pub(crate) async fn cancel_message_batch(&self, id: &str) -> Result<(), LlmCallError> {
        let request = self
            .http
            .post(format!("{}/messages/batches/{id}/cancel", self.base_url));
        self.batch_api_send(request).await.map(|_| ())
    }

    /// Downloads an ended batch's `results_url` and parses it
    /// ([`parse_results_jsonl`]). The URL comes from the API's response and
    /// the request carries the credential, so a URL on any other host than
    /// `LLM_BASE_URL`'s is refused rather than sent the key.
    pub(crate) async fn message_batch_results(
        &self,
        results_url: &str,
    ) -> Result<HashMap<String, BatchOutcome>, LlmCallError> {
        let host = |url: &str| {
            reqwest::Url::parse(url).ok().map(|u| {
                (
                    u.scheme().to_string(),
                    u.host_str().map(str::to_string),
                    u.port_or_known_default(),
                )
            })
        };
        if host(results_url).is_none() || host(results_url) != host(&self.base_url) {
            return Err(LlmCallError::Other(anyhow::anyhow!(
                "Message Batch results_url {results_url:?} is not on LLM_BASE_URL's host; \
                 refusing to send the credential there"
            )));
        }
        let response = self.batch_api_send(self.http.get(results_url)).await?;
        let text = response
            .text()
            .await
            .map_err(|err| LlmCallError::Other(err.into()))?;
        Ok(parse_results_jsonl(&text))
    }
}

/// Mocked-HTTP fixtures for the Messages and Message Batches routes.
#[cfg(test)]
#[expect(
    clippy::too_many_lines,
    reason = "test code: scenario tests read top to bottom"
)]
mod tests;
