//! Client for the generic OpenAI-compatible Chat Completions REST API.
//! Deliberately vendor-agnostic: `base_url`/`api_key`/`model` are the only
//! things that vary between a local llama.cpp/vLLM/Ollama server and any
//! hosted provider that speaks the same schema.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduleWindow {
    /// ISO 8601 weekday numbers, 1 (Monday) through 7 (Sunday).
    pub days_of_week: Vec<u8>,
    /// "HH:MM", 24-hour, Europe/London local time.
    pub start_time: String,
    pub end_time: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PrimaryExtraction {
    pub category: String,
    pub resolution_status: String,
    pub schedule_window: Option<ScheduleWindow>,
    pub eta: Option<DateTime<Utc>>,
}

pub struct LlmClient {
    base_url: String,
    api_key: Option<String>,
    model: String,
    http: reqwest::Client,
}

#[derive(Serialize)]
struct ChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage>,
    response_format: ResponseFormat,
    temperature: f32,
}

#[derive(Serialize)]
struct ChatMessage {
    role: &'static str,
    content: String,
}

#[derive(Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    kind: &'static str,
    json_schema: JsonSchemaSpec,
}

#[derive(Serialize)]
struct JsonSchemaSpec {
    name: &'static str,
    strict: bool,
    schema: serde_json::Value,
}

#[derive(Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Deserialize)]
struct ChatChoiceMessage {
    content: String,
}

const PRIMARY_SCHEMA_NAME: &str = "incident_extraction";

fn primary_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "category": { "type": "string" },
            "resolution_status": { "type": "string", "enum": ["ongoing", "residual", "resolved"] },
            "schedule_window": {
                "type": ["object", "null"],
                "properties": {
                    "days_of_week": { "type": "array", "items": { "type": "integer", "minimum": 1, "maximum": 7 } },
                    "start_time": { "type": "string" },
                    "end_time": { "type": "string" }
                },
                "required": ["days_of_week", "start_time", "end_time"]
            },
            "eta": { "type": ["string", "null"] }
        },
        "required": ["category", "resolution_status", "schedule_window", "eta"]
    })
}

const PRIMARY_PROMPT: &str = "You extract structured facts from UK National Rail Knowledgebase incident \
    text. Read the summary and description exactly as given -- do not speculate beyond what the text \
    states. `resolution_status` is `resolved` only if the text explicitly says the disruption/root cause \
    has ended; `residual` if it says the cause is fixed but knock-on effects continue; `ongoing` otherwise, \
    including whenever the text doesn't clearly say either way. `schedule_window` and `eta` are null unless \
    the text states them explicitly.";

const ADVERSARIAL_SCHEMA_NAME: &str = "adversarial_resolution_check";

fn adversarial_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "resolution_status": { "type": "string", "enum": ["ongoing", "residual", "resolved"] }
        },
        "required": ["resolution_status"]
    })
}

const ADVERSARIAL_PROMPT: &str = "You are reviewing a UK National Rail incident report with a specific \
    job: argue for the most cautious reading. Assume the disruption is still `ongoing` unless the text \
    gives you clear, explicit, unambiguous evidence otherwise. Do not infer resolution from silence, from \
    a lack of new updates, or from an optimistic tone -- only from an explicit statement that the issue is \
    fixed or over.";

#[derive(Deserialize)]
struct AdversarialExtraction {
    resolution_status: String,
}

/// Per-request ceiling on an LLM call. reqwest applies NO request timeout by
/// default, and both callers of this client -- the stream consumer loop and
/// the hourly sweep -- process incidents strictly serially, so a single hung
/// endpoint would stall ALL enrichment indefinitely rather than just losing
/// one incident. 60s is generous for a two-field structured extraction over
/// a short incident summary while still bounding the damage; a timed-out
/// request surfaces as an ordinary `Err`, which `process_incident` already
/// logs and moves past, leaving the incident for the next sweep.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

impl LlmClient {
    pub fn new(base_url: String, api_key: Option<String>, model: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            // Only fails if the TLS backend can't initialize, which would
            // break every request anyway -- there is no useful degraded mode.
            .expect("reqwest client with a timeout must build");
        Self { base_url, api_key, model, http }
    }

    async fn chat_completion(&self, system_prompt: &str, user_content: String, schema_name: &'static str, schema: serde_json::Value) -> anyhow::Result<String> {
        let request = ChatCompletionRequest {
            model: &self.model,
            messages: vec![
                ChatMessage { role: "system", content: system_prompt.to_string() },
                ChatMessage { role: "user", content: user_content },
            ],
            response_format: ResponseFormat {
                kind: "json_schema",
                json_schema: JsonSchemaSpec { name: schema_name, strict: true, schema },
            },
            temperature: 0.0,
        };

        let mut req = self.http.post(format!("{}/chat/completions", self.base_url)).json(&request);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }

        let response = req.send().await?.error_for_status()?;
        let body: ChatCompletionResponse = response.json().await?;
        let content = body
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("chat completion response had no choices"))?
            .message
            .content;
        Ok(content)
    }

    pub async fn extract_primary(&self, summary: &str, description: &str) -> anyhow::Result<PrimaryExtraction> {
        let user_content = format!("Summary: {summary}\nDescription: {description}");
        let content = self
            .chat_completion(PRIMARY_PROMPT, user_content, PRIMARY_SCHEMA_NAME, primary_schema())
            .await?;
        let extraction: PrimaryExtraction = serde_json::from_str(&content)
            .map_err(|err| anyhow::anyhow!("primary extraction returned malformed JSON: {err}"))?;
        Ok(extraction)
    }

    pub async fn extract_adversarial(&self, summary: &str, description: &str) -> anyhow::Result<String> {
        let user_content = format!("Summary: {summary}\nDescription: {description}");
        let content = self
            .chat_completion(ADVERSARIAL_PROMPT, user_content, ADVERSARIAL_SCHEMA_NAME, adversarial_schema())
            .await?;
        let extraction: AdversarialExtraction = serde_json::from_str(&content)
            .map_err(|err| anyhow::anyhow!("adversarial extraction returned malformed JSON: {err}"))?;
        Ok(extraction.resolution_status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn extract_primary_parses_a_well_formed_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "signal_failure",
                            "resolution_status": "resolved",
                            "schedule_window": null,
                            "eta": null
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(server.uri(), None, "test-model".to_string());
        let result = client.extract_primary("Signal failure at Reading", "Now resolved").await.unwrap();

        assert_eq!(result.category, "signal_failure");
        assert_eq!(result.resolution_status, "resolved");
        assert_eq!(result.schedule_window, None);
        assert_eq!(result.eta, None);
    }

    #[tokio::test]
    async fn extract_primary_parses_a_schedule_window() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "rail_replacement",
                            "resolution_status": "ongoing",
                            "schedule_window": {
                                "days_of_week": [1, 2, 3, 4, 5],
                                "start_time": "22:00",
                                "end_time": "06:00"
                            },
                            "eta": null
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(server.uri(), None, "test-model".to_string());
        let result = client
            .extract_primary("Rail replacement buses", "Nightly 22:00-06:00")
            .await
            .unwrap();

        assert_eq!(
            result.schedule_window,
            Some(ScheduleWindow { days_of_week: vec![1, 2, 3, 4, 5], start_time: "22:00".to_string(), end_time: "06:00".to_string() })
        );
    }

    #[tokio::test]
    async fn extract_primary_fails_on_malformed_content() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{ "message": { "content": "not valid json" } }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(server.uri(), None, "test-model".to_string());
        let result = client.extract_primary("Signal failure", "Delays").await;

        assert!(result.is_err(), "malformed content must be rejected, not silently stored");
    }
}
