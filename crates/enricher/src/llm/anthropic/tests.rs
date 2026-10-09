use super::*;
use crate::llm::PrimaryExtraction;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const KEY: &str = "sk-ant-test";

fn client(server: &MockServer, settings: AnthropicSettings) -> LlmClient {
    LlmClient::new(
        server.uri(),
        Some(KEY.to_string()),
        "claude-haiku-5-5".to_string(),
        std::time::Duration::from_secs(30),
    )
    .with_anthropic(settings)
}

fn fast_policy() -> ProviderPolicy {
    ProviderPolicy {
        max_tokens: Some(8192),
        reasoning_effort: Some("low".to_string()),
        max_in_flight: Some(2),
        rate_limit_min_wait: std::time::Duration::from_millis(10),
        max_rate_limit_retries: 2,
        max_gateway_retries: 2,
        gateway_backoff: std::time::Duration::from_millis(10),
    }
}

fn reference_date() -> chrono::DateTime<chrono::Utc> {
    "2026-10-01T08:00:00Z".parse().unwrap()
}

fn primary_text() -> String {
    serde_json::json!({
        "category": "engineering_works",
        "periods": [{
            "scope_description": null,
            "date_range": { "from_date": "2026-10-03T23:00:00Z", "to_date": null },
            "schedule_window": { "days_of_week": [6, 7], "start_time": "00:00", "end_time": "23:59" },
            "resolution_status": "ongoing",
            "apparent_severity": "blocked_or_suspended",
            "impact_type": "rail_replacement_bus"
        }]
    })
    .to_string()
}

/// A Messages API response (the API reference's shape), with an (omitted)
/// thinking block first, as adaptive thinking returns.
fn message_body(text: &str, usage: Value) -> Value {
    serde_json::json!({
        "id": "msg_01",
        "type": "message",
        "role": "assistant",
        "model": "claude-haiku-5-5",
        "content": [
            { "type": "thinking", "thinking": "", "signature": "sig" },
            { "type": "text", "text": text }
        ],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "stop_details": null,
        "usage": usage
    })
}

fn plain_usage() -> Value {
    serde_json::json!({ "input_tokens": 3400, "output_tokens": 320 })
}

fn error_body(kind: &str) -> Value {
    serde_json::json!({
        "type": "error",
        "error": { "type": kind, "message": "fixture" },
        "request_id": "req_fixture"
    })
}

async fn sent_bodies(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .collect()
}

async fn mount_success(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(message_body(&primary_text(), plain_usage())),
        )
        .mount(server)
        .await;
}

/// The normal route: `POST {base}/messages` with `x-api-key` and
/// `anthropic-version`, the system prompt as a cached block, the schema in
/// `output_config.format`, no `temperature`, and the text block (not the
/// thinking block) parsed.
#[tokio::test]
async fn messages_request_shape_and_successful_parse() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .and(header("x-api-key", KEY))
        .and(header("anthropic-version", DEFAULT_VERSION))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(message_body(&primary_text(), plain_usage())),
        )
        .mount(&server)
        .await;
    let client = client(&server, AnthropicSettings::default());
    let primary: PrimaryExtraction = client
        .extract_primary(
            "Engineering works",
            "Buses replace trains",
            reference_date(),
        )
        .await
        .unwrap();
    assert_eq!(primary.category, "engineering_works");
    assert_eq!(
        primary.periods[0].impact_type.as_deref(),
        Some("rail_replacement_bus")
    );

    let requests = server.received_requests().await.unwrap();
    assert!(requests[0].headers.get("authorization").is_none());
    let body = &sent_bodies(&server).await[0];
    assert_eq!(body["model"], "claude-haiku-5-5");
    assert_eq!(body["max_tokens"], DEFAULT_MAX_TOKENS);
    assert_eq!(body["system"][0]["type"], "text");
    assert!(
        body["system"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("You extract structured facts")
    );
    assert_eq!(
        body["system"][0]["cache_control"],
        serde_json::json!({ "type": "ephemeral", "ttl": "1h" })
    );
    assert_eq!(body["messages"][0]["role"], "user");
    assert!(
        body["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("Summary: Engineering works")
    );
    assert_eq!(body["output_config"]["format"]["type"], "json_schema");
    assert!(body["output_config"].get("effort").is_none());
    for absent in [
        "temperature",
        "response_format",
        "thinking",
        "reasoning_effort",
        "call",
    ] {
        assert!(body.get(absent).is_none(), "{absent} was sent: {body}");
    }
}

#[tokio::test]
async fn policy_and_settings_map_to_effort_max_tokens_thinking_and_cache() {
    let server = MockServer::start().await;
    mount_success(&server).await;
    let client = client(
        &server,
        AnthropicSettings {
            version: "2023-06-01".to_string(),
            prompt_cache: PromptCache::Off,
            thinking: Some("disabled".to_string()),
        },
    )
    .with_provider_policy(fast_policy());
    client
        .extract_primary("s", "d", reference_date())
        .await
        .unwrap();
    let body = &sent_bodies(&server).await[0];
    assert_eq!(body["max_tokens"], 8192);
    assert_eq!(body["output_config"]["effort"], "low");
    assert_eq!(body["thinking"], serde_json::json!({ "type": "disabled" }));
    assert!(body["system"][0].get("cache_control").is_none());

    assert_eq!(
        PromptCache::FiveMinutes.marker(),
        Some(serde_json::json!({ "type": "ephemeral" }))
    );
}

/// 529 `overloaded_error` is a gateway-class error: retried under
/// `LLM_GATEWAY_RETRIES`, honouring `retry-after`, and provider-transient.
#[tokio::test]
async fn overloaded_529_is_retried_with_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(529)
                .insert_header("retry-after", "0")
                .insert_header("request-id", "req_529")
                .set_body_json(error_body("overloaded_error")),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_success(&server).await;
    let retrying =
        client(&server, AnthropicSettings::default()).with_provider_policy(fast_policy());
    let raw = retrying.primary_raw("s", "d", reference_date()).await;
    assert!(raw.content.is_ok(), "{:?}", raw.content.err());
    assert_eq!(raw.retries, 1);
    assert_eq!(raw.attempts[0].outcome, "gateway_error");

    // With no retry budget it fails, typed and provider-transient.
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(529).set_body_json(error_body("overloaded_error")))
        .mount(&server)
        .await;
    let strict = client(&server, AnthropicSettings::default());
    let err = strict
        .extract_primary("s", "d", reference_date())
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<LlmCallError>(),
        Some(LlmCallError::GatewayUnavailable { status: 529, .. })
    ));
    assert!(strict.is_provider_transient(&err));
}

#[tokio::test]
async fn rate_limit_429_is_retried_and_402_billing_and_400_are_not() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "0")
                .set_body_json(error_body("rate_limit_error")),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_success(&server).await;
    let client = client(&server, AnthropicSettings::default()).with_provider_policy(fast_policy());
    let raw = client.primary_raw("s", "d", reference_date()).await;
    assert!(raw.content.is_ok());
    assert_eq!(raw.attempts[0].outcome, "rate_limited");

    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(402).set_body_json(error_body("billing_error")))
        .mount(&server)
        .await;
    let err = client
        .primary_raw("s", "d", reference_date())
        .await
        .content
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<LlmCallError>(),
        Some(LlmCallError::QuotaExhausted { .. })
    ));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert!(client.is_provider_transient(&err));

    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(400).set_body_json(error_body("invalid_request_error")))
        .mount(&server)
        .await;
    let err = client
        .primary_raw("s", "d", reference_date())
        .await
        .content
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<LlmCallError>(),
        Some(LlmCallError::Status { status: 400 })
    ));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert!(!client.is_provider_transient(&err));
}

#[test]
fn stop_reasons_map_to_typed_errors() {
    let refusal = serde_json::json!({
        "content": [],
        "stop_reason": "refusal",
        "stop_details": { "type": "refusal", "category": "cyber", "explanation": "declined" },
        "usage": { "input_tokens": 50, "output_tokens": 0 }
    });
    let (usage, result) = completion_from_message(&refusal);
    assert_eq!(usage.unwrap().prompt_tokens, Some(50));
    let err = result.unwrap_err();
    assert!(matches!(&err, LlmCallError::Refused { refusal } if refusal == "cyber: declined"));
    assert_eq!(err.outcome_label(), "refused");

    let truncated = serde_json::json!({
        "content": [{ "type": "text", "text": "{\"category\": \"eng" }],
        "stop_reason": "max_tokens"
    });
    assert!(matches!(
        completion_from_message(&truncated).1,
        Err(LlmCallError::EmptyContent { finish_reason: Some(r) }) if r == "max_tokens"
    ));

    // A tool_use-only answer (no text block) carries no JSON for us.
    let tool_use = serde_json::json!({
        "content": [{ "type": "tool_use", "id": "toolu_1", "name": "x", "input": {} }],
        "stop_reason": "tool_use"
    });
    assert!(matches!(
        completion_from_message(&tool_use).1,
        Err(LlmCallError::EmptyContent { finish_reason: Some(r) }) if r == "tool_use"
    ));
    let no_content = serde_json::json!({ "stop_reason": "end_turn" });
    assert!(matches!(
        completion_from_message(&no_content).1,
        Err(LlmCallError::Other(_))
    ));
    // Several text blocks are joined.
    let split = serde_json::json!({
        "content": [{ "type": "text", "text": "{\"a\":" }, { "type": "text", "text": "1}" }],
        "stop_reason": "end_turn"
    });
    assert_eq!(completion_from_message(&split).1.unwrap(), "{\"a\":1}");
}

#[test]
fn error_statuses_are_classified() {
    let wait = Some(std::time::Duration::from_secs(3));
    assert!(matches!(
        classify_error_status(429, wait),
        LlmCallError::RateLimited { retry_after } if retry_after == wait
    ));
    for status in [500, 502, 503, 504, 529] {
        assert!(
            matches!(
                classify_error_status(status, None),
                LlmCallError::GatewayUnavailable { status: s, .. } if s == status
            ),
            "{status}"
        );
    }
    assert!(matches!(
        classify_error_status(402, None),
        LlmCallError::QuotaExhausted { .. }
    ));
    for status in [400, 401, 403, 404, 413] {
        assert!(matches!(
            classify_error_status(status, None),
            LlmCallError::Status { status: s } if s == status
        ));
    }
}

/// A token counter's value in `rendered`.
fn tokens(rendered: &str, metric: &str, call: &str, kind: &str) -> Option<u64> {
    let prefix = format!("distant_signal_{metric}{{call=\"{call}\",kind=\"{kind}\"}} ");
    rendered
        .lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .map(|v| v.parse().unwrap())
}

/// Cache writes and reads land in the token counter: `prompt` is every
/// input token, `cached` the reads, `cache_write` the writes.
#[tokio::test]
async fn cache_usage_fields_feed_the_token_counter() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _guard = metrics::set_default_local_recorder(&recorder);
    crate::llm::register_usage_metrics("claude-haiku-5-5", DEFAULT_BASE_URL, "claude-default");

    let server = MockServer::start().await;
    let write = serde_json::json!({
        "input_tokens": 180,
        "cache_creation_input_tokens": 3100,
        "cache_read_input_tokens": 0,
        "cache_creation": { "ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 3100 },
        "output_tokens": 400
    });
    let read = serde_json::json!({
        "input_tokens": 160,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 3100,
        "output_tokens": 380
    });
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(message_body(&primary_text(), write)),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(message_body(&primary_text(), read)))
        .mount(&server)
        .await;
    let client = client(&server, AnthropicSettings::default());
    let first = client.primary_raw("s", "d", reference_date()).await;
    assert_eq!(
        first.usage,
        Some(TokenUsage {
            prompt_tokens: Some(3280),
            completion_tokens: Some(400),
            reasoning_tokens: None,
            cached_tokens: Some(0),
            cache_write_tokens: Some(3100),
        })
    );
    client.primary_raw("s", "d", reference_date()).await;

    let rendered = handle.render();
    let m = crate::llm::TOKENS_METRIC;
    assert_eq!(tokens(&rendered, m, "primary", "prompt"), Some(3280 + 3260));
    assert_eq!(tokens(&rendered, m, "primary", "cached"), Some(3100));
    assert_eq!(tokens(&rendered, m, "primary", "cache_write"), Some(3100));
    assert_eq!(tokens(&rendered, m, "primary", "completion"), Some(780));
    assert_eq!(tokens(&rendered, m, "primary", "reasoning"), Some(0));
}

/// Walks a schema and fails on anything Claude's structured outputs
/// reject: a type union, a numeric/string bound, a null inside a typed
/// enum, an object that isn't closed or doesn't require every property.
fn check_claude_subset(node: &Value, at: &str, errors: &mut Vec<String>) {
    match node {
        Value::Object(map) => {
            if map.get("type").is_some_and(Value::is_array) {
                errors.push(format!("{at}: type union"));
            }
            for keyword in UNSUPPORTED_KEYWORDS {
                if map.contains_key(keyword) {
                    errors.push(format!("{at}: {keyword}"));
                }
            }
            if map.get("type").and_then(Value::as_str) == Some("object") {
                if map.get("additionalProperties") != Some(&Value::Bool(false)) {
                    errors.push(format!("{at}: open object"));
                }
                let required: Vec<&str> = map
                    .get("required")
                    .and_then(Value::as_array)
                    .map(|r| r.iter().filter_map(Value::as_str).collect())
                    .unwrap_or_default();
                if let Some(properties) = map.get("properties").and_then(Value::as_object) {
                    for property in properties.keys() {
                        if !required.contains(&property.as_str()) {
                            errors.push(format!("{at}.{property}: not required"));
                        }
                    }
                }
            }
            if map.get("type").is_some_and(Value::is_string)
                && map
                    .get("enum")
                    .and_then(Value::as_array)
                    .is_some_and(|values| values.iter().any(Value::is_null))
            {
                errors.push(format!("{at}: null in a typed enum"));
            }
            for (key, child) in map {
                check_claude_subset(child, &format!("{at}.{key}"), errors);
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                check_claude_subset(child, &format!("{at}[{i}]"), errors);
            }
        }
        _ => {}
    }
}

#[test]
fn every_schema_is_rewritten_into_claudes_subset() {
    for (name, schema) in [
        ("primary", crate::llm::primary_schema()),
        ("adversarial", crate::llm::adversarial_schema()),
        ("severity", crate::llm::severity_adversarial_schema()),
    ] {
        let mut errors = Vec::new();
        check_claude_subset(&claude_schema(&schema), name, &mut errors);
        assert!(errors.is_empty(), "{errors:#?}");
    }
    // The walker does see the originals' problems.
    let mut errors = Vec::new();
    check_claude_subset(&crate::llm::primary_schema(), "primary", &mut errors);
    assert!(
        errors.iter().any(|e| e.contains("type union")),
        "{errors:?}"
    );
    assert!(errors.iter().any(|e| e.contains("minimum")), "{errors:?}");

    let converted = claude_schema(&crate::llm::primary_schema());
    let period = &converted["properties"]["periods"]["items"]["properties"];
    assert_eq!(
        period["impact_type"],
        serde_json::json!({ "anyOf": [
            { "type": "string", "enum": ["rail_replacement_bus", "no_scheduled_service", "diversion"] },
            { "type": "null" }
        ]})
    );
    assert_eq!(
        period["scope_description"],
        serde_json::json!({ "anyOf": [{ "type": "string" }, { "type": "null" }] })
    );
    let days = &period["schedule_window"]["anyOf"][0]["properties"]["days_of_week"]["items"];
    assert_eq!(days["type"], "integer");
    let note = days["description"].as_str().unwrap();
    assert!(
        note.contains("minimum: 1") && note.contains("maximum: 7"),
        "{note}"
    );
    // A property merely named like a keyword survives.
    let odd = serde_json::json!({
        "type": "object",
        "properties": { "minimum": { "type": "integer" } },
        "required": ["minimum"],
        "additionalProperties": false
    });
    assert_eq!(claude_schema(&odd), odd);
}

/// The analysis in docs/enricher-anthropic.md ("Prompt caching") rests on
/// the primary system prompt being far above every current model's minimum
/// cacheable prefix (512 tokens; 1,024 on Sonnet 5 and 4.6), and the
/// adversarial ones below it. If a prompt edit moves either, redo it.
#[test]
fn prompt_sizes_match_the_caching_analysis() {
    let tokens = |text: &str| text.chars().count() / 4;
    let primary = tokens(crate::llm::PRIMARY_PROMPT);
    assert!(primary >= 2 * 1024, "primary prompt ~{primary} tokens");
    for prompt in [
        crate::llm::ADVERSARIAL_PROMPT,
        crate::llm::SEVERITY_ADVERSARIAL_PROMPT,
    ] {
        assert!(
            tokens(prompt) < 512,
            "adversarial prompt ~{} tokens",
            tokens(prompt)
        );
    }
}

/// The batch route: create (requests keyed by `custom_id`, each `params` a
/// full Messages body), poll until `ended`, fetch `results_url` as JSONL and
/// match by `custom_id`.
#[tokio::test]
async fn message_batch_lifecycle() {
    let server = MockServer::start().await;
    let results_url = format!("{}/messages/batches/msgbatch_1/results", server.uri());
    let in_progress = serde_json::json!({
        "id": "msgbatch_1", "type": "message_batch", "processing_status": "in_progress",
        "request_counts": { "processing": 2, "succeeded": 0, "errored": 0, "canceled": 0, "expired": 0 },
        "ended_at": null, "created_at": "2026-10-09T10:00:00Z",
        "expires_at": "2026-10-10T10:00:00Z", "cancel_initiated_at": null, "results_url": null
    });
    let mut ended = in_progress.clone();
    ended["processing_status"] = "ended".into();
    ended["request_counts"] = serde_json::json!({
        "processing": 0, "succeeded": 1, "errored": 0, "canceled": 0, "expired": 1
    });
    ended["results_url"] = results_url.clone().into();
    Mock::given(method("POST"))
        .and(path("/messages/batches"))
        .and(header("x-api-key", KEY))
        .and(header("anthropic-version", DEFAULT_VERSION))
        .respond_with(ResponseTemplate::new(200).set_body_json(in_progress.clone()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/messages/batches/msgbatch_1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(in_progress))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/messages/batches/msgbatch_1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ended))
        .mount(&server)
        .await;
    let jsonl = format!(
        "{}\n{}\n",
        serde_json::json!({ "custom_id": "p-1", "result": { "type": "expired" } }),
        serde_json::json!({ "custom_id": "p-0", "result": {
            "type": "succeeded", "message": message_body(&primary_text(), plain_usage()) } }),
    );
    Mock::given(method("GET"))
        .and(path("/messages/batches/msgbatch_1/results"))
        .and(header("x-api-key", KEY))
        .respond_with(ResponseTemplate::new(200).set_body_string(jsonl))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/messages/batches/gone"))
        .respond_with(ResponseTemplate::new(404).set_body_json(error_body("not_found_error")))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/messages/batches/msgbatch_1/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "msgbatch_1", "processing_status": "canceling"
        })))
        .mount(&server)
        .await;

    let client = client(&server, AnthropicSettings::default());
    let requests: Vec<BatchRequest> = (0..2)
        .map(|i| BatchRequest {
            custom_id: format!("p-{i}"),
            params: client
                .batch_params(&crate::llm::primary_spec(
                    client.prompts(),
                    "s",
                    &format!("d{i}"),
                    reference_date(),
                ))
                .unwrap(),
        })
        .collect();
    let created = client.create_message_batch(&requests).await.unwrap();
    assert_eq!(created.id, "msgbatch_1");
    assert!(!created.has_ended());
    let sent = &sent_bodies(&server).await[0];
    assert_eq!(sent["requests"][1]["custom_id"], "p-1");
    let params = &sent["requests"][1]["params"];
    assert_eq!(params["model"], "claude-haiku-5-5");
    assert!(
        params["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("Description: d1")
    );
    assert_eq!(params["system"][0]["cache_control"]["ttl"], "1h");
    assert!(params.get("stream").is_none());

    assert!(
        !client
            .get_message_batch("msgbatch_1")
            .await
            .unwrap()
            .has_ended()
    );
    let polled = client.get_message_batch("msgbatch_1").await.unwrap();
    assert!(polled.has_ended());
    assert_eq!(polled.request_counts.expired, 1);
    let outcomes = client
        .message_batch_results(polled.results_url.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(outcomes["p-1"], BatchOutcome::Expired);
    let BatchOutcome::Succeeded(message) = &outcomes["p-0"] else {
        panic!("{outcomes:?}")
    };
    assert_eq!(completion_from_message(message).1.unwrap(), primary_text());
    client.cancel_message_batch("msgbatch_1").await.unwrap();

    assert!(matches!(
        client.get_message_batch("gone").await,
        Err(LlmCallError::Status { status: 404 })
    ));
    // The key never goes to a results_url on another host.
    let err = client
        .message_batch_results("https://elsewhere.example/results")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("refusing"), "{err}");
    // The OpenAI provider has no batch route.
    let openai = LlmClient::new(
        server.uri(),
        None,
        "m".into(),
        std::time::Duration::from_secs(5),
    );
    assert!(openai.create_message_batch(&requests).await.is_err());
}

/// Keyless Claude auth: the minted token goes out as `Authorization:
/// Bearer`, with no `x-api-key`.
#[tokio::test]
async fn a_federated_token_is_a_bearer_with_no_api_key() {
    let server = MockServer::start().await;
    let file = crate::auth::tests::token_file("k8s.jwt");
    Mock::given(method("POST"))
        .and(path("/application/o/token/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "authentik.jwt", "token_type": "Bearer", "expires_in": 1800
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/oauth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "sk-ant-oat01-minted", "token_type": "Bearer", "expires_in": 3600
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .and(header("authorization", "Bearer sk-ant-oat01-minted"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(message_body(&primary_text(), plain_usage())),
        )
        .mount(&server)
        .await;
    let source = crate::auth::FederatedTokenSource::new(crate::auth::FederationConfig {
        target: crate::auth::ExchangeTarget::Anthropic {
            federation_rule_id: "fdrl_1".into(),
            organization_id: "org".into(),
            service_account_id: "svac_1".into(),
            workspace_id: None,
        },
        identity_token_file: file.path().to_path_buf(),
        token_exchange_url: format!("{}/v1/oauth/token", server.uri()),
        authentik: Some(crate::auth::AuthentikConfig {
            token_url: format!("{}/application/o/token/", server.uri()),
            client_id: "ds-enricher-anthropic".into(),
            scope: None,
        }),
        refresh_skew: std::time::Duration::from_secs(60),
        exchange_timeout: std::time::Duration::from_secs(5),
    })
    .unwrap();
    let client = LlmClient::new(
        server.uri(),
        None,
        "claude-haiku-5-5".into(),
        std::time::Duration::from_secs(30),
    )
    .with_auth(crate::auth::LlmAuth::Federated(std::sync::Arc::new(source)))
    .with_anthropic(AnthropicSettings::default());
    client
        .extract_primary("s", "d", reference_date())
        .await
        .unwrap();
    let messages: Vec<_> = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path() == "/messages")
        .collect();
    assert_eq!(messages.len(), 1);
    assert!(messages[0].headers.get("x-api-key").is_none());
}

/// Profiles on the wire: the resolved sampling is sent or omitted per
/// provider and profile, and an overridden prompt is the one sent.
#[tokio::test]
async fn profiles_decide_sampling_and_prompts_on_the_wire() {
    use crate::llm::ProviderKind;
    use crate::profile::{Overrides, PromptSet, Setting, resolve};
    let server = MockServer::start().await;
    mount_success(&server).await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "choices": [{ "message": { "content": primary_text() }, "finish_reason": "stop" }]
        })))
        .mount(&server)
        .await;
    let openai = |generation: &crate::profile::Generation| {
        LlmClient::new(
            server.uri(),
            None,
            "local".into(),
            std::time::Duration::from_secs(5),
        )
        .with_generation(generation)
    };
    // The OpenAI default profile: temperature 0, as before profiles.
    let default = resolve(ProviderKind::Openai, "local", None, Overrides::default()).unwrap();
    openai(&default.generation)
        .extract_primary("s", "d", reference_date())
        .await
        .unwrap();
    // `omit`, and a top_p.
    let omitted = resolve(
        ProviderKind::Openai,
        "local",
        None,
        Overrides {
            temperature: Some(Setting::Omit),
            top_p: Some(Setting::Set(0.5)),
            ..Overrides::default()
        },
    )
    .unwrap();
    openai(&omitted.generation)
        .extract_primary("s", "d", reference_date())
        .await
        .unwrap();
    // Claude: none by default; Haiku 4.5 accepts a temperature.
    let claude = resolve(
        ProviderKind::Anthropic,
        "claude-haiku-5-5",
        None,
        Overrides::default(),
    )
    .unwrap();
    client(&server, AnthropicSettings::default())
        .with_generation(&claude.generation)
        .extract_primary("s", "d", reference_date())
        .await
        .unwrap();
    let older = resolve(
        ProviderKind::Anthropic,
        "claude-haiku-4-5",
        None,
        Overrides {
            temperature: Some(Setting::Set(0.0)),
            ..Overrides::default()
        },
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("claude-default.primary.txt"),
        "Custom primary prompt.",
    )
    .unwrap();
    client(&server, AnthropicSettings::default())
        .with_generation(&older.generation)
        .with_prompts(PromptSet::load(Some(dir.path()), older.profile).unwrap())
        .extract_primary("s", "d", reference_date())
        .await
        .unwrap();

    let bodies = sent_bodies(&server).await;
    assert_eq!(bodies[0]["temperature"], 0.0);
    assert!(bodies[0].get("top_p").is_none());
    assert!(bodies[1].get("temperature").is_none());
    assert_eq!(bodies[1]["top_p"], 0.5);
    assert!(bodies[2].get("temperature").is_none());
    assert!(bodies[2].get("top_p").is_none());
    assert_eq!(bodies[3]["temperature"], 0.0);
    assert_eq!(bodies[3]["system"][0]["text"], "Custom primary prompt.");
}
