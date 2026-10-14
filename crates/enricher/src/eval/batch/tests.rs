use std::sync::Arc;

use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::*;
use crate::eval::pipeline::{FakeBackend, FakeReply};
use crate::eval::quality::{ScoreOptions, score};
use crate::llm::ProviderPolicy;
use crate::llm::anthropic::AnthropicSettings;

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

fn case(id: &str) -> Case {
    serde_json::from_value(json!({
        "id": id,
        "summary": format!("Signal failure {id}"),
        "description": "Lines blocked between A and B.",
        "reference_date": "2026-10-01T08:00:00Z",
        "expected": {
            "category_any_of": ["signal_failure"],
            "periods": [{ "resolution_status": "ongoing", "apparent_severity": "severe_disruption" }]
        }
    }))
    .unwrap()
}

fn cases(ids: &[&str]) -> Vec<Case> {
    ids.iter().map(|id| case(id)).collect()
}

fn primary_text() -> String {
    json!({
        "category": "signal_failure",
        "periods": [{
            "scope_description": null, "date_range": null, "schedule_window": null,
            "resolution_status": "ongoing", "apparent_severity": "severe_disruption",
            "impact_type": null
        }]
    })
    .to_string()
}

fn resolution_text() -> String {
    json!({"periods": [{"period_index": 0, "scope_description": null, "resolution_status": "ongoing"}]})
        .to_string()
}

fn severity_text() -> String {
    json!({"periods": [{"period_index": 0, "scope_description": null,
                        "apparent_severity": "severe_disruption"}]})
    .to_string()
}

fn label(model: &str) -> TargetLabel {
    TargetLabel {
        target: "t".to_string(),
        model: model.to_string(),
        adversarial_model: None,
    }
}

fn opts(dir: &Path) -> BatchOptions {
    BatchOptions {
        poll_interval: Duration::from_millis(5),
        max_requests: None,
        retry_failed_sync: false,
        state_path: dir.join("t.batch-state.json"),
        max_poll_failures: 3,
    }
}

fn score_opts() -> ScoreOptions {
    ScoreOptions {
        date_tolerance: chrono::TimeDelta::zero(),
        accept_unhashed: false,
    }
}

fn anthropic_client(server: &MockServer) -> LlmClient {
    LlmClient::new(
        server.uri(),
        Some("sk-ant-test".to_string()),
        "claude-haiku-5-5".to_string(),
        Duration::from_secs(10),
    )
    .with_provider_policy(ProviderPolicy::default())
    .with_anthropic(AnthropicSettings::default())
}

fn openai_client(server: &MockServer) -> LlmClient {
    LlmClient::new(
        server.uri(),
        Some("sk-openai-test".to_string()),
        "gpt-6-luna".to_string(),
        Duration::from_secs(10),
    )
}

/// A Message Batches result line.
fn claude_ok(custom_id: &str, text: &str) -> String {
    json!({"custom_id": custom_id, "result": {"type": "succeeded", "message": {
        "content": [{"type": "text", "text": text}], "stop_reason": "end_turn",
        "usage": {"input_tokens": 100, "cache_read_input_tokens": 2000,
                  "cache_creation_input_tokens": 300, "output_tokens": 50}}}})
    .to_string()
}

fn claude_result(custom_id: &str, kind: &str) -> String {
    let mut result = json!({"type": kind});
    if kind == "errored" {
        result["error"] = json!({"type": "error",
            "error": {"type": "invalid_request_error", "message": "bad request"}});
    }
    json!({"custom_id": custom_id, "result": result}).to_string()
}

/// The custom ids of a Message Batches create body.
fn claude_ids(request: &Request) -> Vec<String> {
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    body["requests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["custom_id"].as_str().unwrap().to_string())
        .collect()
}

/// `POST /messages/batches`: `msgbatch_p<n>` for a primary batch,
/// `msgbatch_a<n>` for an adversarial one (`n` counts per stage).
struct ClaudeCreate {
    primary: std::sync::atomic::AtomicUsize,
    adversarial: std::sync::atomic::AtomicUsize,
}

impl Respond for ClaudeCreate {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        use std::sync::atomic::Ordering;
        let primary = claude_ids(request)[0].starts_with("p-");
        let id = if primary {
            format!("msgbatch_p{}", self.primary.fetch_add(1, Ordering::SeqCst))
        } else {
            format!(
                "msgbatch_a{}",
                self.adversarial.fetch_add(1, Ordering::SeqCst)
            )
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "id": id, "type": "message_batch", "processing_status": "in_progress",
            "request_counts": {"processing": 1}
        }))
    }
}

async fn mount_claude_create(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/messages/batches"))
        .respond_with(ClaudeCreate {
            primary: 0.into(),
            adversarial: 0.into(),
        })
        .mount(server)
        .await;
}

/// `GET /messages/batches/{id}`: `in_progress` `pending` times, then
/// ended with its results (JSONL lines) at `/results/{id}`.
async fn mount_claude_batch(server: &MockServer, id: &str, pending: u64, lines: &[String]) {
    if pending > 0 {
        Mock::given(method("GET"))
            .and(path(format!("/messages/batches/{id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": id, "processing_status": "in_progress", "request_counts": {"processing": 1}
            })))
            .up_to_n_times(pending)
            .with_priority(1)
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path(format!("/messages/batches/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": id, "processing_status": "ended",
            "results_url": format!("{}/results/{id}", server.uri()),
            "request_counts": {"succeeded": 1}
        })))
        .with_priority(2)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/results/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(lines.join("\n")))
        .mount(server)
        .await;
}

async fn requests_to(server: &MockServer, verb: &str, route: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method.as_str() == verb && r.url.path() == route)
        .collect()
}

// ---------------------------------------------------------------------------
// Claude: a whole run.
// ---------------------------------------------------------------------------

/// Submit, poll (pending, then ended), results out of order, the
/// adversarial stage from the parsed primaries, records and scoring.
/// Request `p-1` errored: case `b` is a failed record, never dropped.
#[tokio::test]
async fn a_claude_batch_run_scores_like_the_sync_path() {
    let server = MockServer::start().await;
    mount_claude_create(&server).await;
    mount_claude_batch(
        &server,
        "msgbatch_p0",
        2,
        &[
            claude_ok("p-2", &primary_text()),
            claude_result("p-1", "errored"),
            claude_ok("p-0", &primary_text()),
        ],
    )
    .await;
    mount_claude_batch(
        &server,
        "msgbatch_a0",
        1,
        &[
            claude_ok("s-2", &severity_text()),
            claude_ok("r-0", &resolution_text()),
            claude_ok("s-0", &severity_text()),
            claude_ok("r-2", &resolution_text()),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let client = anthropic_client(&server);
    let cases = cases(&["a", "b", "c"]);
    let run = run_batch(
        &client,
        &label("claude-haiku-5-5"),
        &cases,
        1,
        &opts(dir.path()),
    )
    .await
    .unwrap();

    // The submitted bodies are the synchronous builder's.
    let creates = requests_to(&server, "POST", "/messages/batches").await;
    assert_eq!(creates.len(), 2);
    let primary: Value = serde_json::from_slice(&creates[0].body).unwrap();
    let spec = llm::primary_spec(
        client.prompts(),
        &cases[1].summary,
        &cases[1].description,
        cases[1].reference_date,
    );
    assert_eq!(primary["requests"][1]["custom_id"], "p-1");
    assert_eq!(
        primary["requests"][1]["params"],
        BatchRequest::params_value(&client.batch_params(spec).unwrap())
    );
    assert_eq!(
        primary["requests"][0]["params"]["system"][0]["cache_control"]["ttl"], "1h",
        "the prompt-cache setting goes into batch requests too"
    );
    let mut adversarial = claude_ids(&creates[1]);
    adversarial.sort();
    assert_eq!(adversarial, ["r-0", "r-2", "s-0", "s-2"]);

    // Records: by case, the failure kept with its label.
    let records = &run.records;
    let ids: Vec<&str> = records.iter().map(|r| r.case_id.as_str()).collect();
    assert_eq!(ids, ["a", "b", "c"]);
    assert_eq!(records[1].calls.len(), 1);
    assert_eq!(records[1].calls[0].outcome, "batch_errored");
    assert!(
        records[1].calls[0]
            .error
            .as_deref()
            .unwrap()
            .contains("invalid_request_error")
    );
    for record in [&records[0], &records[2]] {
        assert_eq!(record.calls.len(), 3);
        assert!(record.calls.iter().all(|c| c.outcome == "success"));
        assert_eq!(
            record.calls[0].batch_id.as_deref(),
            Some("msgbatch_p0"),
            "{record:?}"
        );
        assert_eq!(record.calls[1].batch_id.as_deref(), Some("msgbatch_a0"));
        let usage = record.calls[0].usage.as_ref().unwrap();
        assert_eq!(usage.cached_tokens, Some(2000));
        assert_eq!(usage.cache_write_tokens, Some(300));
        assert!(record.outcome().is_ok());
    }

    // Scored exactly as sync records are: the failure is a transport one.
    let (summary, _) = score(&cases, records, score_opts());
    assert_eq!(summary.attempts, 3);
    assert_eq!(summary.completed, 2);
    assert_eq!(summary.transport_failures, 1);
    assert_eq!(summary.exact_match_rate, Some(1.0));

    // Metadata: batch ids, counts, usage, turnaround.
    let meta = &run.meta;
    assert_eq!(meta.provider, "anthropic");
    assert!(!meta.resumed);
    let primary = &meta.stages[0];
    assert_eq!(primary.stage, "primary");
    assert_eq!(primary.batches[0].id, "msgbatch_p0");
    assert_eq!(
        (primary.requests, primary.succeeded, primary.errored),
        (3, 2, 1)
    );
    assert!(primary.turnaround_ms.is_some());
    let adversarial = &meta.stages[1];
    assert_eq!((adversarial.requests, adversarial.succeeded), (4, 4));
    assert_eq!(meta.usage.calls, 6);
    assert_eq!(meta.usage.cache_write_tokens, 6 * 300);
    assert_eq!(meta.usage.prompt_tokens, 6 * 2400);
    assert!(meta.turnaround_ms.is_some());
    let md = meta.markdown();
    assert!(md.contains("`msgbatch_a0`"), "{md}");

    // The state file says it's done.
    let state = BatchState::read(&dir.path().join("t.batch-state.json"))
        .unwrap()
        .unwrap();
    assert!(state.done);
    assert_eq!(state.primary.len(), 1);
    assert!(state.primary[0].ended_at.is_some());
    assert_eq!(state.adversarial[0].custom_ids.len(), 4);
}

/// `adversarial_model`: the primary batch is all the primary model's
/// requests, the adversarial batch all the adversarial model's; the stage
/// metadata and the state file name each stage's model, and a state file
/// of the unsplit run is refused.
#[tokio::test]
async fn a_split_run_sends_each_stage_to_its_own_model() {
    let server = MockServer::start().await;
    mount_claude_create(&server).await;
    mount_claude_batch(
        &server,
        "msgbatch_p0",
        0,
        &[claude_ok("p-0", &primary_text())],
    )
    .await;
    mount_claude_batch(
        &server,
        "msgbatch_a0",
        0,
        &[
            claude_ok("r-0", &resolution_text()),
            claude_ok("s-0", &severity_text()),
        ],
    )
    .await;
    let adversarial = crate::profile::resolve_adversarial(
        llm::ProviderKind::Anthropic,
        "claude-sonnet-5-5",
        Some("claude-haiku-5-5"),
        None,
    )
    .unwrap();
    let client = LlmClient::new(
        server.uri(),
        Some("sk-ant-test".to_string()),
        "claude-sonnet-5-5".to_string(),
        Duration::from_secs(10),
    )
    .with_anthropic(AnthropicSettings::default())
    .with_adversarial(adversarial.as_ref());
    let split_label = TargetLabel {
        target: "t".to_string(),
        model: "claude-sonnet-5-5".to_string(),
        adversarial_model: Some("claude-haiku-5-5".to_string()),
    };
    let dir = tempfile::tempdir().unwrap();
    let run = run_batch(&client, &split_label, &cases(&["a"]), 1, &opts(dir.path()))
        .await
        .unwrap();

    let creates = requests_to(&server, "POST", "/messages/batches").await;
    let models = |request: &Request| {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        body["requests"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["params"]["model"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(models(&creates[0]), ["claude-sonnet-5-5"]);
    assert_eq!(
        models(&creates[1]),
        ["claude-haiku-5-5", "claude-haiku-5-5"]
    );
    let stages: Vec<(&str, &str)> = run
        .meta
        .stages
        .iter()
        .map(|s| (s.stage, s.model.as_str()))
        .collect();
    assert_eq!(
        stages,
        [
            ("primary", "claude-sonnet-5-5"),
            ("adversarial", "claude-haiku-5-5")
        ]
    );
    assert!(run.meta.markdown().contains("`claude-haiku-5-5`"));
    assert_eq!(run.records[0].label, split_label);
    assert!(run.records[0].outcome().is_ok());
    let state_path = dir.path().join("t.batch-state.json");
    let mut state = BatchState::read(&state_path).unwrap().unwrap();
    assert_eq!(state.adversarial_model.as_deref(), Some("claude-haiku-5-5"));

    // An unfinished state file of the same run without the split (or with
    // another adversarial model) belongs to another run.
    state.done = false;
    state.adversarial_model = None;
    state.save(&state_path).unwrap();
    let err = run_batch(&client, &split_label, &cases(&["a"]), 1, &opts(dir.path()))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("its adversarial model differs"), "{err}");
}

/// The same model outputs through the sync pipeline (scripted) and the
/// batch path give identical extractions and scores.
#[tokio::test]
async fn batch_and_sync_records_score_identically() {
    let server = MockServer::start().await;
    mount_claude_create(&server).await;
    mount_claude_batch(
        &server,
        "msgbatch_p0",
        0,
        &[claude_ok("p-0", &primary_text())],
    )
    .await;
    mount_claude_batch(
        &server,
        "msgbatch_a0",
        0,
        &[
            claude_ok("r-0", &resolution_text()),
            claude_ok("s-0", &severity_text()),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let cases = cases(&["a"]);
    let batch = run_batch(
        &anthropic_client(&server),
        &label("m"),
        &cases,
        1,
        &opts(dir.path()),
    )
    .await
    .unwrap();

    let fake = FakeBackend::default()
        .with("a", Pass::Primary, FakeReply::Content(primary_text()))
        .with(
            "a",
            Pass::ResolutionAdversarial,
            FakeReply::Content(resolution_text()),
        )
        .with(
            "a",
            Pass::SeverityAdversarial,
            FakeReply::Content(severity_text()),
        );
    let shared: Arc<[Case]> = cases.clone().into();
    let sync = pipeline::run_jobs(Arc::new(fake), &label("m"), &shared, 1, 1).await;
    assert_eq!(
        batch.records[0].outcome().unwrap(),
        sync[0].outcome().unwrap()
    );
    let (batch_summary, batch_cases) = score(&cases, &batch.records, score_opts());
    let (sync_summary, sync_cases) = score(&cases, &sync, score_opts());
    assert_eq!(
        serde_json::to_value(&batch_summary).unwrap(),
        serde_json::to_value(&sync_summary).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&batch_cases).unwrap(),
        serde_json::to_value(&sync_cases).unwrap()
    );
}

// ---------------------------------------------------------------------------
// OpenAI: a whole run.
// ---------------------------------------------------------------------------

/// The custom ids in an uploaded JSONL file (multipart body).
fn uploaded_ids(request: &Request) -> Vec<String> {
    let form = String::from_utf8_lossy(&request.body);
    form.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
        .filter_map(|v| v["custom_id"].as_str().map(str::to_string))
        .collect()
}

/// `POST /files`: `file-p<n>` for a primary input file, `file-a<n>` for an
/// adversarial one. `POST /batches`: `batch_<file id>`.
struct OpenAiUpload {
    count: std::sync::atomic::AtomicUsize,
}

impl Respond for OpenAiUpload {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let n = self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let stage = if uploaded_ids(request)[0].starts_with("p-") {
            "p"
        } else {
            "a"
        };
        ResponseTemplate::new(200).set_body_json(json!({ "id": format!("file-{stage}{n}") }))
    }
}

struct OpenAiCreate;

impl Respond for OpenAiCreate {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let file = body["input_file_id"].as_str().unwrap();
        ResponseTemplate::new(200).set_body_json(json!({
            "id": format!("batch_{file}"), "status": "validating", "input_file_id": file
        }))
    }
}

fn openai_ok(custom_id: &str, text: &str) -> String {
    json!({"custom_id": custom_id, "error": null, "response": {"status_code": 200, "body": {
        "choices": [{"message": {"content": text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 3000, "completion_tokens": 200,
                  "prompt_tokens_details": {"cached_tokens": 2500, "cache_write_tokens": 100}}}}})
    .to_string()
}

fn openai_error(custom_id: &str, code: &str) -> String {
    json!({"custom_id": custom_id, "response": null, "error": {"code": code, "message": "fixture"}})
        .to_string()
}

/// `GET /batches/{id}`: `in_progress` `pending` times, then `status` with
/// its output and error files.
async fn mount_openai_batch(
    server: &MockServer,
    id: &str,
    pending: u64,
    status: &str,
    output: &[String],
    errors: &[String],
) {
    let input = id.trim_start_matches("batch_");
    if pending > 0 {
        Mock::given(method("GET"))
            .and(path(format!("/batches/{id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": id, "status": "in_progress", "input_file_id": input
            })))
            .up_to_n_times(pending)
            .with_priority(1)
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path(format!("/batches/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": id, "status": status, "input_file_id": input,
            "output_file_id": format!("out-{id}"), "error_file_id": format!("err-{id}"),
            "request_counts": {"total": 2}
        })))
        .with_priority(2)
        .mount(server)
        .await;
    for (file, lines) in [(format!("out-{id}"), output), (format!("err-{id}"), errors)] {
        Mock::given(method("GET"))
            .and(path(format!("/files/{file}/content")))
            .respond_with(ResponseTemplate::new(200).set_body_string(lines.join("\n")))
            .mount(server)
            .await;
    }
}

async fn mount_openai_routes(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/files"))
        .respond_with(OpenAiUpload { count: 0.into() })
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/batches"))
        .respond_with(OpenAiCreate)
        .mount(server)
        .await;
    Mock::given(method("DELETE"))
        .and(path_regex("^/files/.+$"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

/// Upload, create, poll, results from the output and error files (out of
/// order), the adversarial stage, scoring, and the files deleted. Request
/// `p-1` expired: case `b` is a failed record.
#[tokio::test]
async fn an_openai_batch_run_scores_like_the_sync_path() {
    let server = MockServer::start().await;
    mount_openai_routes(&server).await;
    mount_openai_batch(
        &server,
        "batch_file-p0",
        1,
        "expired",
        &[openai_ok("p-0", &primary_text())],
        &[openai_error("p-1", "batch_expired")],
    )
    .await;
    mount_openai_batch(
        &server,
        "batch_file-a1",
        2,
        "completed",
        &[
            openai_ok("s-0", &severity_text()),
            openai_ok("r-0", &resolution_text()),
        ],
        &[],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let client = openai_client(&server);
    let cases = cases(&["a", "b"]);
    let run = run_batch(&client, &label("gpt-6-luna"), &cases, 1, &opts(dir.path()))
        .await
        .unwrap();

    let uploads = requests_to(&server, "POST", "/files").await;
    assert_eq!(uploads.len(), 2);
    assert_eq!(uploaded_ids(&uploads[0]), ["p-0", "p-1"]);
    assert_eq!(uploaded_ids(&uploads[1]), ["r-0", "s-0"]);
    // The JSONL body is the synchronous builder's (model, schema, prompt
    // cache fields and all).
    let form = String::from_utf8_lossy(&uploads[0].body);
    let line: Value = form
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l.trim()).ok())
        .find(|v| v.get("custom_id").is_some())
        .unwrap();
    let spec = llm::primary_spec(
        client.prompts(),
        &cases[0].summary,
        &cases[0].description,
        cases[0].reference_date,
    );
    assert_eq!(
        line["body"],
        BatchRequest::params_value(&client.batch_params(spec).unwrap())
    );
    assert_eq!(line["url"], "/v1/chat/completions");

    let records = &run.records;
    assert_eq!(records[0].calls.len(), 3);
    assert!(records[0].outcome().is_ok());
    assert_eq!(records[1].calls[0].outcome, "batch_expired");
    let (summary, _) = score(&cases, records, score_opts());
    assert_eq!((summary.completed, summary.transport_failures), (1, 1));

    let primary = &run.meta.stages[0];
    assert_eq!((primary.succeeded, primary.expired), (1, 1));
    assert_eq!(run.meta.usage.cache_write_tokens, 300);
    // Every file of both stages is deleted once processed.
    let mut deleted: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method.as_str() == "DELETE")
        .map(|r| r.url.path().to_string())
        .collect();
    deleted.sort();
    assert_eq!(
        deleted,
        [
            "/files/err-batch_file-a1",
            "/files/err-batch_file-p0",
            "/files/file-a1",
            "/files/file-p0",
            "/files/out-batch_file-a1",
            "/files/out-batch_file-p0",
        ]
    );
}

/// `token_expired` lines (the 2026-10-13 run: a batch created with a token
/// near its end) are `batch_auth_expired` failed calls, counted in their
/// own column and called out in the report with what to do.
#[tokio::test]
async fn token_expired_requests_are_batch_auth_expired_in_the_report() {
    let server = MockServer::start().await;
    mount_openai_routes(&server).await;
    mount_openai_batch(
        &server,
        "batch_file-p0",
        0,
        "completed",
        &[openai_ok("p-0", &primary_text())],
        &[openai_error("p-1", "token_expired")],
    )
    .await;
    mount_openai_batch(
        &server,
        "batch_file-a1",
        0,
        "completed",
        &[],
        &[
            openai_error("r-0", "token_expired"),
            openai_error("s-0", "token_expired"),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let client = openai_client(&server);
    let cases = cases(&["a", "b"]);
    let run = run_batch(&client, &label("gpt-6-luna"), &cases, 1, &opts(dir.path()))
        .await
        .unwrap();
    assert_eq!(run.records[1].calls[0].outcome, "batch_auth_expired");
    assert_eq!(run.records[0].calls[1].outcome, "batch_auth_expired");
    let auth_expired: Vec<usize> = run.meta.stages.iter().map(|s| s.auth_expired).collect();
    assert_eq!(auth_expired, [1, 2]);
    assert_eq!(run.meta.stages[1].missing, 0);
    let md = run.meta.markdown();
    assert!(md.contains("| Auth expired |"), "{md}");
    assert!(
        md.contains("batch_auth_expired: 3 request(s) did not run"),
        "{md}"
    );
    assert!(md.contains("mode = \"sync\""), "{md}");
}

/// An `expired` batch's requests without any result line count as
/// expired; a canceled one's as canceled; a line-less request of a
/// completed batch as missing. None of them is dropped.
#[tokio::test]
async fn unanswered_requests_take_the_batch_status() {
    let server = MockServer::start().await;
    mount_openai_routes(&server).await;
    mount_openai_batch(
        &server,
        "batch_file-p0",
        0,
        "cancelled",
        &[openai_ok("p-0", &primary_text())],
        &[],
    )
    .await;
    mount_openai_batch(
        &server,
        "batch_file-a1",
        0,
        "completed",
        &[openai_ok("r-0", &resolution_text())],
        &[],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let run = run_batch(
        &openai_client(&server),
        &label("gpt-6-luna"),
        &cases(&["a", "b"]),
        1,
        &opts(dir.path()),
    )
    .await
    .unwrap();
    assert_eq!(run.records[1].calls[0].outcome, "batch_canceled");
    let outcomes: Vec<&str> = run.records[0]
        .calls
        .iter()
        .map(|c| c.outcome.as_str())
        .collect();
    assert_eq!(outcomes, ["success", "success", "batch_missing"]);
    let stages = &run.meta.stages;
    assert_eq!(stages[0].canceled, 1);
    assert_eq!(stages[1].missing, 1);
}

// ---------------------------------------------------------------------------
// Resume, retry, errors and chunking.
// ---------------------------------------------------------------------------

/// A state file whose primary batch was already submitted: the resumed run
/// polls it and submits only the adversarial stage.
#[tokio::test]
async fn a_run_resumes_from_its_state_file_without_resubmitting() {
    let server = MockServer::start().await;
    mount_claude_create(&server).await;
    mount_claude_batch(
        &server,
        "msgbatch_old",
        1,
        &[claude_ok("p-0", &primary_text())],
    )
    .await;
    mount_claude_batch(
        &server,
        "msgbatch_a0",
        0,
        &[
            claude_ok("r-0", &resolution_text()),
            claude_ok("s-0", &severity_text()),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let client = anthropic_client(&server);
    let cases = cases(&["a"]);
    let opts = opts(dir.path());
    let submitted_at: DateTime<Utc> = "2026-10-12T08:00:00Z".parse().unwrap();
    let state = BatchState {
        version: STATE_VERSION,
        target: "t".into(),
        model: "claude-haiku-5-5".into(),
        adversarial_model: None,
        provider: "anthropic".into(),
        base_url: server.uri(),
        prompt_fingerprint: Backend::prompt_fingerprint(&client),
        jobs: jobs(&cases, 1).into_iter().map(|(job, _)| job).collect(),
        started_at: submitted_at,
        primary: vec![SubmittedBatch {
            id: "msgbatch_old".into(),
            custom_ids: vec!["p-0".into()],
            files: Vec::new(),
            submitted_at,
            ended_at: None,
            summary: None,
        }],
        primary_calls: None,
        adversarial: Vec::new(),
        done: false,
    };
    state.save(&opts.state_path).unwrap();

    let run = run_batch(&client, &label("claude-haiku-5-5"), &cases, 1, &opts)
        .await
        .unwrap();
    let creates = requests_to(&server, "POST", "/messages/batches").await;
    assert_eq!(creates.len(), 1, "only the adversarial stage is submitted");
    assert_eq!(claude_ids(&creates[0]), ["r-0", "s-0"]);
    assert!(run.meta.resumed);
    assert!(run.records[0].outcome().is_ok());
    // The turnaround runs from the original submission.
    assert!(run.records[0].calls[0].elapsed_ms > 0);

    // A state file of another run is refused, not resumed.
    let mut other = BatchState::read(&opts.state_path).unwrap().unwrap();
    other.done = false;
    other.model = "claude-sonnet-5-5".into();
    other.save(&opts.state_path).unwrap();
    let err = run_batch(&client, &label("claude-haiku-5-5"), &cases, 1, &opts)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("belongs to another run (its model differs)"),
        "{err}"
    );
}

/// `retry_failed_sync`: a record whose primary request expired is re-run
/// through the synchronous Messages API.
#[tokio::test]
async fn failed_batch_records_can_be_retried_synchronously() {
    struct SyncMessages;
    impl Respond for SyncMessages {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let system = body["system"][0]["text"].as_str().unwrap_or_default();
            let [primary, adversarial, _] = llm::builtin_prompts();
            let text = if system == primary {
                primary_text()
            } else if system == adversarial {
                resolution_text()
            } else {
                severity_text()
            };
            ResponseTemplate::new(200).set_body_json(json!({
                "content": [{"type": "text", "text": text}], "stop_reason": "end_turn",
                "usage": {"input_tokens": 10, "output_tokens": 5}
            }))
        }
    }
    let server = MockServer::start().await;
    mount_claude_create(&server).await;
    mount_claude_batch(
        &server,
        "msgbatch_p0",
        0,
        &[claude_result("p-0", "expired")],
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(SyncMessages)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let opts = BatchOptions {
        retry_failed_sync: true,
        ..opts(dir.path())
    };
    let run = run_batch(
        &anthropic_client(&server),
        &label("claude-haiku-5-5"),
        &cases(&["a"]),
        1,
        &opts,
    )
    .await
    .unwrap();
    assert_eq!(run.meta.retried_in_sync, 1);
    assert_eq!(run.meta.stages[0].expired, 1, "the batch's own count stays");
    let record = &run.records[0];
    assert!(record.outcome().is_ok(), "{record:?}");
    assert!(record.calls.iter().all(|c| c.batch_id.is_none()));
    assert_eq!(requests_to(&server, "POST", "/messages").await.len(), 3);
}

/// No batch API (404) or no batch permission (403): a clear error naming
/// the sync opt-in, never a silent fallback.
#[tokio::test]
async fn a_missing_batch_api_or_permission_names_the_sync_opt_in() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/files"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let err = run_batch(
        &openai_client(&server),
        &label("m"),
        &cases(&["a"]),
        1,
        &opts(dir.path()),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("has no openai batch API"), "{err}");
    assert!(err.contains("mode = \"sync\""), "{err}");
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path() != "/chat/completions")
    );

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/files"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"message": "missing scope", "type": "invalid_request_error"}
        })))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let err = run_batch(
        &openai_client(&server),
        &label("m"),
        &cases(&["a"]),
        1,
        &opts(dir.path()),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("api.batch.write"), "{err}");
    assert!(err.contains("EVAL_MODE=sync"), "{err}");
}

/// A request cap splits the stage into several batches, each polled and
/// matched; repetitions get their own custom ids.
#[tokio::test]
async fn a_stage_is_split_into_batches_within_the_limits() {
    let dir = tempfile::tempdir().unwrap();
    let opts = BatchOptions {
        max_requests: Some(3),
        ..opts(dir.path())
    };
    let cases = cases(&["a"]);
    // One case, two repetitions: two primary requests (one batch), four
    // adversarial ones (a batch of 3 and one of 1).
    let server = MockServer::start().await;
    mount_claude_create(&server).await;
    mount_claude_batch(
        &server,
        "msgbatch_p0",
        0,
        &[
            claude_ok("p-0", &primary_text()),
            claude_ok("p-1", &primary_text()),
        ],
    )
    .await;
    mount_claude_batch(
        &server,
        "msgbatch_a0",
        0,
        &[
            claude_ok("r-0", &resolution_text()),
            claude_ok("s-0", &severity_text()),
            claude_ok("r-1", &resolution_text()),
        ],
    )
    .await;
    mount_claude_batch(
        &server,
        "msgbatch_a1",
        0,
        &[claude_ok("s-1", &severity_text())],
    )
    .await;
    let run = run_batch(&anthropic_client(&server), &label("m"), &cases, 2, &opts)
        .await
        .unwrap();
    let creates = requests_to(&server, "POST", "/messages/batches").await;
    let sizes: Vec<usize> = creates.iter().map(|r| claude_ids(r).len()).collect();
    assert_eq!(sizes, [2, 3, 1]);
    let reps: Vec<u32> = run.records.iter().map(|r| r.repetition).collect();
    assert_eq!(reps, [0, 1]);
    assert!(run.records.iter().all(|r| r.outcome().is_ok()));
    assert_eq!(run.meta.stages[1].batches.len(), 2);
}

fn request(custom_id: &str, model: &str, filler: usize) -> BatchRequest {
    BatchRequest::new(
        custom_id.to_string(),
        &json!({ "model": model, "text": "x".repeat(filler) }),
    )
    .unwrap()
}

#[test]
fn chunking_respects_count_bytes_and_model() {
    let ids = |chunks: &[Vec<BatchRequest>]| -> Vec<Vec<String>> {
        chunks
            .iter()
            .map(|c| c.iter().map(|r| r.custom_id.clone()).collect())
            .collect()
    };
    let by_count = chunk_requests(
        "openai",
        (0..5)
            .map(|i| request(&format!("p-{i}"), "m", 10))
            .collect(),
        ChunkLimits {
            max_requests: 2,
            max_bytes: 1 << 20,
        },
    )
    .unwrap();
    assert_eq!(
        ids(&by_count),
        [vec!["p-0", "p-1"], vec!["p-2", "p-3"], vec!["p-4"]]
    );

    // Bytes: each OpenAI line is the JSONL line the upload sends.
    let one = request("p-0", "m", 1000);
    let line = request_bytes("openai", &one);
    assert_eq!(
        line,
        llm::openai_batch::batch_jsonl(std::slice::from_ref(&one))
            .unwrap()
            .len()
    );
    let by_bytes = chunk_requests(
        "openai",
        (0..4)
            .map(|i| request(&format!("p-{i}"), "m", 1000))
            .collect(),
        ChunkLimits {
            max_requests: 100,
            max_bytes: 2 * line + ENVELOPE_BYTES,
        },
    )
    .unwrap();
    assert_eq!(by_bytes.len(), 2);
    assert!(
        request_bytes("anthropic", &one) < line,
        "no method/url per entry"
    );

    // One model per batch.
    let by_model = chunk_requests(
        "openai",
        vec![
            request("p-0", "a", 1),
            request("p-1", "b", 1),
            request("p-2", "b", 1),
        ],
        ChunkLimits::for_provider("openai"),
    )
    .unwrap();
    assert_eq!(ids(&by_model), [vec!["p-0"], vec!["p-1", "p-2"]]);

    // A single request over the limit is an error, not an empty batch.
    let err = chunk_requests(
        "anthropic",
        vec![request("p-0", "m", 500)],
        ChunkLimits {
            max_requests: 10,
            max_bytes: 300,
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("over the batch limit"), "{err}");
}

#[test]
fn provider_limits_and_the_cap() {
    assert_eq!(
        ChunkLimits::for_provider("openai"),
        ChunkLimits {
            max_requests: 50_000,
            max_bytes: 200 * 1024 * 1024
        }
    );
    assert_eq!(
        ChunkLimits::for_provider("anthropic"),
        ChunkLimits {
            max_requests: 100_000,
            max_bytes: 256_000_000
        }
    );
    let capped = ChunkLimits::for_provider("openai").capped(Some(10));
    assert_eq!(capped.max_requests, 10);
    // A cap never raises the provider's limit.
    let raised = ChunkLimits::for_provider("openai").capped(Some(1_000_000));
    assert_eq!(raised.max_requests, 50_000);
}

/// The state file round-trips, keys and all.
#[test]
fn the_state_file_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = state_path(dir.path(), "t");
    assert_eq!(BatchState::read(&path).unwrap(), None);
    let at: DateTime<Utc> = "2026-10-12T08:00:00Z".parse().unwrap();
    let mut calls = BTreeMap::new();
    calls.insert(
        3,
        call_record(
            Pass::Primary,
            &RequestResult {
                outcome: Some(BatchOutcome::Expired),
                batch_id: "b".into(),
                elapsed_ms: 7,
            },
        ),
    );
    let state = BatchState {
        version: STATE_VERSION,
        target: "t".into(),
        model: "m".into(),
        adversarial_model: None,
        provider: "openai".into(),
        base_url: "https://api.openai.com/v1".into(),
        prompt_fingerprint: "f".into(),
        jobs: vec![JobKey {
            index: 3,
            case_id: "a".into(),
            input_hash: "h".into(),
            repetition: 0,
        }],
        started_at: at,
        primary: vec![SubmittedBatch {
            id: "b".into(),
            custom_ids: vec!["p-3".into()],
            files: vec!["file-1".into()],
            submitted_at: at,
            ended_at: Some(at + chrono::TimeDelta::seconds(90)),
            summary: Some("expired".into()),
        }],
        primary_calls: Some(calls),
        adversarial: Vec::new(),
        done: false,
    };
    state.save(&path).unwrap();
    assert_eq!(BatchState::read(&path).unwrap().as_ref(), Some(&state));
    assert_eq!(state.primary[0].turnaround_ms(), Some(90_000));
    let raw: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(raw["version"], 1);
    assert_eq!(raw["primary_calls"]["3"]["outcome"], "batch_expired");
}
