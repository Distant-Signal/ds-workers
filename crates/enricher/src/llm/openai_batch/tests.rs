use super::*;
use crate::llm::ProviderPolicy;
use crate::llm::batch_api::BatchMeta;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const KEY: &str = "sk-openai-test";

fn client(server: &MockServer) -> LlmClient {
    LlmClient::new(
        server.uri(),
        Some(KEY.to_string()),
        "gpt-6-luna".to_string(),
        std::time::Duration::from_secs(30),
    )
    .with_provider_policy(ProviderPolicy {
        reasoning_effort: Some("low".to_string()),
        ..ProviderPolicy::default()
    })
}

fn reference_date() -> chrono::DateTime<chrono::Utc> {
    "2026-10-01T08:00:00Z".parse().unwrap()
}

fn primary_text() -> String {
    serde_json::json!({
        "category": "signal_failure",
        "periods": [{
            "scope_description": null, "date_range": null, "schedule_window": null,
            "resolution_status": "ongoing", "apparent_severity": "severe_disruption",
            "impact_type": null
        }]
    })
    .to_string()
}

/// A chat.completion body (the batch guide's output shape).
fn completion(text: &str) -> Value {
    serde_json::json!({
        "id": "chatcmpl-1", "object": "chat.completion", "model": "gpt-6-luna",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": text },
                      "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 3000, "completion_tokens": 200,
                   "prompt_tokens_details": { "cached_tokens": 2800 },
                   "completion_tokens_details": { "reasoning_tokens": 50 } }
    })
}

fn ok_line(custom_id: &str, text: &str) -> String {
    serde_json::json!({
        "id": format!("batch_req_{custom_id}"), "custom_id": custom_id,
        "response": { "status_code": 200, "request_id": "req_1", "body": completion(text) },
        "error": null
    })
    .to_string()
}

fn error_line(custom_id: &str, code: &str) -> String {
    serde_json::json!({
        "id": format!("batch_req_{custom_id}"), "custom_id": custom_id, "response": null,
        "error": { "code": code, "message": "fixture" }
    })
    .to_string()
}

fn batch(status: &str, output: Option<&str>, error: Option<&str>) -> Value {
    serde_json::json!({
        "id": "batch_1", "object": "batch", "endpoint": ENDPOINT, "errors": null,
        "input_file_id": "file-in", "completion_window": "24h", "status": status,
        "output_file_id": output, "error_file_id": error,
        "request_counts": { "total": 3, "completed": 1, "failed": 2 }, "metadata": null
    })
}

fn requests(client: &LlmClient, n: usize) -> Vec<BatchRequest> {
    (0..n)
        .map(|i| BatchRequest {
            custom_id: format!("p-{i}"),
            params: client
                .batch_params(crate::llm::primary_spec(
                    client.prompts(),
                    "Signal failure",
                    &format!("Lines blocked {i}"),
                    reference_date(),
                ))
                .unwrap(),
        })
        .collect()
}

const META: BatchMeta<'static> = BatchMeta {
    stage: "primary",
    model_version: "gpt-6-luna@v1",
};

async fn mount_upload(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/files"))
        .and(header("authorization", format!("Bearer {KEY}").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "file-in", "object": "file", "purpose": "batch", "bytes": 1
        })))
        .mount(server)
        .await;
}

async fn mount_get(server: &MockServer, body: Value) {
    Mock::given(method("GET"))
        .and(path("/batches/batch_1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn mount_content(server: &MockServer, file: &str, lines: &[String]) {
    Mock::given(method("GET"))
        .and(path(format!("/files/{file}/content")))
        .and(header("authorization", format!("Bearer {KEY}").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_string(lines.join("\n") + "\n"))
        .mount(server)
        .await;
}

async fn mount_delete(server: &MockServer, status: u16) {
    Mock::given(method("DELETE"))
        .respond_with(
            ResponseTemplate::new(status).set_body_json(serde_json::json!({
                "id": "x", "object": "file", "deleted": status == 200
            })),
        )
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

fn ended(poll: BatchPoll) -> EndedBatch {
    match poll {
        BatchPoll::Ended(ended) => ended,
        other => panic!("not ended: {other:?}"),
    }
}

/// The batch body is byte-for-byte the synchronous request's body: the
/// same builder (profile sampling, strict schema, reasoning effort).
#[tokio::test]
async fn batch_lines_carry_the_synchronous_request_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(completion(&primary_text())))
        .mount(&server)
        .await;
    let client = client(&server);
    client
        .extract_primary("Signal failure", "Lines blocked 0", reference_date())
        .await
        .unwrap();
    let sync: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();

    let jsonl = batch_jsonl(&requests(&client, 2)).unwrap();
    let lines: Vec<Value> = jsonl
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["custom_id"], "p-0");
    assert_eq!(lines[0]["method"], "POST");
    assert_eq!(lines[0]["url"], "/v1/chat/completions");
    assert_eq!(lines[0]["body"], sync);
    assert_eq!(lines[0]["body"]["temperature"], 0.0);
    assert_eq!(lines[0]["body"]["reasoning_effort"], "low");
    assert_eq!(
        lines[0]["body"]["response_format"]["json_schema"]["strict"],
        true
    );
    assert!(lines[0]["body"].get("stream").is_none());
}

/// Under a profile with explicit prompt caching (`openai-gpt-6-luna`), the
/// batch body still equals the synchronous one, so it carries the same
/// `prompt_cache_options`, `prompt_cache_key` and system-prompt breakpoint.
#[tokio::test]
async fn batch_lines_carry_the_explicit_prompt_cache_fields() {
    use crate::profile::{Overrides, resolve};
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(completion(&primary_text())))
        .mount(&server)
        .await;
    let active = resolve(
        crate::llm::ProviderKind::Openai,
        "gpt-6-luna",
        None,
        Overrides::default(),
    )
    .unwrap();
    let client = client(&server).with_profile(&active);
    client
        .extract_primary("Signal failure", "Lines blocked 0", reference_date())
        .await
        .unwrap();
    let sync: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();

    let jsonl = batch_jsonl(&requests(&client, 1)).unwrap();
    let line: Value = serde_json::from_str(jsonl.lines().next().unwrap()).unwrap();
    let body = &line["body"];
    assert_eq!(*body, sync);
    assert_eq!(
        body["prompt_cache_options"],
        serde_json::json!({ "mode": "explicit" })
    );
    assert_eq!(
        body["prompt_cache_key"],
        "ds-enricher:openai-gpt-6-luna:primary"
    );
    assert_eq!(
        body["messages"][0]["content"][0]["prompt_cache_breakpoint"],
        serde_json::json!({ "mode": "explicit" })
    );
}

/// Upload -> create -> poll (in progress, then completed) -> output and
/// error files, matched by `custom_id` whatever their order -> delete.
#[tokio::test]
async fn full_lifecycle_with_out_of_order_lines_and_cleanup() {
    let server = MockServer::start().await;
    mount_upload(&server).await;
    Mock::given(method("POST"))
        .and(path("/batches"))
        .respond_with(ResponseTemplate::new(200).set_body_json(batch("validating", None, None)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/batches/batch_1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(batch("in_progress", None, None)))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_get(
        &server,
        batch("completed", Some("file-out"), Some("file-err")),
    )
    .await;
    mount_content(
        &server,
        "file-out",
        &[
            ok_line("p-2", "{\"not\": \"used\"}"),
            ok_line("p-0", &primary_text()),
        ],
    )
    .await;
    let mut server_error = serde_json::from_str::<Value>(&ok_line("p-1", "")).unwrap();
    server_error["response"]["status_code"] = 400.into();
    server_error["response"]["body"] = serde_json::json!({
        "error": { "type": "invalid_request_error", "code": "invalid_schema", "message": "bad" }
    });
    mount_content(&server, "file-err", &[server_error.to_string()]).await;
    mount_delete(&server, 200).await;

    let client = client(&server);
    let created = client
        .create_batch(&requests(&client, 3), META)
        .await
        .unwrap();
    assert_eq!(created.id, "batch_1");
    assert_eq!(created.files, ["file-in"]);

    // The upload: purpose=batch, a short expiry, the JSONL as the file.
    let upload = &requests_to(&server, "POST", "/files").await[0];
    let form = String::from_utf8_lossy(&upload.body);
    assert!(
        upload.headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("multipart/form-data"),
    );
    for needle in [
        "name=\"purpose\"\r\n\r\nbatch",
        "name=\"expires_after[anchor]\"\r\n\r\ncreated_at",
        "name=\"expires_after[seconds]\"\r\n\r\n172800",
        "filename=\"enricher-batch.jsonl\"",
        "\"custom_id\":\"p-2\"",
    ] {
        assert!(form.contains(needle), "{needle} not in {form}");
    }
    // The create: the file, the endpoint, a short output expiry, metadata.
    let create: Value =
        serde_json::from_slice(&requests_to(&server, "POST", "/batches").await[0].body).unwrap();
    assert_eq!(
        create,
        serde_json::json!({
            "input_file_id": "file-in",
            "endpoint": "/v1/chat/completions",
            "completion_window": "24h",
            "output_expires_after": { "anchor": "created_at", "seconds": 172_800 },
            "metadata": { "source": "distant-signal-enricher", "stage": "primary",
                          "model_version": "gpt-6-luna@v1" }
        })
    );

    assert_eq!(
        client.poll_batch("batch_1").await.unwrap(),
        BatchPoll::Pending
    );
    let done = ended(client.poll_batch("batch_1").await.unwrap());
    assert_eq!(done.files, ["file-in", "file-out", "file-err"]);
    assert_eq!(done.results.unanswered, None);
    let outcomes = &done.results.outcomes;
    assert_eq!(outcomes.len(), 3);
    let BatchOutcome::Succeeded { usage, content, .. } = &outcomes["p-0"] else {
        panic!("{outcomes:?}")
    };
    assert_eq!(content.as_deref(), Ok(primary_text().as_str()));
    let usage = usage.as_ref().unwrap();
    assert_eq!(usage.cached_tokens, Some(2800));
    assert_eq!(usage.reasoning_tokens, Some(50));
    assert!(matches!(outcomes["p-2"], BatchOutcome::Succeeded { .. }));
    assert_eq!(
        outcomes["p-1"],
        BatchOutcome::Errored {
            kind: Some("invalid_schema".into()),
            message: Some("bad".into())
        }
    );

    assert_eq!(client.delete_batch_files(&done.files).await, 0);
    let deleted: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.method.as_str() == "DELETE")
        .map(|r| r.url.path().to_string())
        .collect();
    assert_eq!(
        deleted,
        ["/files/file-in", "/files/file-out", "/files/file-err"]
    );
}

/// An expired batch: the finished request's output, `batch_expired` error
/// lines, and a request with no line at all also counts as expired.
#[tokio::test]
async fn an_expired_batch_keeps_its_partial_results() {
    let server = MockServer::start().await;
    mount_get(
        &server,
        batch("expired", Some("file-out"), Some("file-err")),
    )
    .await;
    mount_content(&server, "file-out", &[ok_line("p-0", &primary_text())]).await;
    mount_content(&server, "file-err", &[error_line("p-1", "batch_expired")]).await;
    let done = ended(client(&server).poll_batch("batch_1").await.unwrap());
    assert_eq!(done.results.outcomes["p-1"], BatchOutcome::Expired);
    assert!(matches!(
        done.results.outcomes["p-0"],
        BatchOutcome::Succeeded { content: Ok(_), .. }
    ));
    assert_eq!(done.results.unanswered, Some(BatchOutcome::Expired));

    // Planned: p-0 goes on, p-1 and the lineless p-2 are expired.
    let items = (0..3)
        .map(|i| crate::batch::BatchItem {
            index: i,
            incident_id: format!("I{i}"),
            text_hash: "h".into(),
            summary: "s".into(),
            description: "d".into(),
            reference_date: reference_date(),
            text_seen_at: None,
            primary_content: None,
        })
        .collect();
    let plan = crate::batch::plan_primary(items, done.results);
    assert_eq!(plan.next.len(), 1);
    let results: Vec<&str> = plan.records.iter().map(|r| r.result).collect();
    assert_eq!(results, ["succeeded", "expired", "expired"]);
}

/// `failed`: the input file failed validation. No results, no downloads;
/// the reason and the input file come back for the alert and the cleanup.
#[tokio::test]
async fn a_validation_failure_is_its_own_outcome() {
    let server = MockServer::start().await;
    let mut failed = batch("failed", None, None);
    failed["errors"] = serde_json::json!({ "object": "list", "data": [
        { "code": "invalid_request", "message": "model not supported", "line": 1 }
    ]});
    mount_get(&server, failed).await;
    let poll = client(&server).poll_batch("batch_1").await.unwrap();
    assert_eq!(
        poll,
        BatchPoll::ValidationFailed {
            reason: "line 1: invalid_request model not supported".into(),
            files: vec!["file-in".into()],
        }
    );
    assert!(
        requests_to(&server, "GET", "/files/file-out/content")
            .await
            .is_empty()
    );
}

/// Cancel: `POST /batches/{id}/cancel`; while `cancelling` it is pending,
/// once `cancelled` the unfinished requests are canceled.
#[tokio::test]
async fn cancel_then_cancelled_results() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/batches/batch_1/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_json(batch("cancelling", None, None)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/batches/batch_1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(batch("cancelling", None, None)))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_get(&server, batch("cancelled", Some("file-out"), None)).await;
    mount_content(&server, "file-out", &[ok_line("p-0", &primary_text())]).await;
    let client = client(&server);
    client.cancel_batch("batch_1").await.unwrap();
    assert_eq!(
        client.poll_batch("batch_1").await.unwrap(),
        BatchPoll::Pending
    );
    let done = ended(client.poll_batch("batch_1").await.unwrap());
    assert_eq!(done.results.unanswered, Some(BatchOutcome::Canceled));
    assert_eq!(done.files, ["file-in", "file-out"]);
    assert_eq!(
        parse_line(&serde_json::from_str(&error_line("p-1", "batch_cancelled")).unwrap()),
        Some(("p-1".into(), BatchOutcome::Canceled))
    );
}

/// A failed delete is counted, not raised; an already-deleted file (404)
/// is fine.
#[tokio::test]
async fn cleanup_failures_are_counted_not_raised() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/files/file-gone"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    mount_delete(&server, 500).await;
    let files = ["file-in".to_string(), "file-gone".to_string()];
    assert_eq!(client(&server).delete_batch_files(&files).await, 1);
}

/// A create that fails deletes the file it uploaded.
#[tokio::test]
async fn a_failed_create_deletes_its_upload() {
    let server = MockServer::start().await;
    mount_upload(&server).await;
    Mock::given(method("POST"))
        .and(path("/batches"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": { "type": "invalid_request_error", "message": "no" }
        })))
        .mount(&server)
        .await;
    mount_delete(&server, 200).await;
    let client = client(&server);
    let err = client
        .create_batch(&requests(&client, 1), META)
        .await
        .unwrap_err();
    assert!(matches!(err, LlmCallError::Status { status: 400 }), "{err}");
    assert_eq!(
        requests_to(&server, "DELETE", "/files/file-in").await.len(),
        1
    );
}

/// After a restart the enricher only has the batch id (the table row): the
/// batch object names every file, so a fresh client polls, reads and
/// cleans up with nothing else. A batch or results that are gone are
/// their own outcomes.
#[tokio::test]
async fn a_fresh_client_resumes_from_the_batch_id_alone() {
    let server = MockServer::start().await;
    mount_get(&server, batch("completed", Some("file-out"), None)).await;
    mount_content(&server, "file-out", &[ok_line("p-0", &primary_text())]).await;
    Mock::given(method("GET"))
        .and(path("/batches/batch_gone"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let restarted = client(&server);
    let done = ended(restarted.poll_batch("batch_1").await.unwrap());
    assert_eq!(done.files, ["file-in", "file-out"]);
    assert_eq!(done.results.outcomes.len(), 1);
    assert_eq!(
        restarted.poll_batch("batch_gone").await.unwrap(),
        BatchPoll::Gone
    );

    let expired_files = MockServer::start().await;
    mount_get(&expired_files, batch("completed", Some("file-out"), None)).await;
    Mock::given(method("GET"))
        .and(path("/files/file-out/content"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&expired_files)
        .await;
    assert_eq!(
        client(&expired_files).poll_batch("batch_1").await.unwrap(),
        BatchPoll::ResultsGone
    );
}

/// The limits are checked before anything is uploaded.
#[test]
fn oversized_input_is_refused_before_upload() {
    let request = BatchRequest {
        custom_id: "p-0".into(),
        params: Value::Null,
    };
    let too_many = vec![request; MAX_REQUESTS + 1];
    assert!(batch_jsonl(&too_many).is_err());
}

/// Unparseable lines are skipped, and a non-200 response without an error
/// body keeps its status as the kind.
#[test]
fn odd_lines_are_skipped_or_classified() {
    let mut outcomes = HashMap::new();
    let mut bare_500 = serde_json::from_str::<Value>(&ok_line("p-3", "")).unwrap();
    bare_500["response"]["status_code"] = 500.into();
    bare_500["response"]["body"] = Value::Null;
    let refused = serde_json::json!({ "custom_id": "p-4", "error": null, "response": {
        "status_code": 200, "body": { "choices": [{ "message": {
            "content": null, "refusal": "no" }, "finish_reason": "stop" }] } } });
    parse_results_jsonl(
        &format!("garbage\n{bare_500}\n\n{refused}\n"),
        &mut outcomes,
    );
    assert_eq!(outcomes.len(), 2);
    assert_eq!(
        outcomes["p-3"],
        BatchOutcome::Errored {
            kind: Some("http_500".into()),
            message: None
        }
    );
    let BatchOutcome::Succeeded { content, .. } = &outcomes["p-4"] else {
        panic!("{outcomes:?}")
    };
    assert!(content.as_ref().unwrap_err().contains("refused"));
}

/// Keyless auth: the batch routes take the federated token as the same
/// bearer, and a 401 is retried once with a fresh one.
#[tokio::test]
async fn a_federated_token_is_refreshed_on_401() {
    let server = MockServer::start().await;
    let file = crate::auth::tests::token_file("k8s.jwt");
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(crate::auth::tests::openai_token("oai.minted", 3600))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/batches/batch_1"))
        .respond_with(ResponseTemplate::new(401))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/batches/batch_1"))
        .and(header("authorization", "Bearer oai.minted"))
        .respond_with(ResponseTemplate::new(200).set_body_json(batch("in_progress", None, None)))
        .mount(&server)
        .await;
    let auth = crate::auth::LlmAuth::Federated(std::sync::Arc::new(
        crate::auth::FederatedTokenSource::new(crate::auth::tests::kubernetes_config(
            &server,
            file.path(),
        ))
        .unwrap(),
    ));
    let client = client(&server).with_auth(auth);
    assert_eq!(
        client.poll_batch("batch_1").await.unwrap(),
        BatchPoll::Pending
    );
    let polls = requests_to(&server, "GET", "/batches/batch_1").await;
    assert_eq!(polls.len(), 2);
}
