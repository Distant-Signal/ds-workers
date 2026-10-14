use super::*;
use crate::alloc_meter::{Mark, peak_during};
use crate::llm::anthropic::AnthropicSettings;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const MIB: usize = 1024 * 1024;

/// `LLM_BATCH_MAX_ITEMS`' default: the largest primary batch the service
/// submits, and half the requests of the largest adversarial one.
const MAX_ITEMS: usize = 2000;

fn claude(base_url: String) -> LlmClient {
    LlmClient::new(
        base_url,
        Some("sk-ant-test".to_string()),
        "claude-sonnet-5".to_string(),
        std::time::Duration::from_secs(30),
    )
    .with_anthropic(AnthropicSettings::default())
}

fn primary_requests(client: &LlmClient, n: usize) -> Vec<BatchRequest> {
    // About the prod average incident text (summary plus description).
    let description = "Disruption between A and B. ".repeat(42);
    (0..n)
        .map(|i| BatchRequest {
            custom_id: format!("p-{i}"),
            params: client
                .batch_params(crate::llm::primary_spec(
                    client.prompts(),
                    "Signal failure",
                    &description,
                    "2026-10-01T08:00:00Z".parse().unwrap(),
                ))
                .unwrap(),
        })
        .collect()
}

fn text_bytes(requests: &[BatchRequest]) -> usize {
    requests
        .iter()
        .map(|r| r.custom_id.len() + r.params.get().len())
        .sum()
}

#[test]
fn lines_are_split_the_same_however_the_stream_is_chunked() {
    let stream = b"{\"a\":1}\n\n{\"b\":22}\r\n  \n{\"c\":333}";
    let expected: Vec<&[u8]> = vec![b"{\"a\":1}", b"", b"{\"b\":22}\r", b"  ", b"{\"c\":333}"];
    for size in 1..=stream.len() {
        let mut splitter = LineSplitter::default();
        let mut lines: Vec<Vec<u8>> = Vec::new();
        let mut push = |line: &[u8]| lines.push(line.to_vec());
        for chunk in stream.chunks(size) {
            splitter.push(chunk, &mut push);
        }
        splitter.finish(&mut push);
        assert_eq!(lines, expected, "chunk size {size}");
    }
}

/// A batch of `LLM_BATCH_MAX_ITEMS` primary requests is held as its text
/// (a `Value` tree of it was ~3x, ~70 MiB, and the old body build copied
/// that tree again: a 216 MiB peak), and its upload body is one buffer of
/// exactly its final size, written straight from that text.
#[test]
fn a_full_batch_is_held_once_as_text_and_its_body_written_once() {
    let client = claude("http://127.0.0.1:9".to_string());
    let mark = Mark::new();
    let requests = primary_requests(&client, MAX_ITEMS);
    let text = text_bytes(&requests);
    let held = mark.held();
    assert!(text > 20 * MIB, "a realistic batch is {text} bytes");
    assert!(
        held < text + text / 10,
        "{MAX_ITEMS} requests of {text} bytes of JSON hold {held} bytes"
    );
    // Building them never held more than one request's tree on top.
    assert!(mark.peak() < held + MIB, "peak {} held {held}", mark.peak());

    let (body, peak) = peak_during(|| message_batches_body(&requests).unwrap());
    assert!(body.capacity() - body.len() <= 1, "no regrowth");
    assert!(peak <= body.capacity(), "body {} peak {peak}", body.len());
    assert!(body.starts_with(br#"{"requests":[{"custom_id":"p-0","params":{"#));
    assert!(body.ends_with(b"}]}"));

    let (jsonl, peak) = peak_during(|| crate::llm::openai_batch::batch_jsonl(&requests).unwrap());
    assert_eq!(jsonl.capacity(), jsonl.len(), "the size estimate is exact");
    assert!(peak <= jsonl.capacity(), "file {} peak {peak}", jsonl.len());
}

/// A small body parses to exactly the requests it was written from.
#[test]
fn the_body_is_the_requests_json() {
    let client = claude("http://127.0.0.1:9".to_string());
    let requests = primary_requests(&client, 3);
    let body: serde_json::Value =
        serde_json::from_slice(&message_batches_body(&requests).unwrap()).unwrap();
    let entries = body["requests"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    for (entry, request) in entries.iter().zip(&requests) {
        assert_eq!(entry["custom_id"], request.custom_id.as_str());
        assert_eq!(entry["params"], BatchRequest::params_value(&request.params));
    }
    assert_eq!(
        message_batches_body(&[]).unwrap(),
        br#"{"requests":[]}"#.to_vec()
    );
}

/// The results file of a full adversarial batch is parsed as it streams
/// in: on top of the outcomes kept, the download holds about one chunk and
/// one line, never the file (the old `response.text()` held it whole, plus
/// its parse).
#[tokio::test]
async fn results_are_parsed_as_they_stream_in() {
    let server = MockServer::builder()
        .disable_request_recording()
        .start()
        .await;
    let text = format!(
        "{{\"category\":\"other\",\"periods\":[]}}{}",
        " ".repeat(1500)
    );
    let line = |custom_id: &str| {
        serde_json::json!({ "custom_id": custom_id, "result": { "type": "succeeded",
            "message": { "type": "message", "role": "assistant",
                "content": [{ "type": "text", "text": text }], "stop_reason": "end_turn",
                "usage": { "input_tokens": 120, "output_tokens": 210 } } } })
        .to_string()
    };
    let mut jsonl = String::new();
    for i in 0..MAX_ITEMS {
        for call in ["r", "s"] {
            jsonl.push_str(&line(&format!("{call}-{i}")));
            jsonl.push('\n');
        }
    }
    let file_bytes = jsonl.len();
    assert!(file_bytes > 6 * MIB, "{file_bytes}");
    Mock::given(method("GET"))
        .and(path("/v1/messages/batches/msgbatch_1/results"))
        .respond_with(ResponseTemplate::new(200).set_body_string(jsonl))
        .mount(&server)
        .await;
    let client = claude(format!("{}/v1", server.uri()));
    let url = format!("{}/v1/messages/batches/msgbatch_1/results", server.uri());

    let mark = Mark::new();
    let outcomes = client.message_batch_results(&url).await.unwrap();
    let (peak, held) = (mark.peak(), mark.held());
    assert_eq!(outcomes.len(), 2 * MAX_ITEMS);
    let BatchOutcome::Succeeded { content, .. } = &outcomes["s-1999"] else {
        panic!("{:?}", outcomes["s-1999"]);
    };
    assert_eq!(content.as_deref(), Ok(text.as_str()));
    // The download's own footprint (besides the outcomes, which hold every
    // result's text) is bounded, and far below the file.
    let transient = peak - held;
    assert!(
        transient < 2 * MIB,
        "a {file_bytes}-byte file: peak {peak}, outcomes {held}, transient {transient}"
    );
}
