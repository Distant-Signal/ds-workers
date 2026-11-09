//! The presets against real ntfy and Gotify servers (DS-415), driven by
//! `scripts/webhook-e2e.py`, which starts the servers, creates the topic,
//! app and tokens, runs this test and then reads the messages back from the
//! servers to check what arrived.
//!
//! Compiled only with the `insecure-test-egress` feature (debug builds
//! only: lib.rs's `compile_error!` refuses a release build with it), which
//! adds [`WebhookSender::unguarded_for_tests`]: the same sender without the
//! public-address guard, so it can reach 127.0.0.1 over http. The guard
//! itself is untouched: the last test sends the normal sender to those same
//! servers and shows it refuses.
//!
//! `#[ignore]`d: it needs the servers and these variables:
//! `E2E_NTFY_URL` (topic URL), `E2E_NTFY_TOKEN`, `E2E_GOTIFY_URL` (base),
//! `E2E_GOTIFY_TOKEN`, `E2E_CAPTURE_DIR`, `E2E_SITE_URL`.

#![expect(
    clippy::expect_used,
    clippy::too_many_lines,
    reason = "test code: a failed step should panic, and the scenario reads top to bottom"
)]

use std::io::Write as _;

use chrono::{DateTime, Utc};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::webhook::{WebhookDestination, WebhookEvent, WebhookOutcome, WebhookSender};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set (scripts/webhook-e2e.py)"))
}

fn event(title: &str, body: &str, urgent: bool) -> WebhookEvent {
    WebhookEvent {
        event: "train_update",
        title: title.to_string(),
        body: body.to_string(),
        path: "/track/42".to_string(),
        tag: "train-42".to_string(),
        urgent,
        decided_at: DateTime::parse_from_rfc3339("2026-11-09T09:00:00Z")
            .expect("a date")
            .with_timezone(&Utc),
    }
}

fn dest(id: i64, kind: &str, url: &str, secret: Option<&str>) -> WebhookDestination {
    WebhookDestination {
        id,
        kind: kind.to_string(),
        url: url.to_string(),
        secret: secret.map(ToString::to_string),
    }
}

/// A one-shot HTTP server: reads one request, answers 200, and returns the
/// request's header lines and body.
async fn capture_one(listener: tokio::net::TcpListener) -> (Vec<String>, Vec<u8>) {
    let (mut stream, _) = listener.accept().await.expect("a connection");
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let (head_end, content_length) = loop {
        let n = stream.read(&mut chunk).await.expect("read");
        assert!(n > 0, "the client closed early");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
            let length = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            break (pos + 4, length);
        }
    };
    while buf.len() < head_end + content_length {
        let n = stream.read(&mut chunk).await.expect("read body");
        assert!(n > 0, "the client closed early");
        buf.extend_from_slice(&chunk[..n]);
    }
    stream
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await
        .expect("respond");
    let head = String::from_utf8_lossy(&buf[..head_end - 4]).to_string();
    let headers = head.lines().map(ToString::to_string).collect();
    (headers, buf[head_end..head_end + content_length].to_vec())
}

#[tokio::test]
#[ignore = "needs real ntfy and Gotify servers; run scripts/webhook-e2e.py"]
async fn the_presets_deliver_to_real_servers() {
    let site = env("E2E_SITE_URL");
    let sender = WebhookSender::unguarded_for_tests(Some(site));

    // ntfy: with its access token, then with a wrong one.
    let ntfy_url = env("E2E_NTFY_URL");
    let ntfy = dest(1, "ntfy", &ntfy_url, Some(&env("E2E_NTFY_TOKEN")));
    let sent = sender
        .send(
            &ntfy,
            &event(
                "Delayed \u{2013} 09:15 to Leeds",
                "About 20 minutes late.",
                true,
            ),
        )
        .await;
    assert_eq!(sent, WebhookOutcome::Delivered);
    let wrong = dest(
        2,
        "ntfy",
        &ntfy_url,
        Some("tk_wrongwrongwrongwrongwrongwrongw"),
    );
    assert!(matches!(
        sender.send(&wrong, &event("x", "must not arrive", false)).await,
        WebhookOutcome::Failed(reason) if reason.contains("401") || reason.contains("403")
    ));
    let anonymous = dest(3, "ntfy", &ntfy_url, None);
    assert!(matches!(
        sender
            .send(&anonymous, &event("x", "must not arrive", false))
            .await,
        WebhookOutcome::Failed(_)
    ));

    // Gotify: with its app token, then with a wrong one.
    let gotify_url = env("E2E_GOTIFY_URL");
    let gotify = dest(4, "gotify", &gotify_url, Some(&env("E2E_GOTIFY_TOKEN")));
    let sent = sender
        .send(
            &gotify,
            &event("Leave by 08:40", "Your train is 20 minutes late.", false),
        )
        .await;
    assert_eq!(sent, WebhookOutcome::Delivered);
    let wrong = dest(5, "gotify", &gotify_url, Some("Awrongwrongwrong"));
    assert!(matches!(
        sender.send(&wrong, &event("x", "must not arrive", false)).await,
        WebhookOutcome::Failed(reason) if reason.contains("401") || reason.contains("403")
    ));

    // Generic: capture the raw request for scripts/webhook-e2e.py to verify
    // the signature independently.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let secret = "whsec_e2e_signing_secret";
    let generic = dest(
        6,
        "generic",
        &format!("http://127.0.0.1:{port}/hook"),
        Some(secret),
    );
    let capture = tokio::spawn(capture_one(listener));
    let sent = sender
        .send(
            &generic,
            &event("Train cancelled", "The 09:15 to Leeds is cancelled.", true),
        )
        .await;
    assert_eq!(sent, WebhookOutcome::Delivered);
    let (headers, body) = capture.await.expect("capture");
    let capture_file = std::path::Path::new(&env("E2E_CAPTURE_DIR")).join("generic.json");
    let mut file = std::fs::File::create(capture_file).expect("create capture");
    let json = serde_json::json!({
        "secret": secret,
        "headers": headers,
        "body": String::from_utf8(body).expect("utf-8 body"),
    });
    file.write_all(json.to_string().as_bytes())
        .expect("write capture");

    // The guard is untouched: the normal sender refuses these same servers
    // and every private, link-local, metadata and tailnet address.
    let guarded = WebhookSender::new(None).expect("sender");
    let mut refused = vec![
        ntfy_url.clone(),
        gotify_url.clone(),
        ntfy_url.replacen("http://", "https://", 1),
    ];
    refused.extend(
        [
            "https://127.0.0.1/x",
            "https://10.1.2.3/x",
            "https://172.16.0.9/x",
            "https://192.168.1.10/x",
            "https://169.254.169.254/latest/meta-data",
            "https://100.64.0.1/x",
            "https://100.100.100.100/x",
            "https://[::1]/x",
            "https://[fd00::1]/x",
            "https://localhost/x",
        ]
        .map(ToString::to_string),
    );
    for url in refused {
        let outcome = guarded
            .send(
                &dest(9, "ntfy", &url, None),
                &event("x", "must not be sent", false),
            )
            .await;
        assert!(
            matches!(outcome, WebhookOutcome::Failed(_)),
            "{url} was not refused: {outcome:?}"
        );
    }
}
