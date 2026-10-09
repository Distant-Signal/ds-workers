//! `anthropic-wif-authentik`: Authentik's token as the RFC 7523 assertion
//! of the Claude API's exchange, never re-presented.

use wiremock::matchers::{body_json, body_string_contains, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::tests::{K8S_TOKEN, token_file};
use super::*;

fn config(
    server: &MockServer,
    file: &std::path::Path,
    workspace: Option<&str>,
) -> FederationConfig {
    FederationConfig {
        target: ExchangeTarget::Anthropic {
            federation_rule_id: "fdrl_test".into(),
            organization_id: "00000000-0000-0000-0000-000000000001".into(),
            service_account_id: "svac_test".into(),
            workspace_id: workspace.map(str::to_string),
        },
        identity_token_file: file.to_path_buf(),
        token_exchange_url: format!("{}/v1/oauth/token", server.uri()),
        authentik: Some(AuthentikConfig {
            token_url: format!("{}/application/o/token/", server.uri()),
            client_id: "ds-enricher-anthropic".into(),
            scope: None,
        }),
        refresh_skew: Duration::from_secs(60),
        exchange_timeout: Duration::from_secs(5),
    }
}

/// Authentik issues a new token (so a new `jti`) on every request.
struct CountingAuthentik(AtomicU64);

impl Respond for CountingAuthentik {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": format!("authentik.jwt-{n}"),
            "token_type": "Bearer",
            "expires_in": 1800,
        }))
    }
}

fn claude_token(token: &str, expires_in: u64) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": expires_in,
        "scope": "workspace:developer",
    }))
}

fn exchange_body(assertion: &str, workspace: Option<&str>) -> serde_json::Value {
    let mut body = serde_json::json!({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": assertion,
        "federation_rule_id": "fdrl_test",
        "organization_id": "00000000-0000-0000-0000-000000000001",
        "service_account_id": "svac_test",
    });
    if let Some(workspace) = workspace {
        body["workspace_id"] = workspace.into();
    }
    body
}

async fn mount_authentik(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/application/o/token/"))
        .and(body_string_contains("client_id=ds-enricher-anthropic"))
        .and(body_string_contains(format!(
            "client_assertion={K8S_TOKEN}"
        )))
        .respond_with(CountingAuthentik(AtomicU64::new(0)))
        .mount(server)
        .await;
}

/// The chain, the JSON body (with and without `workspace_id`), the cache,
/// and a FRESH Authentik token for every Claude exchange, the 401-driven
/// one included.
#[tokio::test]
async fn every_exchange_presents_a_fresh_authentik_token() {
    let server = MockServer::start().await;
    let file = token_file(K8S_TOKEN);
    mount_authentik(&server).await;
    for (n, token) in [
        (1, "sk-ant-oat01-a"),
        (2, "sk-ant-oat01-b"),
        (3, "sk-ant-oat01-c"),
    ] {
        Mock::given(method("POST"))
            .and(path("/v1/oauth/token"))
            .and(body_json(exchange_body(
                &format!("authentik.jwt-{n}"),
                Some("wrkspc_ds"),
            )))
            .respond_with(claude_token(token, 3600))
            .expect(1)
            .mount(&server)
            .await;
    }
    let clock = Clock::default();
    let source = FederatedTokenSource::with_clock(
        config(&server, file.path(), Some("wrkspc_ds")),
        clock.clone(),
    )
    .unwrap();
    let first = source.bearer().await.unwrap();
    assert_eq!(first.expose(), "sk-ant-oat01-a");
    // Cached: no new exchange.
    assert_eq!(source.bearer().await.unwrap().expose(), "sk-ant-oat01-a");
    // A 401 to it: re-exchanged with a new Authentik token.
    source.invalidate(&first);
    assert_eq!(source.bearer().await.unwrap().expose(), "sk-ant-oat01-b");
    // Expiry: a refresh, again with a new Authentik token.
    clock.advance(Duration::from_secs(3600));
    assert_eq!(source.bearer().await.unwrap().expose(), "sk-ant-oat01-c");
    let authentik_calls = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/application/o/token/")
        .count();
    assert_eq!(
        authentik_calls, 3,
        "one Authentik token per Claude exchange"
    );

    // Without a workspace, the field is left out.
    let server = MockServer::start().await;
    mount_authentik(&server).await;
    Mock::given(method("POST"))
        .and(path("/v1/oauth/token"))
        .and(body_json(exchange_body("authentik.jwt-1", None)))
        .respond_with(claude_token("sk-ant-oat01-z", 3600))
        .expect(1)
        .mount(&server)
        .await;
    let source = FederatedTokenSource::new(config(&server, file.path(), None)).unwrap();
    assert_eq!(source.bearer().await.unwrap().expose(), "sk-ant-oat01-z");
}

/// Every Claude denial is the same opaque 401: `authentication_failed` on
/// `stage="anthropic"`, typed as a credential failure, and counted.
#[tokio::test]
async fn an_opaque_401_is_authentication_failed() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let server = MockServer::start().await;
    let file = token_file(K8S_TOKEN);
    mount_authentik(&server).await;
    Mock::given(method("POST"))
        .and(path("/v1/oauth/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "type": "error",
            "error": { "type": "authentication_error", "message": "Authentication failed" },
            "request_id": "req_x"
        })))
        .mount(&server)
        .await;
    let source = FederatedTokenSource::new(config(&server, file.path(), None)).unwrap();
    let series = |stage: &str, outcome: &str| {
        format!(
            "distant_signal_enricher_llm_token_exchange_total{{stage=\"{stage}\",outcome=\"{outcome}\"}}"
        )
    };
    let rendered = handle.render();
    for stage in ["authentik", "anthropic"] {
        assert!(
            rendered.contains(&format!("{} 0", series(stage, "authentication_failed"))),
            "{rendered}"
        );
    }
    assert!(!rendered.contains("stage=\"openai\""), "{rendered}");
    let err = source.bearer().await.unwrap_err();
    assert!(matches!(
        err,
        LlmCallError::CredentialUnavailable {
            stage: "anthropic",
            kind: "authentication_failed"
        }
    ));
    let rendered = handle.render();
    assert!(
        rendered.contains(&format!(
            "{} 1",
            series("anthropic", "authentication_failed")
        )),
        "{rendered}"
    );
    // A 401 with no body is classified the same way on this stage only.
    assert_eq!(
        exchange_outcome(Stage::Anthropic, 401, &ExchangeErrorBody::default()),
        "authentication_failed"
    );
    assert_eq!(
        exchange_outcome(Stage::Openai, 401, &ExchangeErrorBody::default()),
        "http_error"
    );
}
