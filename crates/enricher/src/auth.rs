//! Credentials for the LLM endpoint: none, a static API key, or a
//! short-lived `OpenAI` access token minted by workload identity federation
//! (WIF), with no static secret anywhere.
//!
//! See docs/enricher-openai.md, "Keyless auth (workload identity
//! federation)". Two federated flows, chosen by `LLM_AUTH`:
//!
//! - `openai-wif-authentik` (primary): the kubelet-projected service-account
//!   token is sent to Authentik's token endpoint as a `client_assertion`
//!   (Authentik's JWT-federation machine-to-machine flow); Authentik's
//!   access token is the subject token of the `OpenAI` exchange.
//! - `openai-wif-kubernetes` (fallback): the projected token is the subject
//!   token itself (`OpenAI` verifies it against an uploaded k3s JWKS).
//!
//! Either way the `OpenAI` exchange (`POST https://auth.openai.com/oauth/token`,
//! RFC 8693 token exchange) returns a bearer token lasting at most an hour,
//! with no refresh token: renewing is exchanging again. Tokens are cached
//! until shortly before they expire, refreshed single-flight, and never
//! logged (every one is a [`Secret`], whose `Debug` prints no value).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::secret::Secret;
use serde::{Deserialize, Serialize};

use crate::llm::LlmCallError;

/// `enricher_llm_token_exchange_total{stage, outcome}`.
pub(crate) const EXCHANGE_METRIC: &str = "enricher_llm_token_exchange_total";
/// `enricher_llm_token_remaining_seconds`: the cached `OpenAI` token's
/// remaining lifetime, set on every exchange and every LLM call.
pub(crate) const REMAINING_METRIC: &str = "enricher_llm_token_remaining_seconds";

/// Every `outcome` label of [`EXCHANGE_METRIC`]; registered at 0 so an
/// alert's `increase()` sees the first failure.
pub(crate) const EXCHANGE_OUTCOMES: [&str; 8] = [
    "success",
    "token_file_error",
    "invalid_grant",
    "invalid_client",
    "invalid_subject_token",
    "http_error",
    "timeout",
    "error",
];

/// Per-request timeout of each token-endpoint POST (Authentik and `OpenAI`).
pub(crate) const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);

/// Lifetime assumed for a token response that states none (neither
/// `expires_in` nor `expires_at`). Neither endpoint does that; short on
/// purpose.
const FALLBACK_LIFETIME: Duration = Duration::from_secs(300);

/// Ceiling on any stated lifetime (`OpenAI` caps its tokens at one hour), so
/// an absurd `expires_in` can neither overflow `Instant` nor pin a token.
const MAX_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

/// A token this close to expiry is not handed out even as a fallback.
const MIN_REMAINING: Duration = Duration::from_secs(5);

const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";
const JWT_BEARER_ASSERTION: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// Longest server-supplied `error_description` kept in a log line.
const MAX_DESCRIPTION_CHARS: usize = 200;

/// `LLM_AUTH`: how the enricher authenticates to the LLM endpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LlmAuthMode {
    /// `LLM_API_KEY` as a static bearer token (or none when unset).
    #[default]
    ApiKey,
    /// k8s token -> Authentik -> `OpenAI` token exchange.
    OpenaiWifAuthentik,
    /// k8s token -> `OpenAI` token exchange.
    OpenaiWifKubernetes,
}

/// Which token endpoint a failure (or success) belongs to; the `stage`
/// label of [`EXCHANGE_METRIC`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    Authentik,
    Openai,
}

impl Stage {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Authentik => "authentik",
            Self::Openai => "openai",
        }
    }
}

/// Authentik's JWT-federation client-credentials settings.
#[derive(Debug, Clone)]
pub(crate) struct AuthentikConfig {
    /// e.g. `https://sso.example.com/application/o/token/`.
    pub token_url: String,
    pub client_id: String,
    /// Sent as `scope` when set (e.g. `profile`, for a `groups` claim).
    pub scope: Option<String>,
}

/// Everything a [`FederatedTokenSource`] needs. No secret in here: the only
/// credential is the projected token file, read on every exchange.
#[derive(Debug, Clone)]
pub(crate) struct FederationConfig {
    pub identity_provider_id: String,
    pub service_account_id: String,
    /// The kubelet-projected service-account token. Re-read on every
    /// exchange: the kubelet rotates it.
    pub identity_token_file: PathBuf,
    /// `OpenAI`'s token-exchange endpoint.
    pub token_exchange_url: String,
    /// `Some` in `openai-wif-authentik` mode.
    pub authentik: Option<AuthentikConfig>,
    /// Lower bound of the refresh margin (`LLM_TOKEN_REFRESH_SKEW_SECS`); the
    /// margin is the larger of this and 10% of the token's lifetime.
    pub refresh_skew: Duration,
    /// Per-request timeout of each token-endpoint POST ([`EXCHANGE_TIMEOUT`];
    /// tests shorten it).
    pub exchange_timeout: Duration,
}

/// The LLM endpoint's credential.
pub(crate) enum LlmAuth {
    /// No `Authorization` header (a local server).
    None,
    /// `LLM_API_KEY`, sent as-is.
    Static(Secret),
    /// A workload-identity-federated `OpenAI` access token.
    Federated(Arc<FederatedTokenSource>),
}

impl std::fmt::Debug for LlmAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("LlmAuth::None"),
            Self::Static(_) => f.write_str("LlmAuth::Static(***)"),
            Self::Federated(source) => write!(f, "LlmAuth::Federated({source:?})"),
        }
    }
}

impl LlmAuth {
    /// The pre-WIF mapping of `LLM_API_KEY`: `Some` (even empty) is sent as
    /// a bearer token, exactly as before.
    pub(crate) fn from_api_key(api_key: Option<String>) -> Self {
        api_key.map_or(Self::None, |key| Self::Static(Secret::new(key)))
    }

    pub(crate) fn is_federated(&self) -> bool {
        matches!(self, Self::Federated(_))
    }

    /// The bearer token for the next request, if any. Only the federated
    /// variant can fail (`LlmCallError::CredentialUnavailable`).
    pub(crate) async fn bearer(&self) -> Result<Option<Secret>, LlmCallError> {
        match self {
            Self::None => Ok(None),
            Self::Static(key) => Ok(Some(key.clone())),
            Self::Federated(source) => source.bearer().await.map(Some),
        }
    }

    /// The endpoint rejected `rejected` (a 401): forget it, so the next
    /// [`Self::bearer`] exchanges again. A no-op unless it is still the
    /// cached token, so concurrent 401s for one token trigger one refresh.
    pub(crate) fn invalidate(&self, rejected: &Secret) {
        if let Self::Federated(source) = self {
            source.invalidate(rejected);
        }
    }
}

/// Monotonic and wall clocks, offset together. Production never advances
/// it; tests do, instead of pausing tokio's clock (which would fire
/// reqwest's timeouts while a request waits on the mock server).
#[derive(Debug, Clone, Default)]
pub(crate) struct Clock {
    offset_ms: Arc<AtomicU64>,
}

impl Clock {
    fn offset(&self) -> Duration {
        Duration::from_millis(self.offset_ms.load(Ordering::Relaxed))
    }

    fn now(&self) -> Instant {
        Instant::now() + self.offset()
    }

    fn unix_now(&self) -> u64 {
        (SystemTime::now() + self.offset())
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }

    #[cfg(test)]
    pub(crate) fn advance(&self, by: Duration) {
        let millis = u64::try_from(by.as_millis()).unwrap_or(u64::MAX);
        self.offset_ms.fetch_add(millis, Ordering::Relaxed);
    }
}

/// A cached access token and its deadlines.
struct CachedToken {
    token: Secret,
    /// Exchange again from here on.
    refresh_at: Instant,
    /// The token's own expiry, as far as we can tell.
    expires_at: Instant,
}

/// A failed exchange: which endpoint, and its outcome label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExchangeFailure {
    stage: Stage,
    outcome: &'static str,
}

impl ExchangeFailure {
    fn into_call_error(self) -> LlmCallError {
        LlmCallError::CredentialUnavailable {
            stage: self.stage.label(),
            kind: self.outcome,
        }
    }
}

/// State only touched under the refresh lock.
#[derive(Default)]
struct RefreshState {
    /// The Authentik access token (authentik mode), cached by its own
    /// `expires_in`.
    authentik: Option<CachedToken>,
    /// The last failed refresh and when it ended: a caller that was already
    /// waiting for the lock gets this error instead of repeating the
    /// exchange, so N concurrent calls during an outage make one request.
    last_failure: Option<(Instant, ExchangeFailure)>,
}

/// Mints, caches and renews federated `OpenAI` access tokens.
pub(crate) struct FederatedTokenSource {
    config: FederationConfig,
    /// Its own client: a 10 s per-request timeout, not the LLM's minutes.
    http: reqwest::Client,
    clock: Clock,
    /// The current `OpenAI` token. A std mutex: never held across an await.
    cached: std::sync::Mutex<Option<CachedToken>>,
    /// Single-flight refresh.
    refresh: tokio::sync::Mutex<RefreshState>,
}

impl std::fmt::Debug for FederatedTokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederatedTokenSource")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// `{access_token, token_type, expires_in, expires_at}` from either token
/// endpoint. No `Debug`: it holds a token.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    expires_at: Option<u64>,
}

/// An error body's OAuth `error` code and `error_description`, from either
/// RFC 6749's flat shape (`{"error": "invalid_grant", ...}`) or an
/// OpenAI-style object (`{"error": {"code"|"type", "message"}}`).
#[derive(Debug, Default, PartialEq, Eq)]
struct ExchangeErrorBody {
    code: Option<String>,
    description: Option<String>,
}

fn parse_exchange_error(body: &str) -> ExchangeErrorBody {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return ExchangeErrorBody::default();
    };
    let text =
        |v: Option<&serde_json::Value>| v.and_then(serde_json::Value::as_str).map(str::to_string);
    match value.get("error") {
        Some(serde_json::Value::String(code)) => ExchangeErrorBody {
            code: Some(code.clone()),
            description: text(value.get("error_description")),
        },
        Some(object @ serde_json::Value::Object(_)) => ExchangeErrorBody {
            code: text(object.get("code")).or_else(|| text(object.get("type"))),
            description: text(object.get("message")),
        },
        _ => ExchangeErrorBody::default(),
    }
}

/// The outcome label of a non-2xx token-endpoint response.
fn exchange_outcome(body: &ExchangeErrorBody) -> &'static str {
    match body.code.as_deref() {
        Some("invalid_grant") => "invalid_grant",
        Some("invalid_client") => "invalid_client",
        Some("invalid_subject_token") => "invalid_subject_token",
        _ => "http_error",
    }
}

/// When to refresh a token issued in a request started at `t0`, and when it
/// expires: lifetime = `min(expires_in, expires_at - now)` (`expires_in`
/// counts from issuance, so measuring it from `t0` is conservative), and
/// the margin is the larger of `skew` and 10% of the lifetime.
fn deadlines(
    t0: Instant,
    unix_now: u64,
    expires_in: Option<u64>,
    expires_at: Option<u64>,
    skew: Duration,
) -> (Instant, Instant) {
    let until_expires_at = expires_at.map(|at| at.saturating_sub(unix_now));
    let lifetime = match (expires_in, until_expires_at) {
        (Some(a), Some(b)) => Duration::from_secs(a.min(b)),
        (Some(a), None) | (None, Some(a)) => Duration::from_secs(a),
        (None, None) => FALLBACK_LIFETIME,
    }
    .min(MAX_LIFETIME);
    let margin = skew.max(lifetime / 10);
    let refresh_at = t0 + lifetime.saturating_sub(margin);
    (refresh_at, t0 + lifetime)
}

fn record_exchange(stage: Stage, outcome: &'static str) {
    metrics::counter!(
        common::metrics::metric_name(EXCHANGE_METRIC),
        "stage" => stage.label(),
        "outcome" => outcome
    )
    .increment(1);
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a token lifetime in seconds is far below 2^52"
)]
fn record_remaining(remaining: Duration) {
    metrics::gauge!(common::metrics::metric_name(REMAINING_METRIC)).set(remaining.as_secs() as f64);
}

impl FederatedTokenSource {
    pub(crate) fn new(config: FederationConfig) -> anyhow::Result<Self> {
        Self::with_clock(config, Clock::default())
    }

    pub(crate) fn with_clock(config: FederationConfig, clock: Clock) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(config.exchange_timeout)
            .build()?;
        let source = Self {
            config,
            http,
            clock,
            cached: std::sync::Mutex::new(None),
            refresh: tokio::sync::Mutex::new(RefreshState::default()),
        };
        source.register_metrics();
        Ok(source)
    }

    /// Every `{stage, outcome}` this mode can produce, at 0.
    fn register_metrics(&self) {
        let stages: &[Stage] = if self.config.authentik.is_some() {
            &[Stage::Authentik, Stage::Openai]
        } else {
            &[Stage::Openai]
        };
        for stage in stages {
            for outcome in EXCHANGE_OUTCOMES {
                metrics::counter!(
                    common::metrics::metric_name(EXCHANGE_METRIC),
                    "stage" => stage.label(),
                    "outcome" => outcome
                )
                .increment(0);
            }
        }
        record_remaining(Duration::ZERO);
    }

    fn lock_cached(&self) -> std::sync::MutexGuard<'_, Option<CachedToken>> {
        // A poisoned lock only means another task panicked mid-update of a
        // plain Option; the value is still usable.
        self.cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The cached token if it is before its refresh deadline.
    fn fresh_token(&self) -> Option<Secret> {
        let now = self.clock.now();
        let cached = self.lock_cached();
        let token = cached.as_ref().filter(|t| now < t.refresh_at)?;
        record_remaining(token.expires_at.saturating_duration_since(now));
        Some(token.token.clone())
    }

    /// The cached token if it has not yet (nearly) expired: the fallback
    /// when a refresh fails inside the refresh margin.
    fn unexpired_token(&self) -> Option<Secret> {
        let now = self.clock.now();
        let cached = self.lock_cached();
        let token = cached
            .as_ref()
            .filter(|t| now + MIN_REMAINING < t.expires_at)?;
        Some(token.token.clone())
    }

    pub(crate) async fn bearer(&self) -> Result<Secret, LlmCallError> {
        if let Some(token) = self.fresh_token() {
            return Ok(token);
        }
        let waiting_since = self.clock.now();
        let mut state = self.refresh.lock().await;
        // Another caller may have refreshed (or failed to) while this one
        // waited for the lock.
        if let Some(token) = self.fresh_token() {
            return Ok(token);
        }
        if let Some((at, failure)) = state.last_failure
            && at >= waiting_since
        {
            return self
                .unexpired_token()
                .ok_or_else(|| failure.into_call_error());
        }
        match self.refresh_locked(&mut state).await {
            Ok(token) => {
                state.last_failure = None;
                Ok(token)
            }
            Err(failure) => {
                state.last_failure = Some((self.clock.now(), failure));
                match self.unexpired_token() {
                    Some(token) => {
                        tracing::warn!(
                            stage = failure.stage.label(),
                            outcome = failure.outcome,
                            "LLM token refresh failed; using the cached token until it expires"
                        );
                        Ok(token)
                    }
                    None => Err(failure.into_call_error()),
                }
            }
        }
    }

    pub(crate) fn invalidate(&self, rejected: &Secret) {
        let mut cached = self.lock_cached();
        if cached
            .as_ref()
            .is_some_and(|t| t.token.expose() == rejected.expose())
        {
            *cached = None;
            record_remaining(Duration::ZERO);
            tracing::info!("LLM endpoint rejected the federated token; it will be re-exchanged");
        }
    }

    /// One full refresh: the subject token (file, or file -> Authentik),
    /// then the `OpenAI` exchange. Caller holds the refresh lock.
    async fn refresh_locked(&self, state: &mut RefreshState) -> Result<Secret, ExchangeFailure> {
        let subject = match &self.config.authentik {
            None => self.read_identity_token(Stage::Openai)?,
            Some(authentik) => self.authentik_token(authentik, state).await?,
        };
        let result = self.openai_exchange(&subject).await;
        if let Err(failure) = &result
            && matches!(failure.outcome, "invalid_grant" | "invalid_subject_token")
        {
            // OpenAI rejected Authentik's token: get a fresh one next time.
            state.authentik = None;
        }
        result
    }

    /// The projected service-account token, re-read every time (the kubelet
    /// rotates it). `stage` is the endpoint it is about to be sent to.
    fn read_identity_token(&self, stage: Stage) -> Result<Secret, ExchangeFailure> {
        let failure = |reason: &str| {
            tracing::warn!(
                stage = stage.label(),
                path = %self.config.identity_token_file.display(),
                reason,
                "cannot read the projected identity token"
            );
            record_exchange(stage, "token_file_error");
            ExchangeFailure {
                stage,
                outcome: "token_file_error",
            }
        };
        match std::fs::read_to_string(&self.config.identity_token_file) {
            Ok(raw) if !raw.trim().is_empty() => Ok(Secret::new(raw.trim())),
            Ok(_) => Err(failure("the file is empty")),
            Err(err) => Err(failure(&err.to_string())),
        }
    }

    /// The cached Authentik token, or a new one from the token endpoint.
    async fn authentik_token(
        &self,
        authentik: &AuthentikConfig,
        state: &mut RefreshState,
    ) -> Result<Secret, ExchangeFailure> {
        let now = self.clock.now();
        if let Some(cached) = state.authentik.as_ref().filter(|t| now < t.refresh_at) {
            return Ok(cached.token.clone());
        }
        state.authentik = None;
        let assertion = self.read_identity_token(Stage::Authentik)?;
        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_id", authentik.client_id.as_str()),
            ("client_assertion_type", JWT_BEARER_ASSERTION),
            ("client_assertion", assertion.expose()),
        ];
        if let Some(scope) = authentik.scope.as_deref() {
            form.push(("scope", scope));
        }
        let request = self.http.post(&authentik.token_url).form(&form);
        let token = self
            .post_token(Stage::Authentik, request, &[&assertion])
            .await?;
        let issued = token.token.clone();
        state.authentik = Some(token);
        Ok(issued)
    }

    async fn openai_exchange(&self, subject: &Secret) -> Result<Secret, ExchangeFailure> {
        #[derive(Serialize)]
        struct ExchangeRequest<'a> {
            grant_type: &'a str,
            subject_token: &'a str,
            subject_token_type: &'a str,
            identity_provider_id: &'a str,
            service_account_id: &'a str,
        }
        let request = self
            .http
            .post(&self.config.token_exchange_url)
            .json(&ExchangeRequest {
                grant_type: TOKEN_EXCHANGE_GRANT,
                subject_token: subject.expose(),
                subject_token_type: JWT_TOKEN_TYPE,
                identity_provider_id: &self.config.identity_provider_id,
                service_account_id: &self.config.service_account_id,
            });
        let token = self.post_token(Stage::Openai, request, &[subject]).await?;
        let issued = token.token.clone();
        record_remaining(token.expires_at.saturating_duration_since(self.clock.now()));
        *self.lock_cached() = Some(token);
        Ok(issued)
    }

    /// Sends one token request and classifies the result. `redact` are the
    /// credentials in the request, scrubbed from any echoed error text.
    async fn post_token(
        &self,
        stage: Stage,
        request: reqwest::RequestBuilder,
        redact: &[&Secret],
    ) -> Result<CachedToken, ExchangeFailure> {
        let fail = |outcome: &'static str| {
            record_exchange(stage, outcome);
            ExchangeFailure { stage, outcome }
        };
        let t0 = self.clock.now();
        let response = request.send().await.map_err(|err| {
            let outcome = if err.is_timeout() { "timeout" } else { "error" };
            tracing::warn!(
                stage = stage.label(),
                outcome,
                error = %err.without_url(),
                "LLM token request failed"
            );
            fail(outcome)
        })?;
        let status = response.status();
        if !status.is_success() {
            let body = parse_exchange_error(&response.text().await.unwrap_or_default());
            let outcome = exchange_outcome(&body);
            let description = body.description.map(|d| {
                let mut shown: String = d.chars().take(MAX_DESCRIPTION_CHARS).collect();
                for secret in redact {
                    if !secret.is_empty() {
                        shown = shown.replace(secret.expose(), "[redacted]");
                    }
                }
                shown
            });
            tracing::warn!(
                stage = stage.label(),
                status = status.as_u16(),
                error_code = body.code.as_deref(),
                error_description = description.as_deref(),
                outcome,
                "LLM token request rejected"
            );
            return Err(fail(outcome));
        }
        let parsed: TokenResponse = response.json().await.map_err(|err| {
            let outcome = if err.is_timeout() { "timeout" } else { "error" };
            tracing::warn!(
                stage = stage.label(),
                outcome,
                "LLM token response was not a usable token response"
            );
            fail(outcome)
        })?;
        if parsed.access_token.is_empty()
            || parsed
                .token_type
                .as_deref()
                .is_some_and(|t| !t.eq_ignore_ascii_case("bearer"))
        {
            tracing::warn!(
                stage = stage.label(),
                token_type = parsed.token_type.as_deref(),
                "LLM token response had no bearer access token"
            );
            return Err(fail("error"));
        }
        let (refresh_at, expires_at) = deadlines(
            t0,
            self.clock.unix_now(),
            parsed.expires_in,
            parsed.expires_at,
            self.config.refresh_skew,
        );
        record_exchange(stage, "success");
        tracing::info!(
            stage = stage.label(),
            lifetime_secs = expires_at.saturating_duration_since(t0).as_secs(),
            refresh_in_secs = refresh_at.saturating_duration_since(t0).as_secs(),
            "LLM token issued"
        );
        Ok(CachedToken {
            token: Secret::new(parsed.access_token),
            refresh_at,
            expires_at,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Write as _;

    use wiremock::matchers::{body_json, body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    pub(crate) const K8S_TOKEN: &str = "k8s.projected.token-1";
    pub(crate) const AUTHENTIK_TOKEN: &str = "authentik.issued.jwt-1";

    pub(crate) fn token_file(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    pub(crate) fn kubernetes_config(
        server: &MockServer,
        file: &std::path::Path,
    ) -> FederationConfig {
        FederationConfig {
            identity_provider_id: "idp_test".into(),
            service_account_id: "svc_acct_test".into(),
            identity_token_file: file.to_path_buf(),
            token_exchange_url: format!("{}/oauth/token", server.uri()),
            authentik: None,
            refresh_skew: Duration::from_secs(60),
            exchange_timeout: Duration::from_secs(5),
        }
    }

    fn authentik_config(server: &MockServer, file: &std::path::Path) -> FederationConfig {
        FederationConfig {
            authentik: Some(AuthentikConfig {
                token_url: format!("{}/application/o/token/", server.uri()),
                client_id: "ds-enricher-client".into(),
                scope: Some("profile".into()),
            }),
            ..kubernetes_config(server, file)
        }
    }

    pub(crate) fn openai_token(token: &str, expires_in: u64) -> ResponseTemplate {
        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + expires_in;
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": token,
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "token_type": "Bearer",
            "expires_in": expires_in,
            "expires_at": expires_at,
            "scope": "api.model.request",
        }))
    }

    fn authentik_response(token: &str, expires_in: u64) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": expires_in,
            "id_token": "unused",
        }))
    }

    pub(crate) fn exchange_body(subject: &str) -> serde_json::Value {
        serde_json::json!({
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": subject,
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "identity_provider_id": "idp_test",
            "service_account_id": "svc_acct_test",
        })
    }

    fn new_source(config: FederationConfig) -> (FederatedTokenSource, Clock) {
        let clock = Clock::default();
        let source = FederatedTokenSource::with_clock(config, clock.clone()).unwrap();
        (source, clock)
    }

    /// Primary flow: the projected token goes to Authentik as a JWT
    /// client assertion (form-encoded), and Authentik's token is the
    /// subject of the `OpenAI` exchange (JSON).
    #[tokio::test]
    async fn authentik_then_openai_chain() {
        let server = MockServer::start().await;
        let file = token_file(&format!("{K8S_TOKEN}\n"));
        Mock::given(method("POST"))
            .and(path("/application/o/token/"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("client_id=ds-enricher-client"))
            .and(body_string_contains(
                "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer",
            ))
            .and(body_string_contains(format!("client_assertion={K8S_TOKEN}&")))
            .and(body_string_contains("scope=profile"))
            .respond_with(authentik_response(AUTHENTIK_TOKEN, 900))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(header("content-type", "application/json"))
            .and(body_json(exchange_body(AUTHENTIK_TOKEN)))
            .respond_with(openai_token("oai-1", 3600))
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = new_source(authentik_config(&server, file.path()));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-1");
        // Cached: no second request to either endpoint.
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-1");
    }

    /// Fallback flow: the projected token is the subject token itself.
    #[tokio::test]
    async fn kubernetes_mode_sends_the_projected_token() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_json(exchange_body(K8S_TOKEN)))
            .respond_with(openai_token("oai-k", 3600))
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = new_source(kubernetes_config(&server, file.path()));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-k");
    }

    /// Concurrent callers on a cold cache make one exchange between them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_callers_share_one_exchange() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/application/o/token/"))
            .respond_with(
                authentik_response(AUTHENTIK_TOKEN, 900).set_delay(Duration::from_millis(100)),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(openai_token("oai-1", 3600).set_delay(Duration::from_millis(200)))
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = new_source(authentik_config(&server, file.path()));
        let source = Arc::new(source);
        let calls: Vec<_> = (0..8)
            .map(|_| {
                let source = Arc::clone(&source);
                tokio::spawn(async move { source.bearer().await.unwrap() })
            })
            .collect();
        for call in calls {
            assert_eq!(call.await.unwrap().expose(), "oai-1");
        }
    }

    /// Concurrent callers during an outage share one failed exchange too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_callers_share_one_failure() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"error": "invalid_grant"}))
                    .set_delay(Duration::from_millis(200)),
            )
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = new_source(kubernetes_config(&server, file.path()));
        let source = Arc::new(source);
        let calls: Vec<_> = (0..6)
            .map(|_| {
                let source = Arc::clone(&source);
                tokio::spawn(async move { source.bearer().await })
            })
            .collect();
        for call in calls {
            assert!(matches!(
                call.await.unwrap(),
                Err(LlmCallError::CredentialUnavailable {
                    stage: "openai",
                    kind: "invalid_grant"
                })
            ));
        }
    }

    /// Refreshed before the deadline: a 3600 s token with the default 60 s
    /// skew refreshes after 3240 s (the 10% margin is larger), not before.
    #[tokio::test]
    async fn refreshes_inside_the_margin_and_not_before() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(openai_token("oai-1", 3600))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(openai_token("oai-2", 3600))
            .expect(1)
            .mount(&server)
            .await;
        let (source, clock) = new_source(kubernetes_config(&server, file.path()));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-1");
        clock.advance(Duration::from_secs(3200));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-1");
        clock.advance(Duration::from_secs(60));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-2");
    }

    /// The Authentik token is cached by its own `expires_in`: a short
    /// `OpenAI` token is renewed with the same Authentik token.
    #[tokio::test]
    async fn authentik_token_is_cached_by_its_own_lifetime() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/application/o/token/"))
            .respond_with(authentik_response(AUTHENTIK_TOKEN, 900))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/application/o/token/"))
            .respond_with(authentik_response("authentik.issued.jwt-2", 900))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_json(exchange_body(AUTHENTIK_TOKEN)))
            .respond_with(openai_token("oai-short", 120))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_json(exchange_body("authentik.issued.jwt-2")))
            .respond_with(openai_token("oai-3", 120))
            .expect(1)
            .mount(&server)
            .await;
        let (source, clock) = new_source(authentik_config(&server, file.path()));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-short");
        // Past the OpenAI token's refresh (120 - 60 s), inside Authentik's
        // (900 - 90 s): one more OpenAI exchange, no Authentik request.
        clock.advance(Duration::from_secs(100));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-short");
        // Past Authentik's refresh too.
        clock.advance(Duration::from_secs(800));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-3");
    }

    /// The token file is re-read on every exchange (the kubelet rotates it).
    #[tokio::test]
    async fn rereads_the_rotated_token_file() {
        let server = MockServer::start().await;
        let file = token_file("k8s-1");
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_json(exchange_body("k8s-1")))
            .respond_with(openai_token("oai-1", 3600))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_json(exchange_body("k8s-2")))
            .respond_with(openai_token("oai-2", 3600))
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = new_source(kubernetes_config(&server, file.path()));
        let first = source.bearer().await.unwrap();
        assert_eq!(first.expose(), "oai-1");
        std::fs::write(file.path(), "k8s-2\n").unwrap();
        source.invalidate(&first);
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-2");
    }

    /// Only the token that was rejected is dropped: a stale 401 for an
    /// older token doesn't throw away a newer one.
    #[tokio::test]
    async fn invalidate_ignores_a_token_that_is_no_longer_cached() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(openai_token("oai-1", 3600))
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = new_source(kubernetes_config(&server, file.path()));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-1");
        source.invalidate(&Secret::new("oai-0"));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-1");
    }

    /// A failed refresh inside the margin keeps using the still-valid token;
    /// once that has expired the failure surfaces.
    #[tokio::test]
    async fn a_failed_refresh_falls_back_to_the_unexpired_token() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(openai_token("oai-1", 3600))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let (source, clock) = new_source(kubernetes_config(&server, file.path()));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-1");
        clock.advance(Duration::from_secs(3300));
        assert_eq!(source.bearer().await.unwrap().expose(), "oai-1");
        clock.advance(Duration::from_secs(400));
        assert!(matches!(
            source.bearer().await,
            Err(LlmCallError::CredentialUnavailable {
                stage: "openai",
                kind: "http_error"
            })
        ));
    }

    #[tokio::test]
    async fn a_missing_token_file_is_a_token_file_error() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let (source, _) = new_source(kubernetes_config(&server, &dir.path().join("absent")));
        assert!(matches!(
            source.bearer().await,
            Err(LlmCallError::CredentialUnavailable {
                stage: "openai",
                kind: "token_file_error"
            })
        ));
        let file = token_file("  \n");
        let (source, _) = new_source(authentik_config(&server, file.path()));
        assert!(matches!(
            source.bearer().await,
            Err(LlmCallError::CredentialUnavailable {
                stage: "authentik",
                kind: "token_file_error"
            })
        ));
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_slow_token_endpoint_times_out() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .respond_with(openai_token("late", 3600).set_delay(Duration::from_secs(2)))
            .mount(&server)
            .await;
        let config = FederationConfig {
            exchange_timeout: Duration::from_millis(200),
            ..kubernetes_config(&server, file.path())
        };
        let (source, _) = new_source(config);
        assert!(matches!(
            source.bearer().await,
            Err(LlmCallError::CredentialUnavailable {
                stage: "openai",
                kind: "timeout"
            })
        ));
    }

    /// Both error-body shapes map to the outcome labels; anything else is
    /// `http_error`.
    #[test]
    fn exchange_error_bodies_map_to_outcomes() {
        let cases = [
            (
                r#"{"error":"invalid_grant","error_description":"Policy denied"}"#,
                "invalid_grant",
                Some("Policy denied"),
            ),
            (r#"{"error":"invalid_client"}"#, "invalid_client", None),
            (
                r#"{"error":{"code":"invalid_subject_token","message":"bad kid"}}"#,
                "invalid_subject_token",
                Some("bad kid"),
            ),
            (
                r#"{"error":{"type":"invalid_grant","message":"no mapping"}}"#,
                "invalid_grant",
                Some("no mapping"),
            ),
            (r#"{"error":"unsupported_grant_type"}"#, "http_error", None),
            ("<html>502 Bad Gateway</html>", "http_error", None),
            ("", "http_error", None),
        ];
        for (body, outcome, description) in cases {
            let parsed = parse_exchange_error(body);
            assert_eq!(exchange_outcome(&parsed), outcome, "{body}");
            assert_eq!(parsed.description.as_deref(), description, "{body}");
        }
    }

    #[tokio::test]
    async fn rejections_are_classified_and_counted() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/application/o/token/"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"error": "invalid_client"})),
            )
            .mount(&server)
            .await;
        let (source, _) = new_source(authentik_config(&server, file.path()));
        assert!(matches!(
            source.bearer().await,
            Err(LlmCallError::CredentialUnavailable {
                stage: "authentik",
                kind: "invalid_client"
            })
        ));
        let rendered = handle.render();
        let metric = "distant_signal_enricher_llm_token_exchange_total";
        for (stage, outcome, count) in [
            ("authentik", "invalid_client", 1),
            ("authentik", "success", 0),
            ("openai", "invalid_grant", 0),
            ("openai", "success", 0),
        ] {
            let line = format!("{metric}{{stage=\"{stage}\",outcome=\"{outcome}\"}} {count}");
            assert!(rendered.contains(&line), "missing {line} in\n{rendered}");
        }
        assert!(rendered.contains("distant_signal_enricher_llm_token_remaining_seconds 0"));
    }

    #[tokio::test]
    async fn success_sets_the_remaining_gauge() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .respond_with(openai_token("oai-1", 3600))
            .mount(&server)
            .await;
        let (source, _) = new_source(kubernetes_config(&server, file.path()));
        source.bearer().await.unwrap();
        let rendered = handle.render();
        assert!(
            rendered.contains(
                r#"distant_signal_enricher_llm_token_exchange_total{stage="openai",outcome="success"} 1"#
            ),
            "{rendered}"
        );
        let remaining: f64 = rendered
            .lines()
            .find_map(|l| l.strip_prefix("distant_signal_enricher_llm_token_remaining_seconds "))
            .unwrap()
            .parse()
            .unwrap();
        assert!((3590.0..=3600.0).contains(&remaining), "{remaining}");
    }

    #[test]
    fn deadlines_use_the_shorter_lifetime_and_the_larger_margin() {
        let t0 = Instant::now();
        let skew = Duration::from_secs(60);
        // expires_at sooner than expires_in (a subject token about to expire).
        let (refresh, expires) = deadlines(t0, 1_000, Some(3600), Some(1_000 + 600), skew);
        assert_eq!(expires - t0, Duration::from_secs(600));
        assert_eq!(refresh - t0, Duration::from_secs(540));
        // 10% of an hour (360 s) beats the 60 s skew.
        let (refresh, _) = deadlines(t0, 0, Some(3600), None, skew);
        assert_eq!(refresh - t0, Duration::from_secs(3240));
        // A lifetime shorter than the margin refreshes at once.
        let (refresh, _) = deadlines(t0, 0, Some(30), None, skew);
        assert_eq!(refresh, t0);
        // expires_at in the past.
        let (refresh, expires) = deadlines(t0, 2_000, None, Some(1_000), skew);
        assert_eq!((refresh, expires), (t0, t0));
        // Nothing stated; absurd values are capped.
        let (_, expires) = deadlines(t0, 0, None, None, skew);
        assert_eq!(expires - t0, FALLBACK_LIFETIME);
        let (_, expires) = deadlines(t0, 0, Some(u64::MAX), None, skew);
        assert_eq!(expires - t0, MAX_LIFETIME);
    }

    /// No token, from any stage, in `Debug` output or in the logs of a
    /// success and a rejection that echoes the assertion back.
    #[tokio::test]
    async fn tokens_never_reach_debug_output_or_logs() {
        let logs = LogCapture::default();
        // The production JSON subscriber, as this (single-threaded) test
        // runtime's default.
        let subscriber = common::logging::json_subscriber(
            "enricher",
            tracing_subscriber::EnvFilter::new("trace"),
            logs.clone(),
        );
        // With exactly one live dispatcher, tracing-core lets a callsite that
        // a parallel test registers compute its interest from THAT thread's
        // (empty) default and cache "never" for everyone. A second live
        // dispatcher turns that shortcut off.
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        let _guard = tracing::subscriber::set_default(subscriber);

        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/application/o/token/"))
            .respond_with(authentik_response(AUTHENTIK_TOKEN, 900))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(openai_token("oai-secret-token", 3600))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_subject_token",
                "error_description": format!("token {AUTHENTIK_TOKEN} has an unknown kid"),
            })))
            .mount(&server)
            .await;
        let auth = LlmAuth::Federated(Arc::new(
            FederatedTokenSource::new(authentik_config(&server, file.path())).unwrap(),
        ));
        let token = auth.bearer().await.unwrap().unwrap();
        assert_eq!(token.expose(), "oai-secret-token");
        auth.invalidate(&token);
        assert!(auth.bearer().await.is_err());

        let debug = format!(
            "{auth:?} {token:?} {:?}",
            LlmAuth::from_api_key(Some("sk-static".into()))
        );
        let logged = logs.contents();
        assert!(logged.contains("LLM token issued"), "{logged}");
        assert!(logged.contains("[redacted] has an unknown kid"), "{logged}");
        for secret in [K8S_TOKEN, AUTHENTIK_TOKEN, "oai-secret-token", "sk-static"] {
            assert!(!debug.contains(secret), "{secret} in {debug}");
            assert!(!logged.contains(secret), "{secret} in {logged}");
        }
    }

    /// A `tracing_subscriber` writer that keeps everything in memory.
    #[derive(Clone, Default)]
    pub(crate) struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);

    impl LogCapture {
        pub(crate) fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    impl std::io::Write for LogCapture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }
}
