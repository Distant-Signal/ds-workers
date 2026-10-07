use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum as _};
use common::secret::Secret;

use crate::auth::{AuthentikConfig, FederatedTokenSource, FederationConfig, LlmAuth, LlmAuthMode};

/// CLI/env configuration for the `enricher` service.
/// `Debug` is safe to log: every credential is a [`common::secret::Secret`]
/// (SVC-12).
#[derive(Debug, Parser)]
pub(crate) struct Config {
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Secret,

    /// May carry a password (`redis://:pw@host`).
    #[arg(long, env, hide_env_values = true)]
    pub redis_url: Secret,

    /// Redis AUTH password (chart `redis.auth`, from a Secret). Unset or
    /// empty: no AUTH, `redis_url` is used as-is. Applied to `redis_url` by
    /// `common::redis_auth::redis_url_with_password`, never logged.
    #[arg(long, env, hide_env_values = true)]
    pub redis_password: Option<Secret>,

    /// Redis ACL user (chart `redis.acl`; ingest architecture phase 0c).
    /// Unset or empty: the `default` user, exactly as before. Combined with
    /// `redis_password` by `common::redis_auth::redis_url_with_credentials`.
    #[arg(long, env)]
    pub redis_username: Option<String>,

    /// Base URL of an OpenAI-compatible Chat Completions endpoint, e.g.
    /// `http://localhost:8080/v1` for a local server. No vendor is assumed.
    #[arg(long, env)]
    pub llm_base_url: String,

    /// Optional -- many local OpenAI-compatible servers don't require one.
    /// Only for `LLM_AUTH=api-key` (the default); setting it in a workload
    /// identity mode is a startup error.
    #[arg(long, env, hide_env_values = true)]
    pub llm_api_key: Option<Secret>,

    /// `LLM_AUTH` and the workload identity federation settings. Off by
    /// default (`api-key`) -- see [`LlmAuthConfig`].
    #[command(flatten)]
    pub llm_auth: LlmAuthConfig,

    /// Model name/identifier as the endpoint expects it.
    #[arg(long, env)]
    pub llm_model: String,

    /// Per-request timeout for a single LLM call (`LLM_REQUEST_TIMEOUT_SECS`).
    /// One incident makes three sequential calls (primary,
    /// resolution-adversarial, severity-adversarial -- see `llm.rs`), each
    /// retried up to `LLM_GATEWAY_RETRIES` times on a timeout. Real endpoints
    /// vary widely in latency; raise this if extractions are timing out.
    ///
    /// Behind a gateway that cuts requests itself (NVIDIA's hosted API
    /// returns 504 at ~302 s), set this a little ABOVE the gateway's cutoff
    /// (e.g. 320) so the gateway's 504 -- the actual cause -- is what gets
    /// reported, rather than a client timeout 2 s earlier.
    ///
    /// Raised from 120 to 300 (2026-08-21) after a live eval against a real
    /// self-hosted endpoint (Ollama, qwen3.5:4b) measured single-call
    /// latencies of 86-104s for the *flat* single-period case alone -- within
    /// striking distance of the old 120s default, and multi-period payloads
    /// (larger prompt/response) push that further. See
    /// docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md.
    #[arg(long, env, default_value_t = 300)]
    pub llm_request_timeout_secs: u64,

    /// Provider policy knobs (`LLM_MAX_TOKENS`, `LLM_REASONING_EFFORT`,
    /// `LLM_MAX_IN_FLIGHT`, `LLM_RATE_LIMIT_*`, `LLM_GATEWAY_RETRIES`). All
    /// off by default -- see [`ProviderPolicyConfig`].
    #[command(flatten)]
    pub provider: ProviderPolicyConfig,

    /// How often the reconciliation sweep runs, independent of the Redis
    /// Stream consumer loop. Backstop for a missed/lost publish.
    #[arg(long, env, default_value_t = 3600)]
    pub sweep_interval_secs: u64,

    /// How often to check the Redis Stream consumer group's pending-entries
    /// list for entries stuck longer than `reclaim_min_idle_secs` -- the
    /// debounced retry path for a request that timed out, or a process that
    /// crashed between processing and acking.
    #[arg(long, env, default_value_t = 60)]
    pub reclaim_interval_secs: u64,

    /// How long a pending entry must have sat unacked before it's eligible
    /// for reclaim -- i.e. the retry delay for a failed extraction.
    ///
    /// No longer a correctness bound: an entry whose incident is still being
    /// processed (by the stream loop, the sweep, or reclaim itself) is
    /// skipped by the reclaim loop via `main.rs`'s `InFlight` set, however
    /// long the attempt takes (in-call retries and 429 waits included). It
    /// just stays pending and is looked at again on a later pass. Keeping
    /// this above the typical worst case (3 x `llm_request_timeout_secs`)
    /// still avoids pointless claim-and-skip churn.
    #[arg(long, env, default_value_t = 1000)]
    pub reclaim_min_idle_secs: u64,

    /// Port for this service's Prometheus `/metrics` endpoint. The enricher
    /// has no HTTP server of its own, so `common::metrics::install_with_buckets`
    /// starts a dedicated listener on this port.
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    /// Whether to start this service's Prometheus `/metrics` listener at
    /// all. Distinct from `metrics_port` (which port to use IF started) --
    /// this is what actually satisfies "metrics.enabled=false leaves the
    /// service working exactly as it does today" (see the Helm chart's
    /// `metrics.enabled` value and this branch's final whole-branch
    /// review, Important finding #2): omitting the containerPort/env/
    /// annotations in the chart alone does not stop the process from
    /// listening, since Kubernetes container ports are purely
    /// declarative.
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,

    /// `CARRY_FORWARD_SEMANTIC_NOOPS`. When true, a same-model text change
    /// that `text_delta::classify` judges a semantic no-op (HTML/whitespace/
    /// entity/case/in-word-punctuation only) re-stamps the existing
    /// extraction's `source_text_hash` instead of running the LLM (see
    /// `queries::carry_forward_extraction` for its guards). Off by default:
    /// with it off, the edit class is still computed, but only to label the
    /// churn metrics (measurement only).
    #[arg(long, env, default_value_t = false)]
    pub carry_forward_semantic_noops: bool,

    /// `/livez` (liveness: loop progress) and `/healthz` (readiness: also
    /// false until the initial database connection is up) -- SVC-08/INF-5.
    #[command(flatten)]
    pub health: common::service_args::HealthArgs,
}

/// Per-provider request/retry knobs, flattened into [`Config`] and also
/// parsed on its own by the ignored live evals (`llm::live_client_from_env`),
/// so an eval run honours exactly the env vars the service does. Every knob
/// defaults to "off": an unset deployment sends byte-for-byte the same
/// request as before and never retries in-call. See `llm::ProviderPolicy`.
#[derive(Debug, Clone, Parser)]
#[expect(
    clippy::struct_field_names,
    reason = "clap derives each env var from the field name, so the prefix is part of the interface"
)]
pub(crate) struct ProviderPolicyConfig {
    /// Sent as `max_tokens` when set (e.g. 8192 for a reasoning model).
    #[arg(long, env)]
    pub llm_max_tokens: Option<u32>,
    /// Sent as `reasoning_effort` when set (e.g. `low`).
    #[arg(long, env)]
    pub llm_reasoning_effort: Option<String>,
    /// Cap on concurrent LLM HTTP attempts across stream/sweep/reclaim.
    #[arg(long, env)]
    pub llm_max_in_flight: Option<usize>,
    /// Minimum 429 back-off before an in-call retry, in seconds.
    #[arg(long, env, default_value_t = 20)]
    pub llm_rate_limit_retry_secs: u64,
    /// In-call retries on 429 (0 = fail the incident, the old behaviour).
    #[arg(long, env, default_value_t = 0)]
    pub llm_rate_limit_retries: u32,
    /// In-call retries on 502/503/504/client timeout (0 = the old
    /// behaviour). Above 0 also stops a timeout/504 feeding the per-text
    /// retry backoff -- see `LlmClient::is_provider_transient`.
    #[arg(long, env, default_value_t = 0)]
    pub llm_gateway_retries: u32,
}

impl ProviderPolicyConfig {
    pub(crate) fn policy(&self) -> crate::llm::ProviderPolicy {
        crate::llm::ProviderPolicy {
            max_tokens: self.llm_max_tokens,
            reasoning_effort: self.llm_reasoning_effort.clone(),
            max_in_flight: self.llm_max_in_flight,
            rate_limit_min_wait: Duration::from_secs(self.llm_rate_limit_retry_secs),
            max_rate_limit_retries: self.llm_rate_limit_retries,
            max_gateway_retries: self.llm_gateway_retries,
            gateway_backoff: crate::llm::DEFAULT_GATEWAY_BACKOFF,
        }
    }
}

/// How the enricher authenticates to the LLM endpoint (`auth.rs`,
/// docs/enricher-openai.md "Keyless auth"). The default, `api-key`, is the
/// pre-WIF behaviour exactly: `LLM_API_KEY` (if set) as a static bearer token,
/// and every other setting here ignored.
#[derive(Debug, Clone, Parser)]
pub(crate) struct LlmAuthConfig {
    /// `api-key` (default), `openai-wif-authentik` (k8s token -> Authentik
    /// -> `OpenAI` token exchange) or `openai-wif-kubernetes` (k8s token ->
    /// `OpenAI` token exchange).
    #[arg(long, env, value_enum, default_value_t = LlmAuthMode::ApiKey)]
    pub llm_auth: LlmAuthMode,
    /// `OpenAI` Workload Identity Provider ID. Required in WIF modes.
    #[arg(long, env)]
    pub openai_identity_provider_id: Option<String>,
    /// `OpenAI` service account ID the mapping resolves to. Required in WIF
    /// modes.
    #[arg(long, env)]
    pub openai_service_account_id: Option<String>,
    /// The kubelet-projected service-account token (re-read on every
    /// exchange). Must exist and be readable at startup in WIF modes.
    #[arg(long, env, default_value = "/var/run/secrets/openai/token")]
    pub llm_identity_token_file: PathBuf,
    /// `OpenAI`'s token-exchange endpoint.
    #[arg(long, env, default_value = "https://auth.openai.com/oauth/token")]
    pub llm_token_exchange_url: String,
    /// Authentik's token endpoint (e.g.
    /// `https://sso.example.com/application/o/token/`). Required in
    /// `openai-wif-authentik` mode.
    #[arg(long, env)]
    pub llm_authentik_token_url: Option<String>,
    /// Client ID of the Authentik `OAuth2` provider. Required in
    /// `openai-wif-authentik` mode.
    #[arg(long, env)]
    pub llm_authentik_client_id: Option<String>,
    /// Sent as `scope` to Authentik when set (e.g. `profile`, whose mapping
    /// emits the `groups` claim).
    #[arg(long, env)]
    pub llm_authentik_scope: Option<String>,
    /// Refresh a token at least this long before it expires (the margin is
    /// the larger of this and 10% of the token's lifetime).
    #[arg(long, env, default_value_t = 60)]
    pub llm_token_refresh_skew_secs: u64,
}

impl LlmAuthConfig {
    /// The federation settings for a WIF mode (`None` in `api-key` mode),
    /// validated without touching the file system.
    pub(crate) fn federation(
        &self,
        api_key: Option<&Secret>,
    ) -> anyhow::Result<Option<FederationConfig>> {
        if self.llm_auth == LlmAuthMode::ApiKey {
            return Ok(None);
        }
        let mode = self
            .llm_auth
            .to_possible_value()
            .map_or_else(String::new, |v| v.get_name().to_string());
        if api_key.is_some_and(|key| !key.is_empty()) {
            anyhow::bail!(
                "LLM_AUTH={mode} uses workload identity federation, which needs no static \
                 secret: unset LLM_API_KEY"
            );
        }
        let required = |value: &Option<String>, name: &str| {
            value
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string)
                .ok_or_else(|| anyhow::anyhow!("LLM_AUTH={mode} requires {name}"))
        };
        let identity_provider_id = required(
            &self.openai_identity_provider_id,
            "OPENAI_IDENTITY_PROVIDER_ID",
        )?;
        let service_account_id =
            required(&self.openai_service_account_id, "OPENAI_SERVICE_ACCOUNT_ID")?;
        let authentik = match self.llm_auth {
            LlmAuthMode::OpenaiWifAuthentik => Some(AuthentikConfig {
                token_url: required(&self.llm_authentik_token_url, "LLM_AUTHENTIK_TOKEN_URL")?,
                client_id: required(&self.llm_authentik_client_id, "LLM_AUTHENTIK_CLIENT_ID")?,
                scope: self
                    .llm_authentik_scope
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
            }),
            LlmAuthMode::OpenaiWifKubernetes | LlmAuthMode::ApiKey => None,
        };
        if self.llm_token_exchange_url.trim().is_empty() {
            anyhow::bail!("LLM_AUTH={mode} requires LLM_TOKEN_EXCHANGE_URL");
        }
        Ok(Some(FederationConfig {
            identity_provider_id,
            service_account_id,
            identity_token_file: self.llm_identity_token_file.clone(),
            token_exchange_url: self.llm_token_exchange_url.trim().to_string(),
            authentik,
            refresh_skew: Duration::from_secs(self.llm_token_refresh_skew_secs),
            exchange_timeout: crate::auth::EXCHANGE_TIMEOUT,
        }))
    }

    /// The client credential: validated settings and, in WIF modes, a
    /// readable, non-empty token file (checked once here so a missing
    /// projected volume fails the pod at startup, not every call).
    pub(crate) fn auth(&self, api_key: Option<&Secret>) -> anyhow::Result<LlmAuth> {
        let Some(federation) = self.federation(api_key)? else {
            return Ok(LlmAuth::from_api_key(
                api_key.map(|key| key.expose().to_string()),
            ));
        };
        let path = &federation.identity_token_file;
        let contents = std::fs::read_to_string(path).map_err(|err| {
            anyhow::anyhow!(
                "LLM_IDENTITY_TOKEN_FILE {} is not readable ({err}); mount the projected \
                 service-account token there",
                path.display()
            )
        })?;
        if contents.trim().is_empty() {
            anyhow::bail!("LLM_IDENTITY_TOKEN_FILE {} is empty", path.display());
        }
        Ok(LlmAuth::Federated(std::sync::Arc::new(
            FederatedTokenSource::new(federation)?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `LLM_REASONING_EFFORT=none` (`OpenAI`'s gpt-6-luna, see
    /// docs/enricher-openai.md) is accepted as-is: the knob is free text,
    /// passed through to `reasoning_effort` unchanged.
    #[test]
    fn reasoning_effort_none_is_accepted_and_passed_through() {
        let config =
            ProviderPolicyConfig::parse_from(["enricher", "--llm-reasoning-effort", "none"]);
        let policy = config.policy();
        assert_eq!(policy.reasoning_effort.as_deref(), Some("none"));
        assert_eq!(policy.max_tokens, None, "LLM_MAX_TOKENS stays unset");
    }

    fn auth_config(args: &[&str]) -> LlmAuthConfig {
        LlmAuthConfig::parse_from(std::iter::once("enricher").chain(args.iter().copied()))
    }

    fn error_of(config: &LlmAuthConfig, api_key: Option<&Secret>) -> String {
        config.federation(api_key).unwrap_err().to_string()
    }

    /// The default is `api-key`: none of the WIF settings are read, and
    /// `LLM_API_KEY` maps exactly as before (set, even empty, is sent).
    #[test]
    fn api_key_mode_is_the_default_and_ignores_wif_settings() {
        let config = auth_config(&[]);
        assert_eq!(config.llm_auth, LlmAuthMode::ApiKey);
        assert_eq!(
            config.llm_identity_token_file,
            PathBuf::from("/var/run/secrets/openai/token")
        );
        assert_eq!(
            config.llm_token_exchange_url,
            "https://auth.openai.com/oauth/token"
        );
        assert_eq!(config.llm_token_refresh_skew_secs, 60);
        assert!(config.federation(None).unwrap().is_none());
        let key = Secret::new("sk-test");
        assert!(matches!(
            config.auth(Some(&key)).unwrap(),
            LlmAuth::Static(k) if k.expose() == "sk-test"
        ));
        assert!(matches!(
            config.auth(Some(&Secret::new(""))).unwrap(),
            LlmAuth::Static(k) if k.is_empty()
        ));
        assert!(matches!(config.auth(None).unwrap(), LlmAuth::None));
    }

    #[test]
    fn wif_modes_need_their_ids_and_no_api_key() {
        let ids = [
            "--openai-identity-provider-id",
            "idp_1",
            "--openai-service-account-id",
            "svc_1",
        ];
        let kubernetes = |extra: &[&str]| {
            let mut args = vec!["--llm-auth", "openai-wif-kubernetes"];
            args.extend_from_slice(extra);
            auth_config(&args)
        };
        assert!(
            error_of(&kubernetes(&[]), None).contains("OPENAI_IDENTITY_PROVIDER_ID"),
            "missing provider id"
        );
        assert!(
            error_of(
                &kubernetes(&["--openai-identity-provider-id", "idp_1"]),
                None
            )
            .contains("OPENAI_SERVICE_ACCOUNT_ID")
        );
        let config = kubernetes(&ids);
        assert!(error_of(&config, Some(&Secret::new("sk-x"))).contains("unset LLM_API_KEY"));
        // An empty LLM_API_KEY is "unset".
        let federation = config.federation(Some(&Secret::new(""))).unwrap().unwrap();
        assert_eq!(federation.identity_provider_id, "idp_1");
        assert_eq!(federation.service_account_id, "svc_1");
        assert!(federation.authentik.is_none());
        assert_eq!(federation.refresh_skew, Duration::from_secs(60));

        let authentik = |extra: &[&str]| {
            let mut args = vec!["--llm-auth", "openai-wif-authentik"];
            args.extend_from_slice(&ids);
            args.extend_from_slice(extra);
            auth_config(&args)
        };
        assert!(error_of(&authentik(&[]), None).contains("LLM_AUTHENTIK_TOKEN_URL"));
        assert!(
            error_of(
                &authentik(&[
                    "--llm-authentik-token-url",
                    "https://sso.example.com/application/o/token/"
                ]),
                None
            )
            .contains("LLM_AUTHENTIK_CLIENT_ID")
        );
        let federation = authentik(&[
            "--llm-authentik-token-url",
            "https://sso.example.com/application/o/token/",
            "--llm-authentik-client-id",
            "client-1",
            "--llm-token-refresh-skew-secs",
            "120",
        ])
        .federation(None)
        .unwrap()
        .unwrap();
        let ak = federation.authentik.unwrap();
        assert_eq!(ak.client_id, "client-1");
        assert_eq!(ak.scope, None);
        assert_eq!(federation.refresh_skew, Duration::from_secs(120));
    }

    /// The token file must exist, be readable and be non-empty at startup.
    #[test]
    fn wif_mode_checks_the_token_file_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        let path_arg = path.to_str().unwrap();
        let config = auth_config(&[
            "--llm-auth",
            "openai-wif-kubernetes",
            "--openai-identity-provider-id",
            "idp_1",
            "--openai-service-account-id",
            "svc_1",
            "--llm-identity-token-file",
            path_arg,
        ]);
        let err = config.auth(None).unwrap_err().to_string();
        assert!(err.contains("is not readable"), "{err}");
        std::fs::write(&path, "\n").unwrap();
        let err = config.auth(None).unwrap_err().to_string();
        assert!(err.contains("is empty"), "{err}");
        std::fs::write(&path, "k8s.jwt\n").unwrap();
        let auth = config.auth(None).unwrap();
        assert!(auth.is_federated());
        // The token itself never appears in Debug.
        assert!(!format!("{auth:?} {config:?}").contains("k8s.jwt"));
    }

    #[test]
    fn unknown_auth_mode_is_rejected() {
        assert!(
            LlmAuthConfig::try_parse_from(["enricher", "--llm-auth", "openai-wif-gcp"]).is_err()
        );
    }
}
