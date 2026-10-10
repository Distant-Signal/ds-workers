use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum as _};
use common::secret::Secret;

use crate::auth::{
    AuthentikConfig, ExchangeTarget, FederatedTokenSource, FederationConfig, LlmAuth, LlmAuthMode,
};
use crate::llm::ProviderKind;
use crate::llm::anthropic::{AnthropicSettings, PromptCache};

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

    /// `LLM_PROVIDER`: `openai` (the default; any OpenAI-compatible Chat
    /// Completions endpoint) or `anthropic` (the Claude API,
    /// docs/enricher-anthropic.md).
    #[arg(long, env, value_enum, default_value_t = ProviderKind::Openai)]
    pub llm_provider: ProviderKind,

    /// Base URL of the endpoint, `/v1` included. `openai`: an
    /// OpenAI-compatible Chat Completions endpoint, e.g.
    /// `http://localhost:8080/v1` for a local server (required; no vendor is
    /// assumed). `anthropic`: defaults to `https://api.anthropic.com/v1`.
    #[arg(long, env)]
    pub llm_base_url: Option<String>,

    /// Optional -- many local OpenAI-compatible servers don't require one.
    /// Only for `LLM_AUTH=api-key` (the default); setting it in a workload
    /// identity mode is a startup error.
    #[arg(long, env, hide_env_values = true)]
    pub llm_api_key: Option<Secret>,

    /// `LLM_AUTH` and the workload identity federation settings. Off by
    /// default (`api-key`) -- see [`LlmAuthConfig`].
    #[command(flatten)]
    pub llm_auth: LlmAuthConfig,

    /// Model name/identifier as the endpoint expects it. Required for
    /// `openai`; `anthropic` defaults to `claude-haiku-5-5`
    /// (`llm::anthropic::DEFAULT_MODEL`).
    #[arg(long, env)]
    pub llm_model: Option<String>,

    /// The Claude-only settings (`LLM_ANTHROPIC_VERSION`, `LLM_PROMPT_CACHE`,
    /// `LLM_THINKING`); ignored by `openai`.
    #[command(flatten)]
    pub anthropic: AnthropicConfig,

    /// `LLM_SWEEP_MODE` and the Message Batches settings. Off by default
    /// (`sync`).
    #[command(flatten)]
    pub batch: BatchConfig,

    /// `LLM_PROFILE`, `LLM_TEMPERATURE`, `LLM_TOP_P`, `LLM_PROMPTS_DIR`
    /// (`profile.rs`). All unset by default: the built-in profile for the
    /// provider and model, with today's settings and prompts.
    #[command(flatten)]
    pub generation: GenerationConfig,

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

/// The endpoint, model and provider after defaults and validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedLlm {
    pub provider: ProviderKind,
    pub base_url: String,
    pub model: String,
}

impl Config {
    /// [`ResolvedLlm`] plus every cross-setting check, so a bad combination
    /// fails the pod at startup instead of every extraction.
    pub(crate) fn resolved_llm(&self) -> anyhow::Result<ResolvedLlm> {
        resolve_llm(
            self.llm_provider,
            self.llm_base_url.as_deref(),
            self.llm_model.as_deref(),
            self.llm_auth.llm_auth,
            self.llm_api_key.as_ref(),
            self.batch.batching_setting(),
        )
    }
}

/// See [`Config::resolved_llm`]; also used by the model-eval targets.
pub(crate) fn resolve_llm(
    provider: ProviderKind,
    base_url: Option<&str>,
    model: Option<&str>,
    auth: LlmAuthMode,
    api_key: Option<&Secret>,
    // The setting that turns Message Batches on (`BatchConfig::batching_setting`).
    batching: Option<&str>,
) -> anyhow::Result<ResolvedLlm> {
    let set = |value: Option<&str>| {
        value
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
    };
    let (base_url, model) = match provider {
        ProviderKind::Openai => {
            if auth.is_anthropic() {
                anyhow::bail!(
                    "LLM_AUTH=anthropic-wif-authentik mints Claude API tokens: it needs \
                     LLM_PROVIDER=anthropic"
                );
            }
            if let Some(setting) = batching {
                anyhow::bail!(
                    "{setting} needs LLM_PROVIDER=anthropic: the enricher has no \
                     OpenAI Batch API support (docs/enricher-anthropic.md, \"Batch mode\")"
                );
            }
            (
                set(base_url).ok_or_else(|| anyhow::anyhow!("LLM_BASE_URL is required"))?,
                set(model).ok_or_else(|| anyhow::anyhow!("LLM_MODEL is required"))?,
            )
        }
        ProviderKind::Anthropic => {
            // The OpenAI workload identity modes mint OpenAI tokens, which
            // the Claude API refuses.
            match auth {
                LlmAuthMode::ApiKey => {
                    if api_key.is_none_or(Secret::is_empty) {
                        anyhow::bail!(
                            "LLM_PROVIDER=anthropic needs LLM_API_KEY (a Claude API key) or \
                             LLM_AUTH=anthropic-wif-authentik"
                        );
                    }
                }
                LlmAuthMode::AnthropicWifAuthentik => {}
                LlmAuthMode::OpenaiWifAuthentik | LlmAuthMode::OpenaiWifKubernetes => {
                    anyhow::bail!(
                        "LLM_PROVIDER=anthropic supports LLM_AUTH=api-key or \
                         anthropic-wif-authentik (the openai-wif-* modes mint OpenAI tokens)"
                    );
                }
            }
            (
                set(base_url)
                    .unwrap_or_else(|| crate::llm::anthropic::DEFAULT_BASE_URL.to_string()),
                set(model).unwrap_or_else(|| crate::llm::anthropic::DEFAULT_MODEL.to_string()),
            )
        }
    };
    Ok(ResolvedLlm {
        provider,
        base_url: base_url.trim_end_matches('/').to_string(),
        model,
    })
}

/// The Claude API's own settings (`LLM_PROVIDER=anthropic` only).
#[derive(Debug, Clone, Parser)]
#[expect(
    clippy::struct_field_names,
    reason = "clap derives each env var from the field name, so the prefix is part of the interface"
)]
pub(crate) struct AnthropicConfig {
    /// The `anthropic-version` header.
    #[arg(long, env, default_value = crate::llm::anthropic::DEFAULT_VERSION)]
    pub llm_anthropic_version: String,
    /// `LLM_PROMPT_CACHE`: `1h` (default), `5m` or `off` -- the
    /// `cache_control` TTL on each call's static system prompt
    /// (docs/enricher-anthropic.md, "Prompt caching").
    #[arg(long, env, value_enum, default_value_t = PromptCache::OneHour)]
    pub llm_prompt_cache: PromptCache,
    /// `LLM_THINKING`: sent as `thinking: {"type": <this>}` when set (e.g.
    /// `disabled` on Haiku 5.5, `between_tools` on Sonnet 5.5). Unset: the
    /// model's default (adaptive thinking).
    #[arg(long, env)]
    pub llm_thinking: Option<String>,
}

impl AnthropicConfig {
    pub(crate) fn settings(&self) -> AnthropicSettings {
        AnthropicSettings {
            version: self.llm_anthropic_version.clone(),
            prompt_cache: self.llm_prompt_cache,
            thinking: self
                .llm_thinking
                .as_deref()
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_string),
        }
    }
}

/// `LLM_SWEEP_MODE`: how the reconciliation sweep runs its extractions in
/// `LLM_MODE=normal`. Ignored in `LLM_MODE=batch` and `batch-only`, which
/// set the sweep's behaviour themselves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum SweepMode {
    /// One incident at a time through the synchronous API, like the stream
    /// loop (the default; the only mode before 2026-10).
    #[default]
    Sync,
    /// Through the Claude Message Batches API (half price, results within
    /// 24 h, usually under an hour), when the sweep finds at least
    /// `LLM_BATCH_MIN_ITEMS` incidents. `anthropic` only.
    Batch,
}

/// `LLM_MODE`: where extractions run. docs/enricher-anthropic.md, "LLM
/// modes".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum LlmMode {
    /// The stream loop and reclaim extract synchronously, seconds after a
    /// text change; the hourly sweep follows `LLM_SWEEP_MODE` (the default,
    /// and the only mode before 2026-10).
    #[default]
    Normal,
    /// The stream loop and reclaim make no LLM call (they ACK and leave the
    /// incident stale). A sweep every `LLM_BATCH_SWEEP_INTERVAL_SECS`
    /// (default 120) batches what it finds when there are at least
    /// `LLM_BATCH_MIN_ITEMS`, and extracts the rest synchronously right
    /// away. `anthropic` only.
    Batch,
    /// As `batch`, but everything is batched, however few (no synchronous
    /// extraction anywhere); the sweep runs every
    /// `LLM_BATCH_SWEEP_INTERVAL_SECS` (default 300). `anthropic` only.
    BatchOnly,
}

impl LlmMode {
    /// The `LLM_MODE` value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Batch => "batch",
            Self::BatchOnly => "batch-only",
        }
    }

    /// `LLM_BATCH_SWEEP_INTERVAL_SECS`'s default (`None` in `normal`, which
    /// sweeps every `SWEEP_INTERVAL_SECS`). `batch`: 120 s, so a quiet
    /// period's text change waits at most ~2 minutes plus its three calls,
    /// while a burst still has time to reach `LLM_BATCH_MIN_ITEMS`; each
    /// sweep is one scan of the live incidents. `batch-only`: 300 s, small
    /// next to a batch's own minutes-to-an-hour, and it keeps the batches
    /// fewer and larger.
    pub(crate) fn default_sweep_interval_secs(self) -> Option<u64> {
        match self {
            Self::Normal => None,
            Self::Batch => Some(120),
            Self::BatchOnly => Some(300),
        }
    }
}

/// The batch-mode knobs (`batch.rs`).
#[derive(Debug, Clone, Parser)]
#[expect(
    clippy::struct_field_names,
    reason = "clap derives each env var from the field name, so the prefix is part of the interface"
)]
pub(crate) struct BatchConfig {
    /// `LLM_MODE`: `normal` (default), `batch` or `batch-only`.
    #[arg(long, env, value_enum, default_value_t = LlmMode::Normal)]
    pub llm_mode: LlmMode,
    /// `LLM_SWEEP_MODE`: `sync` (default) or `batch`. `LLM_MODE=normal`
    /// only; ignored otherwise.
    #[arg(long, env, value_enum, default_value_t = SweepMode::Sync)]
    pub llm_sweep_mode: SweepMode,
    /// A sweep that finds fewer incidents than this runs them synchronously
    /// as before: a batch's latency (minutes to hours) isn't worth half the
    /// price of a handful of calls. Ignored in `batch-only` (effectively 1).
    #[arg(long, env, default_value_t = 20)]
    pub llm_batch_min_items: usize,
    /// Most incidents in one Message Batch (each is 1 request in the
    /// primary batch and 2 in the adversarial one). A bigger sweep is split.
    #[arg(long, env, default_value_t = 2000)]
    pub llm_batch_max_items: usize,
    /// How often in-flight batches are polled, in seconds.
    #[arg(long, env, default_value_t = 60)]
    pub llm_batch_poll_secs: u64,
    /// `LLM_MODE=batch`/`batch-only`: how often the sweep runs, in seconds,
    /// in place of `SWEEP_INTERVAL_SECS` (still the `normal` sweep's).
    /// Unset: 120 (`batch`) or 300 (`batch-only`); see
    /// [`LlmMode::default_sweep_interval_secs`]. Ignored in `normal`.
    #[arg(long, env)]
    pub llm_batch_sweep_interval_secs: Option<u64>,
}

impl BatchConfig {
    /// The setting that turns Message Batches on, for messages
    /// (`LLM_MODE=batch`, `LLM_SWEEP_MODE=batch`, ...), or `None`.
    pub(crate) fn batching_setting(&self) -> Option<&'static str> {
        match (self.llm_mode, self.llm_sweep_mode) {
            (LlmMode::Batch, _) => Some("LLM_MODE=batch"),
            (LlmMode::BatchOnly, _) => Some("LLM_MODE=batch-only"),
            (LlmMode::Normal, SweepMode::Batch) => Some("LLM_SWEEP_MODE=batch"),
            (LlmMode::Normal, SweepMode::Sync) => None,
        }
    }

    /// `Some` whenever batches are on (see [`Self::batching_setting`]).
    pub(crate) fn settings(&self) -> Option<crate::batch::BatchSettings> {
        self.batching_setting()?;
        Some(crate::batch::BatchSettings {
            // Batch-only has no synchronous fallback: one stale incident is
            // a batch.
            min_items: if self.llm_mode == LlmMode::BatchOnly {
                1
            } else {
                self.llm_batch_min_items.max(1)
            },
            max_items: self
                .llm_batch_max_items
                .clamp(1, crate::batch::MAX_BATCH_INCIDENTS),
            poll_interval: Duration::from_secs(self.llm_batch_poll_secs.max(1)),
            mode: self.llm_mode,
        })
    }

    /// The sweep's interval in seconds: `LLM_BATCH_SWEEP_INTERVAL_SECS` (or
    /// its per-mode default) in `batch`/`batch-only`, else
    /// `sweep_interval_secs` (`SWEEP_INTERVAL_SECS`).
    pub(crate) fn sweep_interval_secs(&self, sweep_interval_secs: u64) -> u64 {
        match self.llm_mode.default_sweep_interval_secs() {
            Some(default) => self.llm_batch_sweep_interval_secs.unwrap_or(default).max(1),
            None => sweep_interval_secs,
        }
    }
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

/// The profile layer's own knobs (`profile.rs`). `LLM_MAX_TOKENS`,
/// `LLM_REASONING_EFFORT` and `LLM_THINKING` are overrides of the profile
/// too; they stay where they were.
#[derive(Debug, Clone, Parser)]
#[expect(
    clippy::struct_field_names,
    reason = "clap derives each env var from the field name, so the prefix is part of the interface"
)]
pub(crate) struct GenerationConfig {
    /// `LLM_PROFILE`: a built-in profile by name; unset picks it by provider
    /// and model.
    #[arg(long, env)]
    pub llm_profile: Option<String>,
    /// `LLM_TEMPERATURE`: a number, or `omit` to send none; unset keeps the
    /// profile's (0 for OpenAI-compatible endpoints, none for Claude).
    #[arg(long, env)]
    pub llm_temperature: Option<String>,
    /// `LLM_TOP_P`: a number, or `omit`; unset keeps the profile's (none).
    #[arg(long, env)]
    pub llm_top_p: Option<String>,
    /// `LLM_PROMPTS_DIR`: a directory of prompt overrides
    /// (`<profile>.<call>.txt` or `<call>.txt`); unset uses the built-in
    /// prompts.
    #[arg(long, env)]
    pub llm_prompts_dir: Option<PathBuf>,
}

/// The profile overrides from the service's env: `LLM_TEMPERATURE`,
/// `LLM_TOP_P`, `LLM_MAX_TOKENS`, `LLM_REASONING_EFFORT` (`omit` drops the
/// profile's) and `LLM_THINKING` (likewise).
pub(crate) fn profile_overrides(
    generation: &GenerationConfig,
    policy: &ProviderPolicyConfig,
    thinking: Option<&str>,
) -> anyhow::Result<crate::profile::Overrides> {
    use crate::profile::{Overrides, Setting, parse_setting};
    Ok(Overrides {
        temperature: parse_setting("LLM_TEMPERATURE", generation.llm_temperature.as_deref())?,
        top_p: parse_setting("LLM_TOP_P", generation.llm_top_p.as_deref())?,
        max_tokens: policy.llm_max_tokens.map(Setting::Set),
        reasoning_effort: parse_setting(
            "LLM_REASONING_EFFORT",
            policy.llm_reasoning_effort.as_deref(),
        )?,
        thinking: parse_setting("LLM_THINKING", thinking)?,
    })
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
    /// -> `OpenAI` token exchange), `openai-wif-kubernetes` (k8s token ->
    /// `OpenAI` token exchange) or `anthropic-wif-authentik` (k8s token ->
    /// Authentik -> Claude API token exchange; `LLM_PROVIDER=anthropic`).
    #[arg(long, env, value_enum, default_value_t = LlmAuthMode::ApiKey)]
    pub llm_auth: LlmAuthMode,
    /// `OpenAI` Workload Identity Provider ID. Required in the `openai-wif-*`
    /// modes.
    #[arg(long, env)]
    pub openai_identity_provider_id: Option<String>,
    /// `OpenAI` service account ID the mapping resolves to. Required in the
    /// `openai-wif-*` modes.
    #[arg(long, env)]
    pub openai_service_account_id: Option<String>,
    /// Claude federation rule ID (`fdrl_...`). Required in
    /// `anthropic-wif-authentik` mode. Named like the Claude SDKs' variable.
    #[arg(long, env)]
    pub anthropic_federation_rule_id: Option<String>,
    /// Claude organization UUID. Required in `anthropic-wif-authentik` mode.
    #[arg(long, env)]
    pub anthropic_organization_id: Option<String>,
    /// Claude service account ID (`svac_...`) the rule targets. Required in
    /// `anthropic-wif-authentik` mode.
    #[arg(long, env)]
    pub anthropic_service_account_id: Option<String>,
    /// Claude workspace ID (`wrkspc_...`): only needed when the rule spans
    /// more than one workspace.
    #[arg(long, env)]
    pub anthropic_workspace_id: Option<String>,
    /// The kubelet-projected service-account token (re-read on every
    /// exchange). Must exist and be readable at startup in WIF modes.
    #[arg(long, env, default_value = "/var/run/secrets/openai/token")]
    pub llm_identity_token_file: PathBuf,
    /// The provider's token-exchange endpoint. Unset: `OpenAI`'s
    /// ([`OPENAI_TOKEN_EXCHANGE_URL`]) in the `openai-wif-*` modes, the
    /// Claude API's ([`ANTHROPIC_TOKEN_EXCHANGE_URL`]) in
    /// `anthropic-wif-authentik`.
    #[arg(long, env)]
    pub llm_token_exchange_url: Option<String>,
    /// Authentik's token endpoint (e.g.
    /// `https://sso.example.com/application/o/token/`). Required in the two
    /// Authentik modes.
    #[arg(long, env)]
    pub llm_authentik_token_url: Option<String>,
    /// Client ID of the Authentik `OAuth2` provider. Required in the two
    /// Authentik modes. For Claude, a provider of its own (its own client
    /// ID and audience), so its tokens can't be replayed at `OpenAI`.
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

/// `OpenAI`'s token-exchange endpoint (`LLM_TOKEN_EXCHANGE_URL` default in
/// the `openai-wif-*` modes).
pub(crate) const OPENAI_TOKEN_EXCHANGE_URL: &str = "https://auth.openai.com/oauth/token";
/// The Claude API's token-exchange endpoint (`LLM_TOKEN_EXCHANGE_URL`
/// default in `anthropic-wif-authentik`).
pub(crate) const ANTHROPIC_TOKEN_EXCHANGE_URL: &str = "https://api.anthropic.com/v1/oauth/token";

impl LlmAuthConfig {
    /// `LLM_TOKEN_EXCHANGE_URL`, or this mode's default.
    pub(crate) fn token_exchange_url(&self) -> String {
        match self
            .llm_token_exchange_url
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
        {
            Some(url) => url.to_string(),
            None if self.llm_auth.is_anthropic() => ANTHROPIC_TOKEN_EXCHANGE_URL.to_string(),
            None => OPENAI_TOKEN_EXCHANGE_URL.to_string(),
        }
    }

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
        let target = if self.llm_auth.is_anthropic() {
            ExchangeTarget::Anthropic {
                federation_rule_id: required(
                    &self.anthropic_federation_rule_id,
                    "ANTHROPIC_FEDERATION_RULE_ID",
                )?,
                organization_id: required(
                    &self.anthropic_organization_id,
                    "ANTHROPIC_ORGANIZATION_ID",
                )?,
                service_account_id: required(
                    &self.anthropic_service_account_id,
                    "ANTHROPIC_SERVICE_ACCOUNT_ID",
                )?,
                workspace_id: self
                    .anthropic_workspace_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|w| !w.is_empty())
                    .map(str::to_string),
            }
        } else {
            ExchangeTarget::Openai {
                identity_provider_id: required(
                    &self.openai_identity_provider_id,
                    "OPENAI_IDENTITY_PROVIDER_ID",
                )?,
                service_account_id: required(
                    &self.openai_service_account_id,
                    "OPENAI_SERVICE_ACCOUNT_ID",
                )?,
            }
        };
        let authentik = match self.llm_auth {
            LlmAuthMode::OpenaiWifAuthentik | LlmAuthMode::AnthropicWifAuthentik => {
                Some(AuthentikConfig {
                    token_url: required(&self.llm_authentik_token_url, "LLM_AUTHENTIK_TOKEN_URL")?,
                    client_id: required(&self.llm_authentik_client_id, "LLM_AUTHENTIK_CLIENT_ID")?,
                    scope: self
                        .llm_authentik_scope
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string),
                })
            }
            LlmAuthMode::OpenaiWifKubernetes | LlmAuthMode::ApiKey => None,
        };
        Ok(Some(FederationConfig {
            target,
            identity_token_file: self.llm_identity_token_file.clone(),
            token_exchange_url: self.token_exchange_url(),
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
        assert_eq!(config.llm_token_exchange_url, None);
        assert_eq!(
            config.token_exchange_url(),
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
        assert_eq!(
            federation.target,
            ExchangeTarget::Openai {
                identity_provider_id: "idp_1".into(),
                service_account_id: "svc_1".into()
            }
        );
        assert_eq!(
            federation.token_exchange_url,
            "https://auth.openai.com/oauth/token"
        );
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

    fn config(args: &[&str]) -> Config {
        let base = [
            "enricher",
            "--database-url",
            "postgres://x",
            "--redis-url",
            "redis://x",
        ];
        Config::try_parse_from(base.iter().chain(args.iter()).copied()).unwrap()
    }

    /// Unset, the provider is `openai` and everything is as before: base
    /// URL and model required, sync sweep, no batch settings.
    #[test]
    fn openai_is_the_default_and_needs_url_and_model() {
        let config = config(&["--llm-base-url", "http://l/v1/", "--llm-model", "m"]);
        assert_eq!(config.llm_provider, ProviderKind::Openai);
        assert_eq!(config.batch.llm_sweep_mode, SweepMode::Sync);
        assert!(config.batch.settings().is_none());
        assert_eq!(config.anthropic.llm_prompt_cache, PromptCache::OneHour);
        let resolved = config.resolved_llm().unwrap();
        assert_eq!(resolved.base_url, "http://l/v1");
        assert_eq!(resolved.model, "m");
        let missing = |args: &[&str]| self::config(args).resolved_llm().unwrap_err().to_string();
        assert!(missing(&["--llm-model", "m"]).contains("LLM_BASE_URL"));
        assert!(missing(&["--llm-base-url", "http://l/v1"]).contains("LLM_MODEL"));
        assert!(
            missing(&[
                "--llm-base-url",
                "http://l/v1",
                "--llm-model",
                "m",
                "--llm-sweep-mode",
                "batch"
            ])
            .contains("needs LLM_PROVIDER=anthropic")
        );
    }

    #[test]
    fn anthropic_defaults_and_validation() {
        let resolved = config(&["--llm-provider", "anthropic", "--llm-api-key", "sk-ant"])
            .resolved_llm()
            .unwrap();
        assert_eq!(resolved.provider, ProviderKind::Anthropic);
        assert_eq!(resolved.base_url, "https://api.anthropic.com/v1");
        assert_eq!(resolved.model, "claude-haiku-5-5");

        let custom = config(&[
            "--llm-provider",
            "anthropic",
            "--llm-api-key",
            "sk-ant",
            "--llm-model",
            "claude-sonnet-5-5",
            "--llm-prompt-cache",
            "5m",
            "--llm-thinking",
            "between_tools",
            "--llm-sweep-mode",
            "batch",
            "--llm-batch-max-items",
            "999999",
        ]);
        assert_eq!(custom.resolved_llm().unwrap().model, "claude-sonnet-5-5");
        let settings = custom.anthropic.settings();
        assert_eq!(settings.prompt_cache, PromptCache::FiveMinutes);
        assert_eq!(settings.thinking.as_deref(), Some("between_tools"));
        assert_eq!(settings.version, "2023-06-01");
        let batch = custom.batch.settings().unwrap();
        assert_eq!(batch.min_items, 20);
        assert_eq!(batch.max_items, crate::batch::MAX_BATCH_INCIDENTS);
        assert_eq!(batch.poll_interval, Duration::from_secs(60));

        let err = |args: &[&str]| config(args).resolved_llm().unwrap_err().to_string();
        assert!(err(&["--llm-provider", "anthropic"]).contains("needs LLM_API_KEY"));
        assert!(
            err(&["--llm-provider", "anthropic", "--llm-api-key", ""])
                .contains("needs LLM_API_KEY")
        );
        assert!(
            err(&[
                "--llm-provider",
                "anthropic",
                "--llm-auth",
                "openai-wif-kubernetes"
            ])
            .contains("LLM_AUTH=api-key")
        );
        assert!(
            Config::try_parse_from([
                "enricher",
                "--database-url",
                "postgres://x",
                "--redis-url",
                "redis://x",
                "--llm-prompt-cache",
                "2h"
            ])
            .is_err()
        );
    }

    /// `anthropic-wif-authentik`: Claude only, the Claude IDs and the
    /// Authentik settings required, no API key, the Claude token endpoint
    /// by default.
    #[test]
    fn anthropic_wif_authentik_settings() {
        let wif = [
            "--llm-provider",
            "anthropic",
            "--llm-auth",
            "anthropic-wif-authentik",
        ];
        let ids = [
            "--anthropic-federation-rule-id",
            "fdrl_1",
            "--anthropic-organization-id",
            "org-uuid",
            "--anthropic-service-account-id",
            "svac_1",
            "--llm-authentik-token-url",
            "https://sso.example.com/application/o/token/",
            "--llm-authentik-client-id",
            "ds-enricher-anthropic",
        ];
        let with = |extra: &[&str]| {
            let mut args = wif.to_vec();
            args.extend_from_slice(extra);
            config(&args)
        };
        // No key needed for the provider check.
        assert_eq!(
            with(&ids).resolved_llm().unwrap().provider,
            ProviderKind::Anthropic
        );
        let federation = with(&ids).llm_auth.federation(None).unwrap().unwrap();
        assert_eq!(
            federation.target,
            ExchangeTarget::Anthropic {
                federation_rule_id: "fdrl_1".into(),
                organization_id: "org-uuid".into(),
                service_account_id: "svac_1".into(),
                workspace_id: None,
            }
        );
        assert_eq!(
            federation.token_exchange_url,
            "https://api.anthropic.com/v1/oauth/token"
        );
        assert_eq!(
            federation.authentik.unwrap().client_id,
            "ds-enricher-anthropic"
        );
        let mut workspace = ids.to_vec();
        workspace.extend(["--anthropic-workspace-id", "wrkspc_ds"]);
        assert!(matches!(
            with(&workspace).llm_auth.federation(None).unwrap().unwrap().target,
            ExchangeTarget::Anthropic { workspace_id: Some(w), .. } if w == "wrkspc_ds"
        ));

        let err = |config: Config| config.llm_auth.federation(None).unwrap_err().to_string();
        assert!(err(with(&[])).contains("ANTHROPIC_FEDERATION_RULE_ID"));
        assert!(err(with(&ids[..4])).contains("ANTHROPIC_SERVICE_ACCOUNT_ID"));
        assert!(err(with(&ids[..6])).contains("LLM_AUTHENTIK_TOKEN_URL"));
        // A static key is refused, as in the OpenAI modes.
        let keyed = with(&ids);
        assert!(
            keyed
                .llm_auth
                .federation(Some(&Secret::new("sk-ant")))
                .unwrap_err()
                .to_string()
                .contains("unset LLM_API_KEY")
        );
        // Claude tokens are useless to the openai provider.
        let mut openai = vec!["--llm-auth", "anthropic-wif-authentik"];
        openai.extend(["--llm-base-url", "http://l/v1", "--llm-model", "m"]);
        assert!(
            config(&openai)
                .resolved_llm()
                .unwrap_err()
                .to_string()
                .contains("needs LLM_PROVIDER=anthropic")
        );
    }

    /// `LLM_MODE`: `normal` keeps `LLM_SWEEP_MODE`; `batch` keeps the
    /// minimum and sweeps every 120 s; `batch-only` has a minimum of 1 and
    /// sweeps every 300 s; both ignore `LLM_SWEEP_MODE`.
    #[test]
    fn llm_modes_settings_and_sweep_interval() {
        let anthropic = ["--llm-provider", "anthropic", "--llm-api-key", "sk-ant"];
        let with = |args: &[&str]| config(&[&anthropic[..], args].concat());

        let normal = with(&[]);
        assert_eq!(normal.batch.llm_mode, LlmMode::Normal);
        assert!(normal.batch.settings().is_none());
        assert_eq!(
            normal.batch.sweep_interval_secs(normal.sweep_interval_secs),
            3600
        );
        let sweep_batch = with(&[
            "--llm-sweep-mode",
            "batch",
            "--llm-batch-sweep-interval-secs",
            "60",
        ]);
        let settings = sweep_batch.batch.settings().unwrap();
        assert_eq!(settings.mode, LlmMode::Normal);
        assert!(!settings.defers_stream());
        assert_eq!(
            sweep_batch
                .batch
                .sweep_interval_secs(sweep_batch.sweep_interval_secs),
            3600,
            "normal ignores LLM_BATCH_SWEEP_INTERVAL_SECS"
        );

        // `batch`, with LLM_SWEEP_MODE=sync (ignored).
        let batch = with(&["--llm-mode", "batch", "--llm-sweep-mode", "sync"]);
        assert!(batch.resolved_llm().is_ok());
        let settings = batch.batch.settings().unwrap();
        assert_eq!(settings.mode, LlmMode::Batch);
        assert!(settings.defers_stream() && !settings.batch_only());
        assert_eq!(settings.min_items, 20);
        assert_eq!(
            batch.batch.sweep_interval_secs(batch.sweep_interval_secs),
            120
        );

        let only = with(&["--llm-mode", "batch-only", "--llm-batch-min-items", "50"]);
        let settings = only.batch.settings().unwrap();
        assert!(settings.defers_stream() && settings.batch_only());
        assert_eq!(settings.min_items, 1);
        assert_eq!(
            only.batch.sweep_interval_secs(only.sweep_interval_secs),
            300
        );

        let tuned = with(&[
            "--llm-mode",
            "batch",
            "--llm-batch-sweep-interval-secs",
            "45",
            "--sweep-interval-secs",
            "900",
        ]);
        assert_eq!(
            tuned.batch.sweep_interval_secs(tuned.sweep_interval_secs),
            45
        );

        // Both refused on openai at startup.
        for mode in ["batch", "batch-only"] {
            let err = config(&[
                "--llm-base-url",
                "http://l/v1",
                "--llm-model",
                "m",
                "--llm-mode",
                mode,
            ])
            .resolved_llm()
            .unwrap_err()
            .to_string();
            assert!(
                err.contains(&format!("LLM_MODE={mode} needs LLM_PROVIDER=anthropic")),
                "{err}"
            );
        }
        assert!(
            Config::try_parse_from([
                "enricher",
                "--database-url",
                "postgres://x",
                "--redis-url",
                "redis://x",
                "--llm-mode",
                "nightly"
            ])
            .is_err()
        );
    }
}
