use clap::Parser;

/// CLI/env configuration for the `enricher` service.
#[derive(Debug, Parser)]
pub struct Config {
    #[arg(long, env)]
    pub database_url: String,

    #[arg(long, env)]
    pub redis_url: String,

    /// Base URL of an OpenAI-compatible Chat Completions endpoint, e.g.
    /// `http://localhost:8080/v1` for a local server. No vendor is assumed.
    #[arg(long, env)]
    pub llm_base_url: String,

    /// Optional -- many local OpenAI-compatible servers don't require one.
    #[arg(long, env)]
    pub llm_api_key: Option<String>,

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

    /// Port for this service's Prometheus `/metrics` endpoint. See
    /// docs/superpowers/plans/2026-08-29-metrics.md's Global Constraints
    /// for why this differs from api.service.port -- api reuses its
    /// existing HTTP listener, this service has none, so it needs a new one.
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
}

/// Per-provider request/retry knobs, flattened into [`Config`] and also
/// parsed on its own by the ignored live evals (`llm::live_client_from_env`),
/// so an eval run honours exactly the env vars the service does. Every knob
/// defaults to "off": an unset deployment sends byte-for-byte the same
/// request as before and never retries in-call. See `llm::ProviderPolicy`.
#[derive(Debug, Clone, Parser)]
pub struct ProviderPolicyConfig {
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
    pub fn policy(&self) -> crate::llm::ProviderPolicy {
        crate::llm::ProviderPolicy {
            max_tokens: self.llm_max_tokens,
            reasoning_effort: self.llm_reasoning_effort.clone(),
            max_in_flight: self.llm_max_in_flight,
            rate_limit_min_wait: std::time::Duration::from_secs(self.llm_rate_limit_retry_secs),
            max_rate_limit_retries: self.llm_rate_limit_retries,
            max_gateway_retries: self.llm_gateway_retries,
        }
    }
}
