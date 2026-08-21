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

    /// Per-request timeout for a single LLM call. One incident makes three
    /// sequential calls (primary, resolution-adversarial, severity-adversarial
    /// -- see `llm.rs`), so the worst case for one incident is roughly 3x
    /// this value. Real self-hosted endpoints vary widely in latency; raise
    /// this if extractions are timing out against a slow/remote server, but
    /// raise `reclaim_min_idle_secs` to match (see its doc comment).
    #[arg(long, env, default_value_t = 120)]
    pub llm_request_timeout_secs: u64,

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
    /// for reclaim. Must comfortably exceed the worst-case time to run all
    /// three extraction calls (each bounded by `llm_request_timeout_secs`)
    /// plus the DB write, so a still-in-flight attempt is never reclaimed
    /// out from under itself -- if you raise `llm_request_timeout_secs`,
    /// raise this too (default here is set for the default 120s timeout:
    /// 3 * 120s = 360s worst case, plus headroom).
    #[arg(long, env, default_value_t = 600)]
    pub reclaim_min_idle_secs: u64,
}
