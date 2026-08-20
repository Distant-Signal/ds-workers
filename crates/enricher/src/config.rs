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

    /// How often the reconciliation sweep runs, independent of the Redis
    /// Stream consumer loop. Backstop for a missed/lost publish.
    #[arg(long, env, default_value_t = 3600)]
    pub sweep_interval_secs: u64,
}
