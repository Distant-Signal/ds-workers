//! Eval targets: one model served from one endpoint in one environment --
//! the thing being compared. Read from a TOML file (`EVAL_TARGETS`, format
//! in `crates/enricher/eval/targets.example.toml`) or, without one, from the
//! same `LLM_*` env vars the service reads, as a single target.
//!
//! Every default is the *service's* default (read from `Config`'s own clap
//! definitions, not restated here), so an unset knob benchmarks the
//! deployment as it would really run.

use std::time::Duration;

use clap::{CommandFactory, Parser};
use serde::{Deserialize, Serialize};

use crate::config::{Config, ProviderPolicyConfig};
use crate::eval::pipeline::TargetLabel;
use crate::llm::{LlmClient, ProviderPolicy};

/// Request timeout the quality eval uses unless a target overrides it:
/// generous on purpose, so a slow environment doesn't show up as a quality
/// failure. Timeouts are the perf benchmark's job.
const DEFAULT_QUALITY_TIMEOUT_SECS: u64 = 1800;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
    /// Unique; used in report file names.
    pub name: String,
    /// Free text describing where the model runs (hardware, host, region,
    /// quantization...). Shown in reports; perf numbers mean nothing without it.
    #[serde(default)]
    pub environment: Option<String>,
    /// `LLM_BASE_URL`.
    pub base_url: String,
    /// `LLM_MODEL`.
    pub model: String,
    /// NAME of the env var holding the API key (never the key itself).
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// `LLM_REQUEST_TIMEOUT_SECS`. Perf benchmark only.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Request timeout for the quality eval.
    #[serde(default = "default_quality_timeout_secs")]
    pub quality_timeout_secs: u64,
    /// `RECLAIM_MIN_IDLE_SECS`: the perf report compares document latency
    /// with it.
    #[serde(default = "default_reclaim_min_idle_secs")]
    pub reclaim_min_idle_secs: u64,
    /// `LLM_MAX_TOKENS`.
    #[serde(default)]
    pub max_tokens: Option<u32>,
    /// `LLM_REASONING_EFFORT`.
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    /// `LLM_MAX_IN_FLIGHT`.
    #[serde(default)]
    pub max_in_flight: Option<usize>,
    /// `LLM_RATE_LIMIT_RETRIES`.
    #[serde(default = "default_rate_limit_retries")]
    pub rate_limit_retries: u32,
    /// `LLM_RATE_LIMIT_RETRY_SECS`.
    #[serde(default = "default_rate_limit_retry_secs")]
    pub rate_limit_retry_secs: u64,
    /// `LLM_GATEWAY_RETRIES`.
    #[serde(default = "default_gateway_retries")]
    pub gateway_retries: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetsFile {
    targets: Vec<Target>,
}

/// The default of one of `Config`'s (or its flattened
/// `ProviderPolicyConfig`'s) arguments, from its clap definition -- not
/// from the environment, so a local `LLM_*` variable can't leak into a
/// targets file's defaults.
fn service_default<T: std::str::FromStr>(arg: &str) -> T {
    let command = Config::command();
    let value = command
        .get_arguments()
        .find(|a| a.get_id() == arg)
        .and_then(|a| a.get_default_values().first())
        .and_then(|v| v.to_str())
        .unwrap_or_else(|| panic!("Config has no default for {arg}"));
    value
        .parse()
        .unwrap_or_else(|_| panic!("Config's default for {arg} doesn't parse"))
}

fn default_request_timeout_secs() -> u64 {
    service_default("llm_request_timeout_secs")
}

fn default_quality_timeout_secs() -> u64 {
    DEFAULT_QUALITY_TIMEOUT_SECS
}

fn default_reclaim_min_idle_secs() -> u64 {
    service_default("reclaim_min_idle_secs")
}

fn default_rate_limit_retries() -> u32 {
    service_default("llm_rate_limit_retries")
}

fn default_rate_limit_retry_secs() -> u64 {
    service_default("llm_rate_limit_retry_secs")
}

fn default_gateway_retries() -> u32 {
    service_default("llm_gateway_retries")
}

impl Target {
    pub(crate) fn label(&self) -> TargetLabel {
        TargetLabel {
            target: self.name.clone(),
            model: self.model.clone(),
        }
    }

    pub(crate) fn policy(&self) -> ProviderPolicy {
        ProviderPolicy {
            max_tokens: self.max_tokens,
            reasoning_effort: self.reasoning_effort.clone(),
            max_in_flight: self.max_in_flight,
            rate_limit_min_wait: Duration::from_secs(self.rate_limit_retry_secs),
            max_rate_limit_retries: self.rate_limit_retries,
            max_gateway_retries: self.gateway_retries,
        }
    }

    /// The service's own client, configured as this target with the given
    /// per-request timeout.
    pub(crate) fn client(&self, timeout_secs: u64) -> anyhow::Result<LlmClient> {
        let api_key = match &self.api_key_env {
            Some(var) => Some(std::env::var(var).map_err(|_| {
                anyhow::anyhow!(
                    "target {:?}: env var {var} (api_key_env) is not set",
                    self.name
                )
            })?),
            None => None,
        };
        Ok(LlmClient::new(
            self.base_url.clone(),
            api_key,
            self.model.clone(),
            Duration::from_secs(timeout_secs),
        )
        .with_provider_policy(self.policy()))
    }

    /// Safe in a file name.
    pub(crate) fn file_stem(&self) -> String {
        file_stem(&self.name)
    }
}

pub(crate) fn file_stem(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Parses a targets file and keeps the ones named in `only` (all when
/// `None`).
pub(crate) fn parse_targets(raw: &str, only: Option<&[String]>) -> anyhow::Result<Vec<Target>> {
    let file: TargetsFile = toml::from_str(raw)?;
    let mut stems = std::collections::BTreeSet::new();
    for target in &file.targets {
        if target.name.trim().is_empty() {
            anyhow::bail!("a target has an empty name");
        }
        if !stems.insert(target.file_stem()) {
            anyhow::bail!("duplicate target name {:?}", target.name);
        }
    }
    let Some(only) = only else {
        return Ok(file.targets);
    };
    for name in only {
        if !file.targets.iter().any(|t| &t.name == name) {
            anyhow::bail!("EVAL_TARGET names unknown target {name:?}");
        }
    }
    Ok(file
        .targets
        .into_iter()
        .filter(|t| only.contains(&t.name))
        .collect())
}

/// `EVAL_TARGETS` (+ `EVAL_TARGET` to pick some), else one target from the
/// service's env vars: `LLM_BASE_URL`, `LLM_MODEL`, `LLM_API_KEY`,
/// `LLM_REQUEST_TIMEOUT_SECS` and the provider-policy `LLM_*` knobs, named
/// by `EVAL_TARGET_NAME` (default `env`) and described by
/// `EVAL_ENVIRONMENT`.
pub(crate) fn load_targets() -> anyhow::Result<Vec<Target>> {
    let only = crate::eval::env_list("EVAL_TARGET");
    if let Ok(path) = std::env::var("EVAL_TARGETS") {
        let path = crate::eval::resolve(&path);
        let raw = std::fs::read_to_string(&path)
            .map_err(|err| anyhow::anyhow!("reading {}: {err}", path.display()))?;
        let targets = parse_targets(&raw, only.as_deref())
            .map_err(|err| anyhow::anyhow!("{}: {err}", path.display()))?;
        if targets.is_empty() {
            anyhow::bail!("{} defines no targets", path.display());
        }
        return Ok(targets);
    }
    let var = |name: &str| {
        std::env::var(name).map_err(|_| {
            anyhow::anyhow!("set EVAL_TARGETS to a targets file, or {name} (see the eval docs)")
        })
    };
    let policy = ProviderPolicyConfig::parse_from(["eval"]);
    Ok(vec![Target {
        name: std::env::var("EVAL_TARGET_NAME").unwrap_or_else(|_| "env".to_string()),
        environment: std::env::var("EVAL_ENVIRONMENT").ok(),
        base_url: var("LLM_BASE_URL")?,
        model: var("LLM_MODEL")?,
        api_key_env: std::env::var("LLM_API_KEY")
            .is_ok()
            .then(|| "LLM_API_KEY".to_string()),
        request_timeout_secs: crate::eval::env_parse(
            "LLM_REQUEST_TIMEOUT_SECS",
            default_request_timeout_secs(),
        ),
        quality_timeout_secs: crate::eval::env_parse(
            "EVAL_QUALITY_TIMEOUT_SECS",
            DEFAULT_QUALITY_TIMEOUT_SECS,
        ),
        reclaim_min_idle_secs: crate::eval::env_parse(
            "RECLAIM_MIN_IDLE_SECS",
            default_reclaim_min_idle_secs(),
        ),
        max_tokens: policy.llm_max_tokens,
        reasoning_effort: policy.llm_reasoning_effort,
        max_in_flight: policy.llm_max_in_flight,
        rate_limit_retries: policy.llm_rate_limit_retries,
        rate_limit_retry_secs: policy.llm_rate_limit_retry_secs,
        gateway_retries: policy.llm_gateway_retries,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped example must parse, and unset knobs must take the
    /// service's defaults.
    #[test]
    fn example_targets_file_parses_with_service_defaults() {
        let targets = parse_targets(include_str!("../../eval/targets.example.toml"), None).unwrap();
        assert!(targets.len() >= 2);
        let defaults = ProviderPolicy::default();
        for target in &targets {
            assert!(target.request_timeout_secs > 0);
            assert!(target.quality_timeout_secs >= target.request_timeout_secs);
            if target.rate_limit_retries == 0 {
                assert_eq!(
                    Duration::from_secs(target.rate_limit_retry_secs),
                    defaults.rate_limit_min_wait
                );
            }
        }
    }

    #[test]
    fn minimal_target_takes_service_defaults() {
        let targets = parse_targets(
            "[[targets]]\nname = \"a b\"\nbase_url = \"http://localhost:1/v1\"\nmodel = \"m\"\n",
            None,
        )
        .unwrap();
        let target = &targets[0];
        assert_eq!(
            target.request_timeout_secs,
            service_default::<u64>("llm_request_timeout_secs")
        );
        assert_eq!(
            target.reclaim_min_idle_secs,
            service_default::<u64>("reclaim_min_idle_secs")
        );
        assert_eq!(target.quality_timeout_secs, DEFAULT_QUALITY_TIMEOUT_SECS);
        assert_eq!(target.rate_limit_retries, 0);
        assert_eq!(target.gateway_retries, 0);
        assert_eq!(target.file_stem(), "a-b");
        let policy = target.policy();
        assert_eq!(policy.max_tokens, None);
        assert_eq!(policy.max_gateway_retries, 0);
    }

    #[test]
    fn selection_and_validation() {
        let two = "[[targets]]\nname = \"a\"\nbase_url = \"u\"\nmodel = \"m\"\n\
                   [[targets]]\nname = \"b\"\nbase_url = \"u\"\nmodel = \"m\"\n";
        let picked = parse_targets(two, Some(&["b".to_string()])).unwrap();
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].name, "b");
        assert!(parse_targets(two, Some(&["c".to_string()])).is_err());
        let dup = "[[targets]]\nname = \"a\"\nbase_url = \"u\"\nmodel = \"m\"\n\
                   [[targets]]\nname = \"a\"\nbase_url = \"u\"\nmodel = \"m\"\n";
        assert!(parse_targets(dup, None).is_err());
        let typo = "[[targets]]\nname = \"a\"\nbase_url = \"u\"\nmodel = \"m\"\ntimeout = 1\n";
        assert!(parse_targets(typo, None).is_err());
    }

    #[test]
    fn missing_api_key_env_is_an_error() {
        let targets = parse_targets(
            "[[targets]]\nname = \"a\"\nbase_url = \"u\"\nmodel = \"m\"\n\
             api_key_env = \"ENRICHER_EVAL_TEST_SURELY_UNSET_KEY\"\n",
            None,
        )
        .unwrap();
        assert!(targets[0].client(1).is_err());
    }
}
