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

use common::secret::Secret;

use crate::auth::LlmAuthMode;
use crate::config::{AnthropicConfig, Config, LlmAuthConfig, ProviderPolicyConfig};
use crate::eval::pipeline::TargetLabel;
use crate::llm::anthropic::{AnthropicSettings, PromptCache};
use crate::llm::{LlmClient, ProviderKind, ProviderPolicy};

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
    /// `LLM_PROVIDER`: `openai` (default) or `anthropic` (the Claude API;
    /// docs/enricher-anthropic.md).
    #[serde(default)]
    pub provider: ProviderKind,
    /// `LLM_BASE_URL` (e.g. `https://api.anthropic.com/v1` for `anthropic`).
    pub base_url: String,
    /// `LLM_MODEL`.
    pub model: String,
    /// NAME of the env var holding the API key (never the key itself).
    /// The normal eval route, `OpenAI` included.
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// `LLM_AUTH`: `api-key` (default), `openai-wif-authentik` or
    /// `openai-wif-kubernetes`. The workload identity modes need a projected
    /// service-account token, so they only work from a pod in the cluster.
    #[serde(default)]
    pub auth: LlmAuthMode,
    /// The workload identity settings, for the `openai-wif-*` modes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_identity: Option<WorkloadIdentityTarget>,
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
    /// `LLM_PROMPT_CACHE` (`anthropic` only): `1h` (default), `5m` or `off`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache: Option<PromptCache>,
    /// `LLM_THINKING` (`anthropic` only), e.g. `disabled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    /// `LLM_ANTHROPIC_VERSION` (`anthropic` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anthropic_version: Option<String>,
    /// `LLM_PROFILE`: a built-in profile by name; unset picks it by
    /// provider and model, as the service does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// `LLM_TEMPERATURE`: a number or `"omit"`; unset keeps the profile's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<String>,
    /// `LLM_TOP_P`: a number or `"omit"`; unset keeps the profile's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<String>,
    /// `LLM_PROMPTS_DIR`: prompt overrides (`<profile>.<call>.txt` or
    /// `<call>.txt`), resolved like the targets file's other paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompts_dir: Option<std::path::PathBuf>,
}

/// `[targets.workload_identity]`: the service's WIF env vars, by the same
/// names in lower case without the `LLM_`/`OPENAI_` prefix (the Claude ones
/// keep an `anthropic_` prefix where a name would clash). Unset keys take
/// the service's defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkloadIdentityTarget {
    /// `OPENAI_IDENTITY_PROVIDER_ID`.
    #[serde(default)]
    pub identity_provider_id: Option<String>,
    /// `OPENAI_SERVICE_ACCOUNT_ID`.
    #[serde(default)]
    pub service_account_id: Option<String>,
    /// `LLM_IDENTITY_TOKEN_FILE`.
    #[serde(default)]
    pub identity_token_file: Option<std::path::PathBuf>,
    /// `LLM_TOKEN_EXCHANGE_URL`.
    #[serde(default)]
    pub token_exchange_url: Option<String>,
    /// `LLM_AUTHENTIK_TOKEN_URL`.
    #[serde(default)]
    pub authentik_token_url: Option<String>,
    /// `LLM_AUTHENTIK_CLIENT_ID`.
    #[serde(default)]
    pub authentik_client_id: Option<String>,
    /// `LLM_AUTHENTIK_SCOPE`.
    #[serde(default)]
    pub authentik_scope: Option<String>,
    /// `LLM_TOKEN_REFRESH_SKEW_SECS`.
    #[serde(default)]
    pub token_refresh_skew_secs: Option<u64>,
    /// `ANTHROPIC_FEDERATION_RULE_ID`.
    #[serde(default)]
    pub federation_rule_id: Option<String>,
    /// `ANTHROPIC_ORGANIZATION_ID`.
    #[serde(default)]
    pub organization_id: Option<String>,
    /// `ANTHROPIC_SERVICE_ACCOUNT_ID`.
    #[serde(default)]
    pub anthropic_service_account_id: Option<String>,
    /// `ANTHROPIC_WORKSPACE_ID`.
    #[serde(default)]
    pub workspace_id: Option<String>,
}

impl WorkloadIdentityTarget {
    fn from_config(config: &LlmAuthConfig) -> Self {
        Self {
            identity_provider_id: config.openai_identity_provider_id.clone(),
            service_account_id: config.openai_service_account_id.clone(),
            identity_token_file: Some(config.llm_identity_token_file.clone()),
            token_exchange_url: config.llm_token_exchange_url.clone(),
            authentik_token_url: config.llm_authentik_token_url.clone(),
            authentik_client_id: config.llm_authentik_client_id.clone(),
            authentik_scope: config.llm_authentik_scope.clone(),
            token_refresh_skew_secs: Some(config.llm_token_refresh_skew_secs),
            federation_rule_id: config.anthropic_federation_rule_id.clone(),
            organization_id: config.anthropic_organization_id.clone(),
            anthropic_service_account_id: config.anthropic_service_account_id.clone(),
            workspace_id: config.anthropic_workspace_id.clone(),
        }
    }
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
            gateway_backoff: crate::llm::DEFAULT_GATEWAY_BACKOFF,
        }
    }

    /// This target's `LLM_AUTH` settings, unset keys at the service's
    /// defaults.
    pub(crate) fn auth_config(&self) -> LlmAuthConfig {
        let wif = self.workload_identity.clone().unwrap_or_default();
        LlmAuthConfig {
            llm_auth: self.auth,
            openai_identity_provider_id: wif.identity_provider_id,
            openai_service_account_id: wif.service_account_id,
            llm_identity_token_file: wif
                .identity_token_file
                .unwrap_or_else(|| service_default("llm_identity_token_file")),
            llm_token_exchange_url: wif.token_exchange_url,
            anthropic_federation_rule_id: wif.federation_rule_id,
            anthropic_organization_id: wif.organization_id,
            anthropic_service_account_id: wif.anthropic_service_account_id,
            anthropic_workspace_id: wif.workspace_id,
            llm_authentik_token_url: wif.authentik_token_url,
            llm_authentik_client_id: wif.authentik_client_id,
            llm_authentik_scope: wif.authentik_scope,
            llm_token_refresh_skew_secs: wif
                .token_refresh_skew_secs
                .unwrap_or_else(|| service_default("llm_token_refresh_skew_secs")),
        }
    }

    /// The Claude settings, unset keys at the service's defaults.
    pub(crate) fn anthropic_settings(&self) -> AnthropicSettings {
        let defaults = AnthropicSettings::default();
        AnthropicSettings {
            version: self.anthropic_version.clone().unwrap_or(defaults.version),
            prompt_cache: self.prompt_cache.unwrap_or(defaults.prompt_cache),
            thinking: self.thinking.clone().filter(|t| !t.trim().is_empty()),
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
        let api_key = api_key.map(Secret::new);
        // The service's own provider validation (an `anthropic` target needs
        // a key and LLM_AUTH=api-key).
        let resolved = crate::config::resolve_llm(
            self.provider,
            Some(&self.base_url),
            Some(&self.model),
            self.auth,
            api_key.as_ref(),
            None,
        )
        .map_err(|err| anyhow::anyhow!("target {:?}: {err}", self.name))?;
        let auth = self
            .auth_config()
            .auth(api_key.as_ref())
            .map_err(|err| anyhow::anyhow!("target {:?}: {err}", self.name))?;
        let (active, prompts) = self
            .profile_and_prompts(&resolved.model)
            .map_err(|err| anyhow::anyhow!("target {:?}: {err}", self.name))?;
        let client = LlmClient::new(
            resolved.base_url,
            None,
            resolved.model,
            Duration::from_secs(timeout_secs),
        )
        .with_auth(auth)
        .with_provider_policy(self.policy());
        let client = match self.provider {
            ProviderKind::Openai => client,
            ProviderKind::Anthropic => client.with_anthropic(self.anthropic_settings()),
        };
        Ok(client
            .with_generation(&active.generation)
            .with_prompts(prompts))
    }

    /// The target's profile (the service's resolution, with this target's
    /// settings as the overrides) and its prompts.
    pub(crate) fn profile_and_prompts(
        &self,
        model: &str,
    ) -> anyhow::Result<(crate::profile::Resolved, crate::profile::PromptSet)> {
        use crate::profile::{Overrides, Setting, parse_setting};
        let overrides = Overrides {
            temperature: parse_setting("temperature", self.temperature.as_deref())?,
            top_p: parse_setting("top_p", self.top_p.as_deref())?,
            max_tokens: self.max_tokens.map(Setting::Set),
            reasoning_effort: parse_setting("reasoning_effort", self.reasoning_effort.as_deref())?,
            thinking: parse_setting("thinking", self.thinking.as_deref())?,
        };
        let active =
            crate::profile::resolve(self.provider, model, self.profile.as_deref(), overrides)?;
        let dir = self
            .prompts_dir
            .as_deref()
            .map(|p| crate::eval::resolve(&p.to_string_lossy()));
        let prompts = crate::profile::PromptSet::load(dir.as_deref(), active.profile)?;
        Ok((active, prompts))
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
    let auth = LlmAuthConfig::parse_from(["eval"]);
    let anthropic = AnthropicConfig::parse_from(["eval"]);
    let generation = crate::config::GenerationConfig::parse_from(["eval"]);
    let provider = ProviderKind::from_env()?;
    // `anthropic` has defaults for both, like the service.
    let (base_url, model) = match provider {
        ProviderKind::Openai => (var("LLM_BASE_URL")?, var("LLM_MODEL")?),
        ProviderKind::Anthropic => (
            std::env::var("LLM_BASE_URL")
                .unwrap_or_else(|_| crate::llm::anthropic::DEFAULT_BASE_URL.to_string()),
            std::env::var("LLM_MODEL")
                .unwrap_or_else(|_| crate::llm::anthropic::DEFAULT_MODEL.to_string()),
        ),
    };
    Ok(vec![Target {
        name: std::env::var("EVAL_TARGET_NAME").unwrap_or_else(|_| "env".to_string()),
        environment: std::env::var("EVAL_ENVIRONMENT").ok(),
        provider,
        base_url,
        model,
        api_key_env: std::env::var("LLM_API_KEY")
            .is_ok()
            .then(|| "LLM_API_KEY".to_string()),
        auth: auth.llm_auth,
        workload_identity: (auth.llm_auth != LlmAuthMode::ApiKey)
            .then(|| WorkloadIdentityTarget::from_config(&auth)),
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
        prompt_cache: Some(anthropic.llm_prompt_cache),
        thinking: anthropic.llm_thinking,
        anthropic_version: Some(anthropic.llm_anthropic_version),
        profile: generation.llm_profile,
        temperature: generation.llm_temperature,
        top_p: generation.llm_top_p,
        prompts_dir: generation.llm_prompts_dir,
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

    /// The `OpenAI` example target (docs/enricher-openai.md): effort `none`,
    /// no `max_tokens`, and the documented retry/concurrency settings.
    #[test]
    fn example_openai_target_has_the_documented_settings() {
        let targets = parse_targets(
            include_str!("../../eval/targets.example.toml"),
            Some(&["openai-gpt-6-luna-none".to_string()]),
        )
        .unwrap();
        let target = &targets[0];
        assert_eq!(target.base_url, "https://api.openai.com/v1");
        assert_eq!(target.model, "gpt-6-luna");
        assert_eq!(target.api_key_env.as_deref(), Some("OPENAI_API_KEY"));
        assert_eq!(target.request_timeout_secs, 120);
        let policy = target.policy();
        assert_eq!(policy.reasoning_effort.as_deref(), Some("none"));
        assert_eq!(policy.max_tokens, None);
        assert_eq!(policy.max_in_flight, Some(3));
        assert_eq!(policy.max_rate_limit_retries, 3);
        assert_eq!(policy.max_gateway_retries, 1);
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
        assert_eq!(target.auth, LlmAuthMode::ApiKey);
        assert!(target.workload_identity.is_none());
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

    /// A target can select a workload identity mode; its settings default
    /// to the service's, and its validation is the service's.
    #[test]
    fn workload_identity_target_parses_and_validates() {
        let targets = parse_targets(
            "[[targets]]\nname = \"wif\"\nbase_url = \"https://api.openai.com/v1\"\n\
             model = \"gpt-6-luna\"\nauth = \"openai-wif-kubernetes\"\n\
             [targets.workload_identity]\nidentity_provider_id = \"idp_1\"\n\
             service_account_id = \"svc_1\"\n\
             identity_token_file = \"/surely/absent/enricher-eval-token\"\n",
            None,
        )
        .unwrap();
        let target = &targets[0];
        assert_eq!(target.auth, LlmAuthMode::OpenaiWifKubernetes);
        let config = target.auth_config();
        assert_eq!(
            config.token_exchange_url(),
            "https://auth.openai.com/oauth/token"
        );
        assert_eq!(config.llm_token_refresh_skew_secs, 60);
        let federation = config.federation(None).unwrap().unwrap();
        assert!(matches!(
            federation.target,
            crate::auth::ExchangeTarget::Openai { ref identity_provider_id, .. }
                if identity_provider_id == "idp_1"
        ));
        // Out of cluster there is no projected token: the client refuses.
        let err = target.client(1).err().unwrap().to_string();
        assert!(err.contains("not readable"), "{err}");

        let typo = "[[targets]]\nname = \"a\"\nbase_url = \"u\"\nmodel = \"m\"\n\
                    [targets.workload_identity]\nidentity_provider = \"x\"\n";
        assert!(parse_targets(typo, None).is_err());
        let bad_mode =
            "[[targets]]\nname = \"a\"\nbase_url = \"u\"\nmodel = \"m\"\nauth = \"gcp\"\n";
        assert!(parse_targets(bad_mode, None).is_err());
        let missing_ids = "[[targets]]\nname = \"a\"\nbase_url = \"u\"\nmodel = \"m\"\n\
                           auth = \"openai-wif-authentik\"\n";
        let target = &parse_targets(missing_ids, None).unwrap()[0];
        assert!(target.client(1).is_err());
    }

    /// The Claude example targets (docs/enricher-anthropic.md): the
    /// provider, the documented model ids, and the service's validation (an
    /// `anthropic` target without its key is refused).
    #[test]
    fn example_anthropic_targets_parse_and_validate() {
        let targets = parse_targets(
            include_str!("../../eval/targets.example.toml"),
            Some(&[
                "anthropic-claude-haiku-5-5".to_string(),
                "anthropic-claude-sonnet-5-5".to_string(),
            ]),
        )
        .unwrap();
        assert_eq!(targets.len(), 2);
        for target in &targets {
            assert_eq!(target.provider, ProviderKind::Anthropic);
            assert_eq!(target.base_url, "https://api.anthropic.com/v1");
            assert_eq!(target.api_key_env.as_deref(), Some("ANTHROPIC_API_KEY"));
            assert_eq!(
                target.anthropic_settings().prompt_cache,
                PromptCache::OneHour
            );
        }
        assert_eq!(targets[0].model, "claude-haiku-5-5");
        assert_eq!(targets[1].model, "claude-sonnet-5-5");

        let keyless = parse_targets(
            "[[targets]]\nname = \"c\"\nprovider = \"anthropic\"\n\
             base_url = \"https://api.anthropic.com/v1\"\nmodel = \"claude-haiku-5-5\"\n",
            None,
        )
        .unwrap();
        let err = keyless[0].client(1).err().unwrap().to_string();
        assert!(err.contains("needs LLM_API_KEY"), "{err}");
        let typo =
            "[[targets]]\nname = \"c\"\nprovider = \"claude\"\nbase_url = \"u\"\nmodel = \"m\"\n";
        assert!(parse_targets(typo, None).is_err());
    }

    /// A target picks its profile like the service, and its own settings
    /// override it; a known-rejected setting is refused before any call.
    #[test]
    fn targets_resolve_profiles_like_the_service() {
        let parse = |extra: &str| {
            parse_targets(
                &format!(
                    "[[targets]]\nname = \"t\"\nbase_url = \"http://l/v1\"\nmodel = \"m\"\n{extra}"
                ),
                None,
            )
            .unwrap()
            .remove(0)
        };
        let (active, prompts) = parse("").profile_and_prompts("m").unwrap();
        assert_eq!(active.profile, "openai-default");
        assert_eq!(active.generation.temperature, Some(0.0));
        assert!(prompts.is_builtin());
        let (active, _) = parse("temperature = \"omit\"\ntop_p = \"0.8\"\n")
            .profile_and_prompts("m")
            .unwrap();
        assert_eq!(active.generation.temperature, None);
        assert_eq!(active.generation.top_p, Some(0.8));
        let claude = parse("provider = \"anthropic\"\ntemperature = \"0.2\"\n");
        assert!(claude.profile_and_prompts("claude-sonnet-5-5").is_err());
        let named = parse("profile = \"nope\"\n");
        assert!(named.profile_and_prompts("m").is_err());
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
