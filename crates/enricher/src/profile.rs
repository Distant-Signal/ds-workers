//! Per-provider, per-model generation settings and prompts ("profiles").
//! docs/enricher-anthropic.md and docs/enricher-openai.md, "Tuning per
//! model".
//!
//! **Generation settings.** A profile is picked by provider plus a model
//! glob (`gpt-6-luna*`, `claude-haiku-5*`, `*`), first match wins, or by
//! name (`LLM_PROFILE`). It sets defaults for `temperature`, `top_p`,
//! `max_tokens`, reasoning effort and (Claude) thinking; an explicit env
//! value (`LLM_TEMPERATURE`, `LLM_TOP_P`, `LLM_MAX_TOKENS`,
//! `LLM_REASONING_EFFORT`, `LLM_THINKING`) overrides it, and the value
//! `omit` overrides it with "don't send". Unset means not sent. The built-in
//! profiles keep the pre-profile behaviour: every OpenAI-compatible request
//! sends `temperature: 0`; Claude requests send none.
//!
//! **Capabilities.** A small, best-effort table of models known to reject a
//! sampling parameter (current Claude models reject a non-default
//! `temperature`/`top_p`; `gpt-6-luna` takes `temperature` only at effort
//! `none`). A resolved configuration that would send one fails at startup
//! instead of at the first request. Models not in the table are not
//! checked.
//!
//! **Prompts.** The three system prompts are the built-in shared ones
//! unless `LLM_PROMPTS_DIR` (the chart mounts `enricher.llm.prompts`'s
//! `ConfigMap` there) has `<profile>.<call>.txt` or `<call>.txt` for a call
//! (`primary`, `adversarial`, `severity_adversarial`), most specific first.
//! The schemas and parsers stay shared, so outputs stay compatible. The
//! active prompt set's short hash is its `version`: logged, and part of
//! `model_version` when it isn't the built-in set (so an edited prompt
//! re-extracts, like a model change).

use std::path::Path;

use crate::llm::ProviderKind;

/// A setting's override: a value, or "don't send" (`omit`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Setting<T> {
    Set(T),
    Omit,
}

impl<T> Setting<T> {
    fn into_option(self) -> Option<T> {
        match self {
            Self::Set(value) => Some(value),
            Self::Omit => None,
        }
    }
}

/// The keyword that overrides a profile's value with "don't send".
pub(crate) const OMIT: &str = "omit";

/// Parses an env/values string: unset or empty is no override, `omit` is
/// [`Setting::Omit`], anything else must parse as `T`.
pub(crate) fn parse_setting<T: std::str::FromStr>(
    name: &str,
    value: Option<&str>,
) -> anyhow::Result<Option<Setting<T>>> {
    match value.map(str::trim).filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(v) if v.eq_ignore_ascii_case(OMIT) => Ok(Some(Setting::Omit)),
        Some(v) => v
            .parse()
            .map(|parsed| Some(Setting::Set(parsed)))
            .map_err(|_| anyhow::anyhow!("{name}={v:?} is neither a value nor `{OMIT}`")),
    }
}

/// The resolved generation settings: `None` is never sent.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Generation {
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_tokens: Option<u32>,
    pub reasoning_effort: Option<String>,
    /// Claude only: `thinking.type`.
    pub thinking: Option<String>,
}

/// Explicit overrides from env/values, each `None` when unset.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Overrides {
    pub temperature: Option<Setting<f32>>,
    pub top_p: Option<Setting<f32>>,
    pub max_tokens: Option<Setting<u32>>,
    pub reasoning_effort: Option<Setting<String>>,
    pub thinking: Option<Setting<String>>,
}

/// A built-in profile.
#[derive(Debug)]
pub(crate) struct Profile {
    pub name: &'static str,
    pub provider: ProviderKind,
    /// `*` matches any run of characters.
    pub model_glob: &'static str,
    temperature: Option<f32>,
    top_p: Option<f32>,
    max_tokens: Option<u32>,
    reasoning_effort: Option<&'static str>,
    thinking: Option<&'static str>,
}

impl Profile {
    fn defaults(&self) -> Generation {
        Generation {
            temperature: self.temperature,
            top_p: self.top_p,
            max_tokens: self.max_tokens,
            reasoning_effort: self.reasoning_effort.map(str::to_string),
            thinking: self.thinking.map(str::to_string),
        }
    }
}

/// The built-in profiles, most specific first.
pub(crate) const PROFILES: &[Profile] = &[
    // docs/enricher-openai.md: temperature 0 is only accepted at effort
    // `none`, so that is this model's default effort.
    Profile {
        name: "openai-gpt-6-luna",
        provider: ProviderKind::Openai,
        model_glob: "gpt-6-luna*",
        temperature: Some(0.0),
        top_p: None,
        max_tokens: None,
        reasoning_effort: Some("none"),
        thinking: None,
    },
    // Every other OpenAI-compatible model: today's requests, temperature 0.
    Profile {
        name: "openai-default",
        provider: ProviderKind::Openai,
        model_glob: "*",
        temperature: Some(0.0),
        top_p: None,
        max_tokens: None,
        reasoning_effort: None,
        thinking: None,
    },
    // Claude: no sampling parameters (the current models reject them), the
    // model's default effort and adaptive thinking.
    Profile {
        name: "claude-default",
        provider: ProviderKind::Anthropic,
        model_glob: "*",
        temperature: None,
        top_p: None,
        max_tokens: None,
        reasoning_effort: None,
        thinking: None,
    },
];

/// One row of the capability table.
struct Capability {
    model_glob: &'static str,
    /// The model rejects a `temperature`/`top_p`...
    rejects_sampling: bool,
    /// ...unless the reasoning effort is this.
    sampling_ok_at_effort: Option<&'static str>,
    /// Minimum cacheable prompt prefix, in tokens (Claude).
    cache_minimum: u32,
}

/// Best-effort, from the providers' docs (2026-10-09). First match wins;
/// models not listed are not checked.
const CAPABILITIES: &[Capability] = &[
    Capability {
        model_glob: "gpt-6-luna*",
        rejects_sampling: true,
        sampling_ok_at_effort: Some("none"),
        cache_minimum: 0,
    },
    Capability {
        model_glob: "claude-haiku-4-5*",
        rejects_sampling: false,
        sampling_ok_at_effort: None,
        cache_minimum: 4096,
    },
    Capability {
        model_glob: "claude-haiku-5*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 512,
    },
    Capability {
        model_glob: "claude-sonnet-5-5*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 512,
    },
    Capability {
        model_glob: "claude-sonnet-5*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 1024,
    },
    Capability {
        model_glob: "claude-opus-5-5*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 512,
    },
    Capability {
        model_glob: "claude-opus-5*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 512,
    },
    Capability {
        model_glob: "claude-opus-4-8*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 1024,
    },
    Capability {
        model_glob: "claude-opus-4-7*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 2048,
    },
    Capability {
        model_glob: "claude-fable-5*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 512,
    },
    Capability {
        model_glob: "claude-mythos-5*",
        rejects_sampling: true,
        sampling_ok_at_effort: None,
        cache_minimum: 512,
    },
    Capability {
        model_glob: "claude-*",
        rejects_sampling: false,
        sampling_ok_at_effort: None,
        cache_minimum: 1024,
    },
];

/// `*`-glob match (case-sensitive; `*` is any run of characters).
pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let Some((head, rest)) = pattern.split_once('*') else {
        return pattern == text;
    };
    let Some(mut remaining) = text.strip_prefix(head) else {
        return false;
    };
    let mut parts: Vec<&str> = rest.split('*').collect();
    let last = parts.pop().unwrap_or_default();
    for part in parts {
        match remaining.find(part) {
            Some(at) => remaining = &remaining[at + part.len()..],
            None => return false,
        }
    }
    remaining.ends_with(last) && remaining.len() >= last.len()
}

fn capability(model: &str) -> Option<&'static Capability> {
    CAPABILITIES
        .iter()
        .find(|c| glob_match(c.model_glob, model))
}

/// The model's minimum cacheable prefix in tokens, when known (Claude).
pub(crate) fn cache_minimum(model: &str) -> Option<u32> {
    capability(model)
        .map(|c| c.cache_minimum)
        .filter(|min| *min > 0)
}

/// The active profile and its resolved settings.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Resolved {
    pub profile: &'static str,
    pub generation: Generation,
}

/// Picks the profile (`name`, else the first matching provider + model)
/// and applies `overrides` on top; then refuses a sampling parameter the
/// model is known to reject.
pub(crate) fn resolve(
    provider: ProviderKind,
    model: &str,
    name: Option<&str>,
    overrides: Overrides,
) -> anyhow::Result<Resolved> {
    let profile = match name.map(str::trim).filter(|n| !n.is_empty()) {
        Some(name) => {
            let profile = PROFILES.iter().find(|p| p.name == name).ok_or_else(|| {
                let known: Vec<&str> = PROFILES.iter().map(|p| p.name).collect();
                anyhow::anyhow!("LLM_PROFILE={name:?} is not one of {known:?}")
            })?;
            if profile.provider != provider {
                anyhow::bail!(
                    "LLM_PROFILE={name:?} is for LLM_PROVIDER={:?}, not {provider:?}",
                    profile.provider
                );
            }
            profile
        }
        None => PROFILES
            .iter()
            .find(|p| p.provider == provider && glob_match(p.model_glob, model))
            .ok_or_else(|| anyhow::anyhow!("no profile matches {provider:?} {model:?}"))?,
    };
    let mut generation = profile.defaults();
    let Overrides {
        temperature,
        top_p,
        max_tokens,
        reasoning_effort,
        thinking,
    } = overrides;
    if let Some(v) = temperature {
        generation.temperature = v.into_option();
    }
    if let Some(v) = top_p {
        generation.top_p = v.into_option();
    }
    if let Some(v) = max_tokens {
        generation.max_tokens = v.into_option();
    }
    if let Some(v) = reasoning_effort {
        generation.reasoning_effort = v.into_option();
    }
    if let Some(v) = thinking {
        generation.thinking = v.into_option();
    }
    if let Some(cap) = capability(model).filter(|c| c.rejects_sampling) {
        let allowed = cap
            .sampling_ok_at_effort
            .is_some_and(|effort| generation.reasoning_effort.as_deref() == Some(effort));
        for (param, set) in [
            (
                "temperature (LLM_TEMPERATURE)",
                generation.temperature.is_some(),
            ),
            ("top_p (LLM_TOP_P)", generation.top_p.is_some()),
        ] {
            if set && !allowed {
                let when = cap
                    .sampling_ok_at_effort
                    .map(|e| format!(" unless the reasoning effort is {e:?}"))
                    .unwrap_or_default();
                anyhow::bail!(
                    "{model} is known to reject {param}{when} (profile {}); set it to \
                     `{OMIT}` (docs/enricher-anthropic.md, \"Tuning per model\")",
                    profile.name
                );
            }
        }
    }
    Ok(Resolved {
        profile: profile.name,
        generation,
    })
}

/// One call's system prompt file name stem.
pub(crate) const PROMPT_CALLS: [&str; 3] = ["primary", "adversarial", "severity_adversarial"];

/// The three system prompts in use, and where each came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromptSet {
    pub primary: String,
    pub adversarial: String,
    pub severity_adversarial: String,
    /// Per call: `built-in` or the file it was read from.
    pub sources: [String; 3],
    /// A short hash of the three prompts.
    pub version: String,
}

impl Default for PromptSet {
    fn default() -> Self {
        Self::builtin()
    }
}

fn prompt_version(prompts: [&str; 3]) -> String {
    let mut hash = common::text_hash::text_hash("enricher-system-prompts", &prompts.join("\0"));
    hash.truncate(12);
    hash
}

impl PromptSet {
    /// The shared built-in prompts.
    pub(crate) fn builtin() -> Self {
        let prompts = crate::llm::builtin_prompts();
        Self {
            primary: prompts[0].to_string(),
            adversarial: prompts[1].to_string(),
            severity_adversarial: prompts[2].to_string(),
            sources: [
                "built-in".to_string(),
                "built-in".to_string(),
                "built-in".to_string(),
            ],
            version: prompt_version(prompts),
        }
    }

    /// Whether these are the built-in prompts.
    pub(crate) fn is_builtin(&self) -> bool {
        self.version == Self::builtin().version
    }

    /// The prompts for `profile`: per call, `<dir>/<profile>.<call>.txt`,
    /// else `<dir>/<call>.txt`, else the built-in one. An empty file is an
    /// error, as is an unreadable one that exists.
    pub(crate) fn load(dir: Option<&Path>, profile: &str) -> anyhow::Result<Self> {
        let builtin = Self::builtin();
        let Some(dir) = dir else {
            return Ok(builtin);
        };
        let defaults = [
            builtin.primary,
            builtin.adversarial,
            builtin.severity_adversarial,
        ];
        let mut texts: [String; 3] = Default::default();
        let mut sources: [String; 3] = Default::default();
        for (i, call) in PROMPT_CALLS.iter().enumerate() {
            let candidates = [
                dir.join(format!("{profile}.{call}.txt")),
                dir.join(format!("{call}.txt")),
            ];
            if let Some(path) = candidates.iter().find(|p| p.is_file()) {
                let text = std::fs::read_to_string(path)
                    .map_err(|err| anyhow::anyhow!("reading prompt {}: {err}", path.display()))?;
                let text = text.trim().to_string();
                if text.is_empty() {
                    anyhow::bail!("prompt {} is empty", path.display());
                }
                texts[i] = text;
                sources[i] = path.display().to_string();
            } else {
                texts[i].clone_from(&defaults[i]);
                sources[i] = "built-in".to_string();
            }
        }
        let version = prompt_version([&texts[0], &texts[1], &texts[2]]);
        let [primary, adversarial, severity_adversarial] = texts;
        Ok(Self {
            primary,
            adversarial,
            severity_adversarial,
            sources,
            version,
        })
    }
}

/// `model_version` for the extraction columns: `<model>@periods-v2`, plus
/// `+prompts-<version>` when the prompts aren't the built-in ones, so a
/// prompt change re-extracts like a model change (and switching back to the
/// built-in prompts restores the old value).
pub(crate) fn model_version(model: &str, prompts: &PromptSet) -> String {
    if prompts.is_builtin() {
        format!("{model}@periods-v2")
    } else {
        format!("{model}@periods-v2+prompts-{}", prompts.version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globs() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("claude-haiku-5*", "claude-haiku-5-5"));
        assert!(!glob_match("claude-haiku-5*", "claude-haiku-4-5"));
        assert!(glob_match("gpt-6-luna", "gpt-6-luna"));
        assert!(!glob_match("gpt-6-luna", "gpt-6-luna-mini"));
        assert!(glob_match("a*b*c", "a-x-b-y-c"));
        assert!(!glob_match("a*b*c", "a-x-c"));
        assert!(glob_match("*-5-5", "claude-opus-5-5"));
    }

    /// Without overrides the built-in profiles are today's requests:
    /// `OpenAI` temperature 0, gpt-6-luna also effort `none`, Claude nothing.
    #[test]
    fn builtin_profiles_keep_todays_behaviour() {
        let none = Overrides::default();
        let r = resolve(ProviderKind::Openai, "qwen3.5:4b", None, none.clone()).unwrap();
        assert_eq!(r.profile, "openai-default");
        assert_eq!(r.generation.temperature, Some(0.0));
        assert_eq!(r.generation.reasoning_effort, None);
        assert_eq!(r.generation.max_tokens, None);
        let r = resolve(ProviderKind::Openai, "gpt-6-luna", None, none.clone()).unwrap();
        assert_eq!(r.profile, "openai-gpt-6-luna");
        assert_eq!(r.generation.temperature, Some(0.0));
        assert_eq!(r.generation.reasoning_effort.as_deref(), Some("none"));
        let r = resolve(ProviderKind::Anthropic, "claude-haiku-5-5", None, none).unwrap();
        assert_eq!(r.profile, "claude-default");
        assert_eq!(r.generation, Generation::default());
    }

    /// Precedence: explicit value > `omit` > profile default; a named
    /// profile wins over the model match.
    #[test]
    fn overrides_win_and_omit_removes() {
        let overrides = Overrides {
            temperature: Some(Setting::Omit),
            top_p: Some(Setting::Set(0.9)),
            max_tokens: Some(Setting::Set(4096)),
            reasoning_effort: Some(Setting::Set("low".into())),
            thinking: None,
        };
        let r = resolve(ProviderKind::Openai, "m", None, overrides).unwrap();
        assert_eq!(r.generation.temperature, None);
        assert_eq!(r.generation.top_p, Some(0.9));
        assert_eq!(r.generation.max_tokens, Some(4096));
        assert_eq!(r.generation.reasoning_effort.as_deref(), Some("low"));

        let r = resolve(
            ProviderKind::Openai,
            "my-luna-proxy",
            Some("openai-gpt-6-luna"),
            Overrides::default(),
        )
        .unwrap();
        assert_eq!(r.profile, "openai-gpt-6-luna");
        assert_eq!(r.generation.reasoning_effort.as_deref(), Some("none"));
        // Naming a profile doesn't bypass the capability check: the default
        // profile's temperature without effort `none` on gpt-6-luna.
        assert!(
            resolve(
                ProviderKind::Openai,
                "gpt-6-luna",
                Some("openai-default"),
                Overrides::default(),
            )
            .is_err()
        );

        assert!(
            resolve(
                ProviderKind::Openai,
                "m",
                Some("nope"),
                Overrides::default()
            )
            .is_err()
        );
        let err = resolve(
            ProviderKind::Openai,
            "m",
            Some("claude-default"),
            Overrides::default(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("is for LLM_PROVIDER"), "{err}");
    }

    /// A sampling parameter a model is known to reject fails at startup.
    #[test]
    fn known_rejections_fail_at_startup() {
        let temp = |t: f32| Overrides {
            temperature: Some(Setting::Set(t)),
            ..Overrides::default()
        };
        for model in ["claude-sonnet-5-5", "claude-haiku-5-5", "claude-opus-5-5"] {
            let err = resolve(ProviderKind::Anthropic, model, None, temp(0.0)).unwrap_err();
            assert!(err.to_string().contains("temperature"), "{model}: {err}");
        }
        let top_p = Overrides {
            top_p: Some(Setting::Set(0.5)),
            ..Overrides::default()
        };
        assert!(resolve(ProviderKind::Anthropic, "claude-sonnet-5-5", None, top_p).is_err());
        // Haiku 4.5 takes it; an unknown model isn't checked.
        assert!(resolve(ProviderKind::Anthropic, "claude-haiku-4-5", None, temp(0.0)).is_ok());
        assert!(resolve(ProviderKind::Openai, "some-local-model", None, temp(0.2)).is_ok());
        // gpt-6-luna: temperature only at effort none.
        let low = Overrides {
            reasoning_effort: Some(Setting::Set("low".into())),
            ..Overrides::default()
        };
        let err = resolve(ProviderKind::Openai, "gpt-6-luna", None, low).unwrap_err();
        assert!(
            err.to_string().contains("unless the reasoning effort"),
            "{err}"
        );
        let low_no_temp = Overrides {
            reasoning_effort: Some(Setting::Set("low".into())),
            temperature: Some(Setting::Omit),
            ..Overrides::default()
        };
        assert!(resolve(ProviderKind::Openai, "gpt-6-luna", None, low_no_temp).is_ok());
    }

    #[test]
    fn settings_parse() {
        assert_eq!(parse_setting::<f32>("T", None).unwrap(), None);
        assert_eq!(parse_setting::<f32>("T", Some(" ")).unwrap(), None);
        assert_eq!(
            parse_setting::<f32>("T", Some("OMIT")).unwrap(),
            Some(Setting::Omit)
        );
        assert_eq!(
            parse_setting::<f32>("T", Some("0.3")).unwrap(),
            Some(Setting::Set(0.3))
        );
        assert!(parse_setting::<f32>("T", Some("warm")).is_err());
    }

    /// Prompt files: `<profile>.<call>.txt` beats `<call>.txt` beats the
    /// built-in prompt; the version and `model_version` follow.
    #[test]
    fn prompt_overrides_load_and_fall_back() {
        let builtin = PromptSet::builtin();
        assert!(builtin.is_builtin());
        assert_eq!(model_version("m", &builtin), "m@periods-v2");
        assert_eq!(PromptSet::load(None, "claude-default").unwrap(), builtin);

        let dir = tempfile::tempdir().unwrap();
        // An empty directory is the built-in set.
        assert!(
            PromptSet::load(Some(dir.path()), "claude-default")
                .unwrap()
                .is_builtin()
        );
        std::fs::write(dir.path().join("adversarial.txt"), "shared adversarial\n").unwrap();
        std::fs::write(
            dir.path().join("claude-default.adversarial.txt"),
            "claude adversarial",
        )
        .unwrap();
        std::fs::write(dir.path().join("primary.txt"), "shared primary").unwrap();
        let claude = PromptSet::load(Some(dir.path()), "claude-default").unwrap();
        assert_eq!(claude.primary, "shared primary");
        assert_eq!(claude.adversarial, "claude adversarial");
        assert_eq!(claude.severity_adversarial, builtin.severity_adversarial);
        assert_eq!(claude.sources[2], "built-in");
        assert!(claude.sources[1].ends_with("claude-default.adversarial.txt"));
        assert!(!claude.is_builtin());
        assert_eq!(claude.version.len(), 12);
        assert_eq!(
            model_version("m", &claude),
            format!("m@periods-v2+prompts-{}", claude.version)
        );
        let openai = PromptSet::load(Some(dir.path()), "openai-default").unwrap();
        assert_eq!(openai.adversarial, "shared adversarial");
        assert_ne!(openai.version, claude.version);

        std::fs::write(dir.path().join("severity_adversarial.txt"), "  \n").unwrap();
        let err = PromptSet::load(Some(dir.path()), "x")
            .unwrap_err()
            .to_string();
        assert!(err.contains("is empty"), "{err}");
    }

    /// Per profile: with caching on (the default), the primary system
    /// prompt must stay above the cache minimum of every model the docs
    /// recommend for that profile.
    #[test]
    fn primary_prompt_stays_above_each_profiles_cache_minimum() {
        let tokens = PromptSet::builtin().primary.chars().count() / 4;
        for profile in PROFILES
            .iter()
            .filter(|p| p.provider == ProviderKind::Anthropic)
        {
            for model in [
                "claude-haiku-5-5",
                "claude-sonnet-5-5",
                "claude-opus-5-5",
                "claude-sonnet-5",
            ] {
                if !glob_match(profile.model_glob, model) {
                    continue;
                }
                let min = cache_minimum(model).unwrap();
                assert!(
                    u32::try_from(tokens).unwrap() >= 2 * min,
                    "{}: {model}'s cache minimum {min} vs ~{tokens} prompt tokens",
                    profile.name
                );
            }
        }
        assert_eq!(cache_minimum("claude-haiku-4-5-20251001"), Some(4096));
        assert_eq!(cache_minimum("gpt-6-luna"), None);
    }
}
