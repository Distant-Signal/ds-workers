//! `OpenAI` prompt caching on Chat Completions (`LLM_OPENAI_PROMPT_CACHE`).
//! See docs/enricher-openai.md, "Prompt caching".
//!
//! `OpenAI` documentation this relies on (read 2026-10-10):
//!
//! - Prompt caching guide:
//!   <https://developers.openai.com/api/docs/guides/prompt-caching>
//! - Chat Completions create (`prompt_cache_options`,
//!   `prompt_cache_breakpoint`, `prompt_cache_key`, `usage`):
//!   <https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create>
//! - Pricing: <https://developers.openai.com/api/docs/models/gpt-6-luna>
//!
//! On GPT-5.6 and later models caching is on by default in `implicit` mode:
//! one breakpoint at the end of the latest user message, written at 1.25x
//! the input price. Every enricher call ends in per-incident text, so that
//! write is never read back: the default costs more than no caching at all.
//! [`OpenAiPromptCache::Explicit`] instead switches to `explicit` mode and
//! puts the one breakpoint at the end of the static system prompt, so only
//! the reusable prefix is written and later calls read it at 0.1x.

use serde::Serialize;

use super::LlmCall;

/// `LLM_OPENAI_PROMPT_CACHE` (per profile): what the request says about
/// prompt caching. Unset (or `omit`) sends nothing, i.e. the endpoint's
/// default; the only choice for an OpenAI-compatible server that may reject
/// unknown fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenAiPromptCache {
    /// `prompt_cache_options: {"mode": "explicit"}`, a breakpoint on the
    /// system prompt and a stable `prompt_cache_key` per profile and call.
    Explicit,
    /// `prompt_cache_options: {"mode": "explicit"}` and no breakpoint: no
    /// caching, and no cache-write charge.
    Off,
}

impl std::str::FromStr for OpenAiPromptCache {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "explicit" => Ok(Self::Explicit),
            "off" => Ok(Self::Off),
            other => Err(format!("{other:?} is not one of explicit, off")),
        }
    }
}

/// `prompt_cache_options`. No `ttl`: `30m`, the only value, is the default.
#[derive(Debug, Serialize)]
pub(super) struct PromptCacheOptions {
    mode: &'static str,
}

impl PromptCacheOptions {
    /// Only developer-placed breakpoints are written (never the per-incident
    /// suffix).
    pub(super) const EXPLICIT: Self = Self { mode: "explicit" };
}

/// `prompt_cache_breakpoint` on a content part.
#[derive(Debug, Serialize)]
struct Breakpoint {
    mode: &'static str,
}

/// One `text` content part.
#[derive(Debug, Serialize)]
pub(super) struct TextPart {
    #[serde(rename = "type")]
    kind: &'static str,
    text: String,
    prompt_cache_breakpoint: Breakpoint,
}

/// A chat message's `content`: a plain string (as before), or one text part
/// carrying a cache breakpoint at its end.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(super) enum MessageContent {
    Text(String),
    Parts([TextPart; 1]),
}

impl MessageContent {
    /// `text` with an explicit breakpoint at its end.
    pub(super) fn with_breakpoint(text: String) -> Self {
        Self::Parts([TextPart {
            kind: "text",
            text,
            prompt_cache_breakpoint: Breakpoint { mode: "explicit" },
        }])
    }
}

/// `prompt_cache_key`: stable per profile and call site. On GPT-5.6 and
/// later it only separates cache accounting (routing is automatic); the
/// three calls have different prefixes anyway, so it splits nothing that
/// could have been shared.
pub(super) fn cache_key(profile: &str, call: LlmCall) -> String {
    let profile = if profile.is_empty() {
        "unnamed"
    } else {
        profile
    };
    format!("ds-enricher:{profile}:{}", call.label())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_values() {
        assert_eq!("explicit".parse(), Ok(OpenAiPromptCache::Explicit));
        assert_eq!(" OFF ".parse(), Ok(OpenAiPromptCache::Off));
        assert!("1h".parse::<OpenAiPromptCache>().is_err());
    }

    #[test]
    fn content_serializes_as_string_or_one_part() {
        assert_eq!(
            serde_json::to_value(MessageContent::Text("s".into())).unwrap(),
            serde_json::json!("s")
        );
        assert_eq!(
            serde_json::to_value(MessageContent::with_breakpoint("s".into())).unwrap(),
            serde_json::json!([{
                "type": "text",
                "text": "s",
                "prompt_cache_breakpoint": { "mode": "explicit" }
            }])
        );
    }

    /// GPT-5.6 and later cache nothing below 1,024 visible tokens: the
    /// primary system prompt (the cached prefix) must stay well above it;
    /// the adversarial ones are below it (docs/enricher-openai.md, "Prompt
    /// caching", the size table).
    #[test]
    fn prompt_sizes_match_the_openai_caching_analysis() {
        let tokens = |text: &str| text.chars().count() / 4;
        let primary = tokens(crate::llm::PRIMARY_PROMPT);
        assert!(primary >= 2 * 1024, "primary prompt ~{primary} tokens");
        for prompt in [
            crate::llm::ADVERSARIAL_PROMPT,
            crate::llm::SEVERITY_ADVERSARIAL_PROMPT,
        ] {
            assert!(tokens(prompt) < 1024, "~{} tokens", tokens(prompt));
        }
        let schema = |s: serde_json::Value| tokens(&s.to_string());
        eprintln!(
            "chars/4: primary {primary} + schema {}, adversarial {} + {}, severity {} + {}",
            schema(crate::llm::primary_schema()),
            tokens(crate::llm::ADVERSARIAL_PROMPT),
            schema(crate::llm::adversarial_schema()),
            tokens(crate::llm::SEVERITY_ADVERSARIAL_PROMPT),
            schema(crate::llm::severity_adversarial_schema()),
        );
    }

    #[test]
    fn keys_are_per_profile_and_call() {
        assert_eq!(
            cache_key("openai-gpt-6-luna", LlmCall::Primary),
            "ds-enricher:openai-gpt-6-luna:primary"
        );
        assert_eq!(
            cache_key("", LlmCall::SeverityAdversarial),
            "ds-enricher:unnamed:severity_adversarial"
        );
    }
}
