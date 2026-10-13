//! The eval's cost estimate: the token usage the provider reported on each
//! call, priced like the provider bills it. Every call's `usage` splits
//! into uncached input, cache reads, cache writes and output (the same
//! breakdown as `llm::TOKENS_METRIC`: `cached` and `cache_write` are parts
//! of `prompt`, `reasoning` a part of `completion`), and a call that ran
//! in a provider batch (`CallRecord::batch_id`) is billed at
//! [`BATCH_DISCOUNT`] of the synchronous price -- cache reads and writes
//! included, on both providers.
//!
//! Prices are per million tokens, in USD, from docs/enricher-anthropic.md
//! ("Model", "Pricing and break-even") and docs/enricher-openai.md
//! ("Cost", "Prompt caching"); a target can set its own (`[targets.pricing]`).
//! Only the providers' own APIs are priced: an OpenAI-compatible
//! self-hosted model is never priced as `OpenAI`'s.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::eval::pipeline::PipelineRecord;
use crate::llm::anthropic::PromptCache;
use crate::llm::{ProviderKind, TokenUsage};

/// Batch price as a share of the synchronous one: both batch APIs bill
/// every token kind at half price.
pub(crate) const BATCH_DISCOUNT: f64 = 0.5;

/// Synchronous prices, USD per million tokens.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pricing {
    /// Uncached input.
    pub input: f64,
    pub output: f64,
    /// A cache read (`cached_tokens`, Claude's `cache_read_input_tokens`).
    pub cache_read: f64,
    /// A cache write (`cache_write_tokens`, Claude's
    /// `cache_creation_input_tokens`).
    pub cache_write: f64,
}

/// Claude models: (id, input, output, cache read). Cache writes are 1.25x
/// input with the 5-minute TTL and 2x with the 1-hour one.
const CLAUDE: [(&str, f64, f64, f64); 4] = [
    ("claude-haiku-5-5", 0.10, 0.50, 0.01),
    ("claude-sonnet-5-5", 2.0, 10.0, 0.10),
    ("claude-opus-5-5", 4.0, 20.0, 0.20),
    ("claude-haiku-4-5", 1.0, 5.0, 0.10),
];

/// `OpenAI` models: (id, input, output, cache read, cache write).
const OPENAI: [(&str, f64, f64, f64, f64); 1] = [("gpt-6-luna", 0.10, 0.50, 0.01, 0.125)];

/// `model` is `id`, or `id` with a dated suffix (`claude-haiku-4-5-20251001`).
fn is_model(model: &str, id: &str) -> bool {
    model == id
        || model
            .strip_prefix(id)
            .is_some_and(|rest| rest.starts_with('-'))
}

fn host(base_url: &str) -> Option<String> {
    reqwest::Url::parse(base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
}

/// The built-in price of `model` on `provider`'s own API, or `None` (an
/// unknown model, or any other endpoint).
pub(crate) fn builtin(
    provider: ProviderKind,
    base_url: &str,
    model: &str,
    prompt_cache: PromptCache,
) -> Option<Pricing> {
    let host = host(base_url)?;
    match provider {
        ProviderKind::Anthropic if host == "api.anthropic.com" => {
            let (_, input, output, cache_read) =
                CLAUDE.iter().find(|(id, ..)| is_model(model, id))?;
            let write = match prompt_cache {
                PromptCache::OneHour => 2.0,
                PromptCache::FiveMinutes | PromptCache::Off => 1.25,
            };
            Some(Pricing {
                input: *input,
                output: *output,
                cache_read: *cache_read,
                cache_write: input * write,
            })
        }
        ProviderKind::Openai if host == "api.openai.com" => {
            let (_, input, output, cache_read, cache_write) =
                OPENAI.iter().find(|(id, ..)| is_model(model, id))?;
            Some(Pricing {
                input: *input,
                output: *output,
                cache_read: *cache_read,
                cache_write: *cache_write,
            })
        }
        _ => None,
    }
}

/// The built-in price for a re-score, which knows only the model's name:
/// `claude-*` priced as the Claude API (cache writes at the service's
/// default 1-hour TTL), anything else as the `OpenAI` API.
pub(crate) fn builtin_for_model(model: &str) -> Option<(Pricing, &'static str)> {
    let (provider, base_url) = if model.starts_with("claude-") {
        (
            ProviderKind::Anthropic,
            crate::llm::anthropic::DEFAULT_BASE_URL,
        )
    } else {
        (ProviderKind::Openai, "https://api.openai.com/v1")
    };
    builtin(provider, base_url, model, PromptCache::OneHour).map(|p| (p, "builtin"))
}

/// Summed token counts over a set of calls.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UsageTotals {
    /// Calls that reported any usage.
    pub calls: usize,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub reasoning_tokens: u64,
    pub cached_tokens: u64,
    pub cache_write_tokens: u64,
}

impl UsageTotals {
    pub(crate) fn add(&mut self, usage: &TokenUsage) {
        self.calls += 1;
        self.prompt_tokens += usage.prompt_tokens.unwrap_or(0);
        self.completion_tokens += usage.completion_tokens.unwrap_or(0);
        self.reasoning_tokens += usage.reasoning_tokens.unwrap_or(0);
        self.cached_tokens += usage.cached_tokens.unwrap_or(0);
        self.cache_write_tokens += usage.cache_write_tokens.unwrap_or(0);
    }

    /// USD at `pricing`'s synchronous rates.
    #[expect(
        clippy::cast_precision_loss,
        reason = "token counts are far below 2^52"
    )]
    pub(crate) fn usd(&self, pricing: &Pricing) -> f64 {
        let uncached = self
            .prompt_tokens
            .saturating_sub(self.cached_tokens)
            .saturating_sub(self.cache_write_tokens);
        (uncached as f64 * pricing.input
            + self.cached_tokens as f64 * pricing.cache_read
            + self.cache_write_tokens as f64 * pricing.cache_write
            + self.completion_tokens as f64 * pricing.output)
            / 1e6
    }
}

/// What a run's calls cost.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct CostEstimate {
    /// `builtin` or `target` (the target's own `[targets.pricing]`).
    pub source: &'static str,
    pub pricing: Pricing,
    pub batch_discount: f64,
    /// Synchronous calls' usage.
    pub sync_usage: UsageTotals,
    /// Batch calls' usage (billed at the discount).
    pub batch_usage: UsageTotals,
    /// Calls that reported no usage (not priced: a transport failure, or
    /// a batch request that never ran).
    pub calls_without_usage: usize,
    /// The estimate: synchronous calls at full price, batch calls at the
    /// batch price.
    pub usd: f64,
    /// The same calls all at the synchronous price, for comparison.
    pub usd_at_sync_price: f64,
}

/// The usage of every call in `records`, split into synchronous and batch
/// calls, plus how many calls reported none.
pub(crate) fn usage_totals(records: &[PipelineRecord]) -> (UsageTotals, UsageTotals, usize) {
    let (mut sync, mut batch, mut without) = (UsageTotals::default(), UsageTotals::default(), 0);
    for call in records.iter().flat_map(|r| &r.calls) {
        match (&call.usage, &call.batch_id) {
            (None, _) => without += 1,
            (Some(usage), None) => sync.add(usage),
            (Some(usage), Some(_)) => batch.add(usage),
        }
    }
    (sync, batch, without)
}

/// Prices `records` (`None` without a price).
pub(crate) fn estimate(
    records: &[PipelineRecord],
    pricing: Option<(Pricing, &'static str)>,
) -> Option<CostEstimate> {
    let (pricing, source) = pricing?;
    let (sync_usage, batch_usage, calls_without_usage) = usage_totals(records);
    let sync = sync_usage.usd(&pricing);
    let batch = batch_usage.usd(&pricing);
    Some(CostEstimate {
        source,
        pricing,
        batch_discount: BATCH_DISCOUNT,
        usd: sync + batch * BATCH_DISCOUNT,
        usd_at_sync_price: sync + batch,
        sync_usage,
        batch_usage,
        calls_without_usage,
    })
}

/// One Markdown bullet line for a report.
pub(crate) fn markdown_line(cost: Option<&CostEstimate>) -> String {
    let Some(cost) = cost else {
        return "- Estimated cost: - (no price for this model and endpoint; set \
                `[targets.pricing]`)\n"
            .to_string();
    };
    let tokens = |u: &UsageTotals| {
        format!(
            "prompt {} (cache reads {}, cache writes {}), completion {}",
            u.prompt_tokens, u.cached_tokens, u.cache_write_tokens, u.completion_tokens
        )
    };
    let mut line = format!("- Estimated cost: ${:.4}", cost.usd);
    if cost.batch_usage.calls > 0 {
        let _ = write!(
            line,
            " (batch calls at {:.0}%; ${:.4} at the synchronous price)",
            cost.batch_discount * 100.0,
            cost.usd_at_sync_price
        );
    }
    let _ = write!(
        line,
        "; {} prices per MTok: input ${}, cache read ${}, cache write ${}, output ${}",
        cost.source,
        cost.pricing.input,
        cost.pricing.cache_read,
        cost.pricing.cache_write,
        cost.pricing.output
    );
    if cost.sync_usage.calls > 0 {
        let _ = write!(line, "\n- Sync tokens: {}", tokens(&cost.sync_usage));
    }
    if cost.batch_usage.calls > 0 {
        let _ = write!(line, "\n- Batch tokens: {}", tokens(&cost.batch_usage));
    }
    if cost.calls_without_usage > 0 {
        let _ = write!(
            line,
            "\n- {} call(s) reported no usage and are not priced",
            cost.calls_without_usage
        );
    }
    line.push('\n');
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::pipeline::{CallRecord, Pass, TargetLabel};

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-12
    }

    #[test]
    fn builtin_prices_need_the_providers_own_api() {
        let haiku = builtin(
            ProviderKind::Anthropic,
            "https://api.anthropic.com/v1",
            "claude-haiku-5-5",
            PromptCache::OneHour,
        )
        .unwrap();
        assert!(close(haiku.input, 0.10) && close(haiku.cache_read, 0.01));
        assert!(close(haiku.cache_write, 0.20), "1h TTL writes at 2x");
        let five = builtin(
            ProviderKind::Anthropic,
            "https://api.anthropic.com/v1",
            "claude-haiku-5-5",
            PromptCache::FiveMinutes,
        )
        .unwrap();
        assert!(close(five.cache_write, 0.125), "5m TTL writes at 1.25x");
        let luna = builtin(
            ProviderKind::Openai,
            "https://api.openai.com/v1",
            "gpt-6-luna",
            PromptCache::default(),
        )
        .unwrap();
        assert!(close(luna.cache_write, 0.125) && close(luna.output, 0.50));
        assert!(
            builtin(
                ProviderKind::Anthropic,
                "https://api.anthropic.com/v1",
                "claude-haiku-4-5-20251001",
                PromptCache::Off
            )
            .is_some()
        );
        // A self-hosted model with the same name, or an unknown model: no price.
        for (provider, url, model) in [
            (
                ProviderKind::Openai,
                "http://localhost:11434/v1",
                "gpt-6-luna",
            ),
            (ProviderKind::Openai, "https://api.openai.com/v1", "gpt-9"),
            (
                ProviderKind::Anthropic,
                "https://api.anthropic.com/v1",
                "claude-haiku-5-55",
            ),
        ] {
            assert_eq!(
                builtin(provider, url, model, PromptCache::OneHour),
                None,
                "{model}"
            );
        }
    }

    fn call(usage: TokenUsage, batch: bool) -> CallRecord {
        CallRecord {
            pass: Pass::Primary,
            elapsed_ms: 0,
            retries: 0,
            outcome: "success".into(),
            attempts: Vec::new(),
            content: None,
            error: None,
            usage: Some(usage),
            batch_id: batch.then(|| "b".to_string()),
        }
    }

    /// Uncached input, cache reads, cache writes and output each at their
    /// own rate; batch calls at half of all four.
    #[test]
    fn batch_calls_are_half_price_cache_rates_included() {
        let usage = TokenUsage {
            prompt_tokens: Some(1_000_000),
            completion_tokens: Some(100_000),
            reasoning_tokens: Some(50_000),
            cached_tokens: Some(600_000),
            cache_write_tokens: Some(300_000),
        };
        let mut no_usage = call(usage.clone(), true);
        no_usage.usage = None;
        let record = PipelineRecord {
            label: TargetLabel {
                target: "t".into(),
                model: "m".into(),
            },
            case_id: "a".into(),
            input_hash: String::new(),
            prompt_fingerprint: String::new(),
            repetition: 0,
            elapsed_ms: 0,
            calls: vec![call(usage.clone(), false), call(usage, true), no_usage],
        };
        let pricing = Pricing {
            input: 0.10,
            output: 0.50,
            cache_read: 0.01,
            cache_write: 0.20,
        };
        // 100k uncached x 0.10 + 600k x 0.01 + 300k x 0.20 + 100k x 0.50
        // (reasoning is part of completion, never added) = 0.126 per call.
        let cost = estimate(std::slice::from_ref(&record), Some((pricing, "builtin"))).unwrap();
        assert!(close(cost.usd_at_sync_price, 0.252), "{cost:?}");
        assert!(close(cost.usd, 0.126 + 0.063), "{cost:?}");
        assert_eq!(cost.calls_without_usage, 1);
        assert_eq!(cost.batch_usage.cache_write_tokens, 300_000);
        assert!(estimate(&[record], None).is_none());
        let line = markdown_line(Some(&cost));
        assert!(line.contains("batch calls at 50%"), "{line}");
        assert!(markdown_line(None).contains("[targets.pricing]"));
    }
}
