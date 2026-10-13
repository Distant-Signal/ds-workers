//! Shared report pieces: the target header and Markdown/number formatting.

use std::fmt::Write as _;

use serde::Serialize;

use crate::eval::batch::BatchRunMeta;
use crate::eval::cost::CostEstimate;
use crate::eval::pipeline::PipelineRecord;
use crate::eval::target::Target;

/// How a run's records were produced, and what they cost.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct RunInfo {
    /// `batch` or `sync` (a re-score: `batch` if any call ran in a batch).
    pub mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<BatchRunMeta>,
    pub cost: Option<CostEstimate>,
}

impl Default for RunInfo {
    fn default() -> Self {
        Self {
            mode: "sync",
            batch: None,
            cost: None,
        }
    }
}

impl RunInfo {
    /// `batch` if any of `records`' calls ran in a provider batch.
    pub(crate) fn mode_of(records: &[PipelineRecord]) -> &'static str {
        if records
            .iter()
            .flat_map(|r| &r.calls)
            .any(|c| c.batch_id.is_some())
        {
            "batch"
        } else {
            "sync"
        }
    }

    /// The mode and cost bullet lines.
    pub(crate) fn lines(&self, md: &mut String) {
        let _ = writeln!(md, "- Mode: {}", self.mode);
        md.push_str(&crate::eval::cost::markdown_line(self.cost.as_ref()));
    }

    /// The "Batch" section, in batch mode.
    pub(crate) fn batch_section(&self, md: &mut String) {
        if let Some(batch) = &self.batch {
            md.push_str(&batch.markdown());
        }
    }
}

/// Who/where a report is about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TargetInfo {
    pub name: String,
    pub model: String,
    /// Unknown when re-scoring saved records.
    pub environment: Option<String>,
    pub base_url: Option<String>,
}

impl TargetInfo {
    pub(crate) fn from_target(target: &Target) -> Self {
        Self {
            name: target.name.clone(),
            model: target.model.clone(),
            environment: target.environment.clone(),
            base_url: Some(target.base_url.clone()),
        }
    }
}

/// `n / d`, or `None` when `d` is 0.
#[expect(clippy::cast_precision_loss, reason = "eval counts are far below 2^52")]
pub(crate) fn ratio(n: usize, d: usize) -> Option<f64> {
    (d > 0).then(|| n as f64 / d as f64)
}

/// [`ratio`] for `u64` (millisecond totals).
#[expect(
    clippy::cast_precision_loss,
    reason = "millisecond totals are far below 2^52"
)]
pub(crate) fn ratio_u64(n: u64, d: u64) -> Option<f64> {
    (d > 0).then(|| n as f64 / d as f64)
}

pub(crate) fn pct(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |v| format!("{:.1}%", v * 100.0))
}

pub(crate) fn num(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |v| format!("{v:.2}"))
}

/// Milliseconds as seconds, e.g. `12.35 s`.
pub(crate) fn secs(ms: Option<u64>) -> String {
    ms.and_then(|ms| ratio_u64(ms, 1000))
        .map_or_else(|| "-".to_string(), |s| format!("{s:.2} s"))
}

/// Makes text safe inside a Markdown table cell.
pub(crate) fn escape(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

/// A GitHub-flavoured Markdown table.
pub(crate) fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut out = format!("| {} |\n", headers.join(" | "));
    let _ = writeln!(out, "|{}", " --- |".repeat(headers.len()));
    for row in rows {
        let _ = writeln!(out, "| {} |", row.join(" | "));
    }
    out
}

/// The bullet lines naming the target, shared by both report kinds.
pub(crate) fn target_lines(md: &mut String, target: &TargetInfo) {
    let _ = writeln!(md, "- Model: `{}`", target.model);
    if let Some(url) = &target.base_url {
        let _ = writeln!(md, "- Endpoint: `{url}`");
    }
    let _ = writeln!(
        md,
        "- Environment: {}",
        target.environment.as_deref().unwrap_or("(not described)")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formatting() {
        assert_eq!(pct(Some(0.5)), "50.0%");
        assert_eq!(pct(None), "-");
        assert_eq!(secs(Some(12_345)), "12.35 s");
        assert_eq!(ratio(1, 0), None);
        assert_eq!(ratio_u64(1, 0), None);
        assert_eq!(escape("a|b\nc"), "a\\|b c");
        assert_eq!(
            table(&["A", "B"], &[vec!["1".into(), "2".into()]]),
            "| A | B |\n| --- | --- |\n| 1 | 2 |\n"
        );
    }
}
