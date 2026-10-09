//! End-to-end enrichment latency: from when an incident's current text was
//! first observed to when its extraction was committed, as
//! `distant_signal_enricher_enrichment_latency_seconds{path, outcome}`.
//!
//! The start is `IncidentState::reference_date`: the earliest
//! `incident_history` row of the latest unbroken run with the current
//! text, falling back to `first_seen_at`. The poller writes that row in the
//! same transaction that changes the text, stamped with its `NOW()`, and
//! publishes the `incident-text-changed` stream entry only after it
//! commits, so the row is the earliest record of the text anywhere in the
//! system. It is also on every path: the sweep and Message Batches have no
//! stream entry, and a reclaimed entry can predate a later text change
//! (its id would time the old text). It is the poller's observation time,
//! so the poll interval itself (how late the poller saw a Knowledgebase
//! edit) is not included.
//!
//! Recorded once per committed extraction of a NEW text. Not recorded:
//! - a text that was already extracted (`preflight`'s unchanged skip): it
//!   was counted when it was stored, and a redelivery would count it twice;
//! - a re-run for a model/prompt version change over unchanged text: the
//!   text may be days old, and that backfill says nothing about how fast a
//!   change reaches users;
//! - a failed attempt or a discarded stale result: each retry would add a
//!   sample for one text, and the eventual success already carries the
//!   delay the failures caused (failures are counted by
//!   `enricher_llm_call_total` and the batch counters).

use std::time::Duration;

use chrono::{DateTime, Utc};

/// Bare (unprefixed) name of the histogram, shared by the bucket override
/// in `main` and [`record`] (the override matches by exact name).
pub(crate) const METRIC: &str = "enricher_enrichment_latency_seconds";

/// Seconds to about 30 hours: a synchronous extraction lands in the first
/// few buckets, a Message Batch (up to 24 h to end, then a second stage)
/// in the last ones.
pub(crate) const BUCKETS: [f64; 15] = [
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0, 14400.0, 43200.0,
    86400.0, 108_000.0,
];

/// Which loop committed the extraction: the `path` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Path {
    /// The `incident-text-changed` stream consumer.
    Stream,
    /// A reclaimed stale pending stream entry.
    Reclaim,
    /// The periodic sweep, synchronously (including a carry-forward the
    /// batch-mode sweep does before submitting).
    Sweep,
    /// The adversarial stage of a Message Batch.
    Batch,
}

impl Path {
    #[cfg(test)]
    const ALL: [Self; 4] = [Self::Stream, Self::Reclaim, Self::Sweep, Self::Batch];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Reclaim => "reclaim",
            Self::Sweep => "sweep",
            Self::Batch => "batch",
        }
    }
}

/// How the new text's extraction was committed: the `outcome` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Three LLM passes, written by `write_extraction`.
    Stored,
    /// A semantic no-op edit: the previous extraction carried forward
    /// without an LLM call (`CARRY_FORWARD_SEMANTIC_NOOPS`).
    CarriedForward,
}

impl Outcome {
    #[cfg(test)]
    const ALL: [Self; 2] = [Self::Stored, Self::CarriedForward];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Stored => "stored",
            Self::CarriedForward => "carried_forward",
        }
    }
}

/// The latency from `text_seen_at` to `committed_at`, clamped at zero: the
/// start is the database's clock and the end this pod's, so a little skew
/// can put it in the future.
pub(crate) fn latency(text_seen_at: DateTime<Utc>, committed_at: DateTime<Utc>) -> Duration {
    (committed_at - text_seen_at).to_std().unwrap_or_default()
}

/// Records one committed extraction, timed to now. `text_seen_at` is
/// `None` for a re-run over unchanged text (see the module doc), which
/// records nothing.
pub(crate) fn record(path: Path, outcome: Outcome, text_seen_at: Option<DateTime<Utc>>) {
    record_at(path, outcome, text_seen_at, Utc::now());
}

fn record_at(
    path: Path,
    outcome: Outcome,
    text_seen_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) {
    let Some(text_seen_at) = text_seen_at else {
        return;
    };
    let elapsed = latency(text_seen_at, now);
    tracing::debug!(
        path = path.label(),
        outcome = outcome.label(),
        latency_secs = elapsed.as_secs_f64(),
        "enrichment committed"
    );
    metrics::histogram!(
        common::metrics::metric_name(METRIC),
        "path" => path.label(),
        "outcome" => outcome.label()
    )
    .record(elapsed.as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
    }

    #[test]
    fn latency_is_commit_minus_first_seen_and_never_negative() {
        assert_eq!(
            latency(at("2026-10-09T10:00:00Z"), at("2026-10-09T10:02:30.5Z")),
            Duration::from_millis(150_500)
        );
        assert_eq!(
            latency(at("2026-10-09T10:00:00Z"), at("2026-10-09T09:59:59Z")),
            Duration::ZERO,
            "clock skew clamps to zero"
        );
    }

    #[test]
    fn buckets_are_ascending_and_reach_thirty_hours() {
        assert!(BUCKETS.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(BUCKETS.first(), Some(&1.0));
        assert_eq!(BUCKETS.last(), Some(&(30.0 * 3600.0)));
    }

    #[test]
    fn labels_are_the_documented_fixed_set() {
        let paths: Vec<_> = Path::ALL.iter().map(|p| p.label()).collect();
        assert_eq!(paths, ["stream", "reclaim", "sweep", "batch"]);
        let outcomes: Vec<_> = Outcome::ALL.iter().map(|o| o.label()).collect();
        assert_eq!(outcomes, ["stored", "carried_forward"]);
    }

    #[test]
    fn records_one_sample_with_its_labels_and_skips_unchanged_text() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Full(common::metrics::metric_name(METRIC)),
                &BUCKETS,
            )
            .unwrap()
            .build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let now = at("2026-10-09T12:00:00Z");

        record_at(
            Path::Batch,
            Outcome::Stored,
            Some(at("2026-10-09T06:00:00Z")),
            now,
        );
        // A model-version re-run over old text: nothing.
        record_at(Path::Sweep, Outcome::Stored, None, now);

        let rendered = handle.render();
        let name = common::metrics::metric_name(METRIC);
        for line in [
            format!("{name}_count{{path=\"batch\",outcome=\"stored\"}} 1"),
            format!("{name}_sum{{path=\"batch\",outcome=\"stored\"}} 21600"),
            format!("{name}_bucket{{path=\"batch\",outcome=\"stored\",le=\"14400\"}} 0"),
            format!("{name}_bucket{{path=\"batch\",outcome=\"stored\",le=\"43200\"}} 1"),
        ] {
            assert!(rendered.contains(&line), "missing {line} in\n{rendered}");
        }
        assert!(
            !rendered.contains("path=\"sweep\""),
            "an unchanged-text re-run records nothing:\n{rendered}"
        );
    }
}
