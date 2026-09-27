//! Offline replay harness for the enricher's churn measurement
//! (docs: /home/coder/ds-review/diff-aware-enricher-research.md). Test-only,
//! every test `#[ignore]`d; reads a JSONL export of `incident_history`
//! produced by `scripts/export-incident-history-for-replay.sql` (read-only).
//! Writes nothing anywhere except stdout/stderr.
//!
//! Two modes:
//!
//! - `replay_dry_run_edit_classes` -- no LLM. Classifies every consecutive
//!   text-changing version pair with `text_delta::classify` and prints the
//!   distribution (e.g. what `CARRY_FORWARD_SEMANTIC_NOOPS` would skip).
//! - `replay_live_pairs` -- per edit class, a deterministic sample of pairs:
//!   full extraction of the OLD text (stand-in for the stored extraction),
//!   full extraction of the NEW text, and `churn::compare` between them --
//!   the offline twin of the `enricher_extraction_churn_total{edit_class}`
//!   metric. Optional: a repeat full run of the new text (noise floor).
//!
//! The research prototype's incremental ("diff-aware prompt") modes were
//! deliberately not merged: only the measurement side was approved.
//!
//! Run (the crate is bin-only):
//!
//! ```text
//! REPLAY_HISTORY_JSONL=incident-history.jsonl \
//!   cargo test -p enricher --bin enricher replay_eval:: -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Env: `REPLAY_HISTORY_JSONL` (required), `REPLAY_SINCE` (RFC 3339, only
//! pairs whose new version is at/after it), `REPLAY_SAMPLE_PER_CLASS`
//! (default 15), `REPLAY_REPEAT=1`, plus the live evals' `LLM_BASE_URL`/
//! `LLM_MODEL`/`LLM_API_KEY`/`LIVE_EVAL_TIMEOUT_SECS` and provider-policy
//! env vars (see `llm::live_client_from_env`).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::churn::{self, ChurnReport};
use crate::combine;
use crate::llm::{self, ExtractionPeriod, LlmClient, PrimaryExtraction};
use crate::text_delta::{self, EditClass};

#[derive(Debug, Deserialize)]
struct HistoryRow {
    incident_id: String,
    recorded_at: DateTime<Utc>,
    summary: String,
    description: String,
    is_planned: bool,
    #[serde(default)]
    first_seen_at: Option<DateTime<Utc>>,
}

struct Version {
    recorded_at: DateTime<Utc>,
    summary: String,
    description: String,
}

struct Incident {
    id: String,
    planned: bool,
    reference_date: DateTime<Utc>,
    /// Consecutive duplicates (metadata-only snapshots) removed, so every
    /// adjacent pair is a text change -- exactly the enricher's triggers.
    versions: Vec<Version>,
}

fn load() -> Vec<Incident> {
    let path = std::env::var("REPLAY_HISTORY_JSONL").expect("REPLAY_HISTORY_JSONL must be set");
    let raw = std::fs::read_to_string(&path).expect("read REPLAY_HISTORY_JSONL");
    parse(&raw)
}

/// Groups JSONL history rows into per-incident version lists -- split out
/// of `load` so it's testable without a file.
fn parse(raw: &str) -> Vec<Incident> {
    let mut by_id: BTreeMap<String, Vec<HistoryRow>> = BTreeMap::new();
    for line in raw.lines().filter(|l| !l.trim().is_empty()) {
        let row: HistoryRow = serde_json::from_str(line).expect("valid JSONL row");
        by_id.entry(row.incident_id.clone()).or_default().push(row);
    }
    by_id
        .into_iter()
        .map(|(id, mut rows)| {
            rows.sort_by_key(|r| r.recorded_at);
            let planned = rows.last().is_some_and(|r| r.is_planned);
            let reference_date = rows
                .iter()
                .find_map(|r| r.first_seen_at)
                .unwrap_or(rows[0].recorded_at);
            let mut versions: Vec<Version> = Vec::new();
            for r in rows {
                if versions
                    .last()
                    .is_some_and(|v| v.summary == r.summary && v.description == r.description)
                {
                    continue;
                }
                versions.push(Version {
                    recorded_at: r.recorded_at,
                    summary: r.summary,
                    description: r.description,
                });
            }
            Incident {
                id,
                planned,
                reference_date,
                versions,
            }
        })
        .collect()
}

fn since() -> Option<DateTime<Utc>> {
    std::env::var("REPLAY_SINCE")
        .ok()
        .map(|s| s.parse().expect("REPLAY_SINCE must be RFC 3339"))
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| v == "1")
}

const CLASSES: [EditClass; 6] = [
    EditClass::SemanticNoop,
    EditClass::NumericOnly,
    EditClass::Append,
    EditClass::SmallEdit,
    EditClass::PartialRewrite,
    EditClass::Rewrite,
];

struct Pair<'a> {
    incident: &'a Incident,
    old: &'a Version,
    new: &'a Version,
    class: EditClass,
}

fn classified_pairs(incidents: &[Incident]) -> Vec<Pair<'_>> {
    let since = since();
    let mut out = Vec::new();
    for incident in incidents {
        for w in incident.versions.windows(2) {
            let (old, new) = (&w[0], &w[1]);
            if since.is_some_and(|s| new.recorded_at < s) {
                continue;
            }
            let class = text_delta::classify(
                &old.summary,
                &old.description,
                &new.summary,
                &new.description,
            );
            out.push(Pair {
                incident,
                old,
                new,
                class,
            });
        }
    }
    out
}

#[test]
#[ignore = "offline replay; needs REPLAY_HISTORY_JSONL, see module doc"]
fn replay_dry_run_edit_classes() {
    let incidents = load();
    let pairs = classified_pairs(&incidents);
    let total = pairs.len().max(1);
    println!("text-changing pairs: {}", pairs.len());
    println!("class\tall\tpct\tplanned\tunplanned");
    for class in CLASSES {
        let planned = pairs
            .iter()
            .filter(|p| p.class == class && p.incident.planned)
            .count();
        let unplanned = pairs
            .iter()
            .filter(|p| p.class == class && !p.incident.planned)
            .count();
        println!(
            "{}\t{}\t{:.1}%\t{planned}\t{unplanned}",
            class.label(),
            planned + unplanned,
            100.0 * (planned + unplanned) as f64 / total as f64
        );
    }
}

/// One pipeline result: the primary pass and the combined periods (what
/// production would write).
struct Extraction {
    primary: PrimaryExtraction,
    periods: Vec<ExtractionPeriod>,
}

/// The production pipeline minus the DB: primary, both adversarial passes,
/// `combine_periods` -- same order as `main.rs::process_incident`.
async fn run_pipeline(
    llm: &LlmClient,
    version: &Version,
    reference_date: DateTime<Utc>,
) -> anyhow::Result<Extraction> {
    let primary = llm
        .extract_primary(&version.summary, &version.description, reference_date)
        .await?;
    let resolution = llm
        .extract_adversarial(&version.summary, &version.description, &primary.periods)
        .await?;
    let severity = llm
        .extract_severity_adversarial(&version.summary, &version.description, &primary.periods)
        .await?;
    let periods = combine::combine_periods(&primary.periods, &resolution, &severity)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Extraction { primary, periods })
}

fn compare(a: &Extraction, b: &Extraction) -> ChurnReport {
    churn::compare(
        Some(&a.primary.category),
        &a.periods,
        &b.primary.category,
        &b.periods,
    )
}

fn fields(report: &ChurnReport) -> String {
    report
        .changed
        .iter()
        .map(|f| f.label())
        .collect::<Vec<_>>()
        .join(",")
}

/// Per-class tally of how often a comparison changed anything, and which
/// fields.
#[derive(Default)]
struct Tally {
    n: usize,
    changed: usize,
    per_field: BTreeMap<&'static str, usize>,
}

impl Tally {
    fn add(&mut self, report: &ChurnReport) {
        self.n += 1;
        if !report.changed.is_empty() {
            self.changed += 1;
        }
        for f in &report.changed {
            *self.per_field.entry(f.label()).or_default() += 1;
        }
    }
}

fn print_tallies(title: &str, tallies: &BTreeMap<&'static str, Tally>) {
    println!("\n== {title} ==\nclass\tn\tchanged\tfields");
    for (class, t) in tallies {
        println!(
            "{class}\t{}\t{:.0}%\t{:?}",
            t.n,
            100.0 * t.changed as f64 / t.n.max(1) as f64,
            t.per_field
        );
    }
}

#[tokio::test]
#[ignore = "needs REPLAY_HISTORY_JSONL and a real LLM_BASE_URL (self-hosted); see module doc"]
async fn replay_live_pairs() {
    let llm = llm::live_client_from_env();
    let incidents = load();
    let mut pairs = classified_pairs(&incidents);
    // Deterministic, spread-out sample: order by a hash of (incident,
    // timestamp) rather than taking the first N (which would cluster on a
    // few incidents).
    pairs.sort_by_key(|p| {
        common::text_hash::text_hash(&p.incident.id, &p.new.recorded_at.to_rfc3339())
    });
    let per_class = env_usize("REPLAY_SAMPLE_PER_CLASS", 15);
    let repeat = env_flag("REPLAY_REPEAT");

    let mut full_vs_old: BTreeMap<&'static str, Tally> = BTreeMap::new();
    let mut noise: BTreeMap<&'static str, Tally> = BTreeMap::new();
    for class in CLASSES {
        for pair in pairs.iter().filter(|p| p.class == class).take(per_class) {
            let label = class.label();
            let reference = pair.incident.reference_date;
            let run = async {
                let old = run_pipeline(&llm, pair.old, reference).await?;
                let new = run_pipeline(&llm, pair.new, reference).await?;
                let r = compare(&old, &new);
                eprintln!(
                    "PAIR class={label} incident={} planned={} full_vs_old=[{}]",
                    &pair.incident.id[..8.min(pair.incident.id.len())],
                    pair.incident.planned,
                    fields(&r)
                );
                full_vs_old.entry(label).or_default().add(&r);
                if repeat {
                    let again = run_pipeline(&llm, pair.new, reference).await?;
                    let r = compare(&new, &again);
                    eprintln!("  noise=[{}]", fields(&r));
                    noise.entry(label).or_default().add(&r);
                }
                anyhow::Ok(())
            };
            if let Err(err) = run.await {
                eprintln!("PAIR class={label} FAILED: {err}");
            }
        }
    }
    print_tallies(
        "full(new) vs full(old): what a text-change re-run changes; for semantic_noop this is exactly what carry-forward would NOT change",
        &full_vs_old,
    );
    if repeat {
        print_tallies("full(new) vs full(new) again: noise floor", &noise);
    }
}

/// Not ignored: exercises the JSONL grouping and pair classification the
/// ignored replays depend on, with no file or LLM.
#[test]
fn parse_drops_metadata_only_snapshots_and_classifies_text_changes() {
    let raw = [
        r#"{"incident_id":"A","recorded_at":"2026-09-01T10:00:00Z","summary":"s","description":"<p>Lines closed.</p>","is_planned":false,"first_seen_at":"2026-09-01T09:00:00Z"}"#,
        // Metadata-only snapshot (same text): not a version.
        r#"{"incident_id":"A","recorded_at":"2026-09-01T10:05:00Z","summary":"s","description":"<p>Lines closed.</p>","is_planned":false,"first_seen_at":null}"#,
        r#"{"incident_id":"A","recorded_at":"2026-09-01T10:10:00Z","summary":"s","description":"Lines closed","is_planned":false,"first_seen_at":null}"#,
        r#"{"incident_id":"A","recorded_at":"2026-09-01T10:20:00Z","summary":"s","description":"Lines closed. Buses replace trains.","is_planned":false}"#,
        "",
    ]
    .join("\n");
    let incidents = parse(&raw);
    assert_eq!(incidents.len(), 1);
    assert_eq!(incidents[0].versions.len(), 3);
    assert_eq!(
        incidents[0].reference_date,
        "2026-09-01T09:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    let classes: Vec<EditClass> = classified_pairs(&incidents)
        .iter()
        .map(|p| p.class)
        .collect();
    assert_eq!(classes, [EditClass::SemanticNoop, EditClass::Append]);
}
