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
//!
//! `replay_live_pairs` sends every text it needs (old, new, and the repeat)
//! through the provider's batch API by default, like the model eval
//! (`eval::batch`; docs/enricher-model-eval.md, "Batch mode"):
//! `REPLAY_MODE=sync` runs one synchronous pipeline at a time instead. The
//! batch run's state is `REPLAY_BATCH_STATE` (default
//! `target/enricher-eval/replay/batch-state.json`), so an interrupted
//! replay re-run with the same sample resumes; it is deleted once the
//! replay has finished. `EVAL_BATCH_POLL_SECS`, `EVAL_BATCH_MAX_REQUESTS`
//! and `EVAL_BATCH_RETRY_SYNC` apply.

#![expect(
    clippy::cast_precision_loss,
    reason = "test code: casts of small known test values"
)]

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::churn::{self, ChurnReport};
use crate::eval::batch::{BatchOptions, run_batch};
use crate::eval::dataset::Case;
use crate::eval::pipeline::{Extraction, TargetLabel};
use crate::eval::target::EvalMode;
use crate::llm::{self, LlmClient};
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
#[allow(
    clippy::similar_names,
    reason = "only rustc 1.88's clippy flags these names, so #[expect] can't be used"
)]
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

/// What production would write for one text: the combined extraction
/// (`eval::pipeline::PipelineRecord::outcome`, the service's parsers and
/// `combine_periods`), or why there is none.
type Outcome = Result<Extraction, String>;

fn compare(a: &Extraction, b: &Extraction) -> ChurnReport {
    churn::compare(Some(&a.category), &a.periods, &b.category, &b.periods)
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

/// Runs every text through the pipeline, batched (the default) or
/// synchronously (`REPLAY_MODE=sync`), and returns each case id's outcome.
async fn extract_all(llm: LlmClient, cases: Vec<Case>) -> BTreeMap<String, Outcome> {
    let mode = match std::env::var("REPLAY_MODE") {
        Ok(raw) if !raw.trim().is_empty() => raw
            .parse::<EvalMode>()
            .unwrap_or_else(|err| panic!("REPLAY_MODE: {err}")),
        _ => EvalMode::default(),
    };
    let label = TargetLabel {
        target: "replay".to_string(),
        model: llm.model().to_string(),
    };
    let records = match mode {
        EvalMode::Sync => {
            let cases: std::sync::Arc<[Case]> = cases.into();
            crate::eval::pipeline::run_jobs(std::sync::Arc::new(llm), &label, &cases, 1, 1).await
        }
        EvalMode::Batch => {
            let state = std::env::var("REPLAY_BATCH_STATE").map_or_else(
                |_| crate::eval::resolve("target/enricher-eval/replay/batch-state.json"),
                |p| crate::eval::resolve(&p),
            );
            if let Some(dir) = state.parent() {
                std::fs::create_dir_all(dir).expect("create the batch state's directory");
            }
            let opts = BatchOptions::from_env(state.clone());
            let run = run_batch(&llm, &label, &cases, 1, &opts)
                .await
                .unwrap_or_else(|err| panic!("batch replay failed: {err:#}"));
            eprintln!("{}", run.meta.markdown());
            // Finished: a later replay starts afresh.
            let _ = std::fs::remove_file(&state);
            run.records
        }
    };
    records
        .into_iter()
        .map(|record| {
            let outcome = record
                .outcome()
                .map_err(|failure| format!("{}: {}", failure.outcome, failure.message));
            (record.case_id, outcome)
        })
        .collect()
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
    let sample: Vec<&Pair<'_>> = CLASSES
        .iter()
        .flat_map(|class| pairs.iter().filter(|p| p.class == *class).take(per_class))
        .collect();

    // Every text the comparisons need: old, new and (REPLAY_REPEAT) new again.
    let mut cases = Vec::new();
    for (n, pair) in sample.iter().enumerate() {
        let reference = pair.incident.reference_date;
        let mut add = |which: &str, version: &Version| {
            cases.push(Case::unlabelled(
                format!("pair{n}-{which}"),
                version.summary.clone(),
                version.description.clone(),
                reference,
            ));
        };
        add("old", pair.old);
        add("new", pair.new);
        if repeat {
            add("again", pair.new);
        }
    }
    let outcomes = extract_all(llm, cases).await;
    let outcome = |n: usize, which: &str| -> Result<&Extraction, String> {
        outcomes
            .get(&format!("pair{n}-{which}"))
            .ok_or_else(|| "not run".to_string())
            .and_then(|o| o.as_ref().map_err(Clone::clone))
    };

    let mut full_vs_old: BTreeMap<&'static str, Tally> = BTreeMap::new();
    let mut noise: BTreeMap<&'static str, Tally> = BTreeMap::new();
    for (n, pair) in sample.iter().enumerate() {
        let label = pair.class.label();
        let (old, new) = match (outcome(n, "old"), outcome(n, "new")) {
            (Ok(old), Ok(new)) => (old, new),
            (Err(err), _) | (_, Err(err)) => {
                eprintln!("PAIR class={label} FAILED: {err}");
                continue;
            }
        };
        let r = compare(old, new);
        eprintln!(
            "PAIR class={label} incident={} planned={} full_vs_old=[{}]",
            &pair.incident.id[..8.min(pair.incident.id.len())],
            pair.incident.planned,
            fields(&r)
        );
        full_vs_old.entry(label).or_default().add(&r);
        if repeat {
            match outcome(n, "again") {
                Ok(again) => {
                    let r = compare(new, again);
                    eprintln!("  noise=[{}]", fields(&r));
                    noise.entry(label).or_default().add(&r);
                }
                Err(err) => eprintln!("  noise FAILED: {err}"),
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
