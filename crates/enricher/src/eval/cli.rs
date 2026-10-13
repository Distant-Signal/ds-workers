//! `enricher eval`: the model-eval harness as a subcommand of the service's
//! own binary, so a comparison can run where only the cluster has the
//! credentials (the keyless workload identity modes need the pod's projected
//! service-account token). The `#[ignore]`d test runners stay the way to
//! score against gold labels; this entry point only *records*: it runs every
//! target over the dataset (batch by default, resuming an interrupted run)
//! and writes, per target, the raw records plus the parsed extraction of
//! each one. Scoring and comparison happen offline, on the copied-out files.
//!
//! ```text
//! enricher eval --targets /eval/targets.toml --dataset /eval/dataset.jsonl --out /eval/out/run1
//! ```
//!
//! The output directory is created if absent and *resumed* if not: a
//! target whose records file exists is reused, and a batch target with a
//! state file polls its recorded batches instead of submitting again (the
//! harness's `EVAL_RESUME` behaviour). So re-running the same command after a
//! crash never pays twice. The targets run concurrently.
//!
//! Per target (`<stem>` is the target's name, file-safe):
//!
//! - `<stem>.records.jsonl`: the harness's [`PipelineRecord`]s, one per
//!   (case, repetition): every call's raw output or error, outcome label,
//!   token usage, latency and batch id. Re-scorable with
//!   `eval::quality::replay_quality_score`.
//! - `<stem>.results.jsonl`: the same records, each with what the service
//!   would have written (category and combined periods) or why it would
//!   have written nothing, and each call's estimated cost.
//! - `<stem>.batch-state.json`: the batch run's state (batch ids, times).
//! - `<stem>.summary.json`: the target, the run's mode, batch metadata and
//!   cost estimate, and outcome counts.
//!
//! Plus `run.json`: the run's metadata (dataset, prompt fingerprint, every
//! target's status). The exit code is 0 when every target finished, 2 when
//! some target failed (the others' outputs are still written), 1 on a setup
//! error. A target whose *every* record failed in transport (nothing reached
//! the model: a broken credential, say) counts as failed, and its files are
//! renamed to `<stem>.failed-<stamp>.*` so that re-running retries it.
//!
//! The `EVAL_*` knobs of batch mode apply as documented
//! (docs/enricher-model-eval.md): `EVAL_MODE`, `EVAL_BATCH_POLL_SECS`,
//! `EVAL_BATCH_MAX_REQUESTS`, `EVAL_BATCH_RETRY_SYNC`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use clap::Parser;
use serde::Serialize;

use crate::eval::cost::{self, CostEstimate};
use crate::eval::dataset::{self, Case};
use crate::eval::pipeline::{CallRecord, Extraction, Failure, FailureKind, PipelineRecord};
use crate::eval::report::{RunInfo, TargetInfo};
use crate::eval::target::{self, Target};
use crate::llm::{ExtractionPeriod, TokenUsage};

/// `enricher eval`: run eval targets over a dataset and record the outputs.
#[derive(Debug, Parser)]
#[command(name = "enricher eval", bin_name = "enricher eval", version)]
struct Args {
    /// The targets file (TOML; format in crates/enricher/eval/targets.example.toml).
    #[arg(long, env = "EVAL_TARGETS")]
    targets: PathBuf,
    /// The dataset (JSONL of eval cases; `expected` labels are not needed).
    #[arg(long, env = "EVAL_DATASET")]
    dataset: PathBuf,
    /// The output directory: created if absent, resumed if present.
    #[arg(long, env = "EVAL_OUT")]
    out: PathBuf,
    /// Comma-separated target names to run (default: every target).
    #[arg(long, env = "EVAL_TARGET", value_delimiter = ',')]
    target: Vec<String>,
    /// Comma-separated case ids to run (default: every case).
    #[arg(long, env = "EVAL_CASES", value_delimiter = ',')]
    cases: Vec<String>,
    /// Repetitions per case.
    #[arg(long, env = "EVAL_REPEATS", default_value_t = 1)]
    repeats: u32,
    /// Sync mode only: documents in flight at once.
    #[arg(long, env = "EVAL_CONCURRENCY", default_value_t = 1)]
    concurrency: usize,
    /// Free text saved in `run.json` (e.g. the image tag or the Job name).
    #[arg(long, env = "EVAL_RUN_LABEL")]
    label: Option<String>,
}

/// The subcommand's entry point (`main.rs` dispatches here on
/// `enricher eval`).
pub(crate) async fn main() -> ExitCode {
    common::logging::init("enricher-eval");
    // `skip(1)`: the binary's path; "eval" stands in as the command name.
    let args = Args::parse_from(std::env::args_os().skip(1));
    match run(&args).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(2),
        Err(err) => {
            eprintln!("enricher eval: {err:#}");
            ExitCode::FAILURE
        }
    }
}

/// `run.json`.
#[derive(Debug, Serialize)]
struct RunMeta {
    tool: &'static str,
    enricher_version: &'static str,
    label: Option<String>,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    dataset: DatasetMeta,
    repetitions: u32,
    /// The built-in prompts' fingerprint (a target with `prompts_dir` has
    /// its own, on its records).
    builtin_prompt_fingerprint: String,
    targets: Vec<TargetStatus>,
}

#[derive(Debug, Serialize)]
struct DatasetMeta {
    path: String,
    cases: usize,
    /// A short hash of the file's bytes.
    content_hash: String,
}

#[derive(Debug, Clone, Serialize)]
struct TargetStatus {
    name: String,
    model: String,
    /// `pending`, `ok` or `error`.
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    counts: Option<Counts>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wall_ms: Option<u64>,
}

/// Outcome counts over a target's records.
#[derive(Debug, Clone, Default, Serialize)]
struct Counts {
    records: usize,
    /// The service would have written an extraction.
    completed: usize,
    /// A call failed (timeout, HTTP error, batch errored/expired...).
    transport_failures: usize,
    /// The model answered with something the service rejects.
    invalid_outputs: usize,
    /// Per pass, outcome label to count.
    call_outcomes: BTreeMap<String, BTreeMap<String, usize>>,
}

impl Counts {
    fn of(records: &[PipelineRecord]) -> Self {
        let mut counts = Self {
            records: records.len(),
            ..Self::default()
        };
        for record in records {
            match record.outcome() {
                Ok(_) => counts.completed += 1,
                Err(failure) if failure.kind == FailureKind::Transport => {
                    counts.transport_failures += 1;
                }
                Err(_) => counts.invalid_outputs += 1,
            }
            for call in &record.calls {
                *counts
                    .call_outcomes
                    .entry(call.pass.label().to_string())
                    .or_default()
                    .entry(call.outcome.clone())
                    .or_default() += 1;
            }
        }
        counts
    }
}

/// `<stem>.summary.json`.
#[derive(Debug, Serialize)]
struct TargetSummary<'a> {
    target: TargetInfo,
    /// The whole target as configured (no secrets: keys are named by env
    /// var, workload identity settings are ids).
    config: &'a Target,
    run: &'a RunInfo,
    wall_ms: u64,
    counts: Counts,
}

/// One `<stem>.results.jsonl` line.
#[derive(Debug, Serialize)]
struct ResultLine<'a> {
    target: &'a str,
    model: &'a str,
    provider: &'static str,
    case_id: &'a str,
    repetition: u32,
    input_hash: &'a str,
    prompt_fingerprint: &'a str,
    /// `completed` or `failed`.
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    extraction: Option<ExtractionOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<Failure>,
    /// The record's wall time (in batch mode: the batches' turnarounds).
    elapsed_ms: u64,
    /// Every call made, in order: primary, then the adversarial passes.
    calls: Vec<CallOut<'a>>,
    /// The record's estimated cost (`None` without a price).
    estimated_usd: Option<f64>,
}

/// What the service would have written.
#[derive(Debug, Serialize)]
struct ExtractionOut {
    category: String,
    periods: Vec<ExtractionPeriod>,
    dropped_period_count: usize,
}

impl From<Extraction> for ExtractionOut {
    fn from(extraction: Extraction) -> Self {
        Self {
            category: extraction.category,
            periods: extraction.periods,
            dropped_period_count: extraction.dropped_period_count,
        }
    }
}

#[derive(Debug, Serialize)]
struct CallOut<'a> {
    pass: &'static str,
    outcome: &'a str,
    elapsed_ms: u64,
    retries: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_id: Option<&'a str>,
    /// The raw output, as the provider returned it.
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<&'a TokenUsage>,
    estimated_usd: Option<f64>,
}

fn call_out<'a>(call: &'a CallRecord, pricing: Option<&cost::Pricing>) -> CallOut<'a> {
    CallOut {
        pass: call.pass.label(),
        outcome: &call.outcome,
        elapsed_ms: call.elapsed_ms,
        retries: call.retries,
        batch_id: call.batch_id.as_deref(),
        content: call.content.as_deref(),
        error: call.error.as_deref(),
        usage: call.usage.as_ref(),
        estimated_usd: pricing.zip(call.usage.as_ref()).map(|(pricing, usage)| {
            let mut totals = cost::UsageTotals::default();
            totals.add(usage);
            let usd = totals.usd(pricing);
            if call.batch_id.is_some() {
                usd * cost::BATCH_DISCOUNT
            } else {
                usd
            }
        }),
    }
}

fn result_line<'a>(
    target: &'a Target,
    record: &'a PipelineRecord,
    pricing: Option<&cost::Pricing>,
) -> ResultLine<'a> {
    let (status, extraction, failure) = match record.outcome() {
        Ok(extraction) => ("completed", Some(extraction.into()), None),
        Err(failure) => ("failed", None, Some(failure)),
    };
    let calls: Vec<CallOut<'a>> = record
        .calls
        .iter()
        .map(|call| call_out(call, pricing))
        .collect();
    let estimated_usd = pricing.map(|_| calls.iter().filter_map(|c| c.estimated_usd).sum());
    ResultLine {
        target: &record.label.target,
        model: &record.label.model,
        provider: match target.provider {
            crate::llm::ProviderKind::Openai => "openai",
            crate::llm::ProviderKind::Anthropic => "anthropic",
        },
        case_id: &record.case_id,
        repetition: record.repetition,
        input_hash: &record.input_hash,
        prompt_fingerprint: &record.prompt_fingerprint,
        status,
        extraction,
        failure,
        elapsed_ms: record.elapsed_ms,
        calls,
        estimated_usd,
    }
}

fn write_results(
    path: &Path,
    target: &Target,
    records: &[PipelineRecord],
    cost: Option<&CostEstimate>,
) -> anyhow::Result<()> {
    let pricing = cost.map(|c| c.pricing);
    let mut out = String::new();
    for record in records {
        out.push_str(&serde_json::to_string(&result_line(
            target,
            record,
            pricing.as_ref(),
        ))?);
        out.push('\n');
    }
    crate::eval::write_text(path, &out)
}

/// The first record's failure when *every* record failed in transport:
/// nothing reached the model (a broken credential, a missing batch
/// permission, an expired batch), so the target failed as a whole.
fn all_transport_failed(records: &[PipelineRecord]) -> Option<Failure> {
    let mut first = None;
    for record in records {
        match record.outcome() {
            Err(failure) if failure.kind == FailureKind::Transport => {
                first.get_or_insert(failure);
            }
            _ => return None,
        }
    }
    first
}

/// Renames a failed target's files to `<stem>.failed-<stamp>.*`, so a
/// re-run of the same command (which resumes the directory) retries the
/// target instead of reusing its failed records. Returns the new stem.
fn set_aside(dir: &Path, stem: &str) -> anyhow::Result<String> {
    let moved = format!("{stem}.failed-{}", Utc::now().format("%Y%m%dT%H%M%S%.3fZ"));
    for suffix in [
        "records.jsonl",
        "results.jsonl",
        "summary.json",
        "batch-state.json",
    ] {
        let from = dir.join(format!("{stem}.{suffix}"));
        if from.exists() {
            let to = dir.join(format!("{moved}.{suffix}"));
            std::fs::rename(&from, &to)
                .map_err(|err| anyhow::anyhow!("moving {}: {err}", from.display()))?;
        }
    }
    Ok(moved)
}

/// One target, start to finish; its status for `run.json`.
async fn run_target(
    target: &Target,
    cases: &Arc<[Case]>,
    args: &Args,
    dir: &Path,
) -> anyhow::Result<TargetStatus> {
    eprintln!(
        "{}: {} case(s) x {} in {} mode",
        target.name,
        cases.len(),
        args.repeats,
        target.mode()?.label()
    );
    let (records, run, wall_ms) = crate::eval::run_records(
        target,
        target.quality_timeout_secs,
        cases,
        args.repeats,
        args.concurrency,
        0,
        dir,
    )
    .await?;
    let stem = target.file_stem();
    write_results(
        &dir.join(format!("{stem}.results.jsonl")),
        target,
        &records,
        run.cost.as_ref(),
    )?;
    let counts = Counts::of(&records);
    crate::eval::write_json(
        &dir.join(format!("{stem}.summary.json")),
        &TargetSummary {
            target: TargetInfo::from_target(target),
            config: target,
            run: &run,
            wall_ms,
            counts: counts.clone(),
        },
    )?;
    eprintln!(
        "{}: done: {} record(s), {} completed, {} transport failure(s), {} invalid output(s){}",
        target.name,
        counts.records,
        counts.completed,
        counts.transport_failures,
        counts.invalid_outputs,
        run.cost
            .as_ref()
            .map(|c| format!(", estimated ${:.4}", c.usd))
            .unwrap_or_default()
    );
    if let Some(failure) = all_transport_failed(&records) {
        let moved = set_aside(dir, &stem)?;
        anyhow::bail!(
            "every record failed in transport (first: {}: {}); its files were moved to {moved}.* \
             so that re-running the same command retries this target",
            failure.outcome,
            failure.message
        );
    }
    Ok(TargetStatus {
        name: target.name.clone(),
        model: target.model.clone(),
        status: "ok",
        error: None,
        counts: Some(counts),
        estimated_usd: run.cost.map(|c| c.usd),
        wall_ms: Some(wall_ms),
    })
}

/// Runs every target; `Ok(false)` when some target failed.
async fn run(args: &Args) -> anyhow::Result<bool> {
    let started_at = Utc::now();
    let raw_targets = std::fs::read_to_string(&args.targets)
        .map_err(|err| anyhow::anyhow!("reading {}: {err}", args.targets.display()))?;
    let only = (!args.target.is_empty()).then_some(args.target.as_slice());
    let targets = target::parse_targets(&raw_targets, only)
        .map_err(|err| anyhow::anyhow!("{}: {err}", args.targets.display()))?;
    if targets.is_empty() {
        anyhow::bail!("{} defines no targets", args.targets.display());
    }
    let raw_dataset = std::fs::read_to_string(&args.dataset)
        .map_err(|err| anyhow::anyhow!("reading {}: {err}", args.dataset.display()))?;
    let cases = dataset::parse(&raw_dataset)
        .map_err(|err| anyhow::anyhow!("dataset {}: {err}", args.dataset.display()))?;
    let only_cases = (!args.cases.is_empty()).then_some(args.cases.as_slice());
    let cases: Arc<[Case]> = dataset::filter(cases, only_cases)?.into();
    if cases.is_empty() {
        anyhow::bail!("{} has no cases", args.dataset.display());
    }
    if args.repeats == 0 {
        anyhow::bail!("--repeats must be at least 1");
    }
    std::fs::create_dir_all(&args.out)
        .map_err(|err| anyhow::anyhow!("creating {}: {err}", args.out.display()))?;
    let mut content_hash = common::text_hash::text_hash(&raw_dataset, "");
    content_hash.truncate(16);
    let mut meta = RunMeta {
        tool: "enricher eval",
        enricher_version: env!("CARGO_PKG_VERSION"),
        label: args.label.clone(),
        started_at,
        finished_at: None,
        dataset: DatasetMeta {
            path: args.dataset.display().to_string(),
            cases: cases.len(),
            content_hash,
        },
        repetitions: args.repeats,
        builtin_prompt_fingerprint: crate::llm::prompt_fingerprint(),
        targets: targets
            .iter()
            .map(|t| TargetStatus {
                name: t.name.clone(),
                model: t.model.clone(),
                status: "pending",
                error: None,
                counts: None,
                estimated_usd: None,
                wall_ms: None,
            })
            .collect(),
    };
    let meta_path = args.out.join("run.json");
    crate::eval::write_json(&meta_path, &meta)?;

    let runs = targets
        .iter()
        .map(|target| run_target(target, &cases, args, &args.out));
    let results = futures_util::future::join_all(runs).await;
    let mut all_ok = true;
    for (status, (target, result)) in meta.targets.iter_mut().zip(targets.iter().zip(results)) {
        match result {
            Ok(done) => *status = done,
            Err(err) => {
                all_ok = false;
                eprintln!("{}: failed: {err:#}", target.name);
                status.status = "error";
                status.error = Some(format!("{err:#}"));
            }
        }
    }
    meta.finished_at = Some(Utc::now());
    crate::eval::write_json(&meta_path, &meta)?;
    Ok(all_ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::pipeline::{FakeBackend, TargetLabel, run_pipeline};

    fn case(id: &str) -> Case {
        Case::unlabelled(
            id.to_string(),
            "Signal failure".to_string(),
            "Delays".to_string(),
            "2026-04-01T00:00:00Z".parse().unwrap(),
        )
    }

    fn target() -> Target {
        target::parse_targets(
            "[[targets]]\nname = \"haiku\"\nprovider = \"anthropic\"\n\
             base_url = \"https://api.anthropic.com/v1\"\nmodel = \"claude-haiku-5-5\"\n\
             auth = \"anthropic-wif-authentik\"\n",
            None,
        )
        .unwrap()
        .remove(0)
    }

    #[test]
    fn args_parse_from_the_subcommand_position() {
        let args = Args::try_parse_from([
            "eval",
            "--targets",
            "/t.toml",
            "--dataset",
            "/d.jsonl",
            "--out",
            "/o",
            "--target",
            "a,b",
        ])
        .unwrap();
        assert_eq!(args.target, ["a", "b"]);
        assert_eq!(args.repeats, 1);
        assert!(args.cases.is_empty());
    }

    /// A result line carries the parsed extraction, every call's raw output
    /// and usage, and a per-call cost at the batch price for a batch call.
    #[tokio::test]
    async fn result_lines_carry_the_extraction_raw_outputs_and_cost() {
        let primary = serde_json::json!({
            "category": "signal_failure",
            "periods": [{
                "scope_description": null, "date_range": null, "schedule_window": null,
                "resolution_status": "ongoing", "apparent_severity": "moderate_disruption",
                "impact_type": null
            }]
        });
        let backend = FakeBackend::default().agreeing("a", &primary);
        let label = TargetLabel {
            target: "haiku".to_string(),
            model: "claude-haiku-5-5".to_string(),
        };
        let mut record = run_pipeline(&backend, &label, &case("a"), 0).await;
        for call in &mut record.calls {
            call.batch_id = Some("msgbatch_1".to_string());
            call.usage = Some(TokenUsage {
                prompt_tokens: Some(1_000_000),
                completion_tokens: Some(0),
                reasoning_tokens: None,
                cached_tokens: Some(0),
                cache_write_tokens: Some(0),
            });
        }
        let target = target();
        let pricing = target.pricing().unwrap().0;
        let line = serde_json::to_value(result_line(&target, &record, Some(&pricing))).unwrap();
        assert_eq!(line["status"], "completed");
        assert_eq!(line["provider"], "anthropic");
        assert_eq!(line["extraction"]["category"], "signal_failure");
        assert_eq!(
            line["extraction"]["periods"][0]["severity_confidence"],
            "high"
        );
        assert_eq!(line["calls"].as_array().unwrap().len(), 3);
        assert_eq!(line["calls"][0]["content"], primary.to_string());
        assert_eq!(line["calls"][0]["batch_id"], "msgbatch_1");
        // 1M uncached input tokens at $0.10, half price in a batch.
        let usd = line["calls"][0]["estimated_usd"].as_f64().unwrap();
        assert!((usd - 0.05).abs() < 1e-9, "{usd}");
        let total = line["estimated_usd"].as_f64().unwrap();
        assert!((total - 0.15).abs() < 1e-9, "{total}");

        let mut failed = PipelineRecord {
            calls: record.calls[..1].to_vec(),
            ..record.clone()
        };
        failed.calls[0].content = None;
        failed.calls[0].outcome = "batch_expired".to_string();
        let line = serde_json::to_value(result_line(&target, &failed, None)).unwrap();
        assert_eq!(line["status"], "failed");
        assert_eq!(line["failure"]["kind"], "transport");
        assert_eq!(line["failure"]["outcome"], "batch_expired");
        assert!(line["estimated_usd"].is_null());
        assert_eq!(
            Counts::of(&[record.clone(), failed.clone()]).transport_failures,
            1
        );

        // Only a target whose every record failed in transport failed.
        assert!(all_transport_failed(&[record, failed.clone()]).is_none());
        let first = all_transport_failed(&[failed.clone(), failed]).unwrap();
        assert_eq!(first.outcome, "batch_expired");
        assert!(all_transport_failed(&[]).is_none());
    }

    #[test]
    fn a_failed_targets_files_are_set_aside_for_a_retry() {
        let dir = tempfile::tempdir().unwrap();
        for file in ["t.records.jsonl", "t.batch-state.json", "u.records.jsonl"] {
            std::fs::write(dir.path().join(file), "x").unwrap();
        }
        let moved = set_aside(dir.path(), "t").unwrap();
        assert!(moved.starts_with("t.failed-"), "{moved}");
        assert!(!dir.path().join("t.records.jsonl").exists());
        assert!(!dir.path().join("t.batch-state.json").exists());
        assert!(dir.path().join(format!("{moved}.records.jsonl")).exists());
        assert!(
            dir.path()
                .join(format!("{moved}.batch-state.json"))
                .exists()
        );
        assert!(
            dir.path().join("u.records.jsonl").exists(),
            "others untouched"
        );
    }

    /// A whole run over a sync target that can't connect: the target fails
    /// (exit status 2), the others' files and `run.json` are still written.
    #[tokio::test]
    async fn a_failing_target_is_reported_in_run_json() {
        let dir = tempfile::tempdir().unwrap();
        let targets = dir.path().join("targets.toml");
        std::fs::write(
            &targets,
            "[[targets]]\nname = \"needs-key\"\nprovider = \"anthropic\"\n\
             base_url = \"https://api.anthropic.com/v1\"\nmodel = \"claude-haiku-5-5\"\n\
             api_key_env = \"ENRICHER_EVAL_CLI_TEST_SURELY_UNSET\"\n",
        )
        .unwrap();
        let dataset = dir.path().join("dataset.jsonl");
        std::fs::write(
            &dataset,
            "{\"id\":\"a\",\"summary\":\"s\",\"description\":\"d\",\
             \"reference_date\":\"2026-04-01T00:00:00Z\",\"tags\":[\"kind:new\"]}\n",
        )
        .unwrap();
        let out = dir.path().join("out");
        let args = Args::try_parse_from([
            "eval",
            "--targets",
            targets.to_str().unwrap(),
            "--dataset",
            dataset.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--label",
            "test",
        ])
        .unwrap();
        assert!(!run(&args).await.unwrap());
        let meta: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(out.join("run.json")).unwrap()).unwrap();
        assert_eq!(meta["dataset"]["cases"], 1);
        assert_eq!(meta["label"], "test");
        assert!(meta["finished_at"].is_string());
        assert_eq!(meta["targets"][0]["status"], "error");
        let error = meta["targets"][0]["error"].as_str().unwrap();
        assert!(
            error.contains("ENRICHER_EVAL_CLI_TEST_SURELY_UNSET"),
            "{error}"
        );
    }
}
