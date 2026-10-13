//! Model-comparison harness for the enricher: two separate evaluations of
//! the LLM behind `LLM_BASE_URL`/`LLM_MODEL`, sharing one model-client
//! abstraction (`pipeline::Backend`, implemented by the service's own
//! [`crate::llm::LlmClient`]) and one dataset (`dataset`). Every runner is
//! an `#[ignore]`d test, opt-in and named `live_eval_*` / `replay_*` so the
//! default run and CI's `--ignored` step (which skips those prefixes) never
//! call a model. The one part in the release binary is `cli`, the
//! `enricher eval` subcommand: it records outputs (no scoring) where only
//! the cluster has the credentials. The scorers (`quality`, `perf`) stay
//! test-only. Process, report guide and how to add cases/targets:
//! `docs/enricher-model-eval.md`.
//!
//! - **Quality** (`quality`): does the model extract the right structured
//!   data? Gold-labelled cases, generous timeouts, transport failures kept
//!   apart from wrong answers. Scores from saved raw outputs, so a run can
//!   be re-scored offline (`replay_quality_score`) without the model.
//! - **Performance** (`perf`): how does this model behave in *this*
//!   environment? Latency percentiles per call and per document,
//!   throughput, timeout/error/retry rates (per attempt as well as per
//!   call) and fit against the service's configured timeouts. Ignores
//!   answer quality.
//!
//! Both drive the real pipeline (`pipeline`): the service's prompts,
//! schemas, parsers, provider policy and `combine_periods`, in
//! `process_incident`'s order. By default (`mode = "batch"`) a run sends its
//! calls through the provider's batch API (`batch`) at half price; `sync`
//! makes one call at a time. Either way the records, and so the scoring,
//! are the same (`cost` prices them).
//!
//! Commands (the crate is bin-only, hence `--bin enricher`):
//!
//! ```text
//! # Quality, live (writes target/enricher-eval/quality/<stamp>/):
//! EVAL_TARGETS=crates/enricher/eval/targets.toml \
//!   cargo test -p enricher --bin enricher eval::quality::live_eval_quality -- --ignored --nocapture
//! # Quality, offline re-score of saved records:
//! EVAL_RECORDS=target/enricher-eval/quality/<stamp>/<target>.records.jsonl \
//!   cargo test -p enricher --bin enricher eval::quality::replay_quality_score -- --ignored --nocapture
//! # Performance (writes target/enricher-eval/perf/<stamp>/):
//! EVAL_TARGETS=crates/enricher/eval/targets.toml EVAL_PERF_CONCURRENCY=1 \
//!   cargo test -p enricher --release --bin enricher eval::perf::live_eval_perf -- --ignored --nocapture
//! ```
//!
//! Env (relative paths resolve from the workspace root):
//!
//! - Targets: `EVAL_TARGETS` (TOML file; see `eval/targets.example.toml`),
//!   `EVAL_TARGET` (comma-separated names to run). Without `EVAL_TARGETS`,
//!   one target from the service's `LLM_*` env vars (see
//!   `target::load_targets`).
//! - Both: `EVAL_DATASET` (default `crates/enricher/eval/dataset.jsonl`),
//!   `EVAL_CASES` (comma-separated case ids), `EVAL_OUT_DIR` (default
//!   `target/enricher-eval`).
//! - Quality: `EVAL_REPEATS` (1), `EVAL_CONCURRENCY` (1),
//!   `EVAL_DATE_TOLERANCE_MINS` (0), `EVAL_RECORDS` (replay only),
//!   `EVAL_ACCEPT_UNHASHED` (replay only; `1` scores records saved before
//!   records had an `input_hash`, unchecked; default skips them),
//!   `EVAL_QUALITY_TIMEOUT_SECS` (1800; the env-var target's quality
//!   request timeout, `quality_timeout_secs` in a targets file).
//! - Perf: `EVAL_PERF_REPEATS` (3), `EVAL_PERF_CONCURRENCY` (1),
//!   `EVAL_PERF_WARMUP` (1).
//! - Mode (`batch`): `EVAL_MODE` (`batch` or `sync`; overrides every
//!   target's `mode`, which defaults to `batch`), `EVAL_RESUME` (an earlier
//!   run's directory: resume its batches, reuse its finished targets),
//!   `EVAL_BATCH_POLL_SECS` (30), `EVAL_BATCH_MAX_REQUESTS` (the
//!   provider's limit), `EVAL_BATCH_RETRY_SYNC` (0).

pub(crate) mod batch;
pub(crate) mod cli;
mod cost;
pub(crate) mod dataset;
#[cfg(test)]
mod perf;
pub(crate) mod pipeline;
#[cfg(test)]
mod quality;
mod report;
pub(crate) mod target;

use std::path::{Path, PathBuf};

use std::sync::Arc;

use crate::eval::dataset::Case;
use crate::eval::pipeline::PipelineRecord;
use crate::eval::report::RunInfo;
use crate::eval::target::{EvalMode, Target};

/// The workspace root (two levels above this crate's manifest).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Resolves a user-supplied path: absolute as-is, relative from the
/// workspace root (not the test binary's working directory, which is the
/// crate directory).
pub(crate) fn resolve(path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        workspace_root().join(path)
    }
}

/// `path` relative to the workspace root when it's inside it, for reports.
pub(crate) fn display_path(path: &Path) -> String {
    let root = workspace_root().canonicalize().ok();
    let full = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    root.and_then(|root| full.strip_prefix(root).ok().map(Path::to_path_buf))
        .unwrap_or(full)
        .display()
        .to_string()
}

/// Parses env var `name`, or `default` when unset. A set but invalid value
/// panics (this is test code: fail loudly rather than run something else).
pub(crate) fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name}={raw:?} is not a valid value")),
        Err(_) => default,
    }
}

/// A boolean env var: `1`/`true` is on; unset, empty, `0` or `false` is
/// off. Anything else panics, like [`env_parse`].
pub(crate) fn env_flag(name: &str) -> bool {
    let raw = std::env::var_os(name).map(|raw| {
        raw.into_string().unwrap_or_else(|raw| {
            panic!("{name}={} is not valid UTF-8 (use 1 or 0)", raw.display())
        })
    });
    parse_flag(name, raw.as_deref())
}

fn parse_flag(name: &str, raw: Option<&str>) -> bool {
    let Some(raw) = raw else { return false };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "0" | "false" => false,
        "1" | "true" => true,
        _ => panic!("{name}={raw:?} is not a valid flag (use 1 or 0)"),
    }
}

/// A comma-separated env var as a list (`None` when unset or empty).
pub(crate) fn env_list(name: &str) -> Option<Vec<String>> {
    let items: Vec<String> = std::env::var(name)
        .ok()?
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    (!items.is_empty()).then_some(items)
}

/// Loads `EVAL_DATASET` (filtered by `EVAL_CASES`). Returns the dataset's
/// display path too, for reports.
pub(crate) fn load_cases() -> anyhow::Result<(String, Vec<Case>)> {
    let path = std::env::var("EVAL_DATASET").map_or_else(
        |_| resolve("crates/enricher/eval/dataset.jsonl"),
        |p| resolve(&p),
    );
    let cases = dataset::load(&path)?;
    let cases = dataset::filter(cases, env_list("EVAL_CASES").as_deref())?;
    Ok((display_path(&path), cases))
}

/// A fresh `<EVAL_OUT_DIR>/<kind>/<UTC stamp><suffix>/` directory.
pub(crate) fn run_dir(kind: &str, suffix: &str) -> anyhow::Result<PathBuf> {
    let root = std::env::var("EVAL_OUT_DIR")
        .map_or_else(|_| resolve("target/enricher-eval"), |p| resolve(&p));
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let dir = root.join(kind).join(format!("{stamp}{suffix}"));
    std::fs::create_dir_all(&dir)
        .map_err(|err| anyhow::anyhow!("creating {}: {err}", dir.display()))?;
    Ok(dir)
}

/// A live run's directory: `EVAL_RESUME` (an earlier run's directory, to
/// resume its batch runs and reuse its finished targets' records) when
/// set, else a fresh [`run_dir`].
pub(crate) fn live_run_dir(kind: &str) -> anyhow::Result<PathBuf> {
    match std::env::var("EVAL_RESUME") {
        Ok(dir) if !dir.trim().is_empty() => {
            let dir = resolve(dir.trim());
            if !dir.is_dir() {
                anyhow::bail!("EVAL_RESUME={} is not a directory", dir.display());
            }
            Ok(dir)
        }
        _ => run_dir(kind, ""),
    }
}

pub(crate) fn write_text(path: &Path, contents: &str) -> anyhow::Result<()> {
    std::fs::write(path, contents)
        .map_err(|err| anyhow::anyhow!("writing {}: {err}", path.display()))
}

pub(crate) fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    write_text(path, &(serde_json::to_string_pretty(value)? + "\n"))
}

/// Saves records as JSONL, one [`PipelineRecord`] per line.
pub(crate) fn write_records(path: &Path, records: &[PipelineRecord]) -> anyhow::Result<()> {
    let mut out = String::new();
    for record in records {
        out.push_str(&serde_json::to_string(record)?);
        out.push('\n');
    }
    write_text(path, &out)
}

/// One target's records for a live run, in its mode (`Target::mode`),
/// saved as `<dir>/<stem>.records.jsonl`: synchronously at `concurrency`
/// after `warmup` untimed runs of the first case, or through the batch API
/// (`batch::run_batch`, resuming `<stem>.batch-state.json` if present).
/// When the records file already exists (a resumed run, `EVAL_RESUME`), it
/// is reused. Returns the records, the run's info (mode, batch metadata,
/// cost) and its wall time (a batch run's: first submission to last end).
pub(crate) async fn run_records(
    target: &Target,
    timeout_secs: u64,
    cases: &Arc<[Case]>,
    repetitions: u32,
    concurrency: usize,
    warmup: u32,
    dir: &Path,
) -> anyhow::Result<(Vec<PipelineRecord>, RunInfo, u64)> {
    let mode = target.mode()?;
    let stem = target.file_stem();
    let records_path = dir.join(format!("{stem}.records.jsonl"));
    let state_path = batch::state_path(dir, &stem);
    let label = target.label();
    let reused = records_path.is_file();
    let (records, batch, wall_ms) = if reused {
        eprintln!("{}: reusing {}", target.name, display_path(&records_path));
        let records = read_records(&records_path)?;
        let batch = batch::BatchState::read(&state_path)?
            .map(|state| batch::BatchRunMeta::new(&state, &records, &state_path, true));
        let wall_ms = batch.as_ref().and_then(|b| b.turnaround_ms).unwrap_or(0);
        (records, batch, wall_ms)
    } else {
        let client = Arc::new(target.client(timeout_secs)?);
        match mode {
            EvalMode::Sync => {
                for _ in 0..warmup {
                    let _ = pipeline::run_pipeline(&*client, &label, &cases[0], 0).await;
                }
                let start = tokio::time::Instant::now();
                let records =
                    pipeline::run_jobs(client, &label, cases, repetitions, concurrency).await;
                (records, None, pipeline::millis(start.elapsed()))
            }
            EvalMode::Batch => {
                let opts = batch::BatchOptions::from_env(state_path);
                let run = batch::run_batch(&client, &label, cases, repetitions, &opts).await?;
                let wall_ms = run.meta.turnaround_ms.unwrap_or(0);
                (run.records, Some(run.meta), wall_ms)
            }
        }
    };
    write_records(&records_path, &records)?;
    let run = RunInfo {
        mode: if reused {
            RunInfo::mode_of(&records)
        } else {
            mode.label()
        },
        cost: cost::estimate(&records, target.pricing()),
        batch,
    };
    Ok((records, run, wall_ms))
}

pub(crate) fn parse_records(raw: &str) -> anyhow::Result<Vec<PipelineRecord>> {
    raw.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line).map_err(|err| anyhow::anyhow!("line {}: {err}", index + 1))
        })
        .collect()
}

pub(crate) fn read_records(path: &Path) -> anyhow::Result<Vec<PipelineRecord>> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| anyhow::anyhow!("reading {}: {err}", path.display()))?;
    parse_records(&raw).map_err(|err| anyhow::anyhow!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_paths_resolve_from_the_workspace_root() {
        let dataset = resolve("crates/enricher/eval/dataset.jsonl");
        assert!(dataset.is_file(), "{}", dataset.display());
        assert_eq!(display_path(&dataset), "crates/enricher/eval/dataset.jsonl");
        assert_eq!(resolve("/abs/x"), PathBuf::from("/abs/x"));
    }

    #[test]
    fn flags_parse_strictly() {
        assert!(!parse_flag("F", None));
        for off in ["", "0", "false", " FALSE "] {
            assert!(!parse_flag("F", Some(off)), "{off:?}");
        }
        for on in ["1", "true", "True"] {
            assert!(parse_flag("F", Some(on)), "{on:?}");
        }
        let err = std::panic::catch_unwind(|| parse_flag("F", Some("yes"))).unwrap_err();
        let msg = err.downcast_ref::<String>().unwrap();
        assert_eq!(msg, r#"F="yes" is not a valid flag (use 1 or 0)"#);
    }

    #[test]
    fn bad_record_lines_name_their_line() {
        let err = parse_records("\n{oops}\n").unwrap_err();
        assert!(err.to_string().starts_with("line 2"), "{err}");
    }
}
