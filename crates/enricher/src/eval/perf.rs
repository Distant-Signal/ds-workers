//! Runtime/deployment performance: how one model behaves served from one
//! environment, under the service's real request timeout and provider
//! policy. Says nothing about answer quality: a document "completes" here
//! if the service would have written *something*. (The records it saves can
//! still be quality-scored offline, with transport failures left out of
//! the rates, but under the real, tighter timeout; use the quality eval for
//! quality.)
//!
//! Per target it reports, per pass and over all calls: outcome counts
//! (`main.rs::llm_outcome` labels), timeout/error rates, in-call retries,
//! per-attempt timeouts, end-to-end call latency (what the service's
//! histogram measures), per-attempt send latency and queue/back-off wait;
//! per document: completion, transport vs invalid-output failures, failures
//! by stage and end-to-end latency; throughput at the chosen concurrency;
//! and whether the per-attempt latencies fit the configured timeout.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use chrono::Utc;
use serde::Serialize;

use crate::eval::dataset::Case;
use crate::eval::pipeline::{self, FailureKind, Pass, PipelineRecord};
use crate::eval::report::{self, TargetInfo, pct, ratio, ratio_u64, secs};
use crate::eval::target::Target;

/// p95 above this share of the request timeout is "tight".
const TIGHT_SHARE: f64 = 0.8;

/// Nearest-rank percentile of an ascending slice (`None` when empty).
pub(crate) fn percentile(sorted: &[u64], pct: usize) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (sorted.len() * pct).div_ceil(100).clamp(1, sorted.len());
    Some(sorted[rank - 1])
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub(crate) struct LatencyStats {
    pub count: usize,
    pub min_ms: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub max_ms: u64,
    pub mean_ms: f64,
}

pub(crate) fn latency_stats(samples: &[u64]) -> Option<LatencyStats> {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let at = |p| percentile(&sorted, p);
    Some(LatencyStats {
        count: sorted.len(),
        min_ms: *sorted.first()?,
        p50_ms: at(50)?,
        p95_ms: at(95)?,
        p99_ms: at(99)?,
        max_ms: *sorted.last()?,
        mean_ms: ratio_u64(
            sorted.iter().sum(),
            u64::try_from(sorted.len()).unwrap_or(u64::MAX),
        )?,
    })
}

/// Stats over a set of calls.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct CallStats {
    pub calls: usize,
    /// Final outcome label -> count.
    pub outcomes: BTreeMap<String, usize>,
    /// Calls whose final outcome wasn't `success`.
    pub error_rate: Option<f64>,
    /// Calls whose final outcome was a client-side timeout.
    pub timeout_rate: Option<f64>,
    /// In-call retries spent in total, and how many calls needed any.
    pub retries: u64,
    pub calls_with_retries: usize,
    pub retry_rate: Option<f64>,
    /// HTTP attempts made (one per call plus one per retry).
    pub attempts: usize,
    /// Attempts that hit the client timeout, whether or not a gateway
    /// retry then recovered the call.
    pub attempt_timeouts: usize,
    /// ...of which the call still succeeded.
    pub recovered_timeouts: usize,
    /// End-to-end latency of successful calls: retries, `max_in_flight`
    /// queueing and 429 back-off included, like
    /// `enricher_llm_call_duration_seconds`.
    pub latency: Option<LatencyStats>,
    /// End-to-end latency of every call, failures included (how long a
    /// call occupies the service's loop).
    pub latency_all: Option<LatencyStats>,
    /// Send time of successful attempts (for display).
    pub send_latency: Option<LatencyStats>,
    /// Send time of every attempt that got an HTTP response: successes,
    /// empty content, HTTP errors, 429s... Each of these got its answer
    /// within the request timeout, so this is what the timeout fit judges:
    /// a slow failure is as close to the timeout as a slow success.
    /// Timeouts are left out (they're judged separately), and so are
    /// `error` attempts (connection failures and the like), which never got
    /// a response and would only drag the p95 down.
    pub response_latency: Option<LatencyStats>,
    /// Per call, time spent waiting rather than sending: `max_in_flight`
    /// queueing plus 429 back-off sleeps.
    pub wait: Option<LatencyStats>,
}

pub(crate) fn call_stats<'a>(calls: impl Iterator<Item = &'a pipeline::CallRecord>) -> CallStats {
    let mut stats = CallStats::default();
    let (mut ok, mut all, mut wait) = (Vec::new(), Vec::new(), Vec::new());
    let (mut send, mut responded) = (Vec::new(), Vec::new());
    for call in calls {
        stats.calls += 1;
        *stats.outcomes.entry(call.outcome.clone()).or_default() += 1;
        stats.retries += u64::from(call.retries);
        if call.retries > 0 {
            stats.calls_with_retries += 1;
        }
        all.push(call.elapsed_ms);
        if call.outcome == "success" {
            ok.push(call.elapsed_ms);
        }
        stats.attempts += call.attempts.len();
        let timeouts = call
            .attempts
            .iter()
            .filter(|a| a.outcome == "timeout")
            .count();
        stats.attempt_timeouts += timeouts;
        if call.outcome == "success" {
            stats.recovered_timeouts += timeouts;
        }
        for attempt in &call.attempts {
            if attempt.outcome == "success" {
                send.push(attempt.send_ms);
            }
            // `auth_error`: no token, so nothing reached the endpoint.
            if !matches!(attempt.outcome.as_str(), "timeout" | "error" | "auth_error") {
                responded.push(attempt.send_ms);
            }
        }
        if !call.attempts.is_empty() {
            wait.push(
                call.attempts
                    .iter()
                    .map(|a| a.queued_ms.saturating_add(a.backoff_ms))
                    .sum(),
            );
        }
    }
    let count = |label: &str| stats.outcomes.get(label).copied().unwrap_or(0);
    stats.error_rate = ratio(stats.calls - count("success"), stats.calls);
    stats.timeout_rate = ratio(count("timeout"), stats.calls);
    stats.retry_rate = ratio(stats.calls_with_retries, stats.calls);
    stats.latency = latency_stats(&ok);
    stats.latency_all = latency_stats(&all);
    stats.send_latency = latency_stats(&send);
    stats.response_latency = latency_stats(&responded);
    stats.wait = latency_stats(&wait);
    stats
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct DocStats {
    pub documents: usize,
    pub completed: usize,
    /// Documents stopped by a failed call (timeout, gateway/HTTP error,
    /// 429...): the environment's problem.
    pub transport_failures: usize,
    /// Answers the service would reject (empty content, bad JSON,
    /// misaligned verdicts): a quality problem, but it also costs a retry
    /// in production. Kept apart so perf doesn't blame the environment.
    pub invalid_outputs: usize,
    pub failures_by_stage: BTreeMap<&'static str, usize>,
    pub completed_rate: Option<f64>,
    /// End-to-end latency of completed documents.
    pub latency: Option<LatencyStats>,
    pub latency_all: Option<LatencyStats>,
}

pub(crate) fn doc_stats(records: &[PipelineRecord]) -> DocStats {
    let mut stats = DocStats::default();
    let (mut ok, mut all) = (Vec::new(), Vec::new());
    for record in records {
        stats.documents += 1;
        all.push(record.elapsed_ms);
        match record.outcome() {
            Ok(_) => {
                stats.completed += 1;
                ok.push(record.elapsed_ms);
            }
            Err(failure) => {
                *stats
                    .failures_by_stage
                    .entry(failure.stage.label())
                    .or_default() += 1;
                match failure.kind {
                    FailureKind::Transport => stats.transport_failures += 1,
                    FailureKind::InvalidOutput => stats.invalid_outputs += 1,
                }
            }
        }
    }
    stats.completed_rate = ratio(stats.completed, stats.documents);
    stats.latency = latency_stats(&ok);
    stats.latency_all = latency_stats(&all);
    stats
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Fit {
    /// No attempt timed out, and the send p95 of attempts that got a
    /// response (any outcome but `timeout`) is under 80% of the timeout.
    Fits,
    /// No attempt timed out, but that send p95 is at or above 80% of the
    /// timeout.
    Tight,
    /// At least one attempt hit the client timeout (even if a gateway
    /// retry then recovered the call), or a call's final outcome was a
    /// timeout.
    Exceeds,
    /// No attempts to judge by.
    NoData,
}

impl Fit {
    fn label(self) -> &'static str {
        match self {
            Fit::Fits => "fits",
            Fit::Tight => "tight",
            Fit::Exceeds => "exceeds",
            Fit::NoData => "no data",
        }
    }
}

/// How the latencies compare with the configured timeouts. The request
/// timeout bounds each HTTP attempt, so this judges per-attempt send times
/// and attempt timeouts, not end-to-end call latency (which also holds
/// retries, queueing and 429 back-off).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct TimeoutFit {
    pub request_timeout_secs: u64,
    pub verdict: Fit,
    /// Attempts that hit the client timeout, retried or not.
    pub attempt_timeouts: usize,
    /// ...of which the call still succeeded on a retry.
    pub recovered_timeouts: usize,
    /// Calls whose final outcome was a timeout.
    pub timed_out_calls: usize,
    /// Send p95 of attempts that got a response / request timeout.
    pub p95_share_of_timeout: Option<f64>,
    /// Send max of attempts that got a response / request timeout.
    pub max_share_of_timeout: Option<f64>,
    /// `RECLAIM_MIN_IDLE_SECS`, and completed-document p95 against it (a
    /// document slower than this gets reclaimed and skipped while still in
    /// flight: churn, not an error).
    pub reclaim_min_idle_secs: u64,
    pub doc_p95_share_of_reclaim_idle: Option<f64>,
}

pub(crate) fn timeout_fit(
    calls: &CallStats,
    docs: &DocStats,
    request_timeout_secs: u64,
    reclaim_min_idle_secs: u64,
) -> TimeoutFit {
    let timeout_ms = request_timeout_secs.saturating_mul(1000);
    let send = calls.response_latency;
    let p95_share = send.and_then(|l| ratio_u64(l.p95_ms, timeout_ms));
    let timed_out_calls = calls.outcomes.get("timeout").copied().unwrap_or(0);
    let verdict = match p95_share {
        // A timed-out call always has a timed-out attempt; checking both
        // also covers a record without attempts.
        _ if calls.attempt_timeouts > 0 || timed_out_calls > 0 => Fit::Exceeds,
        None => Fit::NoData,
        Some(share) if share >= TIGHT_SHARE => Fit::Tight,
        Some(_) => Fit::Fits,
    };
    TimeoutFit {
        request_timeout_secs,
        verdict,
        attempt_timeouts: calls.attempt_timeouts,
        recovered_timeouts: calls.recovered_timeouts,
        timed_out_calls,
        p95_share_of_timeout: p95_share,
        max_share_of_timeout: send.and_then(|l| ratio_u64(l.max_ms, timeout_ms)),
        reclaim_min_idle_secs,
        doc_p95_share_of_reclaim_idle: docs
            .latency
            .and_then(|l| ratio_u64(l.p95_ms, reclaim_min_idle_secs.saturating_mul(1000))),
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PerfSummary {
    pub concurrency: usize,
    pub repetitions: u32,
    pub warmup_runs: u32,
    pub wall_ms: u64,
    /// Attempted documents per minute, whatever their outcome: throughput
    /// that doesn't depend on answer quality. The headline figure.
    pub documents_per_minute: Option<f64>,
    /// Completed documents per minute (depends on output validity too).
    pub completed_documents_per_minute: Option<f64>,
    pub calls_per_minute: Option<f64>,
    pub all_calls: CallStats,
    pub per_pass: BTreeMap<&'static str, CallStats>,
    pub documents: DocStats,
    pub timeout_fit: TimeoutFit,
}

/// Run parameters echoed into the report.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PerfRun {
    pub concurrency: usize,
    pub repetitions: u32,
    pub warmup_runs: u32,
    pub wall_ms: u64,
    pub request_timeout_secs: u64,
    pub reclaim_min_idle_secs: u64,
}

pub(crate) fn summarize(records: &[PipelineRecord], run: PerfRun) -> PerfSummary {
    let all_calls = call_stats(records.iter().flat_map(|r| &r.calls));
    let per_pass = Pass::ALL
        .iter()
        .map(|&pass| {
            let calls = records
                .iter()
                .flat_map(|r| &r.calls)
                .filter(move |c| c.pass == pass);
            (pass.label(), call_stats(calls))
        })
        .collect();
    let documents = doc_stats(records);
    let per_minute = |n: usize| {
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        ratio_u64(n.saturating_mul(60_000), run.wall_ms)
    };
    let timeout_fit = timeout_fit(
        &all_calls,
        &documents,
        run.request_timeout_secs,
        run.reclaim_min_idle_secs,
    );
    PerfSummary {
        concurrency: run.concurrency,
        repetitions: run.repetitions,
        warmup_runs: run.warmup_runs,
        wall_ms: run.wall_ms,
        documents_per_minute: per_minute(documents.documents),
        completed_documents_per_minute: per_minute(documents.completed),
        calls_per_minute: per_minute(all_calls.calls),
        all_calls,
        per_pass,
        documents,
        timeout_fit,
    }
}

/// The provider-policy knobs that shape latency, echoed into the report.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PolicyInfo {
    pub request_timeout_secs: u64,
    pub max_tokens: Option<u32>,
    pub reasoning_effort: Option<String>,
    pub max_in_flight: Option<usize>,
    pub rate_limit_retries: u32,
    pub rate_limit_retry_secs: u64,
    pub gateway_retries: u32,
}

impl PolicyInfo {
    fn from_target(target: &Target) -> Self {
        Self {
            request_timeout_secs: target.request_timeout_secs,
            max_tokens: target.max_tokens,
            reasoning_effort: target.reasoning_effort.clone(),
            max_in_flight: target.max_in_flight,
            rate_limit_retries: target.rate_limit_retries,
            rate_limit_retry_secs: target.rate_limit_retry_secs,
            gateway_retries: target.gateway_retries,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PerfReport {
    pub kind: &'static str,
    pub generated_at: String,
    pub target: TargetInfo,
    pub policy: PolicyInfo,
    pub dataset: String,
    pub cases: usize,
    pub summary: PerfSummary,
}

fn call_row(name: &str, s: &CallStats) -> Vec<String> {
    let l = s.latency;
    vec![
        name.to_string(),
        s.calls.to_string(),
        s.outcomes.get("success").copied().unwrap_or(0).to_string(),
        pct(s.timeout_rate),
        pct(s.error_rate),
        format!("{} ({})", s.retries, pct(s.retry_rate)),
        format!("{} / {}", s.attempt_timeouts, s.attempts),
        secs(l.map(|l| l.p50_ms)),
        secs(l.map(|l| l.p95_ms)),
        secs(l.map(|l| l.p99_ms)),
        secs(l.map(|l| l.max_ms)),
        secs(s.latency_all.map(|l| l.max_ms)),
        secs(s.send_latency.map(|l| l.p95_ms)),
        secs(s.send_latency.map(|l| l.max_ms)),
        secs(s.wait.map(|l| l.p95_ms)),
        secs(s.wait.map(|l| l.max_ms)),
    ]
}

pub(crate) fn render_markdown(report: &PerfReport) -> String {
    let s = &report.summary;
    let p = &report.policy;
    let mut md = String::new();
    let _ = writeln!(md, "# Enricher perf benchmark: {}\n", report.target.name);
    report::target_lines(&mut md, &report.target);
    let _ = writeln!(
        md,
        "- Policy: request timeout {}s, max_tokens {}, reasoning_effort {}, max_in_flight {}, \
         429 retries {} (min wait {}s), gateway retries {}",
        p.request_timeout_secs,
        p.max_tokens.map_or("unset".into(), |v| v.to_string()),
        p.reasoning_effort.as_deref().unwrap_or("unset"),
        p.max_in_flight.map_or("unset".into(), |v| v.to_string()),
        p.rate_limit_retries,
        p.rate_limit_retry_secs,
        p.gateway_retries,
    );
    let _ = writeln!(
        md,
        "- Run: {} case(s) x {} repetition(s) at concurrency {} after {} warm-up run(s); wall clock {}\n- Dataset: `{}`\n- Generated: {}\n",
        report.cases,
        s.repetitions,
        s.concurrency,
        s.warmup_runs,
        secs(Some(s.wall_ms)),
        report.dataset,
        report.generated_at
    );

    fit_and_calls_section(&mut md, s);
    outcomes_section(&mut md, s);
    documents_section(&mut md, s);
    md
}

fn fit_and_calls_section(md: &mut String, s: &PerfSummary) {
    let fit = &s.timeout_fit;
    md.push_str("## Timeout fit\n\n");
    let _ = writeln!(
        md,
        "**{}**: {} of {} attempt(s) hit the client timeout ({} recovered by a retry, {} call(s) \
         ended in a timeout); send p95 of the attempts that got a response (any outcome but a \
         timeout or a connection-level error) is {} and max {} of the {}s request timeout. Queue/back-off wait per call: \
         p95 {}, max {} (not part of the fit: the timeout bounds each attempt, not the wait). \
         Completed-document p95 is {} of `RECLAIM_MIN_IDLE_SECS` ({}s).\n",
        fit.verdict.label(),
        fit.attempt_timeouts,
        s.all_calls.attempts,
        fit.recovered_timeouts,
        fit.timed_out_calls,
        pct(fit.p95_share_of_timeout),
        pct(fit.max_share_of_timeout),
        fit.request_timeout_secs,
        secs(s.all_calls.wait.map(|l| l.p95_ms)),
        secs(s.all_calls.wait.map(|l| l.max_ms)),
        pct(fit.doc_p95_share_of_reclaim_idle),
        fit.reclaim_min_idle_secs
    );

    md.push_str("## Calls\n\n");
    let mut rows: Vec<Vec<String>> = s
        .per_pass
        .iter()
        .map(|(name, stats)| call_row(&format!("`{name}`"), stats))
        .collect();
    rows.push(call_row("**all**", &s.all_calls));
    md.push_str(&report::table(
        &[
            "Pass",
            "Calls",
            "OK",
            "Timeouts",
            "Errors",
            "Retries (calls)",
            "Timed-out attempts",
            "p50",
            "p95",
            "p99",
            "Max",
            "Max incl. failures",
            "Send p95",
            "Send max",
            "Wait p95",
            "Wait max",
        ],
        &rows,
    ));
    md.push_str(
        "\n\"Timeouts\" and \"Errors\" are final call outcomes; \"Timed-out attempts\" also counts \
         timeouts a gateway retry recovered. p50 to \"Max incl. failures\" are end-to-end call \
         latency (successful calls unless noted), retries, `max_in_flight` queueing and 429 waits \
         included, like `enricher_llm_call_duration_seconds`. \"Send\" is one successful HTTP \
         attempt; the timeout fit uses every attempt that got a response, failures included. \
         \"Wait\" is per-call queueing plus 429 back-off.\n",
    );
}

fn outcomes_section(md: &mut String, s: &PerfSummary) {
    md.push_str("\n## Outcomes\n\n");
    let labels: std::collections::BTreeSet<&String> = s.all_calls.outcomes.keys().collect();
    let rows: Vec<Vec<String>> = s
        .per_pass
        .iter()
        .map(|(name, stats)| {
            std::iter::once(format!("`{name}`"))
                .chain(
                    labels
                        .iter()
                        .map(|l| stats.outcomes.get(*l).copied().unwrap_or(0).to_string()),
                )
                .collect()
        })
        .collect();
    let headers: Vec<&str> = std::iter::once("Pass")
        .chain(labels.iter().map(|l| l.as_str()))
        .collect();
    md.push_str(&report::table(&headers, &rows));
}

fn documents_section(md: &mut String, s: &PerfSummary) {
    let d = &s.documents;
    md.push_str("\n## Documents\n\n");
    let failures = d
        .failures_by_stage
        .iter()
        .map(|(stage, n)| format!("{stage}: {n}"))
        .collect::<Vec<_>>()
        .join(", ");
    let rows = vec![vec![
        d.documents.to_string(),
        format!("{} ({})", d.completed, pct(d.completed_rate)),
        d.transport_failures.to_string(),
        d.invalid_outputs.to_string(),
        if failures.is_empty() {
            "-".into()
        } else {
            failures
        },
        secs(d.latency.map(|l| l.p50_ms)),
        secs(d.latency.map(|l| l.p95_ms)),
        secs(d.latency.map(|l| l.max_ms)),
        report::num(s.documents_per_minute),
        report::num(s.completed_documents_per_minute),
    ]];
    md.push_str(&report::table(
        &[
            "Documents",
            "Completed",
            "Transport failures",
            "Invalid outputs",
            "Failed at",
            "p50",
            "p95",
            "Max",
            "Docs/min (attempted)",
            "Completed/min",
        ],
        &rows,
    ));
    md.push_str(
        "\nLatency columns are completed documents. A document stops where production stops: \
         after a failed or invalid primary pass the adversarial passes are never sent, so such \
         documents are shorter. Transport failures are the environment's; invalid outputs \
         (including empty content) are the model's or its config's.\n",
    );
}

pub(crate) fn comparison_markdown(reports: &[PerfReport]) -> String {
    let mut md = String::from("# Enricher perf benchmark: comparison\n\n");
    let rows: Vec<Vec<String>> = reports
        .iter()
        .map(|r| {
            let s = &r.summary;
            let l = s.all_calls.latency;
            let d = s.documents.latency;
            let docs = &s.documents;
            vec![
                r.target.name.clone(),
                format!("`{}`", r.target.model),
                r.target.environment.clone().unwrap_or_else(|| "-".into()),
                s.concurrency.to_string(),
                secs(l.map(|l| l.p50_ms)),
                secs(l.map(|l| l.p95_ms)),
                secs(l.map(|l| l.max_ms)),
                secs(s.all_calls.send_latency.map(|l| l.p95_ms)),
                secs(d.map(|l| l.p50_ms)),
                secs(d.map(|l| l.p95_ms)),
                format!(
                    "{} / {}",
                    s.all_calls.attempt_timeouts, s.all_calls.attempts
                ),
                pct(s.all_calls.error_rate),
                pct(s.all_calls.retry_rate),
                format!("{} / {}", docs.transport_failures, docs.documents),
                format!("{} / {}", docs.invalid_outputs, docs.documents),
                report::num(s.documents_per_minute),
                report::num(s.completed_documents_per_minute),
                format!(
                    "{} ({}s)",
                    s.timeout_fit.verdict.label(),
                    s.timeout_fit.request_timeout_secs
                ),
            ]
        })
        .collect();
    md.push_str(&report::table(
        &[
            "Target",
            "Model",
            "Environment",
            "Conc.",
            "Call p50",
            "Call p95",
            "Call max",
            "Send p95",
            "Doc p50",
            "Doc p95",
            "Timed-out attempts",
            "Call errors",
            "Retried",
            "Transport-failed docs",
            "Invalid-output docs",
            "Docs/min",
            "Completed/min",
            "Timeout fit",
        ],
        &rows,
    ));
    md.push_str(
        "\nCall columns are end-to-end successful calls (as the service's histogram measures); \
         \"Send p95\" is one successful HTTP attempt (the timeout fit also counts failed attempts \
         that got a response). \
         Docs/min counts every attempted document, whatever its outcome.\n",
    );
    md
}

async fn run_target(
    target: &Target,
    cases: &Arc<[Case]>,
    dataset: &str,
    dir: &std::path::Path,
) -> anyhow::Result<PerfReport> {
    let repetitions: u32 = crate::eval::env_parse("EVAL_PERF_REPEATS", 3);
    let concurrency: usize = crate::eval::env_parse("EVAL_PERF_CONCURRENCY", 1);
    let warmup: u32 = crate::eval::env_parse("EVAL_PERF_WARMUP", 1);
    let label = target.label();
    let client = Arc::new(target.client(target.request_timeout_secs)?);
    eprintln!(
        "perf: {} ({}): {} warm-up run(s), then {} case(s) x {repetitions} at concurrency {concurrency}, timeout {}s",
        target.name,
        target.model,
        warmup,
        cases.len(),
        target.request_timeout_secs
    );
    // Untimed: absorbs a cold model load, which would otherwise land in
    // the first document's latency.
    for _ in 0..warmup {
        let _ = pipeline::run_pipeline(&*client, &label, &cases[0], 0).await;
    }
    let start = tokio::time::Instant::now();
    let records = pipeline::run_jobs(client, &label, cases, repetitions, concurrency).await;
    let wall_ms = pipeline::millis(start.elapsed());
    let stem = target.file_stem();
    crate::eval::write_records(&dir.join(format!("{stem}.records.jsonl")), &records)?;
    let report = PerfReport {
        kind: "perf",
        generated_at: Utc::now().to_rfc3339(),
        target: TargetInfo::from_target(target),
        policy: PolicyInfo::from_target(target),
        dataset: dataset.to_string(),
        cases: cases.len(),
        summary: summarize(
            &records,
            PerfRun {
                concurrency,
                repetitions,
                warmup_runs: warmup,
                wall_ms,
                request_timeout_secs: target.request_timeout_secs,
                reclaim_min_idle_secs: target.reclaim_min_idle_secs,
            },
        ),
    };
    crate::eval::write_json(&dir.join(format!("{stem}.json")), &report)?;
    crate::eval::write_text(&dir.join(format!("{stem}.md")), &render_markdown(&report))?;
    Ok(report)
}

async fn run_live() -> anyhow::Result<()> {
    let (dataset, cases) = crate::eval::load_cases()?;
    if cases.is_empty() {
        anyhow::bail!("no cases to run");
    }
    let cases: Arc<[Case]> = cases.into();
    let targets = crate::eval::target::load_targets()?;
    let dir = crate::eval::run_dir("perf", "")?;
    let mut reports = Vec::new();
    for target in &targets {
        let report = run_target(target, &cases, &dataset, &dir).await?;
        println!("{}", render_markdown(&report));
        reports.push(report);
    }
    crate::eval::write_text(&dir.join("comparison.md"), &comparison_markdown(&reports))?;
    println!("{}", comparison_markdown(&reports));
    eprintln!("perf: reports written to {}", dir.display());
    Ok(())
}

/// Live perf benchmark. See `crate::eval`'s module doc for the command.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a real LLM endpoint (EVAL_TARGETS or LLM_BASE_URL/LLM_MODEL); see eval module doc"]
async fn live_eval_perf() {
    if let Err(err) = run_live().await {
        panic!("perf benchmark failed: {err:#}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::pipeline::{
        AttemptRecord, CallRecord, FakeBackend, FakeReply, TargetLabel, run_jobs,
    };
    use crate::llm::LlmCallError;

    fn close(a: Option<f64>, b: f64) -> bool {
        a.is_some_and(|a| (a - b).abs() < 1e-9)
    }

    #[test]
    fn nearest_rank_percentiles() {
        let samples: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&samples, 50), Some(50));
        assert_eq!(percentile(&samples, 95), Some(95));
        assert_eq!(percentile(&samples, 99), Some(99));
        assert_eq!(percentile(&samples, 100), Some(100));
        assert_eq!(percentile(&[7], 0), Some(7));
        assert_eq!(percentile(&[7], 99), Some(7));
        assert_eq!(percentile(&[], 50), None);
        // Small samples round the rank up: p95 of 4 values is the max.
        assert_eq!(percentile(&[1, 2, 3, 4], 95), Some(4));
        assert_eq!(percentile(&[1, 2, 3, 4], 50), Some(2));
    }

    #[test]
    fn latency_stats_are_order_independent() {
        let stats = latency_stats(&[300, 100, 200, 400]).unwrap();
        assert_eq!(
            (stats.min_ms, stats.p50_ms, stats.max_ms, stats.count),
            (100, 200, 400, 4)
        );
        assert!((stats.mean_ms - 250.0).abs() < 1e-9);
        assert_eq!(latency_stats(&[]), None);
    }

    /// `(outcome, send_ms, wait_ms)` per attempt; the wait is booked as
    /// back-off after the attempt.
    type Try = (&'static str, u64, u64);

    fn call(pass: Pass, ms: u64, outcome: &str, attempts: &[Try]) -> CallRecord {
        CallRecord {
            pass,
            elapsed_ms: ms,
            retries: u32::try_from(attempts.len().saturating_sub(1)).unwrap(),
            outcome: outcome.to_string(),
            attempts: attempts
                .iter()
                .map(|&(outcome, send_ms, backoff_ms)| AttemptRecord {
                    outcome: outcome.to_string(),
                    queued_ms: 0,
                    send_ms,
                    backoff_ms,
                })
                .collect(),
            content: None,
            error: None,
            usage: None,
        }
    }

    /// A call that succeeded on its only attempt.
    fn ok(ms: u64) -> CallRecord {
        call(Pass::Primary, ms, "success", &[("success", ms, 0)])
    }

    #[test]
    fn call_stats_split_outcomes_retries_and_latency() {
        let calls = [
            ok(1_000),
            call(
                Pass::Primary,
                3_000,
                "success",
                &[
                    ("gateway_error", 500, 0),
                    ("rate_limited", 100, 1_400),
                    ("success", 1_000, 0),
                ],
            ),
            call(
                Pass::Primary,
                300_000,
                "timeout",
                &[("timeout", 150_000, 0), ("timeout", 150_000, 0)],
            ),
            call(
                Pass::Primary,
                50,
                "rate_limited",
                &[("rate_limited", 50, 0)],
            ),
        ];
        let stats = call_stats(calls.iter());
        assert_eq!(stats.calls, 4);
        assert_eq!(stats.outcomes["success"], 2);
        assert!(close(stats.error_rate, 0.5));
        assert!(close(stats.timeout_rate, 0.25));
        assert_eq!((stats.retries, stats.calls_with_retries), (3, 2));
        assert!(close(stats.retry_rate, 0.5));
        assert_eq!((stats.attempts, stats.attempt_timeouts), (7, 2));
        assert_eq!(stats.recovered_timeouts, 0);
        assert_eq!(stats.latency.unwrap().max_ms, 3_000, "successes only");
        assert_eq!(stats.latency_all.unwrap().max_ms, 300_000);
        // Send latency: successful attempts only, without the back-off.
        let send = stats.send_latency.unwrap();
        assert_eq!((send.count, send.max_ms), (2, 1_000));
        // The fit's figure: every attempt but the two timeouts.
        let responded = stats.response_latency.unwrap();
        assert_eq!((responded.count, responded.max_ms), (5, 1_000));
        assert_eq!(responded.min_ms, 50);

        // Connection-level errors never got a response, so they stay out of
        // the fit's figure rather than dragging its p95 down.
        let with_errors = [
            ok(9_000),
            call(Pass::Primary, 5, "error", &[("error", 5, 0)]),
            call(Pass::Primary, 5, "error", &[("error", 5, 0)]),
        ];
        let responded = call_stats(with_errors.iter()).response_latency.unwrap();
        assert_eq!((responded.count, responded.min_ms), (1, 9_000));
        assert_eq!(stats.wait.unwrap().max_ms, 1_400);
    }

    #[test]
    fn timeout_fit_verdicts() {
        let docs = DocStats::default();
        let stats = |calls: &[CallRecord]| call_stats(calls.iter());
        let fit = |stats: &CallStats| timeout_fit(stats, &docs, 10, 1000).verdict;
        assert_eq!(fit(&stats(&[ok(1_000), ok(2_000)])), Fit::Fits);
        assert_eq!(fit(&stats(&[ok(1_000), ok(9_000)])), Fit::Tight);
        let timed_out = call(Pass::Primary, 10_000, "timeout", &[("timeout", 10_000, 0)]);
        assert_eq!(fit(&stats(&[ok(1_000), timed_out.clone()])), Fit::Exceeds);
        assert_eq!(fit(&stats(&[timed_out])), Fit::Exceeds);
        // A fast failure still answered within the timeout.
        assert_eq!(
            fit(&stats(&[call(
                Pass::Primary,
                5,
                "http_error",
                &[("http_error", 5, 0)]
            )])),
            Fit::Fits
        );
        // Nothing sent at all.
        assert_eq!(
            fit(&stats(&[call(Pass::Primary, 0, "error", &[])])),
            Fit::NoData
        );
        let fit_of = |calls: &[CallRecord]| timeout_fit(&stats(calls), &docs, 10, 1000);
        assert!(close(fit_of(&[ok(2_000)]).p95_share_of_timeout, 0.2));

        // A timed-out attempt that a gateway retry recovered still exceeds.
        let recovered = call(
            Pass::Primary,
            11_000,
            "success",
            &[("timeout", 10_000, 0), ("success", 1_000, 0)],
        );
        let fit = fit_of(&[ok(1_000), recovered]);
        assert_eq!(fit.verdict, Fit::Exceeds);
        assert_eq!(
            (
                fit.attempt_timeouts,
                fit.recovered_timeouts,
                fit.timed_out_calls
            ),
            (1, 1, 0)
        );

        // Queueing and 429 back-off inflate the call latency, not the
        // per-attempt send time the fit judges.
        let waited = call(
            Pass::Primary,
            30_000,
            "success",
            &[("rate_limited", 100, 28_000), ("success", 1_900, 0)],
        );
        let fit = fit_of(&[waited]);
        assert_eq!(fit.verdict, Fit::Fits);
        assert!(close(fit.p95_share_of_timeout, 0.19));
    }

    /// Slow answers the service rejects anyway (here empty content, near
    /// the timeout) are as close to timing out as slow successes: the fit
    /// counts them, while "Send" stays success-only.
    #[test]
    fn slow_failed_attempts_count_towards_the_fit() {
        let docs = DocStats::default();
        let empty = || {
            call(
                Pass::Primary,
                9_900,
                "empty_content",
                &[("empty_content", 9_900, 0)],
            )
        };
        let stats = call_stats([ok(1_000), empty(), empty()].iter());
        assert_eq!(stats.send_latency.unwrap().max_ms, 1_000);
        let fit = timeout_fit(&stats, &docs, 10, 1000);
        assert_eq!(fit.verdict, Fit::Tight);
        assert!(close(fit.p95_share_of_timeout, 0.99));
        assert!(close(fit.max_share_of_timeout, 0.99));

        // So does a slow HTTP error that a gateway retry recovered.
        let recovered = call(
            Pass::Primary,
            10_000,
            "success",
            &[("gateway_error", 9_000, 0), ("success", 1_000, 0)],
        );
        let fit = timeout_fit(&call_stats([recovered].iter()), &docs, 10, 1000);
        assert_eq!(fit.verdict, Fit::Tight);
    }

    /// Defensive: a timed-out call with no attempt records (attempt
    /// timeouts unknown) still exceeds.
    #[test]
    fn a_timed_out_call_without_attempts_exceeds() {
        let mut timed_out = call(Pass::Primary, 10_000, "timeout", &[]);
        timed_out.attempts.clear();
        let stats = call_stats([ok(1_000), timed_out].iter());
        assert_eq!(stats.attempt_timeouts, 0);
        let fit = timeout_fit(&stats, &DocStats::default(), 10, 1000);
        assert_eq!((fit.verdict, fit.timed_out_calls), (Fit::Exceeds, 1));
    }

    fn one_period() -> serde_json::Value {
        serde_json::json!({
            "category": "signal_failure",
            "periods": [{
                "scope_description": null,
                "date_range": null,
                "schedule_window": null,
                "resolution_status": "ongoing",
                "apparent_severity": "normal",
                "impact_type": null
            }]
        })
    }

    fn case(id: &str) -> Case {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "summary": "s",
            "description": "d",
            "reference_date": "2026-04-01T00:00:00Z",
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn summary_over_a_fake_run() {
        let mut backend = FakeBackend::default()
            .agreeing("ok", &one_period())
            .with(
                "slow",
                Pass::Primary,
                FakeReply::Error {
                    error: || LlmCallError::ClientTimeout,
                    retries: 1,
                },
            )
            .with("junk", Pass::Primary, FakeReply::Content("nope".into()));
        backend.delay = std::time::Duration::from_millis(5);
        let cases: Arc<[Case]> = vec![case("ok"), case("slow"), case("junk")].into();
        let label = TargetLabel {
            target: "t".into(),
            model: "m".into(),
        };
        let records = run_jobs(Arc::new(backend), &label, &cases, 2, 3).await;
        let summary = summarize(
            &records,
            PerfRun {
                concurrency: 3,
                repetitions: 2,
                warmup_runs: 0,
                wall_ms: 60_000,
                request_timeout_secs: 300,
                reclaim_min_idle_secs: 1000,
            },
        );
        // ok: 3 calls x 2; slow and junk: 1 call x 2 each.
        assert_eq!(summary.all_calls.calls, 10);
        assert_eq!(summary.per_pass["primary"].calls, 6);
        assert_eq!(summary.per_pass["severity_adversarial"].calls, 2);
        assert_eq!(summary.all_calls.outcomes["timeout"], 2);
        assert_eq!(summary.all_calls.retries, 2);
        // slow: two attempts (one retry) per run, both timed out.
        assert_eq!(summary.all_calls.attempt_timeouts, 4);
        assert_eq!(summary.timeout_fit.attempt_timeouts, 4);
        let d = &summary.documents;
        assert_eq!(
            (
                d.documents,
                d.completed,
                d.transport_failures,
                d.invalid_outputs
            ),
            (6, 2, 2, 2)
        );
        assert_eq!(d.failures_by_stage["primary"], 4);
        assert!(close(summary.documents_per_minute, 6.0), "all attempted");
        assert!(close(summary.completed_documents_per_minute, 2.0));
        assert!(close(summary.calls_per_minute, 10.0));
        assert_eq!(summary.timeout_fit.verdict, Fit::Exceeds);
        assert!(
            d.latency.unwrap().min_ms >= 15,
            "three 5 ms calls per document"
        );

        let report = PerfReport {
            kind: "perf",
            generated_at: "now".into(),
            target: TargetInfo {
                name: "t".into(),
                model: "m".into(),
                environment: None,
                base_url: None,
            },
            policy: PolicyInfo {
                request_timeout_secs: 300,
                max_tokens: None,
                reasoning_effort: None,
                max_in_flight: None,
                rate_limit_retries: 0,
                rate_limit_retry_secs: 20,
                gateway_retries: 0,
            },
            dataset: "d".into(),
            cases: 3,
            summary,
        };
        let md = render_markdown(&report);
        assert!(md.contains("**exceeds**"), "{md}");
        assert!(
            md.contains("4 of 12 attempt(s) hit the client timeout"),
            "{md}"
        );
        assert!(md.contains("| `primary` |"));
        assert!(comparison_markdown(&[report]).contains("| t |"));
    }
}
