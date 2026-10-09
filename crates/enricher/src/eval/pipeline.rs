//! The extraction pipeline both runners drive, minus the database: primary,
//! resolution-adversarial and severity-adversarial passes, then
//! `combine::combine_periods` -- the same order and the same stop-on-failure
//! points as `main.rs::process_incident`, through the real prompts, schemas
//! and parsers in `llm.rs` (`LlmClient::*_raw` + `llm::parse_*`).
//!
//! A run produces a [`PipelineRecord`]: every pass's raw output (or error),
//! latency, in-call retry count and per-attempt timings. What the service would have written is
//! derived from the record afterwards by [`PipelineRecord::outcome`], which
//! is pure -- so a record saved to JSONL scores offline exactly as it scored
//! live, and the quality scorer never needs the model.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::combine;
use crate::eval::dataset::Case;
use crate::llm::{self, ExtractionPeriod, LlmClient, RawCall};

/// One LLM call of the pipeline. Labels match the `call` label of
/// `enricher_llm_call_duration_seconds`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Pass {
    Primary,
    ResolutionAdversarial,
    SeverityAdversarial,
}

impl Pass {
    pub(crate) const ALL: [Pass; 3] = [
        Pass::Primary,
        Pass::ResolutionAdversarial,
        Pass::SeverityAdversarial,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Pass::Primary => "primary",
            Pass::ResolutionAdversarial => "resolution_adversarial",
            Pass::SeverityAdversarial => "severity_adversarial",
        }
    }
}

/// Where a model call is sent: the shared model-client abstraction. The
/// real implementation is [`LlmClient`]; tests use [`FakeBackend`].
pub(crate) trait Backend: Send + Sync {
    /// Runs one pass. `periods` is the primary pass's parsed periods (empty
    /// for the primary pass itself), which the adversarial prompts embed.
    fn call(
        &self,
        pass: Pass,
        case: &Case,
        periods: &[ExtractionPeriod],
    ) -> impl Future<Output = RawCall> + Send;

    /// [`llm::prompt_fingerprint`] of the prompts this backend sends.
    fn prompt_fingerprint(&self) -> String {
        llm::prompt_fingerprint()
    }
}

impl Backend for LlmClient {
    async fn call(&self, pass: Pass, case: &Case, periods: &[ExtractionPeriod]) -> RawCall {
        let (summary, description) = (&case.summary, &case.description);
        match pass {
            Pass::Primary => {
                self.primary_raw(summary, description, case.reference_date)
                    .await
            }
            Pass::ResolutionAdversarial => {
                self.adversarial_raw(summary, description, periods).await
            }
            Pass::SeverityAdversarial => {
                self.severity_adversarial_raw(summary, description, periods)
                    .await
            }
        }
    }

    fn prompt_fingerprint(&self) -> String {
        llm::prompt_fingerprint_of(self.prompts())
    }
}

/// Which target produced a record (copied onto every record line, so a
/// records file is self-describing).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TargetLabel {
    pub target: String,
    pub model: String,
}

/// One HTTP attempt within a call (see [`llm::Attempt`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AttemptRecord {
    /// `success`, or the attempt's error label (same labels as
    /// [`CallRecord::outcome`]).
    pub outcome: String,
    /// Waiting for the provider policy's `max_in_flight` permit.
    pub queued_ms: u64,
    /// The HTTP attempt itself: what the request timeout bounds.
    pub send_ms: u64,
    /// 429 back-off slept after this attempt.
    pub backoff_ms: u64,
}

impl From<&llm::Attempt> for AttemptRecord {
    fn from(attempt: &llm::Attempt) -> Self {
        Self {
            outcome: attempt.outcome.to_string(),
            queued_ms: millis(attempt.queued),
            send_ms: millis(attempt.send),
            backoff_ms: millis(attempt.backoff),
        }
    }
}

/// One pass as it happened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CallRecord {
    pub pass: Pass,
    /// Wall time of the whole call, in-call retries, `max_in_flight`
    /// queueing and 429 waits included (what the service's duration
    /// histogram measures).
    pub elapsed_ms: u64,
    /// In-call retries the target's provider policy spent.
    pub retries: u32,
    /// `success`, or `main.rs::llm_outcome`'s label for the error
    /// (`timeout`, `gateway_error`, `rate_limited`, `quota_exhausted`,
    /// `http_error`, `empty_content`, `refused`, `auth_error`,
    /// `unauthorized`, `error`). The *final*
    /// outcome: an attempt that
    /// timed out and was retried successfully shows only in `attempts`.
    pub outcome: String,
    /// Every HTTP attempt, in order. Empty when the call failed before
    /// sending anything.
    #[serde(default)]
    pub attempts: Vec<AttemptRecord>,
    /// The raw `content` string, when the endpoint returned one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The provider's token counts for the successful attempt, when it sent
    /// any (`OpenAI`: prompt, completion, reasoning and cached tokens).
    /// Absent for providers that omit `usage` and in older records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<llm::TokenUsage>,
}

/// One pipeline run of one case: the unit both runners record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PipelineRecord {
    #[serde(flatten)]
    pub label: TargetLabel,
    pub case_id: String,
    /// [`Case::input_hash`] of the case as it was run, so a replay can
    /// tell a record of since-edited case text from a current one.
    #[serde(default)]
    pub input_hash: String,
    /// [`llm::prompt_fingerprint`] of the code that ran: prompts and
    /// schemas. A replay warns about records from other prompts but still
    /// scores them. Empty in records saved before it was added.
    #[serde(default)]
    pub prompt_fingerprint: String,
    pub repetition: u32,
    /// Wall time of the whole document (all passes run).
    pub elapsed_ms: u64,
    pub calls: Vec<CallRecord>,
}

/// What the service would have written.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Extraction {
    pub category: String,
    pub periods: Vec<ExtractionPeriod>,
    pub dropped_period_count: usize,
}

/// Where a pipeline run stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Stage {
    Primary,
    ResolutionAdversarial,
    SeverityAdversarial,
    Combine,
}

impl Stage {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Stage::Primary => "primary",
            Stage::ResolutionAdversarial => "resolution_adversarial",
            Stage::SeverityAdversarial => "severity_adversarial",
            Stage::Combine => "combine",
        }
    }
}

impl From<Pass> for Stage {
    fn from(pass: Pass) -> Self {
        match pass {
            Pass::Primary => Stage::Primary,
            Pass::ResolutionAdversarial => Stage::ResolutionAdversarial,
            Pass::SeverityAdversarial => Stage::SeverityAdversarial,
        }
    }
}

/// Why a run produced nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailureKind {
    /// The call itself failed (timeout, gateway or HTTP error, 429...). An
    /// environment/perf signal; the quality report counts it separately.
    Transport,
    /// The model answered, but with output the service rejects: empty
    /// content (e.g. a reasoning model that spent `max_tokens` thinking --
    /// a model/config problem, not the environment), a safety refusal
    /// (`message.refusal`), malformed or
    /// schema-violating JSON, an empty `periods` array, or adversarial
    /// verdicts that don't align with the primary periods.
    InvalidOutput,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Failure {
    pub stage: Stage,
    pub kind: FailureKind,
    /// The call's outcome label for a transport failure or empty content;
    /// `invalid_output` or `combine_mismatch` otherwise.
    pub outcome: String,
    pub message: String,
}

impl PipelineRecord {
    /// Replays the record through the service's own parsers and
    /// `combine_periods`. Pure and deterministic.
    pub(crate) fn outcome(&self) -> Result<Extraction, Failure> {
        let primary = self.parsed(Pass::Primary, llm::parse_primary)?;
        let resolution = self.parsed(Pass::ResolutionAdversarial, llm::parse_adversarial)?;
        let severity = self.parsed(Pass::SeverityAdversarial, llm::parse_severity_adversarial)?;
        let periods =
            combine::combine_periods(&primary.periods, &resolution, &severity).map_err(|err| {
                Failure {
                    stage: Stage::Combine,
                    kind: FailureKind::InvalidOutput,
                    outcome: "combine_mismatch".to_string(),
                    message: err.to_string(),
                }
            })?;
        Ok(Extraction {
            category: primary.category,
            periods,
            dropped_period_count: primary.dropped_period_count,
        })
    }

    fn parsed<T>(&self, pass: Pass, parse: fn(&str) -> anyhow::Result<T>) -> Result<T, Failure> {
        let Some(call) = self.calls.iter().find(|c| c.pass == pass) else {
            return Err(Failure {
                stage: pass.into(),
                kind: FailureKind::InvalidOutput,
                outcome: "not_recorded".to_string(),
                message: format!("record has no {} call", pass.label()),
            });
        };
        let Some(content) = &call.content else {
            // Empty content (typically a reasoning model out of
            // `max_tokens`) and a safety refusal are the model's answer, so
            // they score as invalid output; perf still sees the call's own
            // `empty_content` / `refused` label.
            let kind = if MODEL_ANSWER_FAILURES.contains(&call.outcome.as_str()) {
                FailureKind::InvalidOutput
            } else {
                FailureKind::Transport
            };
            return Err(Failure {
                stage: pass.into(),
                kind,
                outcome: call.outcome.clone(),
                message: call.error.clone().unwrap_or_default(),
            });
        };
        parse(content).map_err(|err| Failure {
            stage: pass.into(),
            kind: FailureKind::InvalidOutput,
            outcome: "invalid_output".to_string(),
            message: err.to_string(),
        })
    }
}

/// Outcome labels of failed calls that are nonetheless the model's answer
/// (`LlmCallError::EmptyContent` and `LlmCallError::Refused`), not a
/// transport failure.
const MODEL_ANSWER_FAILURES: [&str; 2] = ["empty_content", "refused"];

pub(crate) fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

async fn timed_call<B: Backend>(
    backend: &B,
    pass: Pass,
    case: &Case,
    periods: &[ExtractionPeriod],
) -> CallRecord {
    let start = tokio::time::Instant::now();
    let raw = backend.call(pass, case, periods).await;
    let elapsed_ms = millis(start.elapsed());
    let outcome = crate::llm_outcome(&raw.content).to_string();
    let (content, error) = match raw.content {
        Ok(content) => (Some(content), None),
        Err(err) => (None, Some(format!("{err:#}"))),
    };
    CallRecord {
        pass,
        elapsed_ms,
        retries: raw.retries,
        outcome,
        attempts: raw.attempts.iter().map(AttemptRecord::from).collect(),
        content,
        error,
        usage: raw.usage,
    }
}

/// Runs one case through the pipeline, stopping where `process_incident`
/// would: after a failed or unparseable pass. (So a document whose primary
/// pass fails never sends the adversarial passes, exactly as in production;
/// its perf numbers have fewer calls.)
pub(crate) async fn run_pipeline<B: Backend>(
    backend: &B,
    label: &TargetLabel,
    case: &Case,
    repetition: u32,
) -> PipelineRecord {
    let start = tokio::time::Instant::now();
    let mut calls = Vec::with_capacity(3);
    let primary = timed_call(backend, Pass::Primary, case, &[]).await;
    let periods = primary
        .content
        .as_deref()
        .and_then(|content| llm::parse_primary(content).ok())
        .map(|extraction| extraction.periods);
    calls.push(primary);
    if let Some(periods) = periods {
        for pass in [Pass::ResolutionAdversarial, Pass::SeverityAdversarial] {
            let call = timed_call(backend, pass, case, &periods).await;
            let parsed = call.content.as_deref().is_some_and(|content| match pass {
                Pass::ResolutionAdversarial => llm::parse_adversarial(content).is_ok(),
                _ => llm::parse_severity_adversarial(content).is_ok(),
            });
            calls.push(call);
            if !parsed {
                break;
            }
        }
    }
    PipelineRecord {
        label: label.clone(),
        case_id: case.id.clone(),
        input_hash: case.input_hash(),
        prompt_fingerprint: backend.prompt_fingerprint(),
        repetition,
        elapsed_ms: millis(start.elapsed()),
        calls,
    }
}

/// Runs every case `repetitions` times, at most `concurrency` documents at
/// once (1 = the service's serial stream loop). Returns records ordered by
/// case, then repetition. Jobs start repetition-major, so with
/// `concurrency` 1 each case's repeats are spread across the run rather
/// than back-to-back.
pub(crate) async fn run_jobs<B: Backend + 'static>(
    backend: Arc<B>,
    label: &TargetLabel,
    cases: &Arc<[Case]>,
    repetitions: u32,
    concurrency: usize,
) -> Vec<PipelineRecord> {
    let limit = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut jobs = tokio::task::JoinSet::new();
    for repetition in 0..repetitions {
        for index in 0..cases.len() {
            let (backend, cases, label, limit) = (
                Arc::clone(&backend),
                Arc::clone(cases),
                label.clone(),
                Arc::clone(&limit),
            );
            jobs.spawn(async move {
                let _permit = limit
                    .acquire_owned()
                    .await
                    .expect("the job semaphore is never closed");
                let record = run_pipeline(&*backend, &label, &cases[index], repetition).await;
                (index, record)
            });
        }
    }
    let mut records = Vec::new();
    while let Some(joined) = jobs.join_next().await {
        records.push(joined.expect("an eval job panicked"));
    }
    records.sort_by_key(|(index, record)| (*index, record.repetition));
    records.into_iter().map(|(_, record)| record).collect()
}

/// A scripted [`Backend`] for tests: no network, deterministic.
#[derive(Default)]
pub(crate) struct FakeBackend {
    replies: std::collections::BTreeMap<(String, Pass), FakeReply>,
    /// Sleep before every reply (for concurrency tests).
    pub delay: Duration,
    in_flight: std::sync::atomic::AtomicUsize,
    /// Highest number of calls seen in flight at once.
    pub max_in_flight: std::sync::atomic::AtomicUsize,
}

/// What a [`FakeBackend`] answers for one (case, pass).
#[derive(Clone)]
pub(crate) enum FakeReply {
    /// This exact content string.
    Content(String),
    /// A well-formed adversarial verdict list echoing the periods it was
    /// given, each verdict equal to the period's own value (agreement).
    Agree,
    /// The call fails with this typed error, after `retries` retries
    /// (each recorded as a failed attempt with the same error).
    Error {
        error: fn() -> llm::LlmCallError,
        retries: u32,
    },
    /// Attempts that failed with these errors and were retried, then
    /// `then` (whose own attempts follow).
    Retried {
        failed: Vec<fn() -> llm::LlmCallError>,
        then: Box<FakeReply>,
    },
}

impl FakeBackend {
    pub(crate) fn with(mut self, case: &str, pass: Pass, reply: FakeReply) -> Self {
        self.replies.insert((case.to_string(), pass), reply);
        self
    }

    /// Scripts a primary reply and agreeing adversarial passes.
    pub(crate) fn agreeing(self, case: &str, primary_json: &serde_json::Value) -> Self {
        self.with(
            case,
            Pass::Primary,
            FakeReply::Content(primary_json.to_string()),
        )
        .with(case, Pass::ResolutionAdversarial, FakeReply::Agree)
        .with(case, Pass::SeverityAdversarial, FakeReply::Agree)
    }
}

impl Backend for FakeBackend {
    async fn call(&self, pass: Pass, case: &Case, periods: &[ExtractionPeriod]) -> RawCall {
        use std::sync::atomic::Ordering;

        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        let reply = self.replies.get(&(case.id.clone(), pass)).cloned();
        self.reply(reply, pass, case, periods)
    }
}

impl FakeBackend {
    fn attempt(&self, outcome: &'static str) -> llm::Attempt {
        llm::Attempt {
            outcome,
            queued: Duration::ZERO,
            send: self.delay,
            backoff: Duration::ZERO,
        }
    }

    fn reply(
        &self,
        reply: Option<FakeReply>,
        pass: Pass,
        case: &Case,
        periods: &[ExtractionPeriod],
    ) -> RawCall {
        match reply {
            Some(FakeReply::Content(content)) => RawCall {
                content: Ok(content),
                retries: 0,
                attempts: vec![self.attempt("success")],
                usage: None,
            },
            Some(FakeReply::Agree) => {
                let verdicts: Vec<serde_json::Value> = periods
                    .iter()
                    .enumerate()
                    .map(|(index, period)| {
                        let mut verdict = serde_json::json!({
                            "period_index": index,
                            "scope_description": period.scope_description,
                        });
                        if pass == Pass::ResolutionAdversarial {
                            verdict["resolution_status"] = period.resolution_status.clone().into();
                        } else {
                            verdict["apparent_severity"] = period.apparent_severity.clone().into();
                        }
                        verdict
                    })
                    .collect();
                RawCall {
                    content: Ok(serde_json::json!({ "periods": verdicts }).to_string()),
                    retries: 0,
                    attempts: vec![self.attempt("success")],
                    usage: None,
                }
            }
            Some(FakeReply::Error { error, retries }) => {
                let label = error().outcome_label();
                RawCall {
                    content: Err(error().into()),
                    retries,
                    attempts: (0..=retries).map(|_| self.attempt(label)).collect(),
                    usage: None,
                }
            }
            Some(FakeReply::Retried { failed, then }) => {
                let mut raw = self.reply(Some(*then), pass, case, periods);
                let mut attempts: Vec<llm::Attempt> = failed
                    .iter()
                    .map(|error| self.attempt(error().outcome_label()))
                    .collect();
                attempts.append(&mut raw.attempts);
                raw.attempts = attempts;
                raw.retries += u32::try_from(failed.len()).unwrap_or(u32::MAX);
                raw
            }
            None => RawCall {
                content: Err(anyhow::anyhow!(
                    "no scripted reply for {} / {}",
                    case.id,
                    pass.label()
                )),
                retries: 0,
                attempts: Vec::new(),
                usage: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(id: &str) -> Case {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "summary": "Signal failure",
            "description": "Delays",
            "reference_date": "2026-04-01T00:00:00Z",
        }))
        .unwrap()
    }

    fn label() -> TargetLabel {
        TargetLabel {
            target: "fake".to_string(),
            model: "fake-model".to_string(),
        }
    }

    fn one_period() -> serde_json::Value {
        serde_json::json!({
            "category": "signal_failure",
            "periods": [{
                "scope_description": null,
                "date_range": null,
                "schedule_window": null,
                "resolution_status": "ongoing",
                "apparent_severity": "moderate_disruption",
                "impact_type": null
            }]
        })
    }

    #[tokio::test]
    async fn a_clean_run_records_three_calls_and_replays_to_the_combined_extraction() {
        let backend = FakeBackend::default().agreeing("a", &one_period());
        let record = run_pipeline(&backend, &label(), &case("a"), 0).await;
        assert_eq!(
            record.calls.iter().map(|c| c.pass).collect::<Vec<_>>(),
            Pass::ALL
        );
        assert!(record.calls.iter().all(|c| c.outcome == "success"));
        let extraction = record.outcome().unwrap();
        assert_eq!(extraction.category, "signal_failure");
        assert_eq!(extraction.periods[0].severity_confidence, "high");

        // The record round-trips through JSONL and replays identically.
        let line = serde_json::to_string(&record).unwrap();
        let back: PipelineRecord = serde_json::from_str(&line).unwrap();
        assert_eq!(back, record);
        assert_eq!(back.outcome().unwrap(), extraction);
    }

    #[tokio::test]
    async fn a_transport_failure_stops_the_pipeline_and_keeps_its_typed_label() {
        let backend = FakeBackend::default().with(
            "a",
            Pass::Primary,
            FakeReply::Error {
                error: || llm::LlmCallError::ClientTimeout,
                retries: 2,
            },
        );
        let record = run_pipeline(&backend, &label(), &case("a"), 0).await;
        assert_eq!(record.calls.len(), 1);
        assert_eq!(record.calls[0].outcome, "timeout");
        assert_eq!(record.calls[0].retries, 2);
        let failure = record.outcome().unwrap_err();
        assert_eq!(failure.stage, Stage::Primary);
        assert_eq!(failure.kind, FailureKind::Transport);
        assert_eq!(failure.outcome, "timeout");
    }

    #[tokio::test]
    async fn invalid_output_is_distinguished_from_transport_failure() {
        // Malformed primary JSON: the call succeeded, the output is invalid.
        let backend =
            FakeBackend::default().with("a", Pass::Primary, FakeReply::Content("{not json".into()));
        let record = run_pipeline(&backend, &label(), &case("a"), 0).await;
        assert_eq!(record.calls.len(), 1);
        assert_eq!(record.calls[0].outcome, "success");
        let failure = record.outcome().unwrap_err();
        assert_eq!(failure.kind, FailureKind::InvalidOutput);

        // Misaligned adversarial verdicts: a combine failure.
        let misaligned = serde_json::json!({"periods": [{
            "period_index": 7, "scope_description": null, "resolution_status": "ongoing"
        }]});
        let backend = FakeBackend::default().agreeing("a", &one_period()).with(
            "a",
            Pass::ResolutionAdversarial,
            FakeReply::Content(misaligned.to_string()),
        );
        let failure = run_pipeline(&backend, &label(), &case("a"), 0)
            .await
            .outcome()
            .unwrap_err();
        assert_eq!(failure.stage, Stage::Combine);
        assert_eq!(failure.kind, FailureKind::InvalidOutput);
    }

    #[tokio::test]
    async fn empty_content_is_invalid_output_not_transport() {
        let backend = FakeBackend::default().with(
            "a",
            Pass::Primary,
            FakeReply::Error {
                error: || llm::LlmCallError::EmptyContent {
                    finish_reason: Some("length".into()),
                },
                retries: 0,
            },
        );
        let record = run_pipeline(&backend, &label(), &case("a"), 0).await;
        assert_eq!(
            record.calls[0].outcome, "empty_content",
            "perf keeps its label"
        );
        let failure = record.outcome().unwrap_err();
        assert_eq!(failure.kind, FailureKind::InvalidOutput);
        assert_eq!(failure.outcome, "empty_content");
    }

    /// A safety refusal is the model's answer: invalid output, labelled
    /// `refused`. An exhausted quota is the account's problem: transport.
    #[tokio::test]
    async fn refusal_is_invalid_output_and_quota_is_transport() {
        let backend = FakeBackend::default().with(
            "a",
            Pass::Primary,
            FakeReply::Error {
                error: || llm::LlmCallError::Refused {
                    refusal: "I can't help with that.".into(),
                },
                retries: 0,
            },
        );
        let record = run_pipeline(&backend, &label(), &case("a"), 0).await;
        assert_eq!(record.calls[0].outcome, "refused");
        let failure = record.outcome().unwrap_err();
        assert_eq!(failure.kind, FailureKind::InvalidOutput);
        assert_eq!(failure.outcome, "refused");

        let backend = FakeBackend::default().with(
            "a",
            Pass::Primary,
            FakeReply::Error {
                error: || llm::LlmCallError::QuotaExhausted {
                    code: "insufficient_quota".into(),
                },
                retries: 0,
            },
        );
        let failure = run_pipeline(&backend, &label(), &case("a"), 0)
            .await
            .outcome()
            .unwrap_err();
        assert_eq!(failure.kind, FailureKind::Transport);
        assert_eq!(failure.outcome, "quota_exhausted");
    }

    /// Token usage is optional on a record: an old record (no `usage` key)
    /// still parses, and a recorded one round-trips.
    #[test]
    fn usage_is_optional_and_round_trips() {
        let old =
            r#"{"pass":"primary","elapsed_ms":1,"retries":0,"outcome":"success","content":"{}"}"#;
        let call: CallRecord = serde_json::from_str(old).unwrap();
        assert_eq!(call.usage, None);
        let with = CallRecord {
            usage: Some(llm::TokenUsage {
                prompt_tokens: Some(1200),
                completion_tokens: Some(80),
                reasoning_tokens: Some(0),
                cached_tokens: Some(1024),
                cache_write_tokens: None,
            }),
            ..call
        };
        let line = serde_json::to_string(&with).unwrap();
        assert!(line.contains("\"cached_tokens\":1024"), "{line}");
        assert_eq!(serde_json::from_str::<CallRecord>(&line).unwrap(), with);
    }

    #[tokio::test]
    async fn records_keep_each_attempt_the_case_input_hash_and_the_prompt_fingerprint() {
        let backend = FakeBackend::default().agreeing("a", &one_period()).with(
            "a",
            Pass::Primary,
            FakeReply::Retried {
                failed: vec![|| llm::LlmCallError::ClientTimeout],
                then: Box::new(FakeReply::Content(one_period().to_string())),
            },
        );
        let record = run_pipeline(&backend, &label(), &case("a"), 0).await;
        let primary = &record.calls[0];
        assert_eq!(primary.outcome, "success");
        assert_eq!(primary.retries, 1);
        let outcomes: Vec<&str> = primary
            .attempts
            .iter()
            .map(|a| a.outcome.as_str())
            .collect();
        assert_eq!(outcomes, ["timeout", "success"]);
        assert_eq!(record.input_hash, case("a").input_hash());
        assert_eq!(record.prompt_fingerprint, llm::prompt_fingerprint());
        assert_eq!(record.prompt_fingerprint.len(), 16);
        assert!(record.outcome().is_ok());
    }

    #[tokio::test]
    async fn run_jobs_honours_concurrency_and_orders_records() {
        let mut backend = FakeBackend::default()
            .agreeing("a", &one_period())
            .agreeing("b", &one_period());
        backend.delay = Duration::from_millis(20);
        let backend = Arc::new(backend);
        let cases: Arc<[Case]> = vec![case("a"), case("b")].into();
        let records = run_jobs(Arc::clone(&backend), &label(), &cases, 3, 2).await;
        let order: Vec<(&str, u32)> = records
            .iter()
            .map(|r| (r.case_id.as_str(), r.repetition))
            .collect();
        assert_eq!(
            order,
            [("a", 0), ("a", 1), ("a", 2), ("b", 0), ("b", 1), ("b", 2)]
        );
        let peak = backend
            .max_in_flight
            .load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(peak, 2, "two documents should overlap, never three");
    }
}
