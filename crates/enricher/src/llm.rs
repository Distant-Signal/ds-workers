//! Client for the generic OpenAI-compatible Chat Completions REST API.
//! Deliberately vendor-agnostic: `base_url`/`api_key`/`model` are the only
//! things that vary between a local llama.cpp/vLLM/Ollama server and any
//! hosted provider that speaks the same schema.
//!
//! See
//! docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md
//! for the multi-period shape below (§1/§2), which replaces the original
//! flat `PrimaryExtraction`/`ScheduleWindow` pair from
//! docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A date range as stated (or inferred) by the primary pass. `None` on
/// either side is a real, distinct fact -- not "unknown":
///
/// - `from_date: None` -- the text doesn't state an explicit start for this
///   period; treat it as already active as of the incident's `first_seen_at`.
/// - `to_date: None` -- open-ended / no stated end.
///
/// Both fields, when present, are expected to already be resolved,
/// unambiguous UTC instants -- the *model* is responsible for year
/// inference (relative to the reference date threaded into
/// `extract_primary`'s user content), for treating a stated end day as
/// inclusive (that day's *following* midnight, Europe/London local time,
/// converted to UTC -- not that day's own midnight), and for interpreting a
/// bare date with no stated time-of-day as a Europe/London calendar-day
/// boundary before conversion to UTC. `PRIMARY_PROMPT` below spells out all
/// three conventions explicitly; this struct does no date arithmetic of its
/// own, matching the existing `ScheduleWindow` convention of trusting the
/// model to have already applied the stated local-time semantics.
///
/// Both fields use [`deserialize_lenient_date`] rather than deriving
/// straight through to `DateTime<Utc>`'s own `Deserialize` impl. The JSON
/// schema (`primary_schema` below) only constrains `from_date`/`to_date` to
/// `["string", "null"]` -- it cannot require RFC-3339 shape, so a model that
/// gets every OTHER field right can still emit an unparseable date string on
/// one period's one field (e.g. `"May 2026"` instead of a full timestamp).
/// Deriving straight through would fail `serde_json::from_str::<PrimaryExtraction>`
/// as a whole on that single bad string -- discarding this incident's
/// `category` and every other period's already-correct facts along with it.
/// `deserialize_lenient_date` instead degrades just that one field to
/// `None` (a real, valid state per this struct's own doc above) and logs a
/// warning, so one malformed date poisons only itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct DateRange {
    #[serde(default, deserialize_with = "deserialize_lenient_date")]
    pub from_date: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "deserialize_lenient_date")]
    pub to_date: Option<DateTime<Utc>>,
}

/// Decodes one `DateRange` field leniently: a valid RFC-3339 string parses
/// as today; `null` (or the key being absent entirely -- `#[serde(default)]`
/// on the field covers that) is `None`, same as before; but a PRESENT,
/// non-null value that ISN'T a parseable RFC-3339 timestamp -- the case
/// that used to fail the entire surrounding `PrimaryExtraction` deserialize
/// -- degrades to `None` with a `tracing::warn!` instead of propagating a
/// `serde::de::Error`. See `DateRange`'s own doc comment for why graceful
/// per-field degradation matters more here than strict validation: the
/// alternative is silently discarding a whole incident's correctly-extracted
/// category/resolution/severity signal over one field this schema can't
/// constrain the shape of.
fn deserialize_lenient_date<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match raw {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(s)) => match DateTime::parse_from_rfc3339(&s) {
            Ok(dt) => Some(dt.with_timezone(&Utc)),
            Err(err) => {
                tracing::warn!(
                    value = %s,
                    error = %err,
                    "primary extraction returned an unparseable date; treating this one field \
                     as null rather than discarding the whole extraction"
                );
                None
            }
        },
        Some(other) => {
            tracing::warn!(
                value = %other,
                "primary extraction returned a non-string, non-null date value; treating this \
                 one field as null rather than discarding the whole extraction"
            );
            None
        }
    })
}

/// Nested weekly time-of-day restriction *within* a period's `date_range`,
/// if any -- unchanged in shape from the original design, just scoped to
/// one period instead of the whole incident.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ScheduleWindow {
    /// ISO 8601 weekday numbers, 1 (Monday) through 7 (Sunday).
    pub days_of_week: Vec<u8>,
    /// "HH:MM", 24-hour, Europe/London local time.
    pub start_time: String,
    pub end_time: String,
}

/// One distinct period of an incident's text. A single-fact incident (the
/// overwhelming common case) always collapses to exactly one
/// `ExtractionPeriod` with `date_range: None`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ExtractionPeriod {
    /// Short, display/annotation-only text distinguishing this period's
    /// scope from the incident's other periods (e.g. "platform 2 closed,
    /// calls at platform 1"). Never matched against.
    pub scope_description: Option<String>,
    /// `None` = this "period" is really the whole-incident flat fact with
    /// no distinct date range (today's common case).
    pub date_range: Option<DateRange>,
    pub schedule_window: Option<ScheduleWindow>,
    /// `ongoing` | `residual` | `resolved`.
    pub resolution_status: String,
    /// `normal` | `moderate_disruption` | `severe_disruption` |
    /// `blocked_or_suspended`.
    pub apparent_severity: String,
    /// `rail_replacement_bus` | `no_scheduled_service` | `diversion` | `null`.
    /// Primary-pass-only, like `scope_description` -- no adversarial check
    /// exists for this field (design doc Decision 2), so it is copied
    /// through `combine::combine_periods` unchanged. Deliberately has NO
    /// `#[serde(default)]` -- every sibling field in this struct is
    /// required in the real schema, and this one follows the same
    /// contract (see the aggregator's own mirror struct for the opposite,
    /// backward-compat-driven choice).
    pub impact_type: Option<String>,
    /// NOT part of the primary pass's JSON schema -- the model never
    /// asserts its own confidence, exactly as in the original design.
    /// Populated by `combine::combine_periods` once the adversarial passes
    /// return. `#[serde(default)]` is load-bearing: the primary pass's
    /// response never sends these two fields, so deserializing it straight
    /// into `ExtractionPeriod` would otherwise hard-fail with a serde
    /// "missing field" error on every single response.
    #[serde(default)]
    pub resolution_status_confidence: String,
    #[serde(default)]
    pub severity_confidence: String,
}

/// The primary pass's parsed response. `periods` is documented as always
/// having at least one entry, but nothing in the JSON schema enforces that
/// (no `minItems` -- array-shape constraints aren't reliably enforceable
/// across backends, design §7 item 2) -- `extract_primary` enforces it in
/// Rust after parsing, treating an empty array as a hard parse failure (see
/// its body below).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(crate) struct PrimaryExtraction {
    pub category: String,
    pub periods: Vec<ExtractionPeriod>,
    /// How many periods `extract_primary` dropped to bring the response
    /// within `MAX_PERIODS`, if any. The model never sends this field --
    /// `#[serde(default)]` is load-bearing, the same precedent already
    /// established for `ExtractionPeriod.resolution_status_confidence`/
    /// `severity_confidence` (see that struct's own doc comment above).
    /// `extract_primary` always sets this explicitly after parsing
    /// (0 when under/at the cap); `process_incident` (`main.rs`) reads it
    /// to decide whether to log/count a truncation.
    #[serde(default)]
    pub dropped_period_count: usize,
}

/// One adversarial pass's per-period verdict, echoing back the ordinal
/// position (`period_index`) and the `scope_description` it was given
/// alongside its verdict -- both are required so `combine::combine_periods`
/// can assert positional alignment (not just length) before trusting the
/// array at all. A length-preserving but *reordered* response would
/// silently misattribute verdicts with no other detectable error (design
/// §2/§7 item 4); this is the ordinal-alignment mitigation, not the "single
/// enum" shape the original design's adversarial pass used.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(crate) struct AdversarialPeriodVerdict {
    pub period_index: usize,
    pub scope_description: Option<String>,
    pub resolution_status: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(crate) struct SeverityAdversarialPeriodVerdict {
    pub period_index: usize,
    pub scope_description: Option<String>,
    pub apparent_severity: String,
}

pub(crate) struct LlmClient {
    base_url: String,
    api_key: Option<String>,
    model: String,
    http: reqwest::Client,
    /// See [`ProviderPolicy`]; the default is today's behaviour.
    policy: ProviderPolicy,
    /// `Some` only when `ProviderPolicy::max_in_flight` is set. Held for the duration of one
    /// HTTP attempt only, never across a 429 back-off sleep.
    in_flight: Option<std::sync::Arc<tokio::sync::Semaphore>>,
}

/// One pass's raw chat-completion result: the unparsed `content` (or the
/// typed [`LlmCallError`] / other error that ended the call) plus how many
/// in-call retries ([`ProviderPolicy`]'s 429/gateway budgets) were spent
/// before it, and what each HTTP attempt did. The service only reads
/// `content` (through `extract_*`); the model-eval harness (`eval`) also
/// records `retries` and `attempts`.
pub(crate) struct RawCall {
    pub content: anyhow::Result<String>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read only by the test-only model-eval harness")
    )]
    pub retries: u32,
    /// Every HTTP attempt `chat_completion` made, in order (empty when the
    /// call failed before sending anything). Only observes the retry loop:
    /// collecting it changes no request, retry decision or metric.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read only by the test-only model-eval harness")
    )]
    pub attempts: Vec<Attempt>,
}

/// One HTTP attempt of a [`RawCall`], for the model-eval harness's perf
/// benchmark: a timeout that a gateway retry recovered is still a timeout
/// against the per-attempt request timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Attempt {
    /// `success`, or the attempt's [`LlmCallError::outcome_label`].
    pub outcome: &'static str,
    /// Waiting for an `in_flight` permit before sending.
    pub queued: std::time::Duration,
    /// The attempt itself (`send_once`): what `request_timeout` bounds.
    pub send: std::time::Duration,
    /// 429 back-off slept after this attempt, before the next one.
    pub backoff: std::time::Duration,
}

// ---------------------------------------------------------------------------
// Provider policy (2026-09-27): the per-provider knobs a slow, rate-limited
// hosted endpoint needs (reasoning models, 429s, a gateway that cuts calls
// at ~302 s). Every knob defaults to "off", so `LlmClient::new` sends
// byte-for-byte the same request as before and never retries in-call.
// Configured from `LLM_*` env vars in `config.rs`.
// ---------------------------------------------------------------------------

/// Longest `Retry-After` the in-call 429 retry will sleep for. A longer
/// server-requested wait fails the call instead (as a provider-transient
/// error), so one incident never pins a stream/sweep/reclaim loop -- and its
/// in-flight claim -- for hours; the reclaim loop retries it later.
pub(crate) const MAX_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(600);

/// Per-provider request/retry policy.
#[derive(Debug, Clone)]
pub(crate) struct ProviderPolicy {
    /// Sent as `max_tokens` when `Some`. Reasoning models need an explicit
    /// ceiling (Skye's GLM test: 8192) so a runaway reasoning trace is
    /// bounded rather than consuming the whole gateway window.
    pub max_tokens: Option<u32>,
    /// Sent as `reasoning_effort` when `Some` (GLM-5.3 on NVIDIA honours
    /// only `"low"`; its documented default is `"max"`).
    pub reasoning_effort: Option<String>,
    /// Cap on concurrent HTTP attempts across ALL callers sharing this
    /// client (stream loop + sweep + reclaim). `None` = unlimited, as today.
    pub max_in_flight: Option<usize>,
    /// Minimum wait before retrying a 429, even if `Retry-After` is shorter
    /// or absent.
    pub rate_limit_min_wait: std::time::Duration,
    /// In-call retries for 429. 0 = today's behaviour (fail the incident).
    pub max_rate_limit_retries: u32,
    /// In-call retries for 502/503/504 and client-side timeouts. 0 = today.
    ///
    /// Setting this above 0 also declares that a timeout/504 from this
    /// provider is a provider-side condition, not something the incident's
    /// text caused -- see [`LlmClient::is_provider_transient`].
    pub max_gateway_retries: u32,
}

impl Default for ProviderPolicy {
    fn default() -> Self {
        Self {
            max_tokens: None,
            reasoning_effort: None,
            max_in_flight: None,
            rate_limit_min_wait: std::time::Duration::from_secs(20),
            max_rate_limit_retries: 0,
            max_gateway_retries: 0,
        }
    }
}

/// Typed classification of one failed chat-completion attempt, so callers
/// can tell "the provider is busy" apart from "this text can't be
/// extracted". `main.rs` still sees an `anyhow::Error`; use
/// [`LlmClient::is_provider_transient`] to downcast.
#[derive(Debug)]
pub(crate) enum LlmCallError {
    RateLimited {
        retry_after: Option<std::time::Duration>,
    },
    GatewayUnavailable {
        status: u16,
    },
    ClientTimeout,
    Status {
        status: u16,
    },
    /// 200 OK but `content` was null/empty -- the typical failure of a
    /// reasoning model that spent its whole budget thinking.
    EmptyContent {
        finish_reason: Option<String>,
    },
    Other(anyhow::Error),
}

impl std::fmt::Display for LlmCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited { retry_after } => {
                write!(
                    f,
                    "LLM endpoint rate-limited (429, retry_after={retry_after:?})"
                )
            }
            Self::GatewayUnavailable { status } => {
                write!(f, "LLM endpoint gateway error/timeout ({status})")
            }
            Self::ClientTimeout => write!(f, "LLM request exceeded the client timeout"),
            Self::Status { status } => write!(f, "LLM endpoint returned HTTP {status}"),
            Self::EmptyContent { finish_reason } => write!(
                f,
                "LLM returned no content (finish_reason={finish_reason:?}); likely exhausted \
                 max_tokens on reasoning"
            ),
            Self::Other(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for LlmCallError {}

impl LlmCallError {
    /// Metric label for `enricher_llm_call_total{outcome=...}`.
    pub(crate) fn outcome_label(&self) -> &'static str {
        match self {
            Self::RateLimited { .. } => "rate_limited",
            Self::GatewayUnavailable { .. } => "gateway_error",
            Self::ClientTimeout => "timeout",
            Self::Status { .. } => "http_error",
            Self::EmptyContent { .. } => "empty_content",
            Self::Other(_) => "error",
        }
    }
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<std::time::Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(std::time::Duration::from_secs)
}

#[derive(Serialize)]
struct ChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage>,
    response_format: ResponseFormat,
    temperature: f32,
    /// Omitted from the wire when `None` (the default policy).
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    /// Omitted from the wire when `None` (the default policy).
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<&'a str>,
}

#[derive(Serialize)]
struct ChatMessage {
    role: &'static str,
    content: String,
}

#[derive(Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    kind: &'static str,
    json_schema: JsonSchemaSpec,
}

#[derive(Serialize)]
struct JsonSchemaSpec {
    name: &'static str,
    strict: bool,
    schema: serde_json::Value,
}

#[derive(Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ChatChoiceMessage {
    /// `Option`: reasoning models legitimately return
    /// `null` content when they run out of tokens mid-reasoning; that is
    /// now a typed `LlmCallError::EmptyContent` instead of an opaque serde
    /// error.
    #[serde(default)]
    content: Option<String>,
}

const PRIMARY_SCHEMA_NAME: &str = "incident_extraction";

/// Soft cap on `PrimaryExtraction::periods.len()`, enforced in Rust after
/// parsing rather than in the JSON schema (design §7 item 6: a `maxItems`
/// schema constraint may not be reliably enforced by every backend, and the
/// exact number is a prompt-engineering/eval question, not an architectural
/// one -- the design deliberately leaves the number unresolved and only
/// requires *some* enforcement point exists). 8 is chosen as generous
/// headroom over every motivating example in the design doc (the
/// Wandsworth Town fixture has 2; the design's own soft-cap sanity-check
/// fixture is described as "3+") while still catching runaway
/// over-segmentation (e.g. one period per sentence) before it reaches
/// storage.
const MAX_PERIODS: usize = 8;

/// Cap on each period's `scope_description`, in characters (SVC-13). It is
/// model-written text that is stored and displayed, and incident text (a
/// prompt-injection surface) goes straight into the prompt, so nothing but
/// the model's own output limit used to bound it. Enforced here, after
/// parsing, rather than as a schema `maxLength`: strict structured-output
/// modes may reject that keyword. Real ones are a short phrase.
pub(crate) const MAX_SCOPE_DESCRIPTION_CHARS: usize = 500;

/// Truncates `scope` to [`MAX_SCOPE_DESCRIPTION_CHARS`] characters (never
/// splitting a UTF-8 character), marking the cut with an ellipsis.
fn bound_scope_description(scope: &mut Option<String>) {
    let Some(text) = scope else {
        return;
    };
    if let Some((cut, _)) = text.char_indices().nth(MAX_SCOPE_DESCRIPTION_CHARS) {
        // Leave room for the marker so the result stays within the cap.
        let keep = text
            .char_indices()
            .nth(MAX_SCOPE_DESCRIPTION_CHARS - 1)
            .map_or(cut, |(i, _)| i);
        text.truncate(keep);
        text.push('\u{2026}');
    }
}

/// JSON schema for the primary pass. Deliberately omits
/// `resolution_status_confidence`/`severity_confidence` (design §1) --
/// those don't exist until the combination step runs against the
/// adversarial passes' output.
fn primary_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "category": { "type": "string" },
            "periods": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "scope_description": { "type": ["string", "null"] },
                        "date_range": {
                            "type": ["object", "null"],
                            "properties": {
                                "from_date": { "type": ["string", "null"] },
                                "to_date": { "type": ["string", "null"] }
                            },
                            "required": ["from_date", "to_date"]
                        },
                        "schedule_window": {
                            "type": ["object", "null"],
                            "properties": {
                                "days_of_week": { "type": "array", "items": { "type": "integer", "minimum": 1, "maximum": 7 } },
                                "start_time": { "type": "string" },
                                "end_time": { "type": "string" }
                            },
                            "required": ["days_of_week", "start_time", "end_time"]
                        },
                        "resolution_status": { "type": "string", "enum": ["ongoing", "residual", "resolved"] },
                        "apparent_severity": { "type": "string", "enum": ["normal", "moderate_disruption", "severe_disruption", "blocked_or_suspended"] },
                        "impact_type": {
                            "type": ["string", "null"],
                            "enum": ["rail_replacement_bus", "no_scheduled_service", "diversion", null]
                        }
                    },
                    "required": ["scope_description", "date_range", "schedule_window", "resolution_status", "apparent_severity", "impact_type"]
                }
            }
        },
        "required": ["category", "periods"]
    })
}

const PRIMARY_PROMPT: &str = "You extract structured facts from UK National Rail Knowledgebase incident \
    text. Read the summary and description exactly as given -- do not speculate beyond what the text \
    states. The text describes one incident that may cover one or MORE distinct periods; segment it into \
    the `periods` array. Only split into more than one period where the text itself demarcates a distinct \
    date range and/or a distinct scope/impact -- if the entire text describes one continuous fact with no \
    clearly distinct sub-periods, return a single-element `periods` array with `date_range: null`. Err \
    toward fewer periods when in doubt: do NOT split for stylistic variation, repeated wording, or several \
    stations/lines listed under one shared date range -- that is still one period. When a shared date \
    range covers multiple named route legs, keep them in one period only if every leg is treated \
    identically -- same substitute service or lack of one, same `apparent_severity`, same \
    `resolution_status` -- and describe every affected leg together in `scope_description`. Split into a \
    separate period per leg (or per leg-and-day-of-week combination) whenever the text states a genuinely \
    different treatment for one leg or for specific days within the range -- e.g. one leg gets a rail \
    replacement bus while another has no scheduled service at all, or a leg's rule only applies on certain \
    days of the week and a different rule applies on the rest. A no-scheduled-service statement is never \
    the same fact as a rail-replacement-bus statement, even when both fall inside the same date range and \
    even when the text presents them as neighboring clauses -- do not merge them, and do not let the shared \
    date range alone suggest they are one period. `periods` must always \
    contain at least one element. \
    For each period: `scope_description` is short, display-only text distinguishing what's different about \
    that period from the incident's other periods (e.g. \"platform 2 closed, calls at platform 1\"), or \
    null if there's only one period. `resolution_status` is `resolved` only if the text explicitly says the \
    disruption/root cause has ended; `residual` if it says the cause is fixed but knock-on effects continue; \
    `ongoing` otherwise, including whenever the text doesn't clearly say either way -- judge this \
    per-period, since different periods of the same incident can genuinely have different resolution \
    states. `apparent_severity` is your own read of how severe that period's disruption sounds, independent \
    of any specific keywords: `blocked_or_suspended` if any line, route, or station is described as blocked, \
    suspended, or closed to trains; `severe_disruption` if the text describes major/widespread delays, \
    cancellations, or long journey-time increases without an outright blockage; `moderate_disruption` for a \
    noticeable but contained impact; `normal` for routine minor delay language with no sign of broader \
    impact. \
    `date_range` MUST be populated whenever the text states an explicit date, even approximately -- never \
    leave it null and describe the dates only in `scope_description` instead; `scope_description` is for \
    what's DIFFERENT about the period (platform, direction, route), not a place to restate dates you should \
    have structured. `date_range` is null ONLY when the text truly states no date at all for that period. \
    When present, `from_date`/`to_date` must each be a fully-resolved ISO-8601 UTC timestamp string (or null \
    on either side, meaning no stated start/end respectively). An ETA (\"normal service expected to resume \
    from 18:00\") is expressed as a period whose `date_range.to_date` is that time, with `from_date: null` \
    -- do not add a separate field for it. Apply these conventions when resolving a stated date into that \
    timestamp: (1) Year inference -- the text is given together with a reference date this incident was \
    first reported around; if a date has no stated year, resolve it to whichever occurrence of that \
    month/day falls closest to the reference date -- do NOT invent an unrelated year. (2) Inclusivity -- a \
    stated end day (e.g. \"to Sunday 26 July\") means *through* that day, so its resolved `to_date` must be \
    the *following* day's 00:00 in Europe/London local time, converted to UTC -- not that day's own 00:00. \
    (3) Timezone -- a bare date with no stated time-of-day is a Europe/London calendar-day boundary; convert \
    it to UTC accounting for GMT/BST as appropriate for that date. `schedule_window` is null unless the text \
    states a weekly time-of-day restriction narrower than the period's own date range. \
    When in doubt about `resolution_status`, choose `ongoing` -- never guess `resolved` or `residual` from \
    tone, length, or the absence of further detail; only an explicit statement that the disruption or its \
    root cause has ended justifies anything other than `ongoing`. \
    Worked example, reference date 2026-03-01T00:00:00Z: input \"Monday 6 April to Friday 15 May: Platform 3 \
    at Clapham Junction is closed, trains call at platform 4. Saturday 16 May to Sunday 14 June: Platform 5 \
    is closed, trains call at platform 6.\" segments into exactly two periods -- period 1: \
    `scope_description` \"platform 3 closed, calls at platform 4\", `date_range` `{\"from_date\": \
    \"2026-04-05T23:00:00Z\", \"to_date\": \"2026-05-15T23:00:00Z\"}` (2026 because that's the closest \
    occurrence to the March 2026 reference date; `to_date` is the day AFTER the stated 15 May end; both \
    dates fall in BST, UTC+1, so each Europe/London midnight is 23:00Z on the previous UTC day -- in GMT \
    it would be 00:00Z), \
    `schedule_window: null`, `resolution_status: \"ongoing\"` (no statement that it has ended); period 2: \
    `scope_description` \"platform 5 closed, calls at platform 6\", `date_range` `{\"from_date\": \
    \"2026-05-15T23:00:00Z\", \"to_date\": \"2026-06-14T23:00:00Z\"}`, `resolution_status: \"ongoing\"`. Note \
    both periods got real `date_range` values -- never null when dates are stated -- and neither was marked \
    `resolved` just because the text is matter-of-fact. \
    Second worked example, reference date 2026-08-01T00:00:00Z: input \"From Saturday 29 August to Friday \
    11 September, buses replace trains between Barrhead and Kilmarnock / Dumfries. Monday to Saturday \
    during this period, buses operate between Kilmarnock and Troon, where passengers can connect with \
    trains to / from Ayr. No scheduled services operate between Kilmarnock and Ayr / Stranraer on \
    Sundays.\" segments into exactly three periods, all sharing the same overall date range but none \
    merged into one, because each names a different leg and/or a different treatment: period 1 -- \
    `scope_description` \"buses replace trains, Barrhead to Kilmarnock / Dumfries\", `date_range` \
    `{\"from_date\": \"2026-08-28T23:00:00Z\", \"to_date\": \"2026-09-11T23:00:00Z\"}` (BST again), \
    `schedule_window: null` (applies every day of the range), `apparent_severity: \"severe_disruption\"`; period 2 -- \
    `scope_description` \"buses operate Kilmarnock to Troon, connecting to Ayr trains\", same `date_range`, \
    `schedule_window` `{\"days_of_week\": [1,2,3,4,5,6], \"start_time\": \"00:00\", \"end_time\": \"23:59\"}` \
    (Monday-Saturday only), `apparent_severity: \"severe_disruption\"`; period 3 -- `scope_description` \"no \
    scheduled service, Kilmarnock to Ayr / Stranraer\", same `date_range`, `schedule_window` \
    `{\"days_of_week\": [7], \"start_time\": \"00:00\", \"end_time\": \"23:59\"}` (Sunday only), \
    `apparent_severity: \"blocked_or_suspended\"` (a full withdrawal is more severe than a bus substitute, \
    not the same fact restated). Note periods 2 and 3 are NOT merged despite sharing both the date range \
    and the same underlying Kilmarnock-Ayr/Stranraer leg -- the text states two different treatments for \
    different days, which is exactly the case that must still split even though 'several things under one \
    shared date range' would otherwise argue for merging. \
    `impact_type` is `rail_replacement_bus` if that period states that buses (or another road vehicle) \
    replace, substitute for, or operate in place of trains for some or all of the affected journey -- \
    regardless of the exact phrasing used (\"buses replace trains,\" \"a replacement bus service,\" \"buses \
    will operate between X and Y\"). It is `no_scheduled_service` if that period states plainly that no \
    trains (and no replacement service) run at all -- do not use `rail_replacement_bus` for this; a \
    withdrawn service and a substitute service are different facts even when both are severe. It is \
    `diversion` if that period states trains are running via a different route than usual, without a bus \
    substitute. Use `null` for any period that does not state one of these three specific facts -- an \
    ordinary delay or cancellation notice with no stated substitute-service arrangement is `null`, not a \
    forced guess. \
    Worked example, reference date 2026-08-01T00:00:00Z: input \"Buses operate between Kilmarnock and Troon, \
    where passengers can connect with trains to / from Ayr, Saturdays 29 August to 12 September. No \
    scheduled services operate between Kilmarnock and Ayr / Stranraer on Sundays 30 August to 13 September.\" \
    segments into two periods, each with its own `schedule_window` restricting it to the stated day -- period \
    1: `scope_description` \"Saturday bus, Kilmarnock-Troon\", `schedule_window` restricted to Saturday, \
    `impact_type: \"rail_replacement_bus\"`; period 2: `scope_description` \"Sunday no service, \
    Kilmarnock-Ayr/Stranraer\", `schedule_window` restricted to Sunday, `impact_type: \"no_scheduled_service\"`. \
    Note these are two periods with two different `impact_type` values, not one merged period and not the \
    same tag applied to both -- a substitute bus service and a full withdrawal are different facts even on \
    immediately adjacent days of the same date range.";

const ADVERSARIAL_SCHEMA_NAME: &str = "adversarial_resolution_check";

/// Array length is deliberately not schema-enforced (design §7 item 2/3):
/// the invariant "same length as the primary pass's `periods`" can only be
/// checked in Rust after both calls return.
fn adversarial_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "periods": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "period_index": { "type": "integer" },
                        "scope_description": { "type": ["string", "null"] },
                        "resolution_status": { "type": "string", "enum": ["ongoing", "residual", "resolved"] }
                    },
                    "required": ["period_index", "scope_description", "resolution_status"]
                }
            }
        },
        "required": ["periods"]
    })
}

const ADVERSARIAL_PROMPT: &str = "You are reviewing a UK National Rail incident report with a specific \
    job: argue for the most cautious reading. You are given the incident's summary/description text plus a \
    list of periods already segmented out of that text (each with its `period_index`, `scope_description`, \
    `date_range`, and `schedule_window`). For EACH of those periods, in the SAME order, argue the most \
    cautious reading and return one verdict per period: assume `ongoing` unless the text gives clear, \
    explicit, unambiguous evidence that specific period is `resolved` or `residual`. Do not infer \
    resolution from silence, from a lack of new updates, or from an optimistic tone -- only from an \
    explicit statement that that period's issue is fixed or over. Your response's `periods` array must have \
    exactly one element per period you were given, in the same order, and each element must echo back the \
    exact `period_index` and `scope_description` you were given for that period -- do not renumber, \
    reorder, or reword them.";

#[derive(Deserialize)]
struct AdversarialExtraction {
    periods: Vec<AdversarialPeriodVerdict>,
}

const SEVERITY_ADVERSARIAL_SCHEMA_NAME: &str = "adversarial_severity_check";

fn severity_adversarial_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "periods": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "period_index": { "type": "integer" },
                        "scope_description": { "type": ["string", "null"] },
                        "apparent_severity": {
                            "type": "string",
                            "enum": ["normal", "moderate_disruption", "severe_disruption", "blocked_or_suspended"]
                        }
                    },
                    "required": ["period_index", "scope_description", "apparent_severity"]
                }
            }
        },
        "required": ["periods"]
    })
}

const SEVERITY_ADVERSARIAL_PROMPT: &str = "You are reviewing a UK National Rail incident report with a \
    specific job: argue for the LEAST severe reading each period can honestly support. You are given the \
    incident's summary/description text plus a list of periods already segmented out of that text (each \
    with its `period_index`, `scope_description`, `date_range`, and `schedule_window`). For EACH of those \
    periods, in the SAME order, do not assume a full blockage, suspension, or major disruption unless the \
    text gives clear, explicit, unambiguous evidence of one for that specific period -- vague or \
    routine-sounding delay language should read as `normal` or `moderate_disruption`, not escalated on tone \
    or length alone. Your response's `periods` array must have exactly one element per period you were \
    given, in the same order, and each element must echo back the exact `period_index` and \
    `scope_description` you were given for that period -- do not renumber, reorder, or reword them.";

/// A short hash over every pass's system prompt, schema name and schema,
/// for the model-eval harness: each record stores it, so a replay can warn
/// when saved outputs came from different prompts than the current code.
/// (The user-content wrappers in the `*_raw` methods aren't covered.)
#[cfg(test)]
pub(crate) fn prompt_fingerprint() -> String {
    let mut text = String::new();
    for (prompt, schema_name, schema) in [
        (PRIMARY_PROMPT, PRIMARY_SCHEMA_NAME, primary_schema()),
        (
            ADVERSARIAL_PROMPT,
            ADVERSARIAL_SCHEMA_NAME,
            adversarial_schema(),
        ),
        (
            SEVERITY_ADVERSARIAL_PROMPT,
            SEVERITY_ADVERSARIAL_SCHEMA_NAME,
            severity_adversarial_schema(),
        ),
    ] {
        for part in [prompt, schema_name, &schema.to_string()] {
            text.push_str(part);
            text.push('\0');
        }
    }
    let mut hash = common::text_hash::text_hash("enricher-prompts", &text);
    hash.truncate(16);
    hash
}

#[derive(Deserialize)]
struct SeverityAdversarialExtraction {
    periods: Vec<SeverityAdversarialPeriodVerdict>,
}

// `request_timeout` (below) is the per-request ceiling on an LLM call,
// configured via `Config::llm_request_timeout_secs` (see `config.rs`) --
// reqwest applies NO request timeout by default, and both callers of this
// client -- the stream consumer loop and the hourly sweep -- process
// incidents strictly serially, so a single hung endpoint would stall ALL
// enrichment indefinitely rather than just losing one incident.
// Configurable rather than fixed because real self-hosted endpoints vary
// widely in latency (a small local model on modest hardware, a remote
// tunnel, load from other callers) -- a fixed 60s was observed too tight
// against a real remote server. A timed-out request surfaces as an
// ordinary `Err`, which `process_incident` already logs and moves past;
// `main.rs`'s reclaim loop retries it once it's been idle long enough.

impl LlmClient {
    #[expect(
        clippy::expect_used,
        reason = "a client builder with static settings fails only if TLS init does, which is fatal"
    )]
    pub(crate) fn new(
        base_url: String,
        api_key: Option<String>,
        model: String,
        request_timeout: std::time::Duration,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            // Only fails if the TLS backend can't initialize, which would
            // break every request anyway -- there is no useful degraded mode.
            .expect("reqwest client with a timeout must build");
        Self {
            base_url,
            api_key,
            model,
            http,
            policy: ProviderPolicy::default(),
            in_flight: None,
        }
    }

    /// Opts into a non-default [`ProviderPolicy`].
    pub(crate) fn with_provider_policy(mut self, policy: ProviderPolicy) -> Self {
        self.in_flight = policy
            .max_in_flight
            .map(|n| std::sync::Arc::new(tokio::sync::Semaphore::new(n.max(1))));
        self.policy = policy;
        self
    }

    /// Whether an error returned by any `extract_*` is a provider-side
    /// transient rather than a failure attributable to the incident's text,
    /// so it must not feed `RetryBackoff`'s per-text backoff (30 min -> 24 h):
    ///
    /// - 429 and 502/503 always are -- nothing about the text causes them.
    /// - A client timeout or 504 is ambiguous: a runaway generation on one
    ///   particular text also ends that way. Under the default policy it
    ///   keeps backing off exactly as before; once the operator opts into
    ///   gateway retries (`max_gateway_retries > 0`, i.e. a provider whose
    ///   gateway is known to cut slow calls) it counts as transient.
    #[expect(
        clippy::match_same_arms,
        reason = "separate arms document distinct cases"
    )]
    pub(crate) fn is_provider_transient(&self, err: &anyhow::Error) -> bool {
        match err.downcast_ref::<LlmCallError>() {
            Some(LlmCallError::RateLimited { .. }) => true,
            Some(
                LlmCallError::GatewayUnavailable { status: 504 } | LlmCallError::ClientTimeout,
            ) => self.policy.max_gateway_retries > 0,
            Some(LlmCallError::GatewayUnavailable { .. }) => true,
            _ => false,
        }
    }

    /// One pass's chat completion, in-call retries included. Returns the raw
    /// `content` string (not yet parsed) and how many in-call retries the
    /// provider policy spent on it -- see [`RawCall`].
    async fn chat_completion(
        &self,
        system_prompt: &str,
        user_content: String,
        schema_name: &'static str,
        schema: serde_json::Value,
    ) -> RawCall {
        let request = ChatCompletionRequest {
            model: &self.model,
            messages: vec![
                ChatMessage {
                    role: "system",
                    content: system_prompt.to_string(),
                },
                ChatMessage {
                    role: "user",
                    content: user_content,
                },
            ],
            response_format: ResponseFormat {
                kind: "json_schema",
                json_schema: JsonSchemaSpec {
                    name: schema_name,
                    strict: true,
                    schema,
                },
            },
            temperature: 0.0,
            max_tokens: self.policy.max_tokens,
            reasoning_effort: self.policy.reasoning_effort.as_deref(),
        };

        // Bounded in-call retry for provider-transient failures. With the
        // default policy both budgets are 0, so the first failure is
        // returned exactly as before.
        let mut rate_limit_retries = 0;
        let mut gateway_retries = 0;
        // Observation only (see `RawCall::attempts`): timings around the
        // unchanged retry loop.
        let mut attempts: Vec<Attempt> = Vec::with_capacity(1);
        loop {
            let retries = rate_limit_retries + gateway_retries;
            let (attempt, timing) = match self.limited_send(&request).await {
                Ok(pair) => pair,
                Err(err) => {
                    return RawCall {
                        content: Err(err),
                        retries,
                        attempts,
                    };
                }
            };
            attempts.push(timing);
            match attempt {
                Ok(content) => {
                    return RawCall {
                        content: Ok(content),
                        retries,
                        attempts,
                    };
                }
                Err(LlmCallError::RateLimited { retry_after })
                    if rate_limit_retries < self.policy.max_rate_limit_retries
                        && retry_after.is_none_or(|wait| wait <= MAX_RETRY_AFTER) =>
                {
                    rate_limit_retries += 1;
                    let wait = retry_after
                        .unwrap_or_default()
                        .max(self.policy.rate_limit_min_wait);
                    tracing::warn!(?wait, attempt = rate_limit_retries, "LLM 429; backing off");
                    let sleep_start = tokio::time::Instant::now();
                    tokio::time::sleep(wait).await;
                    if let Some(last) = attempts.last_mut() {
                        last.backoff = sleep_start.elapsed();
                    }
                }
                Err(
                    err @ (LlmCallError::GatewayUnavailable { .. } | LlmCallError::ClientTimeout),
                ) if gateway_retries < self.policy.max_gateway_retries => {
                    gateway_retries += 1;
                    tracing::warn!(error = %err, attempt = gateway_retries, "LLM gateway failure; retrying");
                }
                Err(err) => {
                    return RawCall {
                        content: Err(err.into()),
                        retries,
                        attempts,
                    };
                }
            }
        }
    }

    /// One HTTP attempt under the `in_flight` limit (the permit is held
    /// for the attempt only), plus its [`Attempt`] timing. `Err` only if
    /// the limiter is closed.
    async fn limited_send(
        &self,
        request: &ChatCompletionRequest<'_>,
    ) -> anyhow::Result<(Result<String, LlmCallError>, Attempt)> {
        let queue_start = tokio::time::Instant::now();
        let _permit = match &self.in_flight {
            Some(sem) => Some(
                sem.acquire()
                    .await
                    .map_err(|err| anyhow::anyhow!("in-flight limiter closed: {err}"))?,
            ),
            None => None,
        };
        let send_start = tokio::time::Instant::now();
        let attempt = self.send_once(request).await;
        let timing = Attempt {
            outcome: match &attempt {
                Ok(_) => "success",
                Err(err) => err.outcome_label(),
            },
            queued: send_start.duration_since(queue_start),
            send: send_start.elapsed(),
            backoff: std::time::Duration::ZERO,
        };
        Ok((attempt, timing))
    }

    /// One HTTP attempt, classified.
    async fn send_once(&self, request: &ChatCompletionRequest<'_>) -> Result<String, LlmCallError> {
        let mut req = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .json(request);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }

        let response = req.send().await.map_err(|err| {
            if err.is_timeout() {
                LlmCallError::ClientTimeout
            } else {
                LlmCallError::Other(err.into())
            }
        })?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(LlmCallError::RateLimited {
                retry_after: parse_retry_after(response.headers()),
            });
        }
        if matches!(status.as_u16(), 502..=504) {
            return Err(LlmCallError::GatewayUnavailable {
                status: status.as_u16(),
            });
        }
        if !status.is_success() {
            return Err(LlmCallError::Status {
                status: status.as_u16(),
            });
        }
        let body: ChatCompletionResponse = response.json().await.map_err(|err| {
            if err.is_timeout() {
                LlmCallError::ClientTimeout
            } else {
                LlmCallError::Other(err.into())
            }
        })?;
        let choice = body.choices.into_iter().next().ok_or_else(|| {
            LlmCallError::Other(anyhow::anyhow!("chat completion response had no choices"))
        })?;
        match choice.message.content {
            Some(content) if !content.trim().is_empty() => Ok(content),
            _ => Err(LlmCallError::EmptyContent {
                finish_reason: choice.finish_reason,
            }),
        }
    }

    /// `reference_date` is the incident's `first_seen_at` (or, if the
    /// caller has nothing better, the current time) -- threaded into the
    /// user content so the model can resolve year-less dates in the text
    /// against a concrete anchor (design §1's "year inference" convention).
    pub(crate) async fn extract_primary(
        &self,
        summary: &str,
        description: &str,
        reference_date: DateTime<Utc>,
    ) -> anyhow::Result<PrimaryExtraction> {
        let content = self
            .primary_raw(summary, description, reference_date)
            .await
            .content?;
        parse_primary(&content)
    }

    /// The primary pass's request (real prompt and schema) without the
    /// parse step: `extract_primary` is exactly this followed by
    /// [`parse_primary`]. Split out so the model-eval harness (`eval`) can
    /// record the raw output and score it later, offline, through the same
    /// parse path.
    pub(crate) async fn primary_raw(
        &self,
        summary: &str,
        description: &str,
        reference_date: DateTime<Utc>,
    ) -> RawCall {
        let user_content = format!(
            "This incident was first reported around {}. Resolve any year-less date in the text below \
             relative to that reference date.\nSummary: {summary}\nDescription: {description}",
            reference_date.to_rfc3339()
        );
        self.chat_completion(
            PRIMARY_PROMPT,
            user_content,
            PRIMARY_SCHEMA_NAME,
            primary_schema(),
        )
        .await
    }

    /// `periods` is the primary pass's already-segmented period list
    /// (design §2) -- the adversarial pass does not re-derive periods, it
    /// only returns a per-period resolution-status verdict, index-aligned
    /// and echoing back each period's `period_index`/`scope_description`.
    pub(crate) async fn extract_adversarial(
        &self,
        summary: &str,
        description: &str,
        periods: &[ExtractionPeriod],
    ) -> anyhow::Result<Vec<AdversarialPeriodVerdict>> {
        let content = self
            .adversarial_raw(summary, description, periods)
            .await
            .content?;
        parse_adversarial(&content)
    }

    /// `extract_adversarial` minus the parse step (see [`Self::primary_raw`]).
    pub(crate) async fn adversarial_raw(
        &self,
        summary: &str,
        description: &str,
        periods: &[ExtractionPeriod],
    ) -> RawCall {
        let user_content = match build_period_user_content(summary, description, periods) {
            Ok(content) => content,
            Err(err) => {
                return RawCall {
                    content: Err(err),
                    retries: 0,
                    attempts: Vec::new(),
                };
            }
        };
        self.chat_completion(
            ADVERSARIAL_PROMPT,
            user_content,
            ADVERSARIAL_SCHEMA_NAME,
            adversarial_schema(),
        )
        .await
    }

    pub(crate) async fn extract_severity_adversarial(
        &self,
        summary: &str,
        description: &str,
        periods: &[ExtractionPeriod],
    ) -> anyhow::Result<Vec<SeverityAdversarialPeriodVerdict>> {
        let content = self
            .severity_adversarial_raw(summary, description, periods)
            .await
            .content?;
        parse_severity_adversarial(&content)
    }

    /// `extract_severity_adversarial` minus the parse step (see
    /// [`Self::primary_raw`]).
    pub(crate) async fn severity_adversarial_raw(
        &self,
        summary: &str,
        description: &str,
        periods: &[ExtractionPeriod],
    ) -> RawCall {
        let user_content = match build_period_user_content(summary, description, periods) {
            Ok(content) => content,
            Err(err) => {
                return RawCall {
                    content: Err(err),
                    retries: 0,
                    attempts: Vec::new(),
                };
            }
        };
        self.chat_completion(
            SEVERITY_ADVERSARIAL_PROMPT,
            user_content,
            SEVERITY_ADVERSARIAL_SCHEMA_NAME,
            severity_adversarial_schema(),
        )
        .await
    }
}

/// Parses (and post-processes) the primary pass's raw `content`: the
/// non-empty check, the `MAX_PERIODS` cap and the scope-description bound.
/// Pure, so recorded model output can be re-scored offline exactly as the
/// service would have read it.
pub(crate) fn parse_primary(content: &str) -> anyhow::Result<PrimaryExtraction> {
    let mut extraction: PrimaryExtraction = serde_json::from_str(content)
        .map_err(|err| anyhow::anyhow!("primary extraction returned malformed JSON: {err}"))?;
    if extraction.periods.is_empty() {
        // Design §1: an empty `periods` array parses without a schema
        // error (no `minItems`), but recording it as a "successful"
        // extraction would permanently short-circuit `process_incident`'s
        // unchanged-text guard for this incident on every subsequent
        // sweep/reclaim pass. Treat it as a hard parse failure instead --
        // discarded, existing columns untouched, sweep retries later.
        anyhow::bail!("primary extraction returned an empty `periods` array");
    }
    // Decision 3 of docs/superpowers/specs/2026-09-01-enricher-period-cap-remediation-design.md:
    // an over-cap response used to be a hard failure here (discarded,
    // sweep retries forever, all NLP-derived severity signal lost for
    // this incident). Instead, keep the MAX_PERIODS most-severe/soonest
    // periods and let extraction succeed -- `dropped_period_count`
    // records how many were cut, so `process_incident` (main.rs) can
    // log/count it without any downstream step (extract_adversarial,
    // extract_severity_adversarial, combine::combine_periods,
    // queries::write_extraction) needing to know anything unusual
    // happened; they only ever see an already-in-bounds `periods` list.
    let original_count = extraction.periods.len();
    if original_count > MAX_PERIODS {
        extraction.periods = select_periods_within_cap(extraction.periods);
    }
    extraction.dropped_period_count = original_count.saturating_sub(MAX_PERIODS);
    // Before the adversarial passes, which echo each period's scope back
    // and are checked against exactly what was sent.
    for period in &mut extraction.periods {
        bound_scope_description(&mut period.scope_description);
    }
    Ok(extraction)
}

/// Parses the resolution-adversarial pass's raw `content` (see
/// [`parse_primary`]).
pub(crate) fn parse_adversarial(content: &str) -> anyhow::Result<Vec<AdversarialPeriodVerdict>> {
    let extraction: AdversarialExtraction = serde_json::from_str(content)
        .map_err(|err| anyhow::anyhow!("adversarial extraction returned malformed JSON: {err}"))?;
    Ok(extraction.periods)
}

/// Parses the severity-adversarial pass's raw `content` (see
/// [`parse_primary`]).
pub(crate) fn parse_severity_adversarial(
    content: &str,
) -> anyhow::Result<Vec<SeverityAdversarialPeriodVerdict>> {
    let extraction: SeverityAdversarialExtraction =
        serde_json::from_str(content).map_err(|err| {
            anyhow::anyhow!("severity adversarial extraction returned malformed JSON: {err}")
        })?;
    Ok(extraction.periods)
}

/// `None` (whether from a wholly absent `date_range`, or an explicit
/// `date_range.from_date: null`) sorts first in the truncation selection
/// below -- both already mean "treat as already active" per `DateRange`'s
/// own doc comment (this file, lines 18-19), the most urgent reading.
/// `Option<T>`'s derived `Ord` already puts `None` before `Some(_)`, so no
/// custom comparator is needed for that part.
fn effective_from_date(period: &ExtractionPeriod) -> Option<DateTime<Utc>> {
    period.date_range.as_ref().and_then(|range| range.from_date)
}

/// Keeps the `MAX_PERIODS` periods ranked highest by
/// `(severity_hint_rank(apparent_severity) descending, effective_from_date
/// ascending, None-first)` -- Decision 3 of
/// docs/superpowers/specs/2026-09-01-enricher-period-cap-remediation-design.md.
/// `sort_by_key` is a stable sort, so periods tied on both keys keep the
/// model's own original relative order rather than being reordered
/// arbitrarily. Called only when `periods.len() > MAX_PERIODS`; a caller
/// passing an already-in-bounds list is a no-op that still runs the sort
/// (cheap for at most a handful of periods, and keeping the function
/// total rather than adding an unused early-return branch is simpler).
fn select_periods_within_cap(mut periods: Vec<ExtractionPeriod>) -> Vec<ExtractionPeriod> {
    periods.sort_by_key(|period| {
        (
            std::cmp::Reverse(crate::combine::severity_hint_rank(
                &period.apparent_severity,
            )),
            effective_from_date(period),
        )
    });
    periods.truncate(MAX_PERIODS);
    periods
}

/// Builds the shared user-content shape both adversarial passes send: the
/// original text, plus the primary pass's period list stripped down to just
/// `period_index`/`scope_description`/`date_range`/`schedule_window` --
/// `resolution_status`/`apparent_severity` are deliberately NOT included,
/// since re-showing the primary pass's own verdict back to the adversarial
/// pass would bias it toward agreeing rather than independently arguing the
/// opposite case (design §2).
fn build_period_user_content(
    summary: &str,
    description: &str,
    periods: &[ExtractionPeriod],
) -> anyhow::Result<String> {
    let skeleton: Vec<serde_json::Value> = periods
        .iter()
        .enumerate()
        .map(|(index, period)| {
            serde_json::json!({
                "period_index": index,
                "scope_description": period.scope_description,
                "date_range": period.date_range,
                "schedule_window": period.schedule_window,
            })
        })
        .collect();
    let skeleton_json = serde_json::to_string(&skeleton)?;
    Ok(format!(
        "Summary: {summary}\nDescription: {description}\nPeriods (respond with exactly {} verdict(s), in \
         this exact order, echoing period_index and scope_description exactly as given for each):\n{skeleton_json}",
        periods.len()
    ))
}

/// Test-only client for the ignored live evals (`tests::live_eval_*` here
/// and `replay_eval`): `LLM_BASE_URL`/`LLM_MODEL`/`LLM_API_KEY`, a
/// `LIVE_EVAL_TIMEOUT_SECS` ceiling (default 180 s -- overridable per run,
/// e.g. to give a cold-starting model extra room), and the SAME provider
/// policy env vars the service parses (`config::ProviderPolicyConfig`), so a
/// reasoning model gets its `reasoning_effort`/`max_tokens` in an eval too.
#[cfg(test)]
pub(crate) fn live_client_from_env() -> LlmClient {
    use clap::Parser;

    let base_url = std::env::var("LLM_BASE_URL").expect("LLM_BASE_URL must be set for live eval");
    let api_key = std::env::var("LLM_API_KEY").ok();
    let model = std::env::var("LLM_MODEL").expect("LLM_MODEL must be set for live eval");
    let timeout_secs: u64 = std::env::var("LIVE_EVAL_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(180);
    let policy = crate::config::ProviderPolicyConfig::parse_from(["live-eval"]).policy();
    LlmClient::new(
        base_url,
        api_key,
        model,
        std::time::Duration::from_secs(timeout_secs),
    )
    .with_provider_policy(policy)
}

#[cfg(test)]
mod scope_bound_tests {
    use super::*;

    /// SVC-13: an over-long scope description is cut to the cap (with a
    /// marker, on a character boundary); a normal one is untouched.
    #[test]
    fn scope_description_is_truncated_to_the_cap() {
        let mut short = Some("platform 2 closed".to_string());
        bound_scope_description(&mut short);
        assert_eq!(short.as_deref(), Some("platform 2 closed"));

        let mut none = None;
        bound_scope_description(&mut none);
        assert_eq!(none, None);

        let exact = "a".repeat(MAX_SCOPE_DESCRIPTION_CHARS);
        let mut at_cap = Some(exact.clone());
        bound_scope_description(&mut at_cap);
        assert_eq!(at_cap.as_deref(), Some(exact.as_str()));

        // Multi-byte characters: the cut must land on a char boundary.
        let mut huge = Some("\u{e9}".repeat(1_000_000));
        bound_scope_description(&mut huge);
        let huge = huge.unwrap();
        assert_eq!(huge.chars().count(), MAX_SCOPE_DESCRIPTION_CHARS);
        assert!(huge.ends_with('\u{2026}'));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

    fn reference_date() -> DateTime<Utc> {
        "2026-04-10T09:00:00Z".parse().unwrap()
    }

    #[tokio::test]
    async fn extract_primary_parses_a_single_flat_period() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "signal_failure",
                            "periods": [{
                                "scope_description": null,
                                "date_range": null,
                                "schedule_window": null,
                                "resolution_status": "resolved",
                                "apparent_severity": "normal",
                                "impact_type": null
                            }]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary(
                "Signal failure at Reading",
                "Now resolved",
                reference_date(),
            )
            .await
            .unwrap();

        assert_eq!(result.category, "signal_failure");
        assert_eq!(result.periods.len(), 1);
        assert_eq!(result.periods[0].resolution_status, "resolved");
        assert_eq!(result.periods[0].date_range, None);
        assert_eq!(result.periods[0].schedule_window, None);
        assert_eq!(result.periods[0].apparent_severity, "normal");
        // Confidence fields are never sent by the primary pass; `#[serde(default)]`
        // must fill them in rather than fail to deserialize.
        assert_eq!(result.periods[0].resolution_status_confidence, "");
        assert_eq!(result.periods[0].severity_confidence, "");
    }

    #[tokio::test]
    async fn extract_primary_parses_a_non_null_impact_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "engineering_works",
                            "periods": [{
                                "scope_description": null,
                                "date_range": null,
                                "schedule_window": null,
                                "resolution_status": "ongoing",
                                "apparent_severity": "severe_disruption",
                                "impact_type": "rail_replacement_bus"
                            }]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary(
                "Buses replace trains",
                "Engineering works",
                reference_date(),
            )
            .await
            .unwrap();

        assert_eq!(
            result.periods[0].impact_type.as_deref(),
            Some("rail_replacement_bus")
        );
    }

    #[tokio::test]
    async fn extract_primary_parses_a_null_impact_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "signal_failure",
                            "periods": [{
                                "scope_description": null,
                                "date_range": null,
                                "schedule_window": null,
                                "resolution_status": "ongoing",
                                "apparent_severity": "normal",
                                "impact_type": null
                            }]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary("Signal failure", "Delays", reference_date())
            .await
            .unwrap();

        assert_eq!(result.periods[0].impact_type, None);
    }

    #[tokio::test]
    async fn extract_primary_parses_multiple_periods_with_nested_schedule_windows() {
        // Mirrors the Wandsworth Town motivating example from the design
        // doc: two sequential date ranges, each with its own nested weekly
        // time-of-day restriction and distinct scope_description.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "engineering_works",
                            "periods": [
                                {
                                    "scope_description": "platform 2 closed, calls at platform 1",
                                    "date_range": {
                                        "from_date": "2026-05-11T00:00:00Z",
                                        "to_date": "2026-07-27T00:00:00Z"
                                    },
                                    "schedule_window": {
                                        "days_of_week": [1, 2, 3, 4],
                                        "start_time": "11:00",
                                        "end_time": "14:00"
                                    },
                                    "resolution_status": "ongoing",
                                    "apparent_severity": "moderate_disruption",
                                    "impact_type": null
                                },
                                {
                                    "scope_description": "platform 3 closed, calls at platform 4",
                                    "date_range": {
                                        "from_date": "2026-07-27T00:00:00Z",
                                        "to_date": "2026-10-12T00:00:00Z"
                                    },
                                    "schedule_window": {
                                        "days_of_week": [1, 2, 3, 4],
                                        "start_time": "11:00",
                                        "end_time": "14:00"
                                    },
                                    "resolution_status": "ongoing",
                                    "apparent_severity": "moderate_disruption",
                                    "impact_type": null
                                }
                            ]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary(
                "Wandsworth Town platform closures",
                "Two sequential phases",
                reference_date(),
            )
            .await
            .unwrap();

        assert_eq!(result.periods.len(), 2);
        assert_eq!(
            result.periods[0].scope_description.as_deref(),
            Some("platform 2 closed, calls at platform 1")
        );
        assert_eq!(
            result.periods[1].scope_description.as_deref(),
            Some("platform 3 closed, calls at platform 4")
        );
        assert!(result.periods[0].schedule_window.is_some());
        assert!(result.periods[1].schedule_window.is_some());
    }

    #[tokio::test]
    async fn extract_primary_rejects_an_empty_periods_array() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({ "category": "signal_failure", "periods": [] }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary("Signal failure", "Delays", reference_date())
            .await;

        assert!(
            result.is_err(),
            "an empty periods array must be a hard failure, not a zero-period success"
        );
    }

    #[tokio::test]
    async fn extract_primary_truncates_periods_beyond_the_soft_cap() {
        let server = MockServer::start().await;
        // 13 periods against a cap of 8 -- the same "13 vs 8" shape the
        // design doc's own root-cause research called out as more
        // consistent with real compound incident structure than runaway
        // hallucination. 8 are rank-2 severity (blocked_or_suspended /
        // severe_disruption, tied), 5 are rank-0 (normal) -- distinct
        // severities per period, so this test isolates severity ordering
        // without also exercising the date tiebreak (that's the dedicated
        // test below).
        let severities = [
            "blocked_or_suspended",
            "severe_disruption",
            "blocked_or_suspended",
            "severe_disruption",
            "blocked_or_suspended",
            "severe_disruption",
            "blocked_or_suspended",
            "severe_disruption",
            "normal",
            "normal",
            "normal",
            "normal",
            "normal",
        ];
        let periods: Vec<serde_json::Value> = severities
            .iter()
            .enumerate()
            .map(|(i, severity)| {
                serde_json::json!({
                    "scope_description": format!("p{i}"),
                    "date_range": null,
                    "schedule_window": null,
                    "resolution_status": "ongoing",
                    "apparent_severity": severity,
                    "impact_type": null
                })
            })
            .collect();
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({ "category": "signal_failure", "periods": periods }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary("Signal failure", "Delays", reference_date())
            .await;

        let extraction =
            result.expect("exceeding the soft cap must now truncate and succeed, not fail");
        assert_eq!(extraction.periods.len(), 8);
        assert_eq!(extraction.dropped_period_count, 5);
        let kept: Vec<&str> = extraction
            .periods
            .iter()
            .map(|p| p.scope_description.as_deref().unwrap())
            .collect();
        assert_eq!(
            kept,
            vec!["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"],
            "the 8 rank-2-severity periods must be kept in their original relative order (stable sort, no date tiebreak triggered here); the 5 rank-0 (normal) periods must be dropped"
        );
    }

    #[tokio::test]
    async fn extract_primary_truncation_tiebreaks_by_from_date_ascending_with_none_first() {
        let server = MockServer::start().await;
        // 7 filler periods at blocked_or_suspended (rank 2), spread across
        // distinct dates so none of them tie with each other -- guaranteed
        // to be kept regardless of the two candidates below.
        let mut periods: Vec<serde_json::Value> = (0..7)
            .map(|i| {
                serde_json::json!({
                    "scope_description": format!("filler{i}"),
                    "date_range": { "from_date": format!("2026-0{}-01T00:00:00Z", i + 1), "to_date": null },
                    "schedule_window": null,
                    "resolution_status": "ongoing",
                    "apparent_severity": "blocked_or_suspended",
                    "impact_type": null
                })
            })
            .collect();
        // 2 candidates at moderate_disruption (rank 1, strictly below the
        // fillers' rank 2) competing for the single remaining slot: one
        // with from_date: null, one with a stated future date. Per the
        // "None sorts first" rule, the null one must be kept.
        periods.push(serde_json::json!({
            "scope_description": "candidate_none_date",
            "date_range": { "from_date": null, "to_date": null },
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "moderate_disruption",
            "impact_type": null
        }));
        periods.push(serde_json::json!({
            "scope_description": "candidate_some_date",
            "date_range": { "from_date": "2026-12-01T00:00:00Z", "to_date": null },
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "moderate_disruption",
            "impact_type": null
        }));

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({ "category": "signal_failure", "periods": periods }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let extraction = client
            .extract_primary("Signal failure", "Delays", reference_date())
            .await
            .expect("9 periods against a cap of 8 must truncate and succeed");

        assert_eq!(extraction.periods.len(), 8);
        assert_eq!(extraction.dropped_period_count, 1);
        let kept: Vec<&str> = extraction
            .periods
            .iter()
            .map(|p| p.scope_description.as_deref().unwrap())
            .collect();
        assert!(
            kept.contains(&"candidate_none_date"),
            "the null-from_date candidate must win the tiebreak: {kept:?}"
        );
        assert!(
            !kept.contains(&"candidate_some_date"),
            "the dated candidate must lose the tiebreak: {kept:?}"
        );
    }

    #[tokio::test]
    async fn extract_primary_accepts_periods_exactly_at_the_soft_cap() {
        let server = MockServer::start().await;
        let period = serde_json::json!({
            "scope_description": null,
            "date_range": null,
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "normal",
            "impact_type": null
        });
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "signal_failure",
                            "periods": vec![period; MAX_PERIODS]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary("Signal failure", "Delays", reference_date())
            .await;

        let extraction = result
            .expect("exactly MAX_PERIODS should still be accepted, only exceeding it truncates");
        assert_eq!(extraction.periods.len(), MAX_PERIODS);
        assert_eq!(
            extraction.dropped_period_count, 0,
            "the boundary case must not report any truncation"
        );
    }

    #[tokio::test]
    async fn extract_primary_threads_the_reference_date_into_user_content() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("2026-04-10T09:00:00+00:00"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "signal_failure",
                            "periods": [{
                                "scope_description": null,
                                "date_range": null,
                                "schedule_window": null,
                                "resolution_status": "ongoing",
                                "apparent_severity": "normal",
                                "impact_type": null
                            }]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        // If the reference date weren't included in the request body, the
        // mock's `body_string_contains` matcher above would not match and
        // this call would fail (no mock configured to respond).
        let result = client
            .extract_primary("11 May to 26 July", "no year stated", reference_date())
            .await;

        assert!(
            result.is_ok(),
            "reference date must be present in the request sent to the LLM: {result:?}"
        );
    }

    #[tokio::test]
    async fn extract_adversarial_parses_a_period_aligned_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "periods": [
                                { "period_index": 0, "scope_description": null, "resolution_status": "ongoing" },
                                { "period_index": 1, "scope_description": "phase 2", "resolution_status": "resolved" }
                            ]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let periods = vec![
            ExtractionPeriod {
                scope_description: None,
                date_range: None,
                schedule_window: None,
                resolution_status: "resolved".to_string(),
                apparent_severity: "normal".to_string(),
                impact_type: None,
                resolution_status_confidence: String::new(),
                severity_confidence: String::new(),
            },
            ExtractionPeriod {
                scope_description: Some("phase 2".to_string()),
                date_range: None,
                schedule_window: None,
                resolution_status: "resolved".to_string(),
                apparent_severity: "normal".to_string(),
                impact_type: None,
                resolution_status_confidence: String::new(),
                severity_confidence: String::new(),
            },
        ];

        let result = client
            .extract_adversarial("summary", "description", &periods)
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].period_index, 0);
        assert_eq!(result[0].resolution_status, "ongoing");
        assert_eq!(result[1].period_index, 1);
        assert_eq!(result[1].scope_description.as_deref(), Some("phase 2"));
        assert_eq!(result[1].resolution_status, "resolved");
    }

    #[tokio::test]
    async fn extract_severity_adversarial_parses_a_period_aligned_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "periods": [
                                { "period_index": 0, "scope_description": null, "apparent_severity": "moderate_disruption" }
                            ]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let periods = vec![ExtractionPeriod {
            scope_description: None,
            date_range: None,
            schedule_window: None,
            resolution_status: "ongoing".to_string(),
            apparent_severity: "severe_disruption".to_string(),
            impact_type: None,
            resolution_status_confidence: String::new(),
            severity_confidence: String::new(),
        }];

        let result = client
            .extract_severity_adversarial("Delays", "Minor knock-on delays", &periods)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].period_index, 0);
        assert_eq!(result[0].apparent_severity, "moderate_disruption");
    }

    #[tokio::test]
    async fn extract_primary_fails_on_malformed_content() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{ "message": { "content": "not valid json" } }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary("Signal failure", "Delays", reference_date())
            .await;

        assert!(
            result.is_err(),
            "malformed content must be rejected, not silently stored"
        );
    }

    // -- ProviderPolicy / LlmCallError --

    fn flat_primary_body() -> serde_json::Value {
        serde_json::json!({
            "choices": [{
                "message": {
                    "content": serde_json::json!({
                        "category": "signal_failure",
                        "periods": [{
                            "scope_description": null,
                            "date_range": null,
                            "schedule_window": null,
                            "resolution_status": "ongoing",
                            "apparent_severity": "normal",
                            "impact_type": null
                        }]
                    }).to_string()
                },
                "finish_reason": "stop"
            }]
        })
    }

    fn fast_retry_policy() -> ProviderPolicy {
        ProviderPolicy {
            max_tokens: Some(8192),
            reasoning_effort: Some("low".to_string()),
            max_in_flight: Some(3),
            rate_limit_min_wait: std::time::Duration::from_millis(10),
            max_rate_limit_retries: 3,
            max_gateway_retries: 2,
        }
    }

    #[tokio::test]
    async fn default_policy_omits_max_tokens_and_reasoning_effort() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(flat_primary_body()))
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT);
        client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap();
        let body =
            String::from_utf8(server.received_requests().await.unwrap()[0].body.clone()).unwrap();
        assert!(!body.contains("reasoning_effort"), "{body}");
        assert!(!body.contains("max_tokens"), "{body}");
    }

    #[tokio::test]
    async fn policy_sends_reasoning_effort_and_max_tokens() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_string_contains("\"reasoning_effort\":\"low\""))
            .and(body_string_contains("\"max_tokens\":8192"))
            .respond_with(ResponseTemplate::new(200).set_body_json(flat_primary_body()))
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT)
            .with_provider_policy(fast_retry_policy());
        client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rate_limit_is_retried_within_budget() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(flat_primary_body()))
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT)
            .with_provider_policy(fast_retry_policy());
        client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn gateway_timeout_retries_are_capped_and_classified_transient() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(504))
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT)
            .with_provider_policy(fast_retry_policy());
        let err = client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap_err();
        // 1 initial attempt + max_gateway_retries (2).
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
        assert!(client.is_provider_transient(&err), "{err:?}");
    }

    /// The model-eval harness's per-attempt record: a gateway failure that
    /// a retry recovered still shows up as a failed attempt, and a 429's
    /// back-off is kept apart from the attempt's own send time.
    #[tokio::test]
    async fn raw_call_records_every_attempt() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(504))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(flat_primary_body()))
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT)
            .with_provider_policy(ProviderPolicy {
                rate_limit_min_wait: std::time::Duration::from_millis(50),
                ..fast_retry_policy()
            });
        let raw = client.primary_raw("s", "d", reference_date()).await;
        assert!(raw.content.is_ok());
        assert_eq!(raw.retries, 2);
        let outcomes: Vec<&str> = raw.attempts.iter().map(|a| a.outcome).collect();
        assert_eq!(outcomes, ["gateway_error", "rate_limited", "success"]);
        // The back-off is booked on the 429 attempt only, not as send time.
        assert_eq!(raw.attempts[0].backoff, std::time::Duration::ZERO);
        assert!(raw.attempts[1].backoff >= std::time::Duration::from_millis(50));
        assert_eq!(raw.attempts[2].backoff, std::time::Duration::ZERO);
    }

    /// With `max_in_flight` 1: the permit is held for one HTTP attempt only,
    /// not through a 429 back-off (B runs while A sleeps one off), and an
    /// attempt's time waiting for the permit is booked as `queued`, apart
    /// from its `send` time (C waits for A's retry). Timeline, from the
    /// start: A gets a 429 at ~0 and sleeps 1.5 s; B sends at 0.15 s and
    /// is answered at ~0.95 s; A resends at ~1.5 s and holds the permit
    /// until ~2.3 s; C asks at 1.8 s, so it queues ~0.5 s, then sends for
    /// ~0.8 s. Every assertion leaves at least ~250 ms of slack.
    #[tokio::test]
    async fn permit_is_released_during_429_backoff_and_queueing_is_not_send_time() {
        const BACKOFF: std::time::Duration = std::time::Duration::from_millis(1500);
        const RESPONSE_DELAY: std::time::Duration = std::time::Duration::from_millis(800);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(flat_primary_body())
                    .set_delay(RESPONSE_DELAY),
            )
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT)
            .with_provider_policy(ProviderPolicy {
                max_in_flight: Some(1),
                rate_limit_min_wait: BACKOFF,
                max_rate_limit_retries: 1,
                ..ProviderPolicy::default()
            });
        let call_after = |delay_ms: u64| {
            let client = &client;
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                let start = tokio::time::Instant::now();
                let raw = client.primary_raw("s", "d", reference_date()).await;
                (raw, start.elapsed())
            }
        };
        let ((a, _), (b, b_total), (c, c_total)) =
            tokio::join!(call_after(0), call_after(150), call_after(1800));
        for raw in [&a, &b, &c] {
            assert!(raw.content.is_ok(), "{:?}", raw.content);
        }

        let outcomes: Vec<&str> = a.attempts.iter().map(|x| x.outcome).collect();
        assert_eq!(outcomes, ["rate_limited", "success"]);
        assert!(a.attempts[0].backoff >= BACKOFF);
        // B finished while A was still backing off: the permit was free.
        assert!(
            b_total < std::time::Duration::from_millis(1250),
            "B took {b_total:?}"
        );
        assert!(b.attempts[0].queued < std::time::Duration::from_millis(250));

        // C waited for A's retry to release the permit, and that wait is
        // `queued`, not `send`.
        let c = &c.attempts[0];
        assert!(c.queued >= std::time::Duration::from_millis(250), "{c:?}");
        assert!(c.send >= RESPONSE_DELAY, "{c:?}");
        assert!(
            c.queued + c.send <= c_total + std::time::Duration::from_millis(50),
            "queued {:?} + send {:?} exceeds the call's {c_total:?}",
            c.queued,
            c.send
        );
    }

    /// A client timeout recovered by a gateway retry: the call succeeds,
    /// but its first attempt is recorded as `timeout`.
    #[tokio::test]
    async fn raw_call_keeps_a_recovered_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(flat_primary_body())
                    .set_delay(std::time::Duration::from_secs(5)),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(flat_primary_body()))
            .mount(&server)
            .await;
        let client = LlmClient::new(
            server.uri(),
            None,
            "m".into(),
            std::time::Duration::from_millis(500),
        )
        .with_provider_policy(ProviderPolicy {
            // Headroom in case a timed-out request never reaches the server
            // (see the client-timeout test below).
            max_gateway_retries: 3,
            ..ProviderPolicy::default()
        });
        let raw = client.primary_raw("s", "d", reference_date()).await;
        assert!(raw.content.is_ok(), "{:?}", raw.content.err());
        assert_eq!(raw.attempts[0].outcome, "timeout");
        assert!(raw.attempts[0].send >= std::time::Duration::from_millis(500));
        assert_eq!(raw.attempts.last().map(|a| a.outcome), Some("success"));
        assert_eq!(
            raw.attempts.len(),
            usize::try_from(raw.retries).unwrap() + 1
        );
    }

    #[tokio::test]
    async fn default_policy_does_not_retry_a_504() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(504))
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT);
        assert!(
            client
                .extract_primary("s", "d", reference_date())
                .await
                .is_err()
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn null_content_is_a_typed_non_transient_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{ "message": { "content": null, "reasoning_content": "..." },
                              "finish_reason": "length" }]
            })))
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT)
            .with_provider_policy(fast_retry_policy());
        let err = client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap_err();
        assert!(!client.is_provider_transient(&err));
        assert!(
            matches!(
                err.downcast_ref::<LlmCallError>(),
                Some(LlmCallError::EmptyContent { finish_reason: Some(r) }) if r == "length"
            ),
            "{err:?}"
        );
    }

    async fn error_for_status(
        client: &LlmClient,
        server: &MockServer,
        status: u16,
    ) -> anyhow::Error {
        server.reset().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(status))
            .mount(server)
            .await;
        client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap_err()
    }

    /// 429 and 502/503 never say anything about the text, so they stay out
    /// of the per-text backoff even under the default policy. A 504 or a
    /// client timeout is ambiguous (a runaway generation on one text ends
    /// the same way), so under the default policy it keeps feeding the
    /// backoff exactly as before -- only opting into gateway retries
    /// reclassifies it.
    #[tokio::test]
    async fn default_policy_keeps_timeouts_and_504_in_the_text_backoff() {
        let server = MockServer::start().await;
        let default_client =
            LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT);
        for status in [429, 502, 503] {
            let err = error_for_status(&default_client, &server, status).await;
            assert!(
                default_client.is_provider_transient(&err),
                "{status}: {err:?}"
            );
        }
        let err = error_for_status(&default_client, &server, 504).await;
        assert!(!default_client.is_provider_transient(&err), "{err:?}");
        let err = error_for_status(&default_client, &server, 500).await;
        assert!(!default_client.is_provider_transient(&err), "{err:?}");

        let retrying_client =
            LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT)
                .with_provider_policy(ProviderPolicy {
                    max_gateway_retries: 1,
                    ..ProviderPolicy::default()
                });
        let err = error_for_status(&retrying_client, &server, 504).await;
        assert!(retrying_client.is_provider_transient(&err), "{err:?}");
    }

    #[tokio::test]
    async fn client_timeout_is_typed_and_only_transient_when_gateway_retries_are_on() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(flat_primary_body())
                    .set_delay(std::time::Duration::from_secs(5)),
            )
            .mount(&server)
            .await;
        let short = std::time::Duration::from_millis(500);
        let default_client = LlmClient::new(server.uri(), None, "m".into(), short);
        let err = default_client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<LlmCallError>(),
                Some(LlmCallError::ClientTimeout)
            ),
            "{err:?}"
        );
        assert!(!default_client.is_provider_transient(&err));

        let retrying_client = LlmClient::new(server.uri(), None, "m".into(), short)
            .with_provider_policy(ProviderPolicy {
                max_gateway_retries: 1,
                ..ProviderPolicy::default()
            });
        let err = retrying_client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap_err();
        assert!(retrying_client.is_provider_transient(&err), "{err:?}");
        // No exact request count here: a request that times out client-side
        // may never reach the server under load. The retry budget itself is
        // covered deterministically by the 504 test above.
    }

    /// A `Retry-After` past `MAX_RETRY_AFTER` must fail the call at once
    /// (still provider-transient) rather than pin a loop -- and the
    /// incident's in-flight claim -- for hours.
    #[tokio::test]
    async fn retry_after_beyond_the_cap_fails_fast_instead_of_sleeping() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(429).insert_header(
                "retry-after",
                (MAX_RETRY_AFTER.as_secs() + 1).to_string().as_str(),
            ))
            .mount(&server)
            .await;
        let client = LlmClient::new(server.uri(), None, "m".into(), DEFAULT_REQUEST_TIMEOUT)
            .with_provider_policy(fast_retry_policy());
        let started = std::time::Instant::now();
        let err = client
            .extract_primary("s", "d", reference_date())
            .await
            .unwrap_err();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        assert!(
            matches!(
                err.downcast_ref::<LlmCallError>(),
                Some(LlmCallError::RateLimited {
                    retry_after: Some(_)
                })
            ),
            "{err:?}"
        );
        assert!(client.is_provider_transient(&err));
    }

    /// The outcome labels on `enricher_llm_call_total` are a fixed set.
    #[test]
    fn outcome_labels_are_distinct_per_error_kind() {
        let labels = [
            LlmCallError::RateLimited { retry_after: None }.outcome_label(),
            LlmCallError::GatewayUnavailable { status: 504 }.outcome_label(),
            LlmCallError::ClientTimeout.outcome_label(),
            LlmCallError::Status { status: 500 }.outcome_label(),
            LlmCallError::EmptyContent {
                finish_reason: None,
            }
            .outcome_label(),
            LlmCallError::Other(anyhow::anyhow!("x")).outcome_label(),
        ];
        assert_eq!(
            labels,
            [
                "rate_limited",
                "gateway_error",
                "timeout",
                "http_error",
                "empty_content",
                "error"
            ]
        );
    }

    // -- `DateRange` wire-convention fixtures (design §1's testing-plan
    // additions). The actual year-inference/inclusivity/timezone reasoning
    // happens inside the model's response generation (per PRIMARY_PROMPT
    // above) -- there is no Rust-side date-arithmetic function to unit-test
    // for that reasoning itself (`DateRange.from_date`/`to_date` are typed
    // as already-resolved `DateTime<Utc>` on the wire). These fixtures
    // instead pin the wire-level contract those conventions rely on: a
    // resolved date range deserializes correctly regardless of which side
    // of the reference date it falls on, and an inclusive end-of-day
    // boundary is representable and round-trips exactly.

    #[test]
    fn date_range_resolves_correctly_when_stated_date_is_after_the_reference_year() {
        // Incident first seen in April; "11 May to 26 July" with no stated
        // year resolves to *this* year, per the reference-date-proximity
        // rule -- a "date closest to the reference date" case where the
        // closest occurrence is later the same year.
        let json = serde_json::json!({ "from_date": "2026-05-11T00:00:00Z", "to_date": "2026-07-27T00:00:00Z" });
        let range: DateRange = serde_json::from_value(json).unwrap();
        assert_eq!(
            range.from_date,
            Some("2026-05-11T00:00:00Z".parse().unwrap())
        );
        assert_eq!(range.to_date, Some("2026-07-27T00:00:00Z".parse().unwrap()));
    }

    #[test]
    fn date_range_resolves_correctly_when_stated_date_is_before_the_reference_year() {
        // Incident first seen in December describing a date that has
        // already passed this year almost certainly means next year -- the
        // resolved wire value reflects that rollover.
        let json = serde_json::json!({ "from_date": "2027-01-15T00:00:00Z", "to_date": null });
        let range: DateRange = serde_json::from_value(json).unwrap();
        assert_eq!(
            range.from_date,
            Some("2027-01-15T00:00:00Z".parse().unwrap())
        );
        assert_eq!(range.to_date, None);
    }

    #[test]
    fn date_range_inclusive_end_of_day_is_the_following_days_midnight_utc() {
        // "to Sunday 26 July" (BST, UTC+1) reads as *through* that Sunday,
        // so the wire `to_date` is 27 July 00:00 Europe/London (23:00 UTC on
        // the 26th) -- the following day's midnight local time, not the
        // stated day's own midnight, per design §1's inclusivity rule.
        let json = serde_json::json!({ "from_date": null, "to_date": "2026-07-26T23:00:00Z" });
        let range: DateRange = serde_json::from_value(json).unwrap();
        assert_eq!(range.to_date, Some("2026-07-26T23:00:00Z".parse().unwrap()));
        // Sanity check that this is NOT the stated day's own midnight UTC.
        assert_ne!(range.to_date, Some("2026-07-26T00:00:00Z".parse().unwrap()));
    }

    #[test]
    fn date_range_both_sides_null_is_a_valid_open_ended_range() {
        let json = serde_json::json!({ "from_date": null, "to_date": null });
        let range: DateRange = serde_json::from_value(json).unwrap();
        assert_eq!(
            range,
            DateRange {
                from_date: None,
                to_date: None
            }
        );
    }

    // -- Finding regression: a malformed date on ONE field must not poison
    // the whole extraction. Before `deserialize_lenient_date`, any of the
    // fixtures below would fail `serde_json::from_value::<DateRange>` (and,
    // in the real pipeline, the surrounding `PrimaryExtraction` deserialize
    // in `extract_primary`) outright -- discarding this incident's
    // `category` and every other period's already-correct facts along with
    // the one bad field. `deserialize_lenient_date` now degrades just that
    // field to `None` instead.

    #[test]
    fn date_range_degrades_an_unparseable_from_date_to_null_instead_of_failing() {
        let json =
            serde_json::json!({ "from_date": "May 2026", "to_date": "2026-07-27T00:00:00Z" });
        let range: DateRange =
            serde_json::from_value(json).expect("a bad from_date must not fail the whole struct");
        assert_eq!(
            range.from_date, None,
            "the unparseable value must degrade to null, not surface as a parse error"
        );
        assert_eq!(
            range.to_date,
            Some("2026-07-27T00:00:00Z".parse().unwrap()),
            "the OTHER, well-formed field on the same object must be unaffected"
        );
    }

    #[test]
    fn date_range_degrades_an_unparseable_to_date_to_null_instead_of_failing() {
        let json =
            serde_json::json!({ "from_date": "2026-05-11T00:00:00Z", "to_date": "26th July" });
        let range: DateRange =
            serde_json::from_value(json).expect("a bad to_date must not fail the whole struct");
        assert_eq!(
            range.from_date,
            Some("2026-05-11T00:00:00Z".parse().unwrap())
        );
        assert_eq!(range.to_date, None);
    }

    #[test]
    fn date_range_degrades_a_non_string_date_value_to_null_instead_of_failing() {
        // Not a realistic model output for this schema, but a defensive
        // fixture: any JSON shape other than a string or null must still
        // degrade gracefully rather than propagate a type-mismatch error.
        let json = serde_json::json!({ "from_date": 12345, "to_date": null });
        let range: DateRange =
            serde_json::from_value(json).expect("a non-string date must not fail the whole struct");
        assert_eq!(range.from_date, None);
    }

    #[tokio::test]
    async fn extract_primary_keeps_the_rest_of_a_period_when_one_of_its_dates_is_malformed() {
        // End-to-end version of the fixtures above: a malformed date buried
        // inside `extract_primary`'s real response must not discard the
        // period's `resolution_status`/`apparent_severity`/`category`, or
        // any sibling period, along with it.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::json!({
                            "category": "signal_failure",
                            "periods": [{
                                "scope_description": null,
                                "date_range": { "from_date": "not a date", "to_date": null },
                                "schedule_window": null,
                                "resolution_status": "ongoing",
                                "apparent_severity": "severe_disruption",
                                "impact_type": null
                            }]
                        }).to_string()
                    }
                }]
            })))
            .mount(&server)
            .await;

        let client = LlmClient::new(
            server.uri(),
            None,
            "test-model".to_string(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let result = client
            .extract_primary("Signal failure", "Delays", reference_date())
            .await;

        let extraction =
            result.expect("a malformed date on one field must not fail the whole extraction");
        assert_eq!(extraction.category, "signal_failure");
        assert_eq!(extraction.periods.len(), 1);
        assert_eq!(
            extraction.periods[0]
                .date_range
                .as_ref()
                .and_then(|r| r.from_date),
            None,
            "the malformed field itself degrades to null"
        );
        assert_eq!(
            extraction.periods[0].resolution_status, "ongoing",
            "every OTHER field on the same period must survive intact"
        );
        assert_eq!(extraction.periods[0].apparent_severity, "severe_disruption");
    }

    // --- Live eval against a real OpenAI-compatible endpoint ---
    //
    // Ignored by default (no network/creds in normal CI). Run explicitly with
    // (the crate is bin-only, so `--bin enricher`, not `--lib`):
    //   LLM_BASE_URL=... LLM_API_KEY=... LLM_MODEL=... \
    //     cargo test -p enricher --bin enricher llm::tests::live_eval -- \
    //       --ignored --nocapture --test-threads=1
    // The provider-policy env vars the service reads (`LLM_REASONING_EFFORT`,
    // `LLM_MAX_TOKENS`, `LLM_MAX_IN_FLIGHT`, `LLM_RATE_LIMIT_RETRIES`,
    // `LLM_RATE_LIMIT_RETRY_SECS`, `LLM_GATEWAY_RETRIES`) apply here too --
    // see `live_client_from_env`.
    // This is the design doc's own testing-plan item ("Golden corpus, run as
    // a live eval, not just fixtures") for the two central open risks: does
    // the configured model segment multi-period text correctly (risk #1),
    // and does `strict: true` actually hold for an array-of-objects schema
    // on this backend (risk #2)? Also times each call, which is the
    // ground-truth data point for whether `LLM_REQUEST_TIMEOUT_SECS`'s
    // default is realistic against this specific deployment.

    const WANDSWORTH_TOWN_SUMMARY: &str = "Platform alterations at Wandsworth Town";
    const WANDSWORTH_TOWN_DESCRIPTION: &str = "Monday 11 May to Sunday 26 July: \
        Platform 2 at Wandsworth Town is closed. Trains will call at platform 1 during this period. \
        Monday - Thursday between 11:00 - 14:00: \
        No trains travelling from London Waterloo will call at Wandsworth Town. Passengers for \
        Wandsworth Town should circulate via Putney. \
        Monday 27 July to Sunday 11 October: \
        Platform 3 at Wandsworth Town is closed. Trains will call at platform 4 during this period. \
        Monday - Thursday between 11:00 - 14:00: \
        No trains travelling towards London Waterloo will call at Wandsworth Town. Passengers for \
        Wandsworth Town should circulate via Clapham Junction.";

    const FLAT_ETA_SUMMARY: &str = "Signal failure at Reading";
    const FLAT_ETA_DESCRIPTION: &str = "A signal failure between Reading and Basingstoke is causing \
        delays of up to 20 minutes. Normal service is expected to resume from 18:00.";

    // Over-segmentation trap (design doc risk #1): several stations listed
    // under one SHARED date range should stay one period, not become three.
    const TRAP_SUMMARY: &str = "Reduced ticket office hours at three stations";
    const TRAP_DESCRIPTION: &str = "From Monday 4 May to Friday 26 June, ticket office opening hours will \
        be reduced at Basingstoke, Woking, and Farnborough stations. Ticket offices at all three stations \
        will open at 08:00 instead of 06:00 and close at 18:00 instead of 20:00 for the duration of this \
        period.";

    // Day-of-week-across-legs stress test (design doc Decision 1): two
    // date ranges, each containing three co-existing legs with genuinely
    // different treatments -- the exact shape the new PRIMARY_PROMPT
    // guidance (Task 1) targets. Built only from fragments confirmed
    // quoted in docs/superpowers/specs/2026-09-01-disruption-type-extraction-research.md
    // (lines 325-334), not invented prose.
    const BARRHEAD_DUMFRIES_SUMMARY: &str = "Buses replace trains between Barrhead and Dumfries";
    const BARRHEAD_DUMFRIES_DESCRIPTION: &str = "From Saturday 29 August to Friday 11 September, buses \
        replace trains between Barrhead and Kilmarnock / Dumfries. Monday to Saturday during this period, \
        buses operate between Kilmarnock and Troon, where passengers can connect with trains to / from Ayr. \
        No scheduled services operate between Kilmarnock and Ayr / Stranraer on Sundays. \
        From Saturday 12 September to Sunday 13 September, buses replace trains between Barrhead and \
        Kilmarnock / Carlisle. On Saturday during this period, buses operate between Kilmarnock and Troon, \
        where passengers can connect with trains to / from Ayr. No scheduled services operate between \
        Kilmarnock and Ayr / Stranraer on Sunday.";

    // Undated-aside observational fixture (design doc Decision 1): a
    // dated bus-replacement clause plus a separate, undated, vaguely-scoped
    // clause. Deliberately has NO dedicated hard-count expectation -- the
    // sibling research doc explicitly left "does this get its own period,
    // or fold into scope_description" unresolved; this fixture's job is to
    // observe what the improved prompt actually does, not assert a
    // pre-decided right answer.
    const NORWOOD_JUNCTION_SUMMARY: &str = "Buses replace trains via Norwood Junction";
    const NORWOOD_JUNCTION_DESCRIPTION: &str = "Monday to Thursday overnight, buses will replace trains \
        between the affected stations via Norwood Junction. Some trains will be diverted via an alternative \
        route.";

    // Example 1 from docs/superpowers/specs/2026-09-01-disruption-type-extraction-research.md:
    // a Saturday rail-replacement-bus leg immediately adjacent to a Sunday
    // no-scheduled-service leg within the same overarching date range --
    // the case design doc Decision 4's governing_impact_type collapsing
    // rule (schedule-window disambiguation) is built to handle.
    const IMPACT_BUS_NOSERVICE_SUMMARY: &str = "Buses replace trains between Kilmarnock and Ayr";
    const IMPACT_BUS_NOSERVICE_DESCRIPTION: &str = "From Saturday 29 August to Sunday 13 September, \
        engineering work is taking place between Kilmarnock and Ayr. Buses operate between Kilmarnock and \
        Troon, where passengers can connect with trains to / from Ayr, on Saturdays. No scheduled services \
        operate between Kilmarnock and Ayr / Stranraer on Sundays.";

    // Example 2: a rail-replacement-bus paragraph and a separately-worded
    // diversion clause with no date/scope boundary of its own -- the
    // segmentation ambiguity design doc's Open questions/risks item 1
    // (and the research doc's Open question 1) name as unresolved.
    const IMPACT_DIVERSION_SUMMARY: &str =
        "Rail replacement buses and diversions between London Bridge and Croydon";
    const IMPACT_DIVERSION_DESCRIPTION: &str = "Monday to Thursday nights, buses will replace trains between \
        London Bridge and East / West Croydon while overnight engineering work takes place. Some trains will \
        be diverted via an alternative route.";

    fn eval_reference_date() -> DateTime<Utc> {
        "2026-04-01T00:00:00Z".parse().unwrap()
    }

    /// Runs one primary-extraction attempt and logs a single-line, greppable
    /// summary (fixture label, attempt number, timing, period count, and
    /// each period's key fields) rather than the full pretty-printed debug
    /// dump the earlier one-shot tests use -- built for scanning many runs
    /// across many models at once.
    async fn run_battery_attempt(
        client: &LlmClient,
        label: &str,
        attempt: u32,
        summary: &str,
        description: &str,
    ) {
        let start = std::time::Instant::now();
        match client
            .extract_primary(summary, description, eval_reference_date())
            .await
        {
            Ok(primary) => {
                let elapsed = start.elapsed();
                eprintln!(
                    "BATTERY fixture={label} attempt={attempt} status=ok elapsed={elapsed:?} category={:?} period_count={}",
                    primary.category,
                    primary.periods.len()
                );
                for (i, p) in primary.periods.iter().enumerate() {
                    eprintln!(
                        "  BATTERY fixture={label} attempt={attempt} period[{i}] scope={:?} date_range={:?} \
                         schedule_window={:?} resolution_status={:?} apparent_severity={:?} impact_type={:?}",
                        p.scope_description,
                        p.date_range,
                        p.schedule_window,
                        p.resolution_status,
                        p.apparent_severity,
                        p.impact_type
                    );
                }
            }
            Err(err) => {
                eprintln!(
                    "BATTERY fixture={label} attempt={attempt} status=FAILED elapsed={:?} error={err}",
                    start.elapsed()
                );
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires network access to a real LLM_BASE_URL; run explicitly, see comment above"]
    async fn live_eval_battery() {
        let client = live_client_from_env();
        let repeats: u32 = std::env::var("LIVE_EVAL_REPEATS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);

        for attempt in 1..=repeats {
            run_battery_attempt(
                &client,
                "multi",
                attempt,
                WANDSWORTH_TOWN_SUMMARY,
                WANDSWORTH_TOWN_DESCRIPTION,
            )
            .await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(
                &client,
                "flat",
                attempt,
                FLAT_ETA_SUMMARY,
                FLAT_ETA_DESCRIPTION,
            )
            .await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(&client, "trap", attempt, TRAP_SUMMARY, TRAP_DESCRIPTION).await;
        }
        for attempt in 1..=repeats {
            run_battery_attempt(
                &client,
                "dow_legs",
                attempt,
                BARRHEAD_DUMFRIES_SUMMARY,
                BARRHEAD_DUMFRIES_DESCRIPTION,
            )
            .await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(
                &client,
                "undated_aside",
                attempt,
                NORWOOD_JUNCTION_SUMMARY,
                NORWOOD_JUNCTION_DESCRIPTION,
            )
            .await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(
                &client,
                "impact_bus_noservice",
                attempt,
                IMPACT_BUS_NOSERVICE_SUMMARY,
                IMPACT_BUS_NOSERVICE_DESCRIPTION,
            )
            .await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(
                &client,
                "impact_diversion",
                attempt,
                IMPACT_DIVERSION_SUMMARY,
                IMPACT_DIVERSION_DESCRIPTION,
            )
            .await;
        }
    }

    #[tokio::test]
    #[ignore = "requires network access to a real LLM_BASE_URL; run explicitly, see comment above"]
    async fn live_eval_wandsworth_town_segments_into_two_periods() {
        let client = live_client_from_env();
        let reference_date = "2026-04-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let start = std::time::Instant::now();
        let primary = client
            .extract_primary(
                WANDSWORTH_TOWN_SUMMARY,
                WANDSWORTH_TOWN_DESCRIPTION,
                reference_date,
            )
            .await
            .expect("primary extraction should succeed against a real endpoint");
        eprintln!(
            "primary call took {:?}, category={:?}, periods={}",
            start.elapsed(),
            primary.category,
            primary.periods.len()
        );
        for (i, p) in primary.periods.iter().enumerate() {
            eprintln!(
                "  period[{i}]: scope={:?} date_range={:?} schedule_window={:?} resolution_status={:?} apparent_severity={:?}",
                p.scope_description,
                p.date_range,
                p.schedule_window,
                p.resolution_status,
                p.apparent_severity
            );
        }
        assert!(
            !primary.periods.is_empty(),
            "periods must never be empty on a successful parse"
        );

        let start = std::time::Instant::now();
        let resolution = client
            .extract_adversarial(
                WANDSWORTH_TOWN_SUMMARY,
                WANDSWORTH_TOWN_DESCRIPTION,
                &primary.periods,
            )
            .await
            .expect("resolution-adversarial pass should succeed against a real endpoint");
        eprintln!(
            "resolution-adversarial call took {:?}: {:?}",
            start.elapsed(),
            resolution
        );
        assert_eq!(
            resolution.len(),
            primary.periods.len(),
            "adversarial array must be index-aligned with primary's periods"
        );

        let start = std::time::Instant::now();
        let severity = client
            .extract_severity_adversarial(
                WANDSWORTH_TOWN_SUMMARY,
                WANDSWORTH_TOWN_DESCRIPTION,
                &primary.periods,
            )
            .await
            .expect("severity-adversarial pass should succeed against a real endpoint");
        eprintln!(
            "severity-adversarial call took {:?}: {:?}",
            start.elapsed(),
            severity
        );
        assert_eq!(
            severity.len(),
            primary.periods.len(),
            "adversarial array must be index-aligned with primary's periods"
        );

        // Soft signal, not a hard assertion: this is the segmentation-reliability
        // risk the design doc flags as needing empirical checking, not something
        // a single run should assert pass/fail on.
        if primary.periods.len() != 2 {
            eprintln!(
                "NOTE: expected 2 periods for the Wandsworth Town fixture (two sequential platform \
                 closures), model produced {} -- see design doc risk #1 (segmentation reliability)",
                primary.periods.len()
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires network access to a real LLM_BASE_URL; run explicitly, see comment above"]
    async fn live_eval_barrhead_dumfries_segments_into_six_periods() {
        let client = live_client_from_env();
        let reference_date = "2026-08-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let start = std::time::Instant::now();
        let primary = client
            .extract_primary(
                BARRHEAD_DUMFRIES_SUMMARY,
                BARRHEAD_DUMFRIES_DESCRIPTION,
                reference_date,
            )
            .await
            .expect("primary extraction should succeed against a real endpoint");
        eprintln!(
            "primary call took {:?}, category={:?}, periods={}",
            start.elapsed(),
            primary.category,
            primary.periods.len()
        );
        for (i, p) in primary.periods.iter().enumerate() {
            eprintln!(
                "  period[{i}]: scope={:?} date_range={:?} schedule_window={:?} resolution_status={:?} apparent_severity={:?}",
                p.scope_description,
                p.date_range,
                p.schedule_window,
                p.resolution_status,
                p.apparent_severity
            );
        }
        assert!(
            !primary.periods.is_empty(),
            "periods must never be empty on a successful parse"
        );

        // Soft signal, not a hard assertion -- exactly like
        // live_eval_wandsworth_town_segments_into_two_periods above. The
        // expected count of 6 (three legs x two date ranges) is this
        // plan's own reasoned prediction, not an observed result; it has
        // not been run against a live model.
        if primary.periods.len() != 6 {
            eprintln!(
                "NOTE: expected 6 periods for the Barrhead-Dumfries fixture (three co-existing legs per \
                 date range, two date ranges), model produced {} -- see design doc Decision 1 and its Open \
                 Questions section (counts not yet validated against a live model)",
                primary.periods.len()
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires network access to a real LLM_BASE_URL; run explicitly, see comment above"]
    async fn live_eval_debug_raw_wandsworth_town_response() {
        let client = live_client_from_env();
        let user_content = format!(
            "Summary: {WANDSWORTH_TOWN_SUMMARY}\nDescription: {WANDSWORTH_TOWN_DESCRIPTION}"
        );
        let raw = client
            .chat_completion(
                PRIMARY_PROMPT,
                user_content,
                PRIMARY_SCHEMA_NAME,
                primary_schema(),
            )
            .await
            .content
            .expect("raw chat completion should succeed");
        eprintln!(
            "=== RAW CONTENT ({} bytes) ===\n{raw}\n=== END RAW CONTENT ===",
            raw.len()
        );
    }

    #[tokio::test]
    #[ignore = "requires network access to a real LLM_BASE_URL; run explicitly, see comment above"]
    async fn live_eval_flat_single_fact_incident_stays_one_period() {
        let client = live_client_from_env();
        let reference_date = "2026-04-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let start = std::time::Instant::now();
        let primary = client
            .extract_primary(
                "Signal failure at Reading",
                "A signal failure between Reading and Basingstoke is causing delays of up to 20 minutes. \
                 Normal service is expected to resume from 18:00.",
                reference_date,
            )
            .await
            .expect("primary extraction should succeed against a real endpoint");
        eprintln!(
            "primary call took {:?}: {:?}",
            start.elapsed(),
            primary.periods
        );

        assert_eq!(
            primary.periods.len(),
            1,
            "a flat single-fact incident with no distinct sub-periods should not be over-segmented"
        );
    }
}
