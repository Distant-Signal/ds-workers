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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DateRange {
    pub from_date: Option<DateTime<Utc>>,
    pub to_date: Option<DateTime<Utc>>,
}

/// Nested weekly time-of-day restriction *within* a period's `date_range`,
/// if any -- unchanged in shape from the original design, just scoped to
/// one period instead of the whole incident.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduleWindow {
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
pub struct ExtractionPeriod {
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
pub struct PrimaryExtraction {
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
pub struct AdversarialPeriodVerdict {
    pub period_index: usize,
    pub scope_description: Option<String>,
    pub resolution_status: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct SeverityAdversarialPeriodVerdict {
    pub period_index: usize,
    pub scope_description: Option<String>,
    pub apparent_severity: String,
}

pub struct LlmClient {
    base_url: String,
    api_key: Option<String>,
    model: String,
    http: reqwest::Client,
}

#[derive(Serialize)]
struct ChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage>,
    response_format: ResponseFormat,
    temperature: f32,
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
}

#[derive(Deserialize)]
struct ChatChoiceMessage {
    content: String,
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
    \"2026-04-06T00:00:00Z\", \"to_date\": \"2026-05-16T00:00:00Z\"}` (2026 because that's the closest \
    occurrence to the March 2026 reference date; `to_date` is the day AFTER the stated 15 May end), \
    `schedule_window: null`, `resolution_status: \"ongoing\"` (no statement that it has ended); period 2: \
    `scope_description` \"platform 5 closed, calls at platform 6\", `date_range` `{\"from_date\": \
    \"2026-05-16T00:00:00Z\", \"to_date\": \"2026-06-15T00:00:00Z\"}`, `resolution_status: \"ongoing\"`. Note \
    both periods got real `date_range` values -- never null when dates are stated -- and neither was marked \
    `resolved` just because the text is matter-of-fact. \
    Second worked example, reference date 2026-08-01T00:00:00Z: input \"From Saturday 29 August to Friday \
    11 September, buses replace trains between Barrhead and Kilmarnock / Dumfries. Monday to Saturday \
    during this period, buses operate between Kilmarnock and Troon, where passengers can connect with \
    trains to / from Ayr. No scheduled services operate between Kilmarnock and Ayr / Stranraer on \
    Sundays.\" segments into exactly three periods, all sharing the same overall date range but none \
    merged into one, because each names a different leg and/or a different treatment: period 1 -- \
    `scope_description` \"buses replace trains, Barrhead to Kilmarnock / Dumfries\", `date_range` \
    `{\"from_date\": \"2026-08-29T00:00:00Z\", \"to_date\": \"2026-09-12T00:00:00Z\"}`, `schedule_window: \
    null` (applies every day of the range), `apparent_severity: \"severe_disruption\"`; period 2 -- \
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
    pub fn new(base_url: String, api_key: Option<String>, model: String, request_timeout: std::time::Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            // Only fails if the TLS backend can't initialize, which would
            // break every request anyway -- there is no useful degraded mode.
            .expect("reqwest client with a timeout must build");
        Self { base_url, api_key, model, http }
    }

    async fn chat_completion(&self, system_prompt: &str, user_content: String, schema_name: &'static str, schema: serde_json::Value) -> anyhow::Result<String> {
        let request = ChatCompletionRequest {
            model: &self.model,
            messages: vec![
                ChatMessage { role: "system", content: system_prompt.to_string() },
                ChatMessage { role: "user", content: user_content },
            ],
            response_format: ResponseFormat {
                kind: "json_schema",
                json_schema: JsonSchemaSpec { name: schema_name, strict: true, schema },
            },
            temperature: 0.0,
        };

        let mut req = self.http.post(format!("{}/chat/completions", self.base_url)).json(&request);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }

        let response = req.send().await?.error_for_status()?;
        let body: ChatCompletionResponse = response.json().await?;
        let content = body
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("chat completion response had no choices"))?
            .message
            .content;
        Ok(content)
    }

    /// `reference_date` is the incident's `first_seen_at` (or, if the
    /// caller has nothing better, the current time) -- threaded into the
    /// user content so the model can resolve year-less dates in the text
    /// against a concrete anchor (design §1's "year inference" convention).
    pub async fn extract_primary(&self, summary: &str, description: &str, reference_date: DateTime<Utc>) -> anyhow::Result<PrimaryExtraction> {
        let user_content = format!(
            "This incident was first reported around {}. Resolve any year-less date in the text below \
             relative to that reference date.\nSummary: {summary}\nDescription: {description}",
            reference_date.to_rfc3339()
        );
        let content = self
            .chat_completion(PRIMARY_PROMPT, user_content, PRIMARY_SCHEMA_NAME, primary_schema())
            .await?;
        let mut extraction: PrimaryExtraction = serde_json::from_str(&content)
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
        Ok(extraction)
    }

    /// `periods` is the primary pass's already-segmented period list
    /// (design §2) -- the adversarial pass does not re-derive periods, it
    /// only returns a per-period resolution-status verdict, index-aligned
    /// and echoing back each period's `period_index`/`scope_description`.
    pub async fn extract_adversarial(&self, summary: &str, description: &str, periods: &[ExtractionPeriod]) -> anyhow::Result<Vec<AdversarialPeriodVerdict>> {
        let user_content = build_period_user_content(summary, description, periods)?;
        let content = self
            .chat_completion(ADVERSARIAL_PROMPT, user_content, ADVERSARIAL_SCHEMA_NAME, adversarial_schema())
            .await?;
        let extraction: AdversarialExtraction = serde_json::from_str(&content)
            .map_err(|err| anyhow::anyhow!("adversarial extraction returned malformed JSON: {err}"))?;
        Ok(extraction.periods)
    }

    pub async fn extract_severity_adversarial(&self, summary: &str, description: &str, periods: &[ExtractionPeriod]) -> anyhow::Result<Vec<SeverityAdversarialPeriodVerdict>> {
        let user_content = build_period_user_content(summary, description, periods)?;
        let content = self
            .chat_completion(SEVERITY_ADVERSARIAL_PROMPT, user_content, SEVERITY_ADVERSARIAL_SCHEMA_NAME, severity_adversarial_schema())
            .await?;
        let extraction: SeverityAdversarialExtraction = serde_json::from_str(&content)
            .map_err(|err| anyhow::anyhow!("severity adversarial extraction returned malformed JSON: {err}"))?;
        Ok(extraction.periods)
    }
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
    periods.sort_by_key(|period| (std::cmp::Reverse(crate::combine::severity_hint_rank(&period.apparent_severity)), effective_from_date(period)));
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
fn build_period_user_content(summary: &str, description: &str, periods: &[ExtractionPeriod]) -> anyhow::Result<String> {
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let result = client
            .extract_primary("Signal failure at Reading", "Now resolved", reference_date())
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let result = client
            .extract_primary("Buses replace trains", "Engineering works", reference_date())
            .await
            .unwrap();

        assert_eq!(result.periods[0].impact_type.as_deref(), Some("rail_replacement_bus"));
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let result = client.extract_primary("Signal failure", "Delays", reference_date()).await.unwrap();

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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let result = client
            .extract_primary("Wandsworth Town platform closures", "Two sequential phases", reference_date())
            .await
            .unwrap();

        assert_eq!(result.periods.len(), 2);
        assert_eq!(result.periods[0].scope_description.as_deref(), Some("platform 2 closed, calls at platform 1"));
        assert_eq!(result.periods[1].scope_description.as_deref(), Some("platform 3 closed, calls at platform 4"));
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let result = client.extract_primary("Signal failure", "Delays", reference_date()).await;

        assert!(result.is_err(), "an empty periods array must be a hard failure, not a zero-period success");
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
            "blocked_or_suspended", "severe_disruption", "blocked_or_suspended", "severe_disruption",
            "blocked_or_suspended", "severe_disruption", "blocked_or_suspended", "severe_disruption",
            "normal", "normal", "normal", "normal", "normal",
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let result = client.extract_primary("Signal failure", "Delays", reference_date()).await;

        let extraction = result.expect("exceeding the soft cap must now truncate and succeed, not fail");
        assert_eq!(extraction.periods.len(), 8);
        assert_eq!(extraction.dropped_period_count, 5);
        let kept: Vec<&str> = extraction.periods.iter().map(|p| p.scope_description.as_deref().unwrap()).collect();
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let extraction = client
            .extract_primary("Signal failure", "Delays", reference_date())
            .await
            .expect("9 periods against a cap of 8 must truncate and succeed");

        assert_eq!(extraction.periods.len(), 8);
        assert_eq!(extraction.dropped_period_count, 1);
        let kept: Vec<&str> = extraction.periods.iter().map(|p| p.scope_description.as_deref().unwrap()).collect();
        assert!(kept.contains(&"candidate_none_date"), "the null-from_date candidate must win the tiebreak: {kept:?}");
        assert!(!kept.contains(&"candidate_some_date"), "the dated candidate must lose the tiebreak: {kept:?}");
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let result = client.extract_primary("Signal failure", "Delays", reference_date()).await;

        let extraction = result.expect("exactly MAX_PERIODS should still be accepted, only exceeding it truncates");
        assert_eq!(extraction.periods.len(), MAX_PERIODS);
        assert_eq!(extraction.dropped_period_count, 0, "the boundary case must not report any truncation");
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        // If the reference date weren't included in the request body, the
        // mock's `body_string_contains` matcher above would not match and
        // this call would fail (no mock configured to respond).
        let result = client.extract_primary("11 May to 26 July", "no year stated", reference_date()).await;

        assert!(result.is_ok(), "reference date must be present in the request sent to the LLM: {result:?}");
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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
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

        let result = client.extract_adversarial("summary", "description", &periods).await.unwrap();

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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
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

        let result = client.extract_severity_adversarial("Delays", "Minor knock-on delays", &periods).await.unwrap();

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

        let client = LlmClient::new(server.uri(), None, "test-model".to_string(), DEFAULT_REQUEST_TIMEOUT);
        let result = client.extract_primary("Signal failure", "Delays", reference_date()).await;

        assert!(result.is_err(), "malformed content must be rejected, not silently stored");
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
        assert_eq!(range.from_date, Some("2026-05-11T00:00:00Z".parse().unwrap()));
        assert_eq!(range.to_date, Some("2026-07-27T00:00:00Z".parse().unwrap()));
    }

    #[test]
    fn date_range_resolves_correctly_when_stated_date_is_before_the_reference_year() {
        // Incident first seen in December describing a date that has
        // already passed this year almost certainly means next year -- the
        // resolved wire value reflects that rollover.
        let json = serde_json::json!({ "from_date": "2027-01-15T00:00:00Z", "to_date": null });
        let range: DateRange = serde_json::from_value(json).unwrap();
        assert_eq!(range.from_date, Some("2027-01-15T00:00:00Z".parse().unwrap()));
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
        assert_eq!(range, DateRange { from_date: None, to_date: None });
    }

    // --- Live eval against a real OpenAI-compatible endpoint ---
    //
    // Ignored by default (no network/creds in normal CI). Run explicitly with:
    //   LLM_BASE_URL=... LLM_API_KEY=... LLM_MODEL=... \
    //     cargo test -p enricher --lib llm::tests::live_eval -- --ignored --nocapture
    // This is the design doc's own testing-plan item ("Golden corpus, run as
    // a live eval, not just fixtures") for the two central open risks: does
    // the configured model segment multi-period text correctly (risk #1),
    // and does `strict: true` actually hold for an array-of-objects schema
    // on this backend (risk #2)? Also times each call, which is the
    // ground-truth data point for whether `LLM_REQUEST_TIMEOUT_SECS`'s
    // default is realistic against this specific deployment.

    fn live_client_from_env() -> LlmClient {
        let base_url = std::env::var("LLM_BASE_URL").expect("LLM_BASE_URL must be set for live eval");
        let api_key = std::env::var("LLM_API_KEY").ok();
        let model = std::env::var("LLM_MODEL").expect("LLM_MODEL must be set for live eval");
        // Overridable per-run via LIVE_EVAL_TIMEOUT_SECS -- useful for giving a
        // model extra room on a cold start (first request after this server
        // swaps a different model into memory) without changing every other
        // candidate's ceiling.
        let timeout_secs: u64 = std::env::var("LIVE_EVAL_TIMEOUT_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(180);
        LlmClient::new(base_url, api_key, model, std::time::Duration::from_secs(timeout_secs))
    }

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
    const IMPACT_DIVERSION_SUMMARY: &str = "Rail replacement buses and diversions between London Bridge and Croydon";
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
    async fn run_battery_attempt(client: &LlmClient, label: &str, attempt: u32, summary: &str, description: &str) {
        let start = std::time::Instant::now();
        match client.extract_primary(summary, description, eval_reference_date()).await {
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
                        p.scope_description, p.date_range, p.schedule_window, p.resolution_status, p.apparent_severity, p.impact_type
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
        let repeats: u32 = std::env::var("LIVE_EVAL_REPEATS").ok().and_then(|v| v.parse().ok()).unwrap_or(3);

        for attempt in 1..=repeats {
            run_battery_attempt(&client, "multi", attempt, WANDSWORTH_TOWN_SUMMARY, WANDSWORTH_TOWN_DESCRIPTION).await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(&client, "flat", attempt, FLAT_ETA_SUMMARY, FLAT_ETA_DESCRIPTION).await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(&client, "trap", attempt, TRAP_SUMMARY, TRAP_DESCRIPTION).await;
        }
        for attempt in 1..=repeats {
            run_battery_attempt(&client, "dow_legs", attempt, BARRHEAD_DUMFRIES_SUMMARY, BARRHEAD_DUMFRIES_DESCRIPTION).await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(&client, "undated_aside", attempt, NORWOOD_JUNCTION_SUMMARY, NORWOOD_JUNCTION_DESCRIPTION).await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(&client, "impact_bus_noservice", attempt, IMPACT_BUS_NOSERVICE_SUMMARY, IMPACT_BUS_NOSERVICE_DESCRIPTION).await;
        }
        for attempt in 1..=repeats.min(2) {
            run_battery_attempt(&client, "impact_diversion", attempt, IMPACT_DIVERSION_SUMMARY, IMPACT_DIVERSION_DESCRIPTION).await;
        }
    }

    #[tokio::test]
    #[ignore = "requires network access to a real LLM_BASE_URL; run explicitly, see comment above"]
    async fn live_eval_wandsworth_town_segments_into_two_periods() {
        let client = live_client_from_env();
        let reference_date = "2026-04-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let start = std::time::Instant::now();
        let primary = client
            .extract_primary(WANDSWORTH_TOWN_SUMMARY, WANDSWORTH_TOWN_DESCRIPTION, reference_date)
            .await
            .expect("primary extraction should succeed against a real endpoint");
        eprintln!("primary call took {:?}, category={:?}, periods={}", start.elapsed(), primary.category, primary.periods.len());
        for (i, p) in primary.periods.iter().enumerate() {
            eprintln!(
                "  period[{i}]: scope={:?} date_range={:?} schedule_window={:?} resolution_status={:?} apparent_severity={:?}",
                p.scope_description, p.date_range, p.schedule_window, p.resolution_status, p.apparent_severity
            );
        }
        assert!(!primary.periods.is_empty(), "periods must never be empty on a successful parse");

        let start = std::time::Instant::now();
        let resolution = client
            .extract_adversarial(WANDSWORTH_TOWN_SUMMARY, WANDSWORTH_TOWN_DESCRIPTION, &primary.periods)
            .await
            .expect("resolution-adversarial pass should succeed against a real endpoint");
        eprintln!("resolution-adversarial call took {:?}: {:?}", start.elapsed(), resolution);
        assert_eq!(resolution.len(), primary.periods.len(), "adversarial array must be index-aligned with primary's periods");

        let start = std::time::Instant::now();
        let severity = client
            .extract_severity_adversarial(WANDSWORTH_TOWN_SUMMARY, WANDSWORTH_TOWN_DESCRIPTION, &primary.periods)
            .await
            .expect("severity-adversarial pass should succeed against a real endpoint");
        eprintln!("severity-adversarial call took {:?}: {:?}", start.elapsed(), severity);
        assert_eq!(severity.len(), primary.periods.len(), "adversarial array must be index-aligned with primary's periods");

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
            .extract_primary(BARRHEAD_DUMFRIES_SUMMARY, BARRHEAD_DUMFRIES_DESCRIPTION, reference_date)
            .await
            .expect("primary extraction should succeed against a real endpoint");
        eprintln!("primary call took {:?}, category={:?}, periods={}", start.elapsed(), primary.category, primary.periods.len());
        for (i, p) in primary.periods.iter().enumerate() {
            eprintln!(
                "  period[{i}]: scope={:?} date_range={:?} schedule_window={:?} resolution_status={:?} apparent_severity={:?}",
                p.scope_description, p.date_range, p.schedule_window, p.resolution_status, p.apparent_severity
            );
        }
        assert!(!primary.periods.is_empty(), "periods must never be empty on a successful parse");

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
        let user_content = format!("Summary: {WANDSWORTH_TOWN_SUMMARY}\nDescription: {WANDSWORTH_TOWN_DESCRIPTION}");
        let raw = client
            .chat_completion(PRIMARY_PROMPT, user_content, PRIMARY_SCHEMA_NAME, primary_schema())
            .await
            .expect("raw chat completion should succeed");
        eprintln!("=== RAW CONTENT ({} bytes) ===\n{raw}\n=== END RAW CONTENT ===", raw.len());
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
        eprintln!("primary call took {:?}: {:?}", start.elapsed(), primary.periods);

        assert_eq!(
            primary.periods.len(),
            1,
            "a flat single-fact incident with no distinct sub-periods should not be over-segmented"
        );
    }
}
