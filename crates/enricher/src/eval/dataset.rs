//! The eval dataset: JSONL, one [`Case`] per line (default
//! `crates/enricher/eval/dataset.jsonl`). Both runners read it: the quality
//! eval scores a case's output against its `expected` gold labels, and the
//! perf benchmark only needs its text (a case with no `expected` is fine
//! there). Format and labelling rules: `docs/enricher-model-eval.md`.
//!
//! Every gold field is optional, and an absent field is *not scored*, which
//! is different from a field given as `null` (scored: the model must return
//! null). That is how a case labels only what its text settles, and leaves
//! a judgment call (e.g. whether an undated aside earns its own period)
//! unscored.

use std::collections::BTreeSet;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};

use crate::llm::ScheduleWindow;

/// The primary schema's enum values (`llm::primary_schema`). The dataset
/// validator rejects any other gold value, so a typo can't silently score
/// every model as wrong.
pub(crate) const RESOLUTION_STATUSES: [&str; 3] = ["ongoing", "residual", "resolved"];
pub(crate) const SEVERITIES: [&str; 4] = [
    "normal",
    "moderate_disruption",
    "severe_disruption",
    "blocked_or_suspended",
];
pub(crate) const IMPACT_TYPES: [&str; 3] =
    ["rail_replacement_bus", "no_scheduled_service", "diversion"];

/// The most gold periods a case may have: the quality eval's exact period
/// matcher (`quality::match_periods`) costs time doubling with each one.
/// Well above the service's own cap on predicted periods
/// (`llm::MAX_PERIODS`, 8), so a gold period past it could never be matched
/// anyway.
pub(crate) const MAX_GOLD_PERIODS: usize = 12;

/// One incident text plus (optionally) its gold labels.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Case {
    /// Unique, stable id (kebab-case by convention). Recorded outputs refer
    /// to cases by id, so renaming one orphans its recordings.
    pub id: String,
    pub summary: String,
    pub description: String,
    /// What `process_incident` passes as `first_seen_at`: the anchor the
    /// model resolves year-less dates (and "18:00" with no date) against.
    pub reference_date: DateTime<Utc>,
    /// Free-form labels (e.g. `multi_period`, `negation`) for slicing.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Why the case exists / how the labels were decided.
    #[serde(default)]
    #[expect(dead_code, reason = "documentation for dataset authors; never read")]
    pub notes: Option<String>,
    /// Gold labels. `None`: an unlabelled case (perf benchmark only).
    #[serde(default)]
    pub expected: Option<Expected>,
}

impl Case {
    /// A short hash of exactly what the model is sent from this case
    /// (`summary`, `description`, `reference_date`), stored on every
    /// record. A replay skips a record whose hash no longer matches: the
    /// case's text was edited after the run, so its output answers a
    /// different question. Gold labels aren't hashed -- re-scoring old
    /// outputs against corrected labels is what replay is for. The fields
    /// are NUL-separated; validation rejects a NUL inside the text, so two
    /// different inputs can't hash alike by moving one across a boundary.
    pub(crate) fn input_hash(&self) -> String {
        let description = format!("{}\0{}", self.description, self.reference_date.to_rfc3339());
        let mut hash = common::text_hash::text_hash(&self.summary, &description);
        hash.truncate(16);
        hash
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Expected {
    /// Accepted `category` values, compared after [`normalize_label`].
    /// `category` is free text in the schema (no enum), so list the
    /// plausible synonyms; leave it out to not score category at all.
    #[serde(default)]
    pub category_any_of: Option<Vec<String>>,
    /// Gold periods, in the order the text gives them. `None`: segmentation
    /// isn't labelled (only completion and category are scored).
    #[serde(default)]
    pub periods: Option<Vec<GoldPeriod>>,
}

impl Expected {
    /// Whether these labels score anything beyond completion.
    pub(crate) fn scores_anything(&self) -> bool {
        self.category_any_of.is_some() || self.periods.is_some()
    }
}

/// One gold period. See the module doc for absent vs `null`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GoldPeriod {
    /// Human-readable reminder of which period this is. Never scored:
    /// `scope_description` is free text.
    #[serde(default)]
    pub scope_hint: Option<String>,
    /// The whole range: scores `date_range`, `from_date` and `to_date`.
    #[serde(default)]
    pub date_range: Gold<GoldDateRange>,
    /// One side only, for a case where the other side is a judgment call.
    /// Not allowed together with `date_range`.
    #[serde(default)]
    pub from_date: Gold<DateTime<Utc>>,
    #[serde(default)]
    pub to_date: Gold<DateTime<Utc>>,
    #[serde(default)]
    pub schedule_window: Gold<ScheduleWindow>,
    #[serde(default)]
    pub resolution_status: Option<OneOrMany<String>>,
    #[serde(default)]
    pub apparent_severity: Option<OneOrMany<String>>,
    /// May include `null` among the accepted values.
    #[serde(default, deserialize_with = "present")]
    pub impact_type: Option<OneOrMany<Option<String>>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GoldDateRange {
    pub from_date: Option<DateTime<Utc>>,
    pub to_date: Option<DateTime<Utc>>,
}

/// A gold field that may be left unscored: absent from the JSON is
/// [`Gold::Unscored`]; present (`null` included) is [`Gold::Expect`].
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum Gold<T> {
    #[default]
    Unscored,
    Expect(Option<T>),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Gold<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<T>::deserialize(deserializer).map(Gold::Expect)
    }
}

impl<T> Gold<T> {
    fn is_scored(&self) -> bool {
        matches!(self, Gold::Expect(_))
    }
}

impl GoldPeriod {
    /// Gold `from_date`: from `date_range` when that is given (`null` range
    /// means a null `from_date`), else the standalone `from_date`.
    pub(crate) fn gold_from_date(&self) -> Gold<DateTime<Utc>> {
        match &self.date_range {
            Gold::Expect(range) => Gold::Expect(range.as_ref().and_then(|r| r.from_date)),
            Gold::Unscored => self.from_date.clone(),
        }
    }

    pub(crate) fn gold_to_date(&self) -> Gold<DateTime<Utc>> {
        match &self.date_range {
            Gold::Expect(range) => Gold::Expect(range.as_ref().and_then(|r| r.to_date)),
            Gold::Unscored => self.to_date.clone(),
        }
    }
}

/// A single accepted value, or a list of them (any one is correct).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub(crate) enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    pub(crate) fn accepted(&self) -> &[T] {
        match self {
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values,
        }
    }
}

/// Wraps a present field in `Some`, so a `null` value deserializes to
/// `Some(null-ish)` instead of collapsing into "absent" (`None`) -- the
/// [`Gold`] distinction, for the any-of `impact_type`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Lowercase, and every run of non-alphanumerics becomes one `_`:
/// `"Signal Failure"`, `"signal-failure"` and `"signal_failure"` all compare
/// equal.
pub(crate) fn normalize_label(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    for ch in label.trim().chars() {
        if ch.is_alphanumeric() {
            out.extend(ch.to_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

/// Reads and validates a dataset file.
pub(crate) fn load(path: &Path) -> anyhow::Result<Vec<Case>> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| anyhow::anyhow!("reading dataset {}: {err}", path.display()))?;
    parse(&raw).map_err(|err| anyhow::anyhow!("dataset {}: {err}", path.display()))
}

/// Parses JSONL (blank lines skipped) and validates every case.
pub(crate) fn parse(raw: &str) -> anyhow::Result<Vec<Case>> {
    let mut cases = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let case: Case = serde_json::from_str(line)
            .map_err(|err| anyhow::anyhow!("line {}: {err}", index + 1))?;
        cases.push(case);
    }
    validate(&cases)?;
    Ok(cases)
}

/// Keeps only the cases whose id is in `ids` (all of them when `None`);
/// an unknown id is an error rather than a silently smaller run.
pub(crate) fn filter(cases: Vec<Case>, ids: Option<&[String]>) -> anyhow::Result<Vec<Case>> {
    let Some(ids) = ids else {
        return Ok(cases);
    };
    for id in ids {
        if !cases.iter().any(|c| &c.id == id) {
            anyhow::bail!("EVAL_CASES names unknown case id {id:?}");
        }
    }
    Ok(cases.into_iter().filter(|c| ids.contains(&c.id)).collect())
}

fn validate(cases: &[Case]) -> anyhow::Result<()> {
    let mut ids = BTreeSet::new();
    for case in cases {
        let id = &case.id;
        if id.trim().is_empty() {
            anyhow::bail!("a case has an empty id");
        }
        if !ids.insert(id.as_str()) {
            anyhow::bail!("duplicate case id {id:?}");
        }
        if case.summary.trim().is_empty() && case.description.trim().is_empty() {
            anyhow::bail!("case {id:?} has no text");
        }
        // `input_hash` joins the fields with NUL separators, so a NUL inside
        // one could make two different inputs hash alike.
        if case.summary.contains('\0') || case.description.contains('\0') {
            anyhow::bail!("case {id:?}: summary and description must not contain NUL characters");
        }
        let Some(expected) = &case.expected else {
            continue;
        };
        if let Some(categories) = &expected.category_any_of
            && categories.iter().all(|c| normalize_label(c).is_empty())
        {
            anyhow::bail!("case {id:?}: category_any_of has no usable value");
        }
        if let Some(periods) = &expected.periods
            && periods.len() > MAX_GOLD_PERIODS
        {
            anyhow::bail!(
                "case {id:?} has {} gold periods; at most {MAX_GOLD_PERIODS} are supported",
                periods.len()
            );
        }
        for (index, period) in expected.periods.iter().flatten().enumerate() {
            validate_period(period)
                .map_err(|err| anyhow::anyhow!("case {id:?} period {index}: {err}"))?;
        }
    }
    Ok(())
}

fn validate_period(period: &GoldPeriod) -> anyhow::Result<()> {
    if period.date_range.is_scored() && (period.from_date.is_scored() || period.to_date.is_scored())
    {
        anyhow::bail!("give either date_range or from_date/to_date, not both");
    }
    for status in period
        .resolution_status
        .iter()
        .flat_map(OneOrMany::accepted)
    {
        if !RESOLUTION_STATUSES.contains(&status.as_str()) {
            anyhow::bail!("unknown resolution_status {status:?}");
        }
    }
    for severity in period
        .apparent_severity
        .iter()
        .flat_map(OneOrMany::accepted)
    {
        if !SEVERITIES.contains(&severity.as_str()) {
            anyhow::bail!("unknown apparent_severity {severity:?}");
        }
    }
    for impact in period
        .impact_type
        .iter()
        .flat_map(OneOrMany::accepted)
        .flatten()
    {
        if !IMPACT_TYPES.contains(&impact.as_str()) {
            anyhow::bail!("unknown impact_type {impact:?}");
        }
    }
    if let Gold::Expect(Some(window)) = &period.schedule_window {
        if window.days_of_week.is_empty()
            || window.days_of_week.iter().any(|d| !(1..=7).contains(d))
        {
            anyhow::bail!("schedule_window.days_of_week must be ISO weekdays 1-7");
        }
        for time in [&window.start_time, &window.end_time] {
            if !is_hh_mm(time) {
                anyhow::bail!("schedule_window time {time:?} is not HH:MM");
            }
        }
    }
    Ok(())
}

fn is_hh_mm(time: &str) -> bool {
    let Some((hours, minutes)) = time.split_once(':') else {
        return false;
    };
    hours.len() == 2
        && minutes.len() == 2
        && hours.parse::<u8>().is_ok_and(|h| h < 24)
        && minutes.parse::<u8>().is_ok_and(|m| m < 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped seed dataset must always load and validate -- this is the
    /// CI check for anyone adding a case.
    #[test]
    fn shipped_dataset_is_valid() {
        let cases = parse(include_str!("../../eval/dataset.jsonl")).unwrap();
        assert!(cases.len() >= 10, "seed dataset unexpectedly small");
        assert!(cases.iter().all(|c| c.expected.is_some()));
    }

    #[test]
    fn absent_and_null_gold_fields_are_distinct() {
        let cases = parse(
            r#"{"id":"a","summary":"s","description":"d","reference_date":"2026-01-01T00:00:00Z","expected":{"periods":[{"impact_type":null,"date_range":null},{"impact_type":["diversion",null]}]}}"#,
        )
        .unwrap();
        let periods = cases[0]
            .expected
            .as_ref()
            .unwrap()
            .periods
            .as_ref()
            .unwrap();
        assert_eq!(periods[0].impact_type, Some(OneOrMany::One(None)));
        assert_eq!(periods[0].gold_from_date(), Gold::Expect(None));
        assert_eq!(
            periods[1].impact_type,
            Some(OneOrMany::Many(vec![Some("diversion".to_string()), None]))
        );
        // Absent everywhere: nothing scored.
        assert_eq!(periods[1].gold_from_date(), Gold::Unscored);
        assert_eq!(periods[1].resolution_status, None);
        assert_eq!(periods[1].schedule_window, Gold::Unscored);
    }

    #[test]
    fn validation_rejects_bad_gold_values() {
        let line = |expected: &str| {
            format!(
                r#"{{"id":"a","summary":"s","description":"d","reference_date":"2026-01-01T00:00:00Z","expected":{expected}}}"#
            )
        };
        for bad in [
            r#"{"periods":[{"resolution_status":"fixed"}]}"#,
            r#"{"periods":[{"apparent_severity":["normal","bad"]}]}"#,
            r#"{"periods":[{"impact_type":"bus"}]}"#,
            r#"{"periods":[{"schedule_window":{"days_of_week":[0],"start_time":"00:00","end_time":"23:59"}}]}"#,
            r#"{"periods":[{"schedule_window":{"days_of_week":[1],"start_time":"7am","end_time":"23:59"}}]}"#,
            r#"{"periods":[{"date_range":null,"to_date":null}]}"#,
            r#"{"periods":[{"typo_field":1}]}"#,
            r#"{"category_any_of":["  "]}"#,
        ] {
            assert!(parse(&line(bad)).is_err(), "accepted {bad}");
        }
        let dup = format!("{}\n{}", line("{}"), line("{}"));
        assert!(parse(&dup).is_err(), "accepted a duplicate id");
        let too_many = format!(
            r#"{{"periods":[{}]}}"#,
            vec!["{}"; MAX_GOLD_PERIODS + 1].join(",")
        );
        assert!(
            parse(&line(&too_many)).is_err(),
            "accepted too many periods"
        );
        let enough = format!(r#"{{"periods":[{}]}}"#, ["{}"; MAX_GOLD_PERIODS].join(","));
        assert!(parse(&line(&enough)).is_ok());
    }

    /// `input_hash` separates its fields with NUL, so a NUL inside the
    /// text could make two different inputs collide: rejected outright.
    #[test]
    fn validation_rejects_nul_in_the_text() {
        for (summary, description) in [("a\\u0000b", "c"), ("a", "b\\u0000c")] {
            let line = format!(
                r#"{{"id":"a","summary":"{summary}","description":"{description}","reference_date":"2026-01-01T00:00:00Z"}}"#
            );
            let err = parse(&line).unwrap_err();
            assert!(err.to_string().contains("NUL"), "{err}");
        }
    }

    #[test]
    fn input_hash_covers_the_model_input_only() {
        let base =
            r#"{"id":"a","summary":"s","description":"d","reference_date":"2026-01-01T00:00:00Z"}"#;
        let hash = |line: &str| parse(line).unwrap()[0].input_hash();
        let original = hash(base);
        assert_eq!(original.len(), 16);
        assert_eq!(original, hash(base), "deterministic");
        for edited in [
            base.replace(r#""s""#, r#""s2""#),
            base.replace(r#""d""#, r#""d2""#),
            base.replace("2026-01-01", "2026-01-02"),
        ] {
            assert_ne!(original, hash(&edited), "{edited}");
        }
        // Not the id, tags or labels.
        let relabelled = base.replace(
            r#""id":"a""#,
            r#""id":"b","tags":["x"],"expected":{"category_any_of":["y"]}"#,
        );
        assert_eq!(original, hash(&relabelled));
    }

    #[test]
    fn labels_normalize() {
        assert_eq!(normalize_label(" Signal Failure "), "signal_failure");
        assert_eq!(normalize_label("signal-failure"), "signal_failure");
        assert_eq!(normalize_label("Signal__failure!"), "signal_failure");
    }

    #[test]
    fn filter_keeps_named_cases_and_rejects_unknown_ids() {
        let cases = parse(include_str!("../../eval/dataset.jsonl")).unwrap();
        let first = cases[0].id.clone();
        let kept = filter(cases.clone(), Some(std::slice::from_ref(&first))).unwrap();
        assert_eq!(kept.len(), 1);
        assert!(filter(cases, Some(&["no-such-case".to_string()])).is_err());
    }
}
