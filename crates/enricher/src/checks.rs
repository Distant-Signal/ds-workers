//! Deterministic checks on a combined extraction, run for every model after
//! the three calls and `combine::combine_periods` (docs/enricher-anthropic.md,
//! "Output checks"). They come from the 2026-10-13 blind model evaluation:
//!
//! - **Rail replacement guard** ([`mentions_replacement_service`]):
//!   `impact_type: rail_replacement_bus` stays only when the incident text
//!   mentions a replacement service. A list of local buses that accept rail
//!   tickets is not one; the commonest false tag in the evaluation. The tag
//!   is cleared to `null` otherwise.
//! - **Stated end time** ([`stated_end_times`]): when the text gives an
//!   "expected until HH:MM" (or "until approximately HH:MM", or "expected
//!   until the end of the day") end time, an ETA-shaped period whose
//!   `to_date` is an hour or more from every time the text states is
//!   flagged. The usual causes: a UK clock time written as UTC, a day-roll
//!   mistake, a stale time. When the extraction has exactly one ETA-shaped
//!   period it is corrected to the one stated end time, or, with no single
//!   unambiguous one, its end time is dropped; with several ETA-shaped
//!   periods (a phased notice), or a time placed only by a bare weekday,
//!   it is only flagged.
//! - **Stated year** ([`years::check_years`]): a period date on a day and
//!   month the text states without a year ("Sunday 3 January") must be in
//!   the year [`years::resolve`] gives it against the reference date; one
//!   that isn't is moved there, with the rest of its period.
//!
//! The third check, that an adversarial pass's verdicts echo the primary
//! periods, is `combine::echo_mismatch` (and the retry around it).
//!
//! Everything here is pure: the service applies it before writing
//! (`main.rs::finish_extraction`), and `enricher eval` applies it to each
//! record's extraction with the case's text, so recorded outputs re-check
//! offline exactly as the service would.

use std::sync::LazyLock;

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Europe::London;
use regex::Regex;
use serde::Serialize;

use crate::llm::ExtractionPeriod;

pub(crate) mod years;

pub(crate) use years::{YearAction, YearFlag};

/// The incident text the checks read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckText<'a> {
    pub summary: &'a str,
    pub description: &'a str,
    /// The reference time the primary pass was given.
    pub reference_date: DateTime<Utc>,
}

/// What [`apply`] changed or flagged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct CheckReport {
    /// Periods whose `rail_replacement_bus` was cleared to `null`.
    pub rail_replacement_downgraded: usize,
    /// Every period whose end time was flagged, and what was done.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub end_times: Vec<EndTimeFlag>,
    /// Every period whose year was flagged, and what was done.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub years: Vec<YearFlag>,
}

impl CheckReport {
    /// Whether nothing was changed or flagged.
    pub(crate) fn is_clean(&self) -> bool {
        self.rail_replacement_downgraded == 0 && self.end_times.is_empty() && self.years.is_empty()
    }
}

/// One flagged period end time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct EndTimeFlag {
    pub period_index: usize,
    /// The model's `to_date`.
    pub to_date: DateTime<Utc>,
    /// The end times the text states (UTC), in order.
    pub stated: Vec<DateTime<Utc>>,
    pub action: EndTimeAction,
}

/// What the stated-end-time check did with a flagged period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EndTimeAction {
    /// `to_date` set to the one stated end time.
    Corrected,
    /// No single stated end time: `to_date` cleared.
    Dropped,
    /// Several ETA-shaped periods, or a time placed only by a bare
    /// weekday: left as it was, only reported.
    Flagged,
}

impl EndTimeAction {
    pub(crate) const ALL: [Self; 3] = [Self::Corrected, Self::Dropped, Self::Flagged];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Corrected => "corrected",
            Self::Dropped => "dropped",
            Self::Flagged => "flagged",
        }
    }
}

/// Applies the checks to `periods` in place. The year check runs first,
/// so the end time check compares stated times with corrected dates.
pub(crate) fn apply(text: &CheckText<'_>, periods: &mut [ExtractionPeriod]) -> CheckReport {
    let years = years::check_years(text, periods);
    CheckReport {
        rail_replacement_downgraded: guard_rail_replacement(text, periods),
        end_times: check_end_times(text, periods),
        years,
    }
}

// ---------------------------------------------------------------------------
// Text normalisation.
// ---------------------------------------------------------------------------

/// Lower-case plain text: tags become sentence breaks, the common entities
/// are decoded, whitespace collapses.
pub(crate) fn plain_text(html: &str) -> String {
    let no_tags = PATTERNS.tag.replace_all(html, " . ");
    let mut text = no_tags.to_lowercase();
    for (entity, replacement) in [
        ("&amp;", "&"),
        ("&nbsp;", " "),
        ("&#39;", "'"),
        ("&quot;", "\""),
        ("&rsquo;", "'"),
        ("&lsquo;", "'"),
        ("\u{2019}", "'"),
        ("\u{a0}", " "),
    ] {
        text = text.replace(entity, replacement);
    }
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ---------------------------------------------------------------------------
// Rail replacement guard.
// ---------------------------------------------------------------------------

/// Road vehicles that can stand in for a train.
const VEHICLE: &str = r"(?:bus|buses|coach|coaches|road vehicles?|taxis?)";

/// Phrases that state a replacement service (lower case, `{V}` = [`VEHICLE`]).
const REPLACEMENT_PHRASES: &[&str] = &[
    r"rail replacement",
    r"replacement (?:bus|buses|coach|coaches|road transport|road vehicles?|taxis?|service)",
    r"replacement transport",
    r"replacement vehicles? (?:will|are|is)",
    r"{V} (?:will |are |is )?(?:replace|replacing)",
    r"replaced by (?:a |rail replacement |replacement )?{V}",
    r"in place of (?:the )?trains?",
    r"{V} (?:will |are |is |shall )?(?:run|running|runs|operate|operating|operates) (?:between|from)",
    r"{V} (?:will |is )?(?:depart|departs|departing|leave|leaves|leaving) ",
    r"{V} (?:to|will) (?:run|operate)",
    r"{V} (?:will be|has been|have been|are being|is being|are|is) (?:provided|arranged|organised|organized|sourced|requested|confirmed|laid on)",
    r"(?:arranged|organised|organized|sourced|requested|provided|confirmed) (?:for )?(?:\w+ ){0,2}(?:road transport|alternative transport|{V})",
    r"\d+x? (?:\w+ ){0,2}{V} (?:confirmed|arranged|requested|provided)",
    r"bus service between",
    r"(?:bus|coach) shuttle",
    r"shuttle (?:bus|buses|coach|coaches)",
    r"alternative road transport",
    r"(?:road|alternative) transport (?:has been|have been|will|is|are|to)",
    r"travelling by road transport",
];

/// A sentence with one of these is advice or boilerplate, not a statement
/// that a replacement runs ("for help if you need to travel on a rail
/// replacement service...", "rail replacement buses will not be provided").
const NOT_A_REPLACEMENT: &[&str] = &[
    "travel advice",
    "if you need to travel",
    "if all or part of your journey",
    "including train times",
    "after travelling on",
    "are not permitted on",
    "not be provided",
    "won't be provided",
    "no rail replacement",
    "no replacement",
    "unable to provide",
    "not able to provide",
    "cannot be provided",
    "services page",
    "pick-up and drop-off",
    "pick up and drop off",
];

/// Whether the text states that some replacement service runs (or has
/// been requested). Lists of local buses that accept rail tickets ("your
/// ticket will be accepted on Stagecoach route 700") don't count.
pub(crate) fn mentions_replacement_service(summary: &str, description: &str) -> bool {
    let text = plain_text(&format!("{summary} . {description}"));
    PATTERNS.sentence_break.split(&text).any(|sentence| {
        PATTERNS.replacement.is_match(sentence)
            && !NOT_A_REPLACEMENT
                .iter()
                .any(|hedge| sentence.contains(hedge))
    })
}

/// Clears `rail_replacement_bus` on every period when the text mentions no
/// replacement service; returns how many it cleared.
fn guard_rail_replacement(text: &CheckText<'_>, periods: &mut [ExtractionPeriod]) -> usize {
    let tagged = periods
        .iter()
        .filter(|p| p.impact_type.as_deref() == Some("rail_replacement_bus"))
        .count();
    if tagged == 0 || mentions_replacement_service(text.summary, text.description) {
        return 0;
    }
    for period in periods
        .iter_mut()
        .filter(|p| p.impact_type.as_deref() == Some("rail_replacement_bus"))
    {
        period.impact_type = None;
    }
    tagged
}

// ---------------------------------------------------------------------------
// Stated end times.
// ---------------------------------------------------------------------------

/// A flagged end time is this far or more from every stated time. An hour
/// exactly is the local-time-as-UTC error, so it counts.
const END_TIME_TOLERANCE: Duration = Duration::minutes(60);

/// A stated "HH:MM" with no day that falls more than this before the
/// reference time is read as tomorrow's ("until approximately 05:30" at
/// 22:50); within it, as today's (a time that has just passed).
const ROLL_FORWARD_AFTER: Duration = Duration::hours(2);

/// An ETA-shaped period starts no later than this after the reference time.
const ETA_START_SLACK: Duration = Duration::hours(1);

const MONTHS: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

/// The end times a text states, in UTC.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StatedTimes {
    /// "expected until ..." style end times (the ETA).
    pub eta: Vec<DateTime<Utc>>,
    /// Other "until HH:MM" times ("revised until 15:30 today"): accepted
    /// as a period's end, never used as a correction.
    pub other: Vec<DateTime<Utc>>,
    /// Some ETA's day came from a bare weekday ("until 18:00 on Sunday"),
    /// read as the next one; a notice published a week or more ahead
    /// means a later one, so such a check only flags.
    pub uncertain: bool,
}

/// How the day of a stated time was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Day {
    /// No day named: the reference day (rolled forward when long past).
    Reference,
    /// "today", "tomorrow" or a date.
    Named,
    /// A bare weekday: its next occurrence.
    Weekday,
}

/// The end times `text` (HTML or plain) states, resolved against
/// `reference` in Europe/London.
pub(crate) fn stated_end_times(text: &str, reference: DateTime<Utc>) -> StatedTimes {
    let text = plain_text(text);
    let reference_local = reference.with_timezone(&London).naive_local();
    let mut stated = StatedTimes::default();
    let mut eta_spans = Vec::new();
    for pattern in [&PATTERNS.expected_until, &PATTERNS.until_approximately] {
        for caps in pattern.captures_iter(&text) {
            if let Some(whole) = caps.get(0) {
                eta_spans.push(whole.range());
            }
            if let Some((at, day)) = place_clock_time(&caps, reference_local) {
                push_unique(&mut stated.eta, at);
                stated.uncertain |= day == Day::Weekday;
            }
        }
    }
    for caps in PATTERNS.end_of_day.captures_iter(&text) {
        if let Some(whole) = caps.get(0) {
            eta_spans.push(whole.range());
        }
        let after = caps.name("after").map_or("", |m| m.as_str());
        let (day, kind) = resolve_day(after, reference_local);
        stated.uncertain |= kind == Day::Weekday;
        if let Some(at) = day
            .succ_opt()
            .and_then(|next| london_to_utc(next.and_hms_opt(0, 0, 0)?))
        {
            push_unique(&mut stated.eta, at);
        }
    }
    for caps in PATTERNS.any_until.captures_iter(&text) {
        let Some(whole) = caps.get(0) else { continue };
        if eta_spans.iter().any(|span| span.contains(&whole.start())) {
            continue;
        }
        if let Some((at, _)) = place_clock_time(&caps, reference_local)
            && !stated.eta.contains(&at)
        {
            push_unique(&mut stated.other, at);
        }
    }
    stated
}

fn push_unique(times: &mut Vec<DateTime<Utc>>, at: DateTime<Utc>) {
    if !times.contains(&at) {
        times.push(at);
    }
}

/// An `hh`/`mm`/`after` capture as a UTC instant, and how its day was found.
fn place_clock_time(
    caps: &regex::Captures<'_>,
    reference_local: NaiveDateTime,
) -> Option<(DateTime<Utc>, Day)> {
    let mut hour: u32 = caps.name("hh")?.as_str().parse().ok()?;
    match caps.name("ampm").map(|m| m.as_str()) {
        Some("pm") if hour < 12 => hour += 12,
        Some("am") if hour == 12 => hour = 0,
        _ => {}
    }
    let minute: u32 = caps.name("mm")?.as_str().parse().ok()?;
    let after = caps.name("after").map_or("", |m| m.as_str());
    let (day, kind) = resolve_day(after, reference_local);
    let mut local = day.and_hms_opt(hour, minute, 0)?;
    if kind == Day::Reference && local < reference_local - ROLL_FORWARD_AFTER {
        local += Duration::days(1);
    }
    Some((london_to_utc(local)?, kind))
}

/// The day the words after a stated time name ("tomorrow", "on Saturday
/// 19 September", "Tuesday"), and how; else the reference day.
fn resolve_day(after: &str, reference_local: NaiveDateTime) -> (NaiveDate, Day) {
    let today = reference_local.date();
    let Some(caps) = PATTERNS.day_words.captures(after) else {
        return (today, Day::Reference);
    };
    if caps.name("tomorrow").is_some() {
        return (today + Duration::days(1), Day::Named);
    }
    if caps.name("today").is_some() {
        return (today, Day::Named);
    }
    if let (Some(dom), Some(month)) = (caps.name("dom"), caps.name("month")) {
        // The year rule the stated-year check uses (`years::resolve`).
        let stated = dom
            .as_str()
            .parse()
            .ok()
            .zip(years::month_number(month.as_str()))
            .map(|(day, month)| years::StatedDate {
                month,
                day,
                weekday: caps
                    .name("weekday")
                    .and_then(|w| years::weekday(w.as_str())),
                year: None,
            });
        if let Some(date) = stated.and_then(|stated| years::resolve(stated, today)) {
            return (date, Day::Named);
        }
    }
    if let Some(weekday) = caps.name("weekday") {
        let index = WEEKDAYS
            .iter()
            .position(|d| *d == weekday.as_str())
            .unwrap_or(0);
        let from = today.weekday().num_days_from_monday() as usize;
        let ahead = (index + 7 - from) % 7;
        return (
            today + Duration::days(i64::try_from(ahead).unwrap_or(0)),
            Day::Weekday,
        );
    }
    (today, Day::Reference)
}

const WEEKDAYS: [&str; 7] = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
];

/// A Europe/London wall-clock time as UTC: the earlier instant in the
/// autumn's repeated hour, the instant an hour on in the spring's gap.
fn london_to_utc(local: NaiveDateTime) -> Option<DateTime<Utc>> {
    London
        .from_local_datetime(&local)
        .earliest()
        .or_else(|| {
            London
                .from_local_datetime(&(local + Duration::hours(1)))
                .earliest()
        })
        .map(|at| at.with_timezone(&Utc))
}

/// Whether a period reads as "this disruption, until its ETA": an end and
/// no weekly window, starting no later than shortly after the reference.
fn is_eta_shaped(period: &ExtractionPeriod, reference: DateTime<Utc>) -> bool {
    let Some(range) = &period.date_range else {
        return false;
    };
    range.to_date.is_some()
        && period.schedule_window.is_none()
        && range
            .from_date
            .is_none_or(|from| from <= reference + ETA_START_SLACK)
}

fn check_end_times(text: &CheckText<'_>, periods: &mut [ExtractionPeriod]) -> Vec<EndTimeFlag> {
    let summary = stated_end_times(text.summary, text.reference_date);
    let description = stated_end_times(text.description, text.reference_date);
    // The summary is the headline the operator updates with each new
    // estimate; a different "expected until" left in the description is
    // usually the previous one. So the summary's ETA wins when it has one.
    let source = if summary.eta.is_empty() {
        &description
    } else {
        &summary
    };
    let (eta, uncertain) = (source.eta.clone(), source.uncertain);
    if eta.is_empty() {
        return Vec::new();
    }
    let mut accepted = eta.clone();
    for at in summary.other.iter().chain(&description.other) {
        push_unique(&mut accepted, *at);
    }
    let eta_shaped: Vec<usize> = periods
        .iter()
        .enumerate()
        .filter(|(_, p)| is_eta_shaped(p, text.reference_date))
        .map(|(i, _)| i)
        .collect();
    let correction = match eta.as_slice() {
        [only] => Some(*only),
        _ => None,
    };
    let mut flags = Vec::new();
    for &index in &eta_shaped {
        let period = &mut periods[index];
        let Some(range) = period.date_range.as_mut() else {
            continue;
        };
        let Some(to_date) = range.to_date else {
            continue;
        };
        if accepted
            .iter()
            .any(|at| (to_date - *at).abs() < END_TIME_TOLERANCE)
        {
            continue;
        }
        let action = if eta_shaped.len() > 1 || uncertain {
            EndTimeAction::Flagged
        } else if let Some(at) = correction {
            range.to_date = Some(at);
            EndTimeAction::Corrected
        } else {
            range.to_date = None;
            if range.from_date.is_none() {
                period.date_range = None;
            }
            EndTimeAction::Dropped
        };
        flags.push(EndTimeFlag {
            period_index: index,
            to_date,
            stated: eta.clone(),
            action,
        });
    }
    flags
}

// ---------------------------------------------------------------------------
// Patterns.
// ---------------------------------------------------------------------------

struct Patterns {
    tag: Regex,
    sentence_break: Regex,
    replacement: Regex,
    expected_until: Regex,
    until_approximately: Regex,
    end_of_day: Regex,
    any_until: Regex,
    day_words: Regex,
    stated_date: Regex,
    any_year: Regex,
}

#[expect(
    clippy::expect_used,
    reason = "constant regex literals, compiled by the tests"
)]
static PATTERNS: LazyLock<Patterns> = LazyLock::new(|| {
    let compile = |pattern: &str| Regex::new(pattern).expect("valid regex");
    let qualifier = r"(?:at least |approximately |approx\.? |around |about |circa |roughly )?";
    let time = r"(?P<hh>[01]?\d|2[0-3])[:.](?P<mm>[0-5]\d)(?:\s?(?P<ampm>am|pm))?";
    let after = r"(?P<after>[^.]{0,40})";
    let expect = r"\b(?:expected|due|likely|anticipated|estimated) (?:to (?:continue|last|remain|run|cause disruption) )?until ";
    let phrases: Vec<String> = REPLACEMENT_PHRASES
        .iter()
        .map(|p| format!("(?:{})", p.replace("{V}", VEHICLE)))
        .collect();
    Patterns {
        tag: compile(r"<[^>]*>"),
        sentence_break: compile(r"[.!?:;]\s+"),
        replacement: compile(&phrases.join("|")),
        expected_until: compile(&format!(
            r"(?:{expect}|\bto cause disruption until ){qualifier}{time}{after}"
        )),
        until_approximately: compile(&format!(
            r"\buntil (?:approximately|approx\.?|around|about|circa) {time}{after}"
        )),
        end_of_day: compile(&format!(r"{expect}(?:the )?end of (?:the )?day{after}")),
        any_until: compile(&format!(r"\buntil {qualifier}{time}{after}")),
        day_words: compile(&format!(
            r"^\s*,?\s*(?:(?P<tomorrow>tomorrow)|(?P<today>today|tonight)|(?:on\s+)?(?:(?P<weekday>monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s*,?\s*)?(?:(?P<dom>\d{{1,2}})(?:st|nd|rd|th)?\s+(?P<month>{}))?)",
            MONTHS.join("|")
        )),
        stated_date: compile(&format!(
            r"\b(?:(?P<weekday>{})\s*,?\s+)?(?P<dom>[0-3]?\d)(?:st|nd|rd|th)?\s+(?P<month>{})\b(?:\s*,?\s*(?P<year>20\d\d)\b)?",
            WEEKDAYS.join("|"),
            MONTHS.join("|")
        )),
        any_year: compile(r"\b20\d\d\b"),
    }
});

#[cfg(test)]
mod tests;
