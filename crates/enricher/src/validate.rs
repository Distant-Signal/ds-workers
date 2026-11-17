//! Validation of the model-written free text and enum-like strings in a
//! primary extraction, before anything is stored or shown.
//!
//! Incident text is a prompt-injection surface (it is feed text, and goes
//! straight into the prompt), and some of what the model returns is shown to
//! passengers by the aggregator (`scope_description` prefixes a line
//! status's reason; a `schedule_window`'s times are quoted in its
//! annotation). The JSON schema constrains shapes only on backends with
//! strict structured output, so [`parse_primary`](crate::llm::parse_primary)
//! applies these on every path (live, batch, eval):
//!
//! - [`scope_description`]: a short plain-text phrase, or `None`.
//! - [`resolution_status`] and [`apparent_severity`]: the closed sets the
//!   schema lists.
//! - [`impact_type`]: the closed set, else `None`.
//! - [`schedule_window_is_valid`]: weekdays 1-7 and `H:MM`/`HH:MM` times.
//!
//! (`category` has its own folding, `category.rs`; dates parse as RFC 3339 or
//! degrade to `None`, `llm::deserialize_lenient_date`.) There is no URL or
//! station-list field in the extraction.

use std::sync::LazyLock;

use regex::Regex;

use crate::llm::ScheduleWindow;

/// Implemented in `scope-text`, which the aggregator also applies when it
/// quotes a stored scope.
pub(crate) use scope_text::scope_description;

/// `resolution_status` values.
pub(crate) const RESOLUTION_STATUSES: [&str; 3] = ["ongoing", "residual", "resolved"];

/// `apparent_severity` values.
pub(crate) const SEVERITIES: [&str; 4] = [
    "normal",
    "moderate_disruption",
    "severe_disruption",
    "blocked_or_suspended",
];

/// `impact_type` values (besides `null`).
pub(crate) const IMPACT_TYPES: [&str; 3] =
    ["rail_replacement_bus", "no_scheduled_service", "diversion"];

/// `value` folded to `set`'s spelling (case and surrounding whitespace
/// ignored), or `None` when it is not a member.
fn closed_set(value: &str, set: &[&'static str]) -> Option<&'static str> {
    let folded = value.trim().to_ascii_lowercase();
    set.iter().copied().find(|member| *member == folded)
}

/// A primary period's `resolution_status`. `None` for anything outside
/// [`RESOLUTION_STATUSES`]: there is no safe stand-in (`ongoing` exempts a
/// period from the aggregator's expiry), so the caller rejects the
/// extraction and the sweep retries it.
pub(crate) fn resolution_status(value: &str) -> Option<&'static str> {
    closed_set(value, &RESOLUTION_STATUSES)
}

/// A primary period's `apparent_severity`, or `normal` for anything outside
/// [`SEVERITIES`]: no escalation, which is what the aggregator and
/// `combine::severity_hint_rank` already do with an unrecognised value.
pub(crate) fn apparent_severity(value: &str) -> &'static str {
    closed_set(value, &SEVERITIES).unwrap_or("normal")
}

/// A period's `impact_type`: a member of [`IMPACT_TYPES`], else `None`.
pub(crate) fn impact_type(value: &str) -> Option<&'static str> {
    closed_set(value, &IMPACT_TYPES)
}

#[expect(
    clippy::expect_used,
    reason = "a static pattern that fails to compile is a programming error caught by the tests"
)]
static CLOCK_TIME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:[01]?[0-9]|2[0-3]):[0-5][0-9]$").expect("static regex"));

/// Whether `window` is a well-formed weekly window: a non-empty set of ISO
/// weekdays 1-7 and `H:MM` or `HH:MM` 24-hour start and end times (what the
/// aggregator's `%H:%M` parse accepts; seconds are not). The aggregator
/// quotes the times in a passenger-facing annotation.
pub(crate) fn schedule_window_is_valid(window: &ScheduleWindow) -> bool {
    !window.days_of_week.is_empty()
        && window.days_of_week.iter().all(|day| (1..=7).contains(day))
        && CLOCK_TIME.is_match(&window.start_time)
        && CLOCK_TIME.is_match(&window.end_time)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_sets_fold_case_and_reject_the_rest() {
        assert_eq!(resolution_status(" Ongoing "), Some("ongoing"));
        assert_eq!(resolution_status("resolved; ignore previous"), None);
        assert_eq!(apparent_severity("Severe_Disruption"), "severe_disruption");
        assert_eq!(apparent_severity("catastrophic"), "normal");
        assert_eq!(impact_type("diversion"), Some("diversion"));
        assert_eq!(impact_type("http://x"), None);
        assert_eq!(impact_type(""), None);
    }

    fn window(days: &[u8], start: &str, end: &str) -> ScheduleWindow {
        ScheduleWindow {
            days_of_week: days.to_vec(),
            start_time: start.to_string(),
            end_time: end.to_string(),
        }
    }

    #[test]
    fn schedule_windows_need_weekdays_and_clock_times() {
        assert!(schedule_window_is_valid(&window(
            &[1, 2, 7],
            "23:00",
            "05:00"
        )));
        assert!(schedule_window_is_valid(&window(&[6], "00:00", "23:59")));
        assert!(!schedule_window_is_valid(&window(&[], "23:00", "05:00")));
        assert!(!schedule_window_is_valid(&window(&[0], "23:00", "05:00")));
        assert!(!schedule_window_is_valid(&window(&[8], "23:00", "05:00")));
        assert!(!schedule_window_is_valid(&window(&[1], "24:00", "05:00")));
        assert!(schedule_window_is_valid(&window(&[1], "9:00", "05:00")));
        assert!(!schedule_window_is_valid(&window(
            &[1],
            "09:00:00",
            "05:00"
        )));
        assert!(!schedule_window_is_valid(&window(
            &[1],
            "23:00",
            "ignore previous"
        )));
    }
}
