//! The stated-year check, and the year rule it shares with the stated end
//! time check.
//!
//! National Rail notices name days without a year: "Sunday 3 January",
//! "from Monday 4 to Thursday 7 January". The primary pass resolves them
//! against the reference date it is given, and mostly gets it right, but in
//! October 2026 it put three January 2027 notices in January 2026, nine
//! months in the past.
//!
//! [`resolve`] is the deterministic rule: a stated year always wins;
//! otherwise the occurrence inside a window from [`LOOKBACK_DAYS`] before
//! the reference day to [`LOOKAHEAD_DAYS`] after it (notices announce work
//! ahead, so the window leans forward). Where two occurrences fall inside
//! it, or none does, one on the named weekday wins, then the nearest. The
//! window comes before the weekday because notices get weekdays wrong: a
//! "Thursday 1 January" notice of October 2026 is for Friday 1 January
//! 2027 (the feed's own validity period says so).
//!
//! [`check_years`] compares each period's dates with the dates the text
//! states. A period endpoint on a stated day and month (a `to_date` at
//! local midnight also matches the day before, the inclusive end) whose
//! year isn't the stated date's resolved year moves, with the period's
//! other endpoint, by the difference (`corrected`). Endpoints that disagree
//! about the difference, or (in a text that states no year at all) an
//! endpoint matching no stated date and outside a year back to
//! [`LOOKAHEAD_DAYS`] ahead, are only reported (`flagged`).

use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc, Weekday};
use chrono_tz::Europe::London;
use serde::Serialize;

use super::{CheckText, MONTHS, PATTERNS, WEEKDAYS, london_to_utc, plain_text};
use crate::llm::ExtractionPeriod;

/// How far before the reference day a year-less date may resolve: work
/// that started a few months ago and is still running.
pub(crate) const LOOKBACK_DAYS: i64 = 92;
/// How far after it: notices announce work up to about nine months ahead.
pub(crate) const LOOKAHEAD_DAYS: i64 = 274;
/// An endpoint matching no stated date, in a text with no year, is
/// flagged when it is more than this before the reference time.
const FLAG_BEFORE_DAYS: i64 = 366;

/// A day and month the text states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatedDate {
    pub month: u32,
    pub day: u32,
    /// The weekday named with it ("Sunday 3 January").
    pub weekday: Option<Weekday>,
    /// The year stated with it ("3 January 2027").
    pub year: Option<i32>,
}

/// The calendar date `stated` means, read on `reference` (a Europe/London
/// day). `None` only for a day that doesn't exist (31 April; 29 February
/// with a stated non-leap year).
pub(crate) fn resolve(stated: StatedDate, reference: NaiveDate) -> Option<NaiveDate> {
    if let Some(year) = stated.year {
        return NaiveDate::from_ymd_opt(year, stated.month, stated.day);
    }
    let earliest = reference - Duration::days(LOOKBACK_DAYS);
    let latest = reference + Duration::days(LOOKAHEAD_DAYS);
    (reference.year() - 1..=reference.year() + 1)
        .filter_map(|year| NaiveDate::from_ymd_opt(year, stated.month, stated.day))
        .min_by_key(|date| {
            let outside = if *date < earliest {
                (earliest - *date).num_days()
            } else if *date > latest {
                (*date - latest).num_days()
            } else {
                0
            };
            let off_weekday = stated.weekday.is_some_and(|w| w != date.weekday());
            (outside, off_weekday, (*date - reference).num_days().abs())
        })
}

/// The dates `text` (HTML or plain) states with a month: "Sunday 3
/// January", "3rd January 2027". A bare day before a range's month ("4" in
/// "Monday 4 to Thursday 7 January") isn't one.
pub(crate) fn stated_dates(text: &str) -> Vec<StatedDate> {
    PATTERNS
        .stated_date
        .captures_iter(&plain_text(text))
        .filter_map(|caps| {
            Some(StatedDate {
                month: month_number(caps.name("month")?.as_str())?,
                day: caps.name("dom")?.as_str().parse().ok()?,
                weekday: caps.name("weekday").and_then(|w| weekday(w.as_str())),
                year: caps.name("year").and_then(|y| y.as_str().parse().ok()),
            })
        })
        .collect()
}

/// "january" -> 1.
pub(super) fn month_number(name: &str) -> Option<u32> {
    MONTHS
        .iter()
        .position(|m| *m == name)
        .and_then(|i| u32::try_from(i + 1).ok())
}

/// "monday" -> Monday.
pub(super) fn weekday(name: &str) -> Option<Weekday> {
    let index = WEEKDAYS.iter().position(|d| *d == name)?;
    Weekday::try_from(u8::try_from(index).ok()?).ok()
}

/// One period whose year was flagged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct YearFlag {
    pub period_index: usize,
    /// The model's dates.
    pub from_date: Option<DateTime<Utc>>,
    pub to_date: Option<DateTime<Utc>>,
    /// Years added to both dates (`corrected`); 0 when only flagged.
    pub shifted_years: i32,
    pub action: YearAction,
}

/// What the stated-year check did with a flagged period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum YearAction {
    /// Both dates moved to the stated dates' year.
    Corrected,
    /// Left as it was, only reported.
    Flagged,
}

impl YearAction {
    pub(crate) const ALL: [Self; 2] = [Self::Corrected, Self::Flagged];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Corrected => "corrected",
            Self::Flagged => "flagged",
        }
    }
}

/// Checks every dated period's year against the dates the text states
/// (module doc), correcting in place.
pub(crate) fn check_years(text: &CheckText<'_>, periods: &mut [ExtractionPeriod]) -> Vec<YearFlag> {
    let stated: Vec<StatedDate> = stated_dates(text.summary)
        .into_iter()
        .chain(stated_dates(text.description))
        .collect();
    let states_a_year = stated.iter().any(|s| s.year.is_some())
        || PATTERNS.any_year.is_match(&plain_text(text.summary))
        || PATTERNS.any_year.is_match(&plain_text(text.description));
    let reference_day = text.reference_date.with_timezone(&London).date_naive();
    let resolved: Vec<NaiveDate> = stated
        .iter()
        .filter_map(|s| resolve(*s, reference_day))
        .collect();
    let mut flags = Vec::new();
    for (period_index, period) in periods.iter_mut().enumerate() {
        let Some(range) = period.date_range.as_mut() else {
            continue;
        };
        let (from_date, to_date) = (range.from_date, range.to_date);
        let mut shifts = Vec::new();
        let mut unmatched_implausible = false;
        for (at, is_end) in [(from_date, false), (to_date, true)] {
            let Some(at) = at else { continue };
            match year_shift(at, is_end, &resolved) {
                Some(shift) => shifts.push(shift),
                None => {
                    unmatched_implausible |= !states_a_year
                        && (at < text.reference_date - Duration::days(FLAG_BEFORE_DAYS)
                            || at > text.reference_date + Duration::days(LOOKAHEAD_DAYS));
                }
            }
        }
        let action = match shifts.as_slice() {
            [] if unmatched_implausible => Some((YearAction::Flagged, 0)),
            [] => None,
            [first, rest @ ..] if rest.iter().all(|s| s == first) => match *first {
                0 if unmatched_implausible => Some((YearAction::Flagged, 0)),
                0 => None,
                shift => {
                    let moved = (
                        from_date.map(|at| shift_years(at, shift)),
                        to_date.map(|at| shift_years(at, shift)),
                    );
                    match moved {
                        (Some(None), _) | (_, Some(None)) => Some((YearAction::Flagged, 0)),
                        (from, to) => {
                            range.from_date = from.flatten();
                            range.to_date = to.flatten();
                            Some((YearAction::Corrected, shift))
                        }
                    }
                }
            },
            _ => Some((YearAction::Flagged, 0)),
        };
        if let Some((action, shifted_years)) = action {
            flags.push(YearFlag {
                period_index,
                from_date,
                to_date,
                shifted_years,
                action,
            });
        }
    }
    flags
}

/// The years to add to `at` to land on the resolved stated date with its
/// day and month: `Some(0)` when it already does, `None` when it has no
/// stated day and month. An end at local midnight is also the inclusive end
/// of the day before. When several resolved dates share the day and month,
/// any year among them is accepted.
fn year_shift(at: DateTime<Utc>, is_end: bool, resolved: &[NaiveDate]) -> Option<i32> {
    let local = at.with_timezone(&London).naive_local();
    let mut days = vec![local.date()];
    if is_end && local.hour() == 0 && local.minute() == 0 {
        days.extend(local.date().pred_opt());
    }
    let mut shift = None;
    for day in days {
        let years: Vec<i32> = resolved
            .iter()
            .filter(|r| r.month() == day.month() && r.day() == day.day())
            .map(Datelike::year)
            .collect();
        if years.contains(&day.year()) {
            return Some(0);
        }
        if let [only] = years.as_slice() {
            shift.get_or_insert(*only - day.year());
        }
    }
    shift
}

/// `at` moved by `years` on the Europe/London calendar, keeping its local
/// time of day (so a BST midnight in October stays a midnight in GMT
/// January). `None` for 29 February into a non-leap year.
fn shift_years(at: DateTime<Utc>, years: i32) -> Option<DateTime<Utc>> {
    let local = at.with_timezone(&London).naive_local();
    let day = NaiveDate::from_ymd_opt(local.year() + years, local.month(), local.day())?;
    london_to_utc(day.and_time(local.time()))
}

#[cfg(test)]
mod tests;
