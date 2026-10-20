use chrono::{DateTime, NaiveDate, Utc, Weekday};

use super::*;
use crate::checks::{CheckReport, apply, stated_end_times};
use crate::llm::DateRange;

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

fn day(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

fn stated(day: u32, month: u32, weekday: Option<Weekday>, year: Option<i32>) -> StatedDate {
    StatedDate {
        month,
        day,
        weekday,
        year,
    }
}

fn period(from: Option<&str>, to: Option<&str>) -> ExtractionPeriod {
    ExtractionPeriod {
        scope_description: None,
        date_range: Some(DateRange {
            from_date: from.map(at),
            to_date: to.map(at),
        }),
        schedule_window: None,
        resolution_status: "ongoing".to_string(),
        apparent_severity: "blocked_or_suspended".to_string(),
        impact_type: None,
        resolution_status_confidence: "high".to_string(),
        severity_confidence: "high".to_string(),
    }
}

fn range(period: &ExtractionPeriod) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    let range = period.date_range.as_ref().unwrap();
    (range.from_date, range.to_date)
}

fn check(
    summary: &str,
    description: &str,
    reference: &str,
    periods: &mut [ExtractionPeriod],
) -> Vec<YearFlag> {
    check_years(
        &CheckText {
            summary,
            description,
            reference_date: at(reference),
        },
        periods,
    )
}

// ---------------------------------------------------------------------------
// The year rule.
// ---------------------------------------------------------------------------

/// A January date read in October is next January, not the one nine
/// months gone, with or without its weekday.
#[test]
fn a_year_less_date_earlier_in_the_year_is_next_years() {
    let reference = day("2026-10-19");
    assert_eq!(
        resolve(stated(3, 1, None, None), reference),
        Some(day("2027-01-03"))
    );
    assert_eq!(
        resolve(stated(3, 1, Some(Weekday::Sun), None), reference),
        Some(day("2027-01-03"))
    );
}

/// Work that started a few months ago and is still running stays in the
/// past.
#[test]
fn a_recent_past_date_stays_this_year() {
    let reference = day("2026-10-19");
    assert_eq!(
        resolve(stated(3, 8, Some(Weekday::Mon), None), reference),
        Some(day("2026-08-03"))
    );
    assert_eq!(
        resolve(stated(1, 9, None, None), reference),
        Some(day("2026-09-01"))
    );
}

/// The window beats a wrong weekday. Incident 1EC8CE1D708241B4AAA084873CCBDCE3
/// (first seen 16 October 2026) says "Thursday 1 January"; 1 January 2026
/// was a Thursday, but the feed's validity period is 1 January 2027 (a
/// Friday), nine months nearer.
#[test]
fn the_window_beats_a_wrong_weekday() {
    assert_eq!(
        resolve(stated(1, 1, Some(Weekday::Thu), None), day("2026-10-16")),
        Some(day("2027-01-01"))
    );
    // From 1 June the window (about nine months ahead) still reaches the
    // next 1 January.
    assert_eq!(
        resolve(stated(1, 1, None, None), day("2026-06-01")),
        Some(day("2027-01-01"))
    );
}

/// When both occurrences are inside the window (19 July 2026 is 92 days
/// before 19 October, 19 July 2027 273 after), a named weekday picks one;
/// without one, the nearer.
#[test]
fn a_named_weekday_breaks_a_tie_in_the_window() {
    let reference = day("2026-10-19");
    assert_eq!(
        resolve(stated(19, 7, Some(Weekday::Mon), None), reference),
        Some(day("2027-07-19"))
    );
    assert_eq!(
        resolve(stated(19, 7, Some(Weekday::Sun), None), reference),
        Some(day("2026-07-19"))
    );
    assert_eq!(
        resolve(stated(19, 7, None, None), reference),
        Some(day("2026-07-19"))
    );
}

/// Across the new year, both ways.
#[test]
fn december_and_january_cross_the_year_boundary() {
    assert_eq!(
        resolve(stated(28, 12, None, None), day("2026-12-30")),
        Some(day("2026-12-28"))
    );
    assert_eq!(
        resolve(stated(4, 1, None, None), day("2026-12-30")),
        Some(day("2027-01-04"))
    );
    assert_eq!(
        resolve(stated(28, 12, Some(Weekday::Mon), None), day("2027-01-02")),
        Some(day("2026-12-28"))
    );
    assert_eq!(
        resolve(stated(31, 12, None, None), day("2027-01-01")),
        Some(day("2026-12-31"))
    );
}

/// No more than about nine months ahead without a year: further out, the
/// recent past is nearer.
#[test]
fn the_window_is_bounded() {
    let reference = day("2026-10-19");
    // 2027-08-01 is 286 days ahead, past the window; 2026-08-01 is 79
    // days back, inside it.
    assert_eq!(
        resolve(stated(1, 8, None, None), reference),
        Some(day("2026-08-01"))
    );
}

/// A stated year is the year, however far away.
#[test]
fn an_explicit_year_is_respected() {
    let reference = day("2026-10-19");
    assert_eq!(
        resolve(stated(3, 1, None, Some(2026)), reference),
        Some(day("2026-01-03"))
    );
    assert_eq!(
        resolve(stated(30, 4, None, Some(2028)), reference),
        Some(day("2028-04-30"))
    );
    assert_eq!(resolve(stated(29, 2, None, Some(2027)), reference), None);
    // A year-less 29 February is the leap year in reach.
    assert_eq!(
        resolve(stated(29, 2, None, None), day("2027-11-01")),
        Some(day("2028-02-29"))
    );
}

#[test]
fn stated_dates_are_read_with_their_weekday_and_year() {
    assert_eq!(
        stated_dates(
            "<p>Buses replace trains on <strong>Sunday 3 January</strong>.</p>\
             <p>From Monday 4 to Thursday, 7th January 2027 and 12 Feb.</p>"
        ),
        vec![
            stated(3, 1, Some(Weekday::Sun), None),
            stated(7, 1, Some(Weekday::Thu), Some(2027)),
        ]
    );
}

// ---------------------------------------------------------------------------
// The stated-year check.
// ---------------------------------------------------------------------------

/// Incident DA8E46F4C991469D9CEF1DA078E04BCB as stored in production: a
/// Sunday 3 January notice first seen on 19 October 2026, extracted into
/// January 2026 (a Saturday). Moved to 2027.
#[test]
fn a_january_notice_put_in_the_past_year_is_moved_forward() {
    let mut periods = [period(
        Some("2026-01-03T00:00:00Z"),
        Some("2026-01-04T00:00:00Z"),
    )];
    let flags = check(
        "Buses replace trains between Hastings and Rye / Ashford International on Sunday 3 January",
        "<p>Engineering work is taking place between Hastings and Ashford International, \
         closing all lines.</p><p>A replacement bus service will run.</p>",
        "2026-10-19T08:39:07Z",
        &mut periods,
    );
    assert_eq!(
        flags,
        vec![YearFlag {
            period_index: 0,
            from_date: Some(at("2026-01-03T00:00:00Z")),
            to_date: Some(at("2026-01-04T00:00:00Z")),
            shifted_years: 1,
            action: YearAction::Corrected,
        }]
    );
    assert_eq!(
        range(&periods[0]),
        (
            Some(at("2027-01-03T00:00:00Z")),
            Some(at("2027-01-04T00:00:00Z"))
        )
    );
}

/// A range whose first day has no month of its own: the inclusive end
/// (local midnight after "Thursday 7 January") carries the year.
#[test]
fn a_range_is_moved_by_its_stated_end() {
    let mut periods = [period(
        Some("2026-01-04T00:00:00Z"),
        Some("2026-01-08T00:00:00Z"),
    )];
    let flags = check(
        "Amended 21:52 Liverpool Lime Street to York service from Monday 4 to Thursday 7 January",
        "",
        "2026-10-20T09:00:00Z",
        &mut periods,
    );
    assert_eq!(flags.len(), 1);
    assert_eq!(flags[0].action, YearAction::Corrected);
    assert_eq!(
        range(&periods[0]),
        (
            Some(at("2027-01-04T00:00:00Z")),
            Some(at("2027-01-08T00:00:00Z"))
        )
    );
}

/// The reported incident (2D8C8B610B654F93BEC3A7B377A9B2A0, the real
/// summary, first seen 16 October 2026): its stored period is right and
/// stays. Had it come back a year late, it moves back, and the move keeps
/// local midnights across BST and GMT: the start is BST midnight on the
/// clocks-change Sunday (23:00Z the day before), the inclusive end GMT
/// midnight (00:00Z).
#[test]
fn the_reported_october_notice_is_left_alone_or_moved_back() {
    let summary = "Buses replace trains between Middlesbrough and Saltburn on Sunday 25 October";
    let description = "<p>Engineering work is taking place between South Bank and Longbeck, \
                       closing all lines.</p><p>A replacement bus service will run between \
                       Middlesbrough and Saltburn.</p>";
    let reference = "2026-10-16T07:45:27Z";
    let stored = period(Some("2026-10-24T23:00:00Z"), Some("2026-10-26T00:00:00Z"));
    let mut periods = [stored.clone()];
    assert!(check(summary, description, reference, &mut periods).is_empty());
    assert_eq!(periods[0], stored);

    // A year late: 25 October 2027 is a Monday, still BST (the clocks go
    // back on 31 October 2027), so both ends are 23:00Z.
    let mut periods = [period(
        Some("2027-10-24T23:00:00Z"),
        Some("2027-10-25T23:00:00Z"),
    )];
    let flags = check(summary, description, reference, &mut periods);
    assert_eq!(flags.len(), 1);
    assert_eq!(flags[0].shifted_years, -1);
    assert_eq!(periods[0], stored);
}

/// A stated year is never second-guessed, even nine months back.
#[test]
fn a_period_in_the_stated_year_is_left_alone() {
    for (summary, from, to) in [
        (
            "No trains on Sunday 3 January 2027",
            "2027-01-03T00:00:00Z",
            "2027-01-04T00:00:00Z",
        ),
        (
            "Station closed since Saturday 3 January 2026",
            "2026-01-03T00:00:00Z",
            "2026-01-04T00:00:00Z",
        ),
    ] {
        let mut periods = [period(Some(from), Some(to))];
        assert!(
            check(summary, "", "2026-10-19T08:00:00Z", &mut periods).is_empty(),
            "{summary}"
        );
    }
}

/// No stated date to match, no year in the text and a period more than
/// nine months out: reported, not changed.
#[test]
fn an_implausible_unmatched_year_is_only_flagged() {
    let stored = period(Some("2027-10-16T23:00:00Z"), Some("2027-10-18T23:00:00Z"));
    let mut periods = [stored.clone()];
    let flags = check(
        "Engineering work this weekend",
        "<p>Buses replace trains on Saturday and Sunday this week.</p>",
        "2026-10-16T07:00:00Z",
        &mut periods,
    );
    assert_eq!(flags.len(), 1);
    assert_eq!(flags[0].action, YearAction::Flagged);
    assert_eq!(periods[0], stored);

    // The same dates in a text that states a year are not flagged.
    let mut periods = [stored.clone()];
    assert!(
        check(
            "Engineering work in October 2027",
            "",
            "2026-10-16T07:00:00Z",
            &mut periods,
        )
        .is_empty()
    );
}

/// Ends that disagree on the shift: reported, not changed.
#[test]
fn ends_that_disagree_are_only_flagged() {
    let stored = period(Some("2026-01-03T00:00:00Z"), Some("2027-01-11T00:00:00Z"));
    let mut periods = [stored.clone()];
    let flags = check(
        "No trains from Sunday 3 January to Sunday 10 January",
        "",
        "2026-10-19T08:00:00Z",
        &mut periods,
    );
    assert_eq!(flags.len(), 1);
    assert_eq!(flags[0].action, YearAction::Flagged);
    assert_eq!(periods[0], stored);
}

/// Incident 7F2EA7F43BED48428BD2BEF249F53EA4 (first seen Monday 12 October
/// 2026): the feed cut its summary off before the month ("from Monday 4 to
/// Friday "), and its text says nothing about "this week". The only Monday
/// the 4th within the window is 4 January 2027, which is also the feed's
/// validity period (4-8 January 2027). With no stated month there is
/// nothing to correct against: the stored weekly-window periods (no
/// `date_range`) and a 4-8 January 2027 range stay as they are. A range
/// on Monday 4 October 2027, a year out, is only flagged.
#[test]
fn a_summary_cut_off_before_its_month_is_not_corrected() {
    let summary =
        "Amended late night / early morning South Western Railway service from Monday 4 to Friday ";
    let description = "<p>Engineering work is taking place overnight in the Eastleigh area, \
                       closing some lines.</p><p><strong>Monday to Thursday:</strong></p>\
                       <ul><li>23:05 London Waterloo to Southampton service terminates at \
                       Basingstoke.</li></ul><p><strong>Tuesday to Friday:</strong></p>\
                       <p>Repalcement buses will run between Basingstoke and Southampton \
                       Central.</p>";
    let reference = "2026-10-12T10:23:09Z";
    assert!(stated_dates(summary).is_empty());

    let undated = ExtractionPeriod {
        date_range: None,
        ..period(None, None)
    };
    let mut periods = [undated.clone()];
    assert!(check(summary, description, reference, &mut periods).is_empty());
    assert_eq!(periods[0], undated);

    let mut periods = [period(
        Some("2027-01-04T00:00:00Z"),
        Some("2027-01-09T00:00:00Z"),
    )];
    assert!(check(summary, description, reference, &mut periods).is_empty());

    let october = period(Some("2027-10-03T23:00:00Z"), Some("2027-10-08T23:00:00Z"));
    let mut periods = [october.clone()];
    let flags = check(summary, description, reference, &mut periods);
    assert_eq!(flags.len(), 1);
    assert_eq!(flags[0].action, YearAction::Flagged);
    assert_eq!(periods[0], october);
}

/// Undated periods and periods in the right year are untouched, and
/// `apply` reports nothing for them.
#[test]
fn right_years_and_undated_periods_are_clean() {
    let mut periods = [
        period(Some("2027-01-09T00:00:00Z"), Some("2027-01-11T00:00:00Z")),
        ExtractionPeriod {
            date_range: None,
            ..period(None, None)
        },
    ];
    let report = apply(
        &CheckText {
            summary: "No trains between Romford and Upminster on Saturday 9 and Sunday 10 January",
            description: "",
            reference_date: at("2026-10-19T08:00:00Z"),
        },
        &mut periods,
    );
    assert_eq!(report, CheckReport::default());
}

// ---------------------------------------------------------------------------
// The shared rule in the stated end time check, and its weekdays.
// ---------------------------------------------------------------------------

/// "until 18:00 on Friday 1 January" from 1 June is 2027, inside the
/// window, though 1 January 2026 is nearer (the rule this check used
/// before).
#[test]
fn stated_end_times_use_the_year_rule() {
    let times = stated_end_times(
        "Disruption expected until 18:00 on Friday 1 January",
        at("2026-06-01T09:00:00Z"),
    );
    assert_eq!(times.eta, vec![at("2027-01-01T18:00:00Z")]);
}

/// A bare weekday is its next occurrence on the Europe/London calendar:
/// from a Monday, the Sunday of the same Monday-to-Sunday week; from that
/// Sunday, the day itself. 25 October 2026 is the day the clocks go back,
/// so its 18:00 is GMT.
#[test]
fn a_bare_weekday_is_in_the_reference_week() {
    for reference in ["2026-10-19T08:00:00Z", "2026-10-25T08:00:00Z"] {
        let times = stated_end_times("Disruption expected until 18:00 on Sunday", at(reference));
        assert_eq!(times.eta, vec![at("2026-10-25T18:00:00Z")], "{reference}");
        assert!(times.uncertain);
    }
}

/// Just after midnight BST is already the next London day: at 23:30Z on
/// Sunday 18 October (00:30 on Monday in London), "Monday" is that
/// Monday, not the one a week later, and its 18:00 is 17:00Z.
#[test]
fn a_weekday_just_after_bst_midnight_is_the_london_day() {
    let times = stated_end_times(
        "Disruption expected until 18:00 on Monday",
        at("2026-10-18T23:30:00Z"),
    );
    assert_eq!(times.eta, vec![at("2026-10-19T17:00:00Z")]);
}
