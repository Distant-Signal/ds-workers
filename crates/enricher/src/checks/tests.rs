use chrono::{DateTime, Utc};

use super::*;
use crate::llm::{DateRange, ScheduleWindow};

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

fn period(from: Option<&str>, to: Option<&str>, impact: Option<&str>) -> ExtractionPeriod {
    ExtractionPeriod {
        scope_description: None,
        date_range: (from.is_some() || to.is_some()).then(|| DateRange {
            from_date: from.map(at),
            to_date: to.map(at),
        }),
        schedule_window: None,
        resolution_status: "ongoing".to_string(),
        apparent_severity: "moderate_disruption".to_string(),
        impact_type: impact.map(str::to_string),
        resolution_status_confidence: "high".to_string(),
        severity_confidence: "high".to_string(),
    }
}

fn text<'a>(summary: &'a str, description: &'a str, reference: &str) -> CheckText<'a> {
    CheckText {
        summary,
        description,
        reference_date: at(reference),
    }
}

fn to_date(period: &ExtractionPeriod) -> Option<DateTime<Utc>> {
    period.date_range.as_ref().and_then(|r| r.to_date)
}

#[test]
fn every_pattern_compiles() {
    LazyLock::force(&PATTERNS);
}

#[test]
fn plain_text_strips_tags_and_entities() {
    assert_eq!(
        plain_text("<p>Buses&nbsp;Replace</p><p>Trains &amp; more</p>"),
        ". buses replace . . trains & more ."
    );
}

// ---------------------------------------------------------------------------
// Rail replacement guard.
// ---------------------------------------------------------------------------

#[test]
fn replacement_services_are_recognised() {
    for description in [
        "<p>Buses replace trains between Uckfield and Crowborough.</p>",
        "<p>Rail replacement buses have been requested.</p>",
        "<p>A replacement bus service operates between Ely and Cambridge.</p>",
        "<p>Trains are replaced by coaches between Bedford and Peterborough.</p>",
        "<p>A bus will be provided from Preston to Blackpool North.</p>",
        "<p>Northern have sourced road transport to run between Kettering and Corby.</p>",
        "<p>Replacement transport:</p><p>Road transport has been confirmed to run as follows.</p>",
        "<p>A bus shuttle service is in place between the two stations.</p>",
        "<p>1x shuttle bus confirmed at Gourock at approx 13:20.</p>",
    ] {
        assert!(
            mentions_replacement_service("Disruption", description),
            "{description}"
        );
    }
}

#[test]
fn ticket_acceptance_and_boilerplate_are_not_a_replacement() {
    for description in [
        "<p>Your ticket will be accepted on Stagecoach route 700 between Chichester and Portsmouth.</p>",
        "<p>You can use your ticket at no extra cost on London Buses route 261 between Bromley North and Grove Park.</p>",
        "<p>Use your ticket on the following bus routes: 157, 410.</p>",
        "<p>Replacement bus travel advice:</p><p>For helpful advice if you need to travel on a rail \
         replacement service, please use our rail replacement services page.</p>",
        "<p>You can find the pick-up and drop-off location of rail replacement services by checking \
         station signage.</p>",
        "<p>Strike action on 11, 18 and 25 October. Rail replacement buses will not be provided.</p>",
    ] {
        assert!(
            !mentions_replacement_service("Disruption", description),
            "{description}"
        );
    }
}

#[test]
fn an_unsupported_rail_replacement_tag_is_cleared() {
    let mut periods = vec![
        period(None, None, Some("rail_replacement_bus")),
        period(None, None, Some("no_scheduled_service")),
    ];
    let report = apply(
        &text(
            "No trains between Sydenham and Crystal Palace",
            "<p>From 23:00 until the end of service each night, no London Overground trains will run. \
             You can use your ticket on the following bus routes: 157, 410.</p>",
            "2026-09-20T12:00:00Z",
        ),
        &mut periods,
    );
    assert_eq!(report.rail_replacement_downgraded, 1);
    assert_eq!(periods[0].impact_type, None);
    assert_eq!(
        periods[1].impact_type.as_deref(),
        Some("no_scheduled_service")
    );
}

#[test]
fn a_supported_rail_replacement_tag_is_kept() {
    let mut periods = vec![period(None, None, Some("rail_replacement_bus"))];
    let report = apply(
        &text(
            "Lines blocked between Ely and Cambridge",
            "<p>All lines are blocked. Rail replacement buses have been requested.</p>",
            "2026-09-20T12:00:00Z",
        ),
        &mut periods,
    );
    assert!(report.is_clean(), "{report:?}");
    assert_eq!(
        periods[0].impact_type.as_deref(),
        Some("rail_replacement_bus")
    );
}

// ---------------------------------------------------------------------------
// Stated end times.
// ---------------------------------------------------------------------------

fn check_one(
    summary: &str,
    description: &str,
    reference: &str,
    to: &str,
) -> (CheckReport, ExtractionPeriod) {
    let mut periods = vec![period(None, Some(to), None)];
    let report = apply(&text(summary, description, reference), &mut periods);
    (report, periods.remove(0))
}

/// Summer: "expected until 21:00" on a BST Sunday is 20:00Z. The
/// local-time-as-UTC answer (21:00Z, i.e. 22:00 UK) is corrected.
#[test]
fn bst_local_time_written_as_utc_is_corrected() {
    let summary = "Disruption between Leeds and York expected until 21:00";
    let description = "<p>Disruption is expected until 21:00.</p>";
    let reference = "2026-09-20T15:00:00Z";
    let (report, fixed) = check_one(summary, description, reference, "2026-09-20T21:00:00Z");
    assert_eq!(report.end_times.len(), 1, "{report:?}");
    assert_eq!(report.end_times[0].action, EndTimeAction::Corrected);
    assert_eq!(report.end_times[0].stated, vec![at("2026-09-20T20:00:00Z")]);
    assert_eq!(to_date(&fixed), Some(at("2026-09-20T20:00:00Z")));

    for fine in ["2026-09-20T20:00:00Z", "2026-09-20T20:30:00Z"] {
        let (report, kept) = check_one(summary, description, reference, fine);
        assert!(report.is_clean(), "{fine}: {report:?}");
        assert_eq!(to_date(&kept), Some(at(fine)));
    }
}

/// Winter: after the clocks go back (25 Oct 2026) 13:00 UK is 13:00Z, so
/// summer time kept past the change (12:00Z) is an hour out.
#[test]
fn gmt_after_the_clock_change_is_checked_in_gmt() {
    let summary = "Disruption expected until 13:00";
    let reference = "2026-10-26T10:00:00Z";
    let (report, fixed) = check_one(summary, "", reference, "2026-10-26T12:00:00Z");
    assert_eq!(report.end_times[0].action, EndTimeAction::Corrected);
    assert_eq!(to_date(&fixed), Some(at("2026-10-26T13:00:00Z")));
    let (report, _) = check_one(summary, "", reference, "2026-10-26T13:00:00Z");
    assert!(report.is_clean(), "{report:?}");
}

/// Overnight: "until approximately 05:30" at 22:50 BST on Monday is 04:30Z
/// on Tuesday; Monday's 04:30Z had already passed.
#[test]
fn an_overnight_end_time_rolls_past_midnight() {
    let description = "<p>Reduced service between Watford Junction and London Euston until approximately 05:30.</p>";
    let reference = "2026-09-21T21:50:00Z";
    let (report, fixed) = check_one(
        "Reduced service",
        description,
        reference,
        "2026-09-21T04:30:00Z",
    );
    assert_eq!(report.end_times[0].action, EndTimeAction::Corrected);
    assert_eq!(to_date(&fixed), Some(at("2026-09-22T04:30:00Z")));
    let (report, _) = check_one(
        "Reduced service",
        description,
        reference,
        "2026-09-22T04:30:00Z",
    );
    assert!(report.is_clean(), "{report:?}");
}

/// "End of the day" just after midnight: 00:15 BST on Tuesday 8 September
/// is 23:15Z on Monday, but the day is Tuesday, ending 23:00Z on Tuesday.
/// The end of Monday (already past) is corrected.
#[test]
fn end_of_the_day_just_after_midnight_is_the_local_day() {
    let description = "<p>This is expected until the end of the day.</p>";
    let reference = "2026-09-07T23:15:00Z";
    let (report, fixed) = check_one("Disruption", description, reference, "2026-09-07T23:00:00Z");
    assert_eq!(report.end_times[0].action, EndTimeAction::Corrected);
    assert_eq!(to_date(&fixed), Some(at("2026-09-08T23:00:00Z")));
}

/// The end of the day on the clock-change Sunday is midnight GMT.
#[test]
fn end_of_the_day_on_the_clock_change_day() {
    let summary = "Disruption expected until the end of the day";
    let (report, _) = check_one(summary, "", "2026-10-25T09:00:00Z", "2026-10-26T00:00:00Z");
    assert!(report.is_clean(), "{report:?}");
    let (report, fixed) = check_one(summary, "", "2026-10-25T09:00:00Z", "2026-10-25T23:00:00Z");
    assert_eq!(report.end_times[0].action, EndTimeAction::Corrected);
    assert_eq!(to_date(&fixed), Some(at("2026-10-26T00:00:00Z")));
}

/// The repeated hour when the clocks go back reads as the first (BST) one.
#[test]
fn the_repeated_hour_reads_as_the_earlier_instant() {
    let times = stated_end_times(
        "Disruption is expected until approximately 01:30.",
        at("2026-10-24T23:30:00Z"),
    );
    assert_eq!(times.eta, vec![at("2026-10-25T00:30:00Z")]);
}

/// A day or date after the time places it on that day, without a roll.
#[test]
fn a_named_day_or_date_places_the_time() {
    let reference = at("2026-09-10T11:14:00Z"); // Thursday
    let times = stated_end_times(
        "Disruption expected until the end of the day on Saturday 19 September",
        reference,
    );
    assert_eq!(times.eta, vec![at("2026-09-19T23:00:00Z")]);
    let times = stated_end_times("Disruption expected until 10:00 on Saturday", reference);
    assert_eq!(times.eta, vec![at("2026-09-12T09:00:00Z")]);
    let times = stated_end_times(
        "Reduced service expected until the end of the day tomorrow 11 September",
        reference,
    );
    assert_eq!(times.eta, vec![at("2026-09-11T23:00:00Z")]);
    let times = stated_end_times("Disruption expected until 9:30pm", reference);
    assert_eq!(times.eta, vec![at("2026-09-10T20:30:00Z")]);
}

/// A time placed only by a bare weekday may mean a later week: flag only.
#[test]
fn a_bare_weekday_only_flags() {
    let (report, kept) = check_one(
        "No service between Avonmere and Bexcombe",
        "<p>No trains will run until approximately 18:00 on Sunday.</p>",
        "2026-09-07T14:30:00Z",
        "2026-09-20T17:00:00Z",
    );
    assert_eq!(report.end_times.len(), 1, "{report:?}");
    assert_eq!(report.end_times[0].action, EndTimeAction::Flagged);
    assert_eq!(to_date(&kept), Some(at("2026-09-20T17:00:00Z")));
}

/// A stale "expected until" left in the description loses to the
/// summary's: the period on the description's time is corrected.
#[test]
fn the_summary_end_time_wins_over_a_stale_description() {
    let (report, fixed) = check_one(
        "Disruption between Middlesbrough and Saltburn expected until 11:30",
        "<p>Disruption is expected until 10:30.</p>",
        "2026-09-09T09:44:00Z",
        "2026-09-09T09:30:00Z",
    );
    assert_eq!(report.end_times[0].action, EndTimeAction::Corrected);
    assert_eq!(to_date(&fixed), Some(at("2026-09-09T10:30:00Z")));
}

/// Another stated "until HH:MM" is an accepted end ("revised until 15:30
/// today, then disruption until 17:30").
#[test]
fn another_stated_until_time_is_accepted() {
    let (report, _) = check_one(
        "Disruption between Leeds and York expected until 17:30",
        "<p>Trains will be cancelled or revised until 15:30 today.</p>\
         <p>Disruption is expected to then continue until 17:30.</p>",
        "2026-09-17T11:43:00Z",
        "2026-09-17T14:30:00Z",
    );
    assert!(report.is_clean(), "{report:?}");
}

/// With several ETA-shaped periods the check only flags.
#[test]
fn a_phased_extraction_is_only_flagged() {
    let mut periods = vec![
        period(None, Some("2026-09-20T21:00:00Z"), None),
        period(None, Some("2026-09-20T20:00:00Z"), None),
    ];
    let report = apply(
        &text(
            "Disruption expected until 21:00",
            "",
            "2026-09-20T15:00:00Z",
        ),
        &mut periods,
    );
    assert_eq!(report.end_times.len(), 1);
    assert_eq!(report.end_times[0].period_index, 0);
    assert_eq!(report.end_times[0].action, EndTimeAction::Flagged);
    assert_eq!(to_date(&periods[0]), Some(at("2026-09-20T21:00:00Z")));
}

/// Two different stated ETAs and an end far from both: dropped.
#[test]
fn an_end_time_with_no_single_stated_time_is_dropped() {
    let (report, fixed) = check_one(
        "Disruption at Exeter St Davids",
        "<p>Disruption is expected until 10:00.</p><p>Major disruption is expected until 12:00.</p>",
        "2026-10-11T07:00:00Z",
        "2026-10-11T16:00:00Z",
    );
    assert_eq!(report.end_times[0].action, EndTimeAction::Dropped);
    assert_eq!(fixed.date_range, None);
}

/// Periods that aren't an ETA (a weekly window, a later start) and texts
/// with no stated end time are left alone.
#[test]
fn non_eta_periods_and_texts_without_an_end_time_are_skipped() {
    let mut windowed = period(None, Some("2026-09-27T23:00:00Z"), None);
    windowed.schedule_window = Some(ScheduleWindow {
        days_of_week: vec![6],
        start_time: "00:00".to_string(),
        end_time: "23:59".to_string(),
    });
    let mut periods = vec![
        windowed,
        period(
            Some("2026-09-26T23:00:00Z"),
            Some("2026-09-27T23:00:00Z"),
            None,
        ),
    ];
    let report = apply(
        &text(
            "Disruption expected until 21:00",
            "",
            "2026-09-20T15:00:00Z",
        ),
        &mut periods,
    );
    assert!(report.is_clean(), "{report:?}");

    let (report, _) = check_one(
        "Disruption between Ely and Cambridge",
        "<p>Trains may be delayed.</p>",
        "2026-09-20T15:00:00Z",
        "2026-09-20T21:00:00Z",
    );
    assert!(report.is_clean(), "{report:?}");
}
