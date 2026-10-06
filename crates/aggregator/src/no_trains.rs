//! "No trains between X and Y": an incident summary that closes a section
//! of line, resolved against the stations of one matched line.
//!
//! The phrase matches none of `aggregation::severity_from_incident`'s
//! keywords, and an operator-wide report is capped at Minor Delays, so a
//! whole branch closed ("No trains between Par and Newquay") read Minor
//! (2026-10-02 production study: 2,749 in-effect operator-wide rows were
//! Minor against 33 Part Suspended). When BOTH ends resolve to stations of
//! the line, that is line-specific evidence:
//!
//! - the closed section covers at least a third of the line's stations
//!   ([`PART_SUSPENDED_SHARE`]): **Part Suspended** (a branch, or most of a
//!   line);
//! - a shorter section of a longer line: **Reduced Service** -- the rest of
//!   the line runs, so the whole line is not shown at the Severe tier. The
//!   status still names the section (the summary, `affected_stops` and
//!   `affected_routes`).
//!
//! See the "Decisions (2026-10-02, incident sections)" section of
//! docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md.

use common::{LineDefinition, Severity};

/// The phrases that introduce a closed section. "No service(s) between" is
/// rarer (three planned notices in the production archive) but means the
/// same.
const PHRASES: [&str; 3] = [
    "no trains between ",
    "no services between ",
    "no service between ",
];

/// Words before the phrase that mean the closure is over ("CLEARED: No
/// trains between ...", "Disruption ended: no trains between ...").
const ENDED_MARKERS: [&str; 6] = [
    "cleared",
    "ended",
    "reopened",
    "resumed",
    "restored",
    "no longer",
];

/// Where the second station name stops: the time, cause or route that
/// follows it ("... until approximately 13:00", "... via Pontyclun").
const TERMINATORS: [&str; 19] = [
    " until ",
    " expected ",
    " due ",
    " while ",
    " because ",
    " from ",
    " on ",
    " via ",
    " before ",
    " after ",
    " late ",
    " and also ",
    " for ",
    "(",
    ",",
    ". ",
    ";",
    ":",
    " - ",
];

/// A closed section covering at least this share of the line's stations
/// (1 in 3) reads Part Suspended; a shorter one Reduced Service.
pub(crate) const PART_SUSPENDED_SHARE: (usize, usize) = (1, 3);

/// A section of one line that an incident says has no trains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClosedSection {
    /// The ends as named, in line order.
    pub from_crs: String,
    pub to_crs: String,
    /// Every station of the line from one end to the other, in line order.
    pub stations: Vec<String>,
    /// Part Suspended for a third of the line or more, else Reduced
    /// Service.
    pub severity: Severity,
}

/// Every station's names (the `stations` reference table plus aliases),
/// normalised. Shared with the matcher's place resolver since 2026-10-06
/// (`common::station_resolver`), so "No trains between Seaford and Lewes"
/// resolves "Seaford" (reference name "Seaford Sussex") here too.
pub(crate) use common::station_resolver::StationGazetteer;
use common::station_resolver::normalise_name;

/// The index in `line.stations` of the station `part` names: an exact
/// (normalised) name or alias, else "London <part>", else the one station
/// whose name starts with `part` (at least 4 characters: "Falmouth Dock"
/// for Falmouth Docks). `None` when nothing, or more than one, matches.
fn resolve(part: &str, line: &LineDefinition, names: &StationGazetteer) -> Option<usize> {
    let wanted = normalise_name(part);
    if wanted.is_empty() {
        return None;
    }
    let candidates: Vec<(usize, &[String])> = line
        .stations
        .iter()
        .enumerate()
        .map(|(i, s)| (i, names.names_of(&s.crs)))
        .filter(|(_, n)| !n.is_empty())
        .collect();
    let london = format!("london {wanted}");
    for target in [wanted.as_str(), london.as_str()] {
        if let Some((i, _)) = candidates
            .iter()
            .find(|(_, n)| n.iter().any(|name| name == target))
        {
            return Some(*i);
        }
    }
    if wanted.chars().count() < 4 {
        return None;
    }
    let mut prefixed = candidates
        .iter()
        .filter(|(_, n)| n.iter().any(|name| name.starts_with(&wanted)));
    match (prefixed.next(), prefixed.next()) {
        (Some((i, _)), None) => Some(*i),
        _ => None,
    }
}

/// The resolved stations of one side ("Caterham / Tattenham Corner"): at
/// least one alternative must be on the line.
fn resolve_side(side: &str, line: &LineDefinition, names: &StationGazetteer) -> Vec<usize> {
    side.split('/')
        .filter_map(|part| resolve(part, line, names))
        .collect()
}

/// The section of `line` that `summary` says has no trains, when it names
/// two different stations of the line. `None` for any other summary, one
/// that says the closure is over, or a name that does not resolve on this
/// line (then the incident is classified exactly as before).
pub(crate) fn closed_section(
    summary: &str,
    line: &LineDefinition,
    names: &StationGazetteer,
) -> Option<ClosedSection> {
    if names.is_empty() || line.stations.is_empty() {
        return None;
    }
    // "St. Albans": an abbreviation's full stop is not the sentence's end.
    let lower = summary.to_lowercase().replace("st. ", "st ");
    let (start, phrase) = PHRASES
        .iter()
        .filter_map(|p| lower.find(p).map(|i| (i, *p)))
        .min_by_key(|(i, _)| *i)?;
    let before = &lower[..start];
    if ENDED_MARKERS.iter().any(|m| before.contains(m)) {
        return None;
    }
    let rest = &lower[start + phrase.len()..];
    let end = TERMINATORS
        .iter()
        .filter_map(|t| rest.find(t))
        .min()
        .unwrap_or(rest.len());
    let span = &rest[..end];
    // A station name may itself contain " and ": try every split.
    let section = span.match_indices(" and ").find_map(|(i, sep)| {
        let a = resolve_side(&span[..i], line, names);
        let b = resolve_side(&span[i + sep.len()..], line, names);
        let (&first_a, &first_b) = (a.first()?, b.first()?);
        (first_a != first_b).then(|| {
            let all = a.iter().chain(b.iter());
            let lo = *all.clone().min().unwrap_or(&first_a);
            let hi = *all.max().unwrap_or(&first_b);
            (first_a.min(first_b), first_a.max(first_b), lo, hi)
        })
    });
    let (from, to, lo, hi) = section?;
    let stations: Vec<String> = line.stations[lo..=hi]
        .iter()
        .map(|s| s.crs.clone())
        .collect();
    let (num, den) = PART_SUSPENDED_SHARE;
    let severity = if stations.len() * den >= line.stations.len() * num {
        Severity::PartSuspended
    } else {
        Severity::ReducedService
    };
    Some(ClosedSection {
        from_crs: line.stations[from].crs.clone(),
        to_crs: line.stations[to].crs.clone(),
        stations,
        severity,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn line(id: &str, crs: &[&str]) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "national-rail".to_string(),
            category: "regional".to_string(),
            operators: vec!["GW".to_string()],
            stations: crs
                .iter()
                .map(|c| common::Station {
                    crs: (*c).to_string(),
                    tiploc: None,
                    role: "minor".to_string(),
                    segment: None,
                })
                .collect(),
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: HashMap::new(),
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
            pass_through: Vec::new(),
        }
    }

    fn names() -> StationGazetteer {
        StationGazetteer::new([
            ("PAR", "Par"),
            ("LUX", "Luxulyan"),
            ("BGL", "Bugle"),
            ("ROC", "Roche"),
            ("SCR", "St Columb Road"),
            ("QUI", "Quintrell Downs"),
            ("NQY", "Newquay"),
            ("TRU", "Truro"),
            ("FAL", "Falmouth Docks"),
            ("FMT", "Falmouth Town"),
            ("SLO", "Slough"),
            ("WNC", "Windsor & Eton Central"),
            ("SPB", "Shepherd's Bush"),
            ("RMD", "Richmond (London)"),
            ("WAT", "London Waterloo"),
        ])
    }

    fn atlantic() -> LineDefinition {
        line(
            "gwr-atlantic-coast",
            &["PAR", "LUX", "BGL", "ROC", "SCR", "QUI", "NQY"],
        )
    }

    #[test]
    fn a_whole_branch_is_part_suspended() {
        let s = closed_section(
            "No trains between Par and Newquay until approximately 13:00",
            &atlantic(),
            &names(),
        )
        .unwrap();
        assert_eq!(s.severity, Severity::PartSuspended);
        assert_eq!((s.from_crs.as_str(), s.to_crs.as_str()), ("PAR", "NQY"));
        assert_eq!(s.stations.len(), 7);
        // Named the other way round, the section is the same.
        let r = closed_section("No trains between Newquay and Par ", &atlantic(), &names());
        assert_eq!(r.unwrap().stations, s.stations);
    }

    #[test]
    fn a_short_section_of_a_long_line_is_reduced_service() {
        let mut crs: Vec<String> = (0..20).map(|i| format!("X{i:02}")).collect();
        crs.insert(5, "PAR".to_string());
        crs.insert(7, "LUX".to_string());
        let refs: Vec<&str> = crs.iter().map(String::as_str).collect();
        let long = line("long", &refs);
        let s = closed_section("No trains between Par and Luxulyan", &long, &names()).unwrap();
        assert_eq!(s.stations.len(), 3);
        assert_eq!(s.severity, Severity::ReducedService);
        // A third of the line is enough for Part Suspended.
        let short = line(
            "short",
            &[
                "X00", "PAR", "X01", "LUX", "X02", "X03", "X04", "X05", "X06",
            ],
        );
        let s = closed_section("No trains between Par and Luxulyan", &short, &names()).unwrap();
        assert_eq!((s.stations.len(), s.severity), (3, Severity::PartSuspended));
    }

    #[test]
    fn names_are_normalised_and_prefix_matched() {
        let maritime = line("gwr-maritime-line", &["TRU", "FMT", "FAL"]);
        let s = closed_section(
            "No trains between Truro and Falmouth Dock",
            &maritime,
            &names(),
        );
        assert_eq!(s.unwrap().to_crs, "FAL");
        let windsor = line("gwr-windsor-branch", &["SLO", "WNC"]);
        let s = closed_section(
            "No trains between Slough and Windsor and Eton Central on Saturday",
            &windsor,
            &names(),
        );
        assert_eq!(s.unwrap().to_crs, "WNC", "a name containing 'and'");
        let s = closed_section(
            "No trains between Slough and Windsor & Eton Central",
            &windsor,
            &names(),
        );
        assert!(s.is_some());
        let lo = line("lo", &["SPB", "RMD", "WAT"]);
        let s = closed_section(
            "No trains between Shepherds Bush and Richmond (operator-wide)",
            &lo,
            &names(),
        );
        assert_eq!(s.unwrap().stations, vec!["SPB", "RMD"]);
        let s = closed_section("No trains between Waterloo and Richmond", &lo, &names());
        assert_eq!(s.unwrap().from_crs, "RMD", "'London' may be left off");
        // Alternatives: one of them on the line is enough.
        let s = closed_section(
            "No trains between Par / Lostwithiel and Roche / Nowhere",
            &atlantic(),
            &names(),
        );
        assert_eq!(s.unwrap().stations, vec!["PAR", "LUX", "BGL", "ROC"]);
    }

    #[test]
    fn unresolved_ended_or_other_phrasings_are_none() {
        let a = atlantic();
        let n = names();
        for summary in [
            "No trains between Liskeard and Looe",
            "No trains between Par and Liskeard",
            "No trains between Par and Par",
            "CLEARED: No trains between Par and Newquay",
            "Disruption ended: no trains between Par and Newquay",
            "Disruption between Par and Newquay",
            "Trains between Par and Newquay may be cancelled",
            "No trains between Pa and Newquay",
        ] {
            assert_eq!(closed_section(summary, &a, &n), None, "{summary}");
        }
        assert_eq!(
            closed_section(
                "No trains between Par and Newquay",
                &a,
                &StationGazetteer::default()
            ),
            None,
            "no station names loaded"
        );
    }

    #[test]
    fn the_second_name_ends_at_the_sentence_or_the_cause() {
        let abbey = line("lnwr-abbey-line", &["WFJ", "X01", "X02", "SAA"]);
        let n = StationGazetteer::new([("WFJ", "Watford Junction"), ("SAA", "St Albans Abbey")]);
        for summary in [
            "No trains between Watford Junction and St. Albans Abbey",
            "No trains between Watford Junction and St Albans Abbey.",
            "No trains between Watford Junction and St Albans Abbey. Buses replace them.",
            "No trains between Watford Junction and St Albans Abbey: a points failure",
            "No trains between Watford Junction and St Albans Abbey due to a points failure",
        ] {
            let s = closed_section(summary, &abbey, &n);
            assert_eq!(s.map(|s| s.to_crs), Some("SAA".to_string()), "{summary}");
        }
    }

    #[test]
    fn no_service_between_reads_the_same() {
        let s = closed_section("No service between Par and Newquay", &atlantic(), &names());
        assert_eq!(s.unwrap().severity, Severity::PartSuspended);
    }
}
