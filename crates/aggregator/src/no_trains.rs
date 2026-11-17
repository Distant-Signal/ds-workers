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

use std::collections::HashSet;

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

/// Every pair of stations that some line's trains run between with no
/// station in between: the consecutive places of each line's route runs
/// (`LineDefinition::route_runs`, its stations and the stations its trains
/// pass between them), from every line of the catalogue.
#[derive(Debug, Clone, Default)]
pub(crate) struct TrackNeighbours(HashSet<(String, String)>);

impl TrackNeighbours {
    pub(crate) fn new<'a>(lines: impl IntoIterator<Item = &'a LineDefinition>) -> Self {
        let mut pairs = HashSet::new();
        for line in lines {
            for run in line.route_runs() {
                for pair in run.windows(2) {
                    pairs.insert(ordered_pair(pair[0], pair[1]));
                }
            }
        }
        Self(pairs)
    }

    fn adjacent(&self, a: &str, b: &str) -> bool {
        self.0.contains(&ordered_pair(a, b))
    }
}

fn ordered_pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

/// Whether the catalogue order steps from one branch to another between
/// `line.stations[i]` and the next station: a break (a pair none of the
/// line's trains runs between, `PassThroughLeg::runs` false) that also
/// changes segment, between two stations no line's trains run between
/// directly. A branch starts its own segment (lines/SCHEMA.md), so a break
/// inside one segment is a stopping-pattern effect (Coventry, Canley and
/// Tile Hill on the West Coast's Birmingham line: no single train calls at
/// two of them in a row), and so is one across a segment change where
/// another line's trains run from one station straight to the other
/// (Higham to Strood on the high-speed line, which the North Kent line's
/// trains run). The c2c's Shoeburyness to Dagenham Dock and Purfleet to
/// Ockendon, and the Brighton Main Line's Clapham Junction to London
/// Bridge, are boundaries.
fn branch_boundary(line: &LineDefinition, i: usize, neighbours: &TrackNeighbours) -> bool {
    let (Some(a), Some(b)) = (line.stations.get(i), line.stations.get(i + 1)) else {
        return false;
    };
    a.segment != b.segment
        && line
            .pass_through
            .iter()
            .any(|leg| !leg.runs && leg.from == a.crs && leg.to == b.crs)
        && !neighbours.adjacent(&a.crs, &b.crs)
}

/// The section of `line` that `summary` says has no trains, when it names
/// two different stations of the line. `None` for any other summary, one
/// that says the closure is over, a name that does not resolve on this
/// line, or ends on different branches (the catalogue order between them
/// crosses a [`branch_boundary`], so the stations between them in that
/// order are not the stations between them on the ground: c2c's Upminster
/// and Grays, either side of the Southend line). Then the incident is
/// classified exactly as before.
pub(crate) fn closed_section(
    summary: &str,
    line: &LineDefinition,
    names: &StationGazetteer,
    neighbours: &TrackNeighbours,
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
    if (lo..hi).any(|i| branch_boundary(line, i, neighbours)) {
        return None;
    }
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

    /// [`closed_section`] with no other line to show two stations are
    /// neighbours.
    fn section(
        summary: &str,
        line: &LineDefinition,
        names: &StationGazetteer,
    ) -> Option<ClosedSection> {
        closed_section(summary, line, names, &TrackNeighbours::default())
    }

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
            crs_aliases: std::collections::BTreeMap::new(),
            trunk_for: Vec::new(),
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
        let s = section(
            "No trains between Par and Newquay until approximately 13:00",
            &atlantic(),
            &names(),
        )
        .unwrap();
        assert_eq!(s.severity, Severity::PartSuspended);
        assert_eq!((s.from_crs.as_str(), s.to_crs.as_str()), ("PAR", "NQY"));
        assert_eq!(s.stations.len(), 7);
        // Named the other way round, the section is the same.
        let r = section("No trains between Newquay and Par ", &atlantic(), &names());
        assert_eq!(r.unwrap().stations, s.stations);
    }

    #[test]
    fn a_short_section_of_a_long_line_is_reduced_service() {
        let mut crs: Vec<String> = (0..20).map(|i| format!("X{i:02}")).collect();
        crs.insert(5, "PAR".to_string());
        crs.insert(7, "LUX".to_string());
        let refs: Vec<&str> = crs.iter().map(String::as_str).collect();
        let long = line("long", &refs);
        let s = section("No trains between Par and Luxulyan", &long, &names()).unwrap();
        assert_eq!(s.stations.len(), 3);
        assert_eq!(s.severity, Severity::ReducedService);
        // A third of the line is enough for Part Suspended.
        let short = line(
            "short",
            &[
                "X00", "PAR", "X01", "LUX", "X02", "X03", "X04", "X05", "X06",
            ],
        );
        let s = section("No trains between Par and Luxulyan", &short, &names()).unwrap();
        assert_eq!((s.stations.len(), s.severity), (3, Severity::PartSuspended));
    }

    #[test]
    fn names_are_normalised_and_prefix_matched() {
        let maritime = line("gwr-maritime-line", &["TRU", "FMT", "FAL"]);
        let s = section(
            "No trains between Truro and Falmouth Dock",
            &maritime,
            &names(),
        );
        assert_eq!(s.unwrap().to_crs, "FAL");
        let windsor = line("gwr-windsor-branch", &["SLO", "WNC"]);
        let s = section(
            "No trains between Slough and Windsor and Eton Central on Saturday",
            &windsor,
            &names(),
        );
        assert_eq!(s.unwrap().to_crs, "WNC", "a name containing 'and'");
        let s = section(
            "No trains between Slough and Windsor & Eton Central",
            &windsor,
            &names(),
        );
        assert!(s.is_some());
        let lo = line("lo", &["SPB", "RMD", "WAT"]);
        let s = section(
            "No trains between Shepherds Bush and Richmond (operator-wide)",
            &lo,
            &names(),
        );
        assert_eq!(s.unwrap().stations, vec!["SPB", "RMD"]);
        let s = section("No trains between Waterloo and Richmond", &lo, &names());
        assert_eq!(s.unwrap().from_crs, "RMD", "'London' may be left off");
        // Alternatives: one of them on the line is enough.
        let s = section(
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
            assert_eq!(section(summary, &a, &n), None, "{summary}");
        }
        assert_eq!(
            section(
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
            let s = section(summary, &abbey, &n);
            assert_eq!(s.map(|s| s.to_crs), Some("SAA".to_string()), "{summary}");
        }
    }

    #[test]
    fn no_service_between_reads_the_same() {
        let s = section("No service between Par and Newquay", &atlantic(), &names());
        assert_eq!(s.unwrap().severity, Severity::PartSuspended);
    }

    fn leg(from: &str, to: &str, runs: bool) -> common::pass_through::PassThroughLeg {
        common::pass_through::PassThroughLeg {
            from: from.to_string(),
            to: to.to_string(),
            via: vec![],
            runs,
        }
    }

    /// A main line `AAA`-`BBB`-`CCC`-`DDD` with a spur `EEE`-`FFF` leaving
    /// it at the junction `BBB`, listed after the main line's terminus, as
    /// the catalogue lists a branch.
    fn branchy() -> LineDefinition {
        let mut l = line("branchy", &["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]);
        for (station, segment) in l
            .stations
            .iter_mut()
            .zip(["trunk", "trunk", "main", "main", "spur", "spur"])
        {
            station.segment = Some(segment.to_string());
        }
        l.pass_through = vec![
            // No train calls at both (a stopping-pattern break in one
            // segment): still one stretch of track.
            leg("CCC", "DDD", false),
            // The spur starts here in the station order, not on the ground.
            leg("DDD", "EEE", false),
        ];
        l
    }

    fn branchy_names() -> StationGazetteer {
        StationGazetteer::new([
            ("AAA", "Aston"),
            ("BBB", "Bexley"),
            ("CCC", "Crofton"),
            ("DDD", "Denby"),
            ("EEE", "Eastwick"),
            ("FFF", "Fenwick"),
        ])
    }

    #[test]
    fn a_range_across_a_branch_boundary_is_none() {
        let (l, n) = (branchy(), branchy_names());
        for summary in [
            "No trains between Bexley and Fenwick",
            "No trains between Crofton and Eastwick",
            "No trains between Denby and Eastwick",
            // An alternative on the other branch counts too.
            "No trains between Aston and Bexley / Fenwick",
        ] {
            assert_eq!(section(summary, &l, &n), None, "{summary}");
        }
        // Either side of the boundary, and across the break inside one
        // segment, the section is as before.
        let s = section("No trains between Aston and Denby", &l, &n).unwrap();
        assert_eq!(s.stations, ["AAA", "BBB", "CCC", "DDD"]);
        let s = section("No trains between Eastwick and Fenwick", &l, &n).unwrap();
        assert_eq!(s.stations, ["EEE", "FFF"]);
    }

    #[test]
    fn a_break_another_line_runs_through_is_no_boundary() {
        let (l, n) = (branchy(), branchy_names());
        // Another line's trains run Denby to Eastwick directly: the
        // segment changes there, but the track runs on.
        let mut other = line("other", &["DDD", "EEE"]);
        other.pass_through = vec![leg("DDD", "EEE", true)];
        let neighbours = TrackNeighbours::new([&other, &l]);
        let s = closed_section(
            "No trains between Crofton and Eastwick",
            &l,
            &n,
            &neighbours,
        );
        assert_eq!(s.unwrap().stations, ["CCC", "DDD", "EEE"]);
        // A break with no segments at all (a custom line) is no boundary.
        let mut plain = line("plain", &["AAA", "BBB", "EEE"]);
        plain.pass_through = vec![leg("BBB", "EEE", false)];
        let s = section("No trains between Aston and Eastwick", &plain, &n);
        assert_eq!(s.unwrap().stations, ["AAA", "BBB", "EEE"]);
    }

    /// The embedded catalogue, its lines' track neighbours, and a
    /// gazetteer over the names the cases below use.
    struct Real {
        lines: HashMap<String, LineDefinition>,
        neighbours: TrackNeighbours,
        names: StationGazetteer,
    }

    fn real() -> Real {
        let lines: HashMap<String, LineDefinition> = ds_lines::embedded()
            .expect("embedded catalogue")
            .0
            .into_iter()
            .map(|l| (l.id.clone(), l))
            .collect();
        let neighbours = TrackNeighbours::new(lines.values());
        let names = StationGazetteer::new([
            ("BGN", "Bridgend"),
            ("BHI", "Birmingham International"),
            ("BHM", "Birmingham New Street"),
            ("BMO", "Birmingham Moor Street"),
            ("BTN", "Brighton"),
            ("CDF", "Cardiff Central"),
            ("CLJ", "Clapham Junction"),
            ("COV", "Coventry"),
            ("ECR", "East Croydon"),
            ("EUS", "London Euston"),
            ("FAV", "Faversham"),
            ("GRV", "Gravesend"),
            ("GRY", "Grays"),
            ("GTW", "Gatwick Airport"),
            ("LBG", "London Bridge"),
            ("LMS", "Leamington Spa"),
            ("MKC", "Milton Keynes Central"),
            ("OCK", "Ockendon"),
            ("SIT", "Sittingbourne"),
            ("SOO", "Strood"),
            ("SWA", "Swansea"),
            ("TIL", "Tilbury Town"),
            ("UPM", "Upminster"),
            ("VIC", "London Victoria"),
        ]);
        Real {
            lines,
            neighbours,
            names,
        }
    }

    /// Each case's `(first, last, station count, severity)`, or `None`.
    fn real_case(
        real: &Real,
        line: &str,
        summary: &str,
    ) -> Option<(String, String, usize, Severity)> {
        let line = real.lines.get(line).expect(line);
        closed_section(summary, line, &real.names, &real.neighbours).map(|s| {
            (
                s.stations.first().cloned().unwrap_or_default(),
                s.stations.last().cloned().unwrap_or_default(),
                s.stations.len(),
                s.severity,
            )
        })
    }

    #[test]
    fn real_sections_inside_one_branch_resolve_as_before() {
        let real = real();
        let pr = Severity::PartSuspended;
        let rs = Severity::ReducedService;
        // Every one crosses a break (no single train calls at both ends of
        // some consecutive pair in it), none a branch boundary.
        for (line, a, b, expected) in [
            (
                "wcml-birmingham",
                "Coventry",
                "Birmingham New Street",
                ("COV", "BHM", 11, pr),
            ),
            (
                "wcml-birmingham",
                "Coventry",
                "Birmingham International",
                ("COV", "BHI", 6, rs),
            ),
            (
                "wcml-birmingham",
                "London Euston",
                "Milton Keynes Central",
                ("EUS", "MKC", 5, rs),
            ),
            (
                "gwr-south-wales",
                "Bridgend",
                "Swansea",
                ("BGN", "SWA", 9, pr),
            ),
            (
                "gwr-south-wales",
                "Cardiff Central",
                "Swansea",
                ("CDF", "SWA", 13, pr),
            ),
            (
                "chiltern-main-line",
                "Leamington Spa",
                "Birmingham Moor Street",
                ("LMS", "BMO", 14, pr),
            ),
            (
                "southeastern-highspeed",
                "Gravesend",
                "Strood",
                ("GRV", "SOO", 3, rs),
            ),
            (
                "southeastern-highspeed",
                "Sittingbourne",
                "Faversham",
                ("SIT", "FAV", 3, rs),
            ),
            // Inside the c2c's Tilbury loop, and the Brighton Main Line
            // from London Bridge.
            ("c2c", "Ockendon", "Tilbury Town", ("OCK", "TIL", 4, rs)),
            (
                "southern-brighton-main-line",
                "London Bridge",
                "Brighton",
                ("LBG", "BTN", 17, pr),
            ),
        ] {
            let summary = format!("No trains between {a} and {b}");
            let (first, last, count, severity) = expected;
            assert_eq!(
                real_case(&real, line, &summary),
                Some((first.to_string(), last.to_string(), count, severity)),
                "{line}: {summary}"
            );
        }
    }

    #[test]
    fn real_sections_across_branches_are_none() {
        let real = real();
        for (line, a, b) in [
            // Upminster is on the line to Southend, Grays on the Tilbury
            // loop: the station order between them is the whole Southend
            // line (19 of 26 stations), which is not the section.
            ("c2c", "Upminster", "Grays"),
            // Victoria's trains join London Bridge's at East Croydon; the
            // station order lists London Bridge between Clapham Junction
            // and East Croydon.
            (
                "southern-brighton-main-line",
                "Clapham Junction",
                "East Croydon",
            ),
            (
                "southern-brighton-main-line",
                "London Victoria",
                "Gatwick Airport",
            ),
        ] {
            let summary = format!("No trains between {a} and {b}");
            assert_eq!(real_case(&real, line, &summary), None, "{line}: {summary}");
        }
    }
}
