//! Decide which lines a Knowledgebase incident affects, and classify the
//! scope of each match. Ported from `src/matcher.py`.

use std::collections::{HashMap, HashSet};

use common::{IncidentMessage, LineDefinition};

use crate::segments::SegmentRegistry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchScope {
    ExclusiveSegment,
    SharedSegment,
    StationHit,
    KeywordOnly,
    OperatorOnly,
}

// `segments`/`operators`/`keywords` are faithful ports of the Python
// prototype's `evidence` dict (`src/matcher.py`) and are intentionally kept
// as API surface for future consumers (e.g. richer disruption messages,
// debugging) even though only `.stations` is read today.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Evidence {
    pub stations: Vec<String>,
    pub segments: Vec<String>,
    pub operators: Vec<String>,
    pub keywords: Vec<String>,
}

pub struct Match<'a> {
    pub line: &'a LineDefinition,
    pub scope: MatchScope,
    pub evidence: Evidence,
}

/// Return all lines the incident could plausibly affect, classified.
pub fn lines_affected_by<'a>(
    incident: &IncidentMessage,
    lines: &'a HashMap<String, LineDefinition>,
    registry: &SegmentRegistry,
) -> Vec<Match<'a>> {
    let haystack = format!("{} {}", incident.summary, incident.description).to_lowercase();
    let mut out: Vec<Match<'a>> = Vec::new();

    for line in lines.values() {
        if is_excluded(line, &haystack) {
            continue;
        }
        if let Some(m) = match_one(line, incident, registry, &haystack) {
            out.push(m);
        }
    }

    // If any precise match exists, drop operator-only matches — they're
    // almost certainly false positives where another line on the same
    // operator is the actual target.
    let has_precise = out.iter().any(|m| m.scope != MatchScope::OperatorOnly);
    if has_precise {
        out.retain(|m| m.scope != MatchScope::OperatorOnly);
    }

    out
}

fn match_one<'a>(
    line: &'a LineDefinition,
    incident: &IncidentMessage,
    registry: &SegmentRegistry,
    haystack: &str,
) -> Option<Match<'a>> {
    let operator_overlap: Vec<String> = line
        .operators
        .iter()
        .filter(|op| incident.operators.contains(op))
        .cloned()
        .collect();
    let station_hits: Vec<String> = incident
        .affected_stations
        .iter()
        .filter(|crs| line.has_station(crs))
        .cloned()
        .collect();
    let keyword_hits: Vec<String> = line
        .match_keywords
        .iter()
        .filter(|kw| haystack.contains(&kw.to_lowercase()))
        .cloned()
        .collect();

    // Tier 1: station hits — try to classify by segment.
    if !station_hits.is_empty() {
        let segments: HashSet<String> = registry.segments_touched_by(line, &station_hits);
        let evidence = Evidence {
            stations: station_hits,
            segments: segments.iter().cloned().collect(),
            operators: operator_overlap,
            keywords: keyword_hits,
        };

        if !segments.is_empty() && segments.iter().all(|s| registry.is_exclusive_to(s, &line.id)) {
            return Some(Match { line, scope: MatchScope::ExclusiveSegment, evidence });
        }
        if !segments.is_empty() && segments.iter().any(|s| registry.is_shared(s)) {
            return Some(Match { line, scope: MatchScope::SharedSegment, evidence });
        }
        return Some(Match { line, scope: MatchScope::StationHit, evidence });
    }

    // Tier 2: keyword match.
    if !keyword_hits.is_empty() {
        return Some(Match {
            line,
            scope: MatchScope::KeywordOnly,
            evidence: Evidence { stations: vec![], segments: vec![], operators: operator_overlap, keywords: keyword_hits },
        });
    }

    // Tier 3: operator only.
    if !operator_overlap.is_empty() {
        return Some(Match {
            line,
            scope: MatchScope::OperatorOnly,
            evidence: Evidence { stations: vec![], segments: vec![], operators: operator_overlap, keywords: vec![] },
        });
    }

    None
}

fn is_excluded(line: &LineDefinition, haystack: &str) -> bool {
    line.excluded_keywords.iter().any(|kw| haystack.contains(&kw.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn load_line(id: &str) -> HashMap<String, LineDefinition> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        let all = LineDefinition::from_dir(&dir).expect("lines/ directory should parse");
        all.into_iter()
            .filter(|l| l.id == id)
            .map(|l| (l.id.clone(), l))
            .collect()
    }

    fn load_all_lines() -> HashMap<String, LineDefinition> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        LineDefinition::from_dir(&dir)
            .expect("lines/ directory should parse")
            .into_iter()
            .map(|l| (l.id.clone(), l))
            .collect()
    }

    fn incident(id: &str, summary: &str, description: &str, operators: &[&str], affected_stations: &[&str]) -> IncidentMessage {
        IncidentMessage {
            incident_id: id.to_string(),
            summary: summary.to_string(),
            description: description.to_string(),
            operators: operators.iter().map(|s| s.to_string()).collect(),
            affected_stations: affected_stations.iter().map(|s| s.to_string()).collect(),
            priority: 0,
            validity: vec![],
            is_planned: false,
            is_cleared: false,
        }
    }

    #[test]
    fn excluded_keyword_vetoes_match() {
        let lines = load_line("wcml");
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("T1", "Cross Country delays", "Cross Country services are delayed at Rugby.", &[], &["RUG"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        assert!(matches.is_empty(), "excluded keyword should veto match");
    }

    #[test]
    fn keyword_only_match() {
        let lines = load_line("wcml");
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("T2", "WCML engineering", "Overnight engineering work on the West Coast Main Line.", &[], &[]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].scope, MatchScope::KeywordOnly);
    }

    #[test]
    fn swr_shared_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("SWR-1", "Signal failure at Woking", "Signal failure causing delays to SWR services.", &["SW"], &["WOK"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("swr-south-west-main"));
        assert!(matched_ids.contains("swr-portsmouth-direct"));
        assert!(matched_ids.contains("swr-alton"));
        for m in &matches {
            if m.line.id.starts_with("swr-") {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    #[test]
    fn xc_hub_incident_propagates_to_every_cross_country_arm() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("XC-1", "Signal failure at Birmingham New Street", "Services are delayed.", &["XC"], &["BHM"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "cross-country".to_string(),
                "xc-manchester".to_string(),
                "xc-cardiff".to_string(),
                "xc-south-coast".to_string(),
                "xc-stansted".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    #[test]
    fn elizabeth_branch_incident_stays_on_its_branch() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("XR-1", "Trespass at Shenfield", "Trespass incident causing delays.", &["XR"], &["SNF"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["elizabeth-shenfield".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn swr_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("SWR-2", "Power supply issue at Alton", "Power supply problem causing delays at Alton.", &["SW"], &["AON"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["swr-alton".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Liberty line (Romford - Emerson Park - Upminster)
    // is a standalone line with no interchange with any sibling Overground
    // line (confirmed by its own sourcing in the line-catalogue research
    // pass). Per the Global Constraints, standalone lines get an
    // exclusive-segment test only — no shared-segment propagation test is
    // possible or required, the same exception class as c2c/Merseyrail.
    #[test]
    fn overground_liberty_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LO-1", "Signal failure at Upminster", "Signal failure causing delays at Upminster.", &["LO"], &["UPM"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["overground-liberty".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Lioness line (former Watford DC line, Euston -
    // Watford Junction) shares only a single station (Willesden Junction)
    // with the Mildmay line, on physically separate track either side of
    // it — a station-level overlap, not a shared segment. Standalone for
    // the shared-segment testing convention: exclusive-segment test only,
    // same exception class as c2c/Merseyrail.
    //
    // Uses Bushey (BSH) rather than Watford Junction (WFJ): WFJ also
    // appears on `west-coast-main-line.toml` (a real station-level overlap
    // the brief didn't call out), which would make an incident there match
    // both lines and defeat the point of this exclusive-segment test.
    #[test]
    fn overground_lioness_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LO-2", "Points failure at Bushey", "Points failure causing delays at Bushey.", &["LO"], &["BSH"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["overground-lioness".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Mildmay line (former North London line core +
    // West London line north end) — exclusive segment test, mirroring
    // `elizabeth_branch_incident_stays_on_its_branch`. Mildmay's
    // shared-segment propagation test (`overground-canonbury-curve` with
    // the Windrush line) lives alongside `overground-windrush`'s own tests
    // below, since it needs both lines' files to exist.
    //
    // Uses Richmond (RMD) rather than Stratford (SRA): SRA also appears on
    // `elizabeth-shenfield.toml` (a real station-level overlap the brief
    // didn't call out), which would make an incident there match both
    // lines and defeat the point of this exclusive-segment test.
    #[test]
    fn overground_mildmay_exclusive_segment_incident_stays_on_its_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LO-3", "Trespass at Richmond", "Trespass incident causing delays at Richmond.", &["LO"], &["RMD"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["overground-mildmay".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Suffragette line (former Gospel Oak to Barking
    // line) has no genuine shared segment with any sibling Overground
    // line — its only touchpoint (Gospel Oak, with Mildmay) is a
    // station-level overlap, and South Tottenham (this line) is a
    // genuinely different station from Seven Sisters (Weaver line)
    // despite being nearby. Standalone for the shared-segment testing
    // convention: exclusive-segment test only, same exception class as
    // c2c/Merseyrail.
    #[test]
    fn overground_suffragette_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LO-4", "Points failure at Barking Riverside", "Points failure causing delays at Barking Riverside.", &["LO"], &["BGV"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["overground-suffragette".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Weaver line (former Lea Valley lines) has no
    // genuine shared segment with any sibling Overground line — its
    // Enfield Town/Cheshunt sub-trunk sharing is internal to this one
    // line, and Seven Sisters (this line) is a genuinely different
    // station from Suffragette's South Tottenham despite proximity.
    // Standalone for the shared-segment testing convention:
    // exclusive-segment test only, same exception class as c2c/Merseyrail.
    #[test]
    fn overground_weaver_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LO-5", "Signal failure at Chingford", "Signal failure causing delays at Chingford.", &["LO"], &["CHI"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["overground-weaver".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Windrush line (former East London line,
    // extended) — exclusive segment test for its West Croydon branch,
    // well clear of the shared Canonbury curve.
    #[test]
    fn overground_windrush_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LO-6", "Signal failure at West Croydon", "Signal failure causing delays at West Croydon.", &["LO"], &["WCY"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["overground-windrush".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // The `overground-canonbury-curve` shared segment (Highbury &
    // Islington, Canonbury) is the only genuine cross-line shared trunk
    // among the six London Overground lines — a real curve of track
    // connecting the North London (Mildmay) and East London (Windrush)
    // route alignments. Mirrors `swr_shared_trunk_incident_propagates`;
    // needs both `overground-mildmay` and `overground-windrush` loaded,
    // hence `load_all_lines()` and placement here (after both files exist).
    #[test]
    fn overground_canonbury_curve_incident_propagates_to_mildmay_and_windrush() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LO-7", "Signal failure at Canonbury", "Signal failure causing delays at Canonbury.", &["LO"], &["CNN"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["overground-mildmay".to_string(), "overground-windrush".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // `lines/thameslink-bedford.toml` (Batch 5, Task 5.12) does not exist in
    // this worktree's `lines/` directory as of authoring, so
    // `emr-mml-south` cannot be tested as a shared trunk here - only the
    // Nottingham spur's exclusive-segment behaviour is guaranteed testable
    // right now (see the ruling comment in
    // `lines/emr-midland-main-line.toml`).
    #[test]
    fn emr_nottingham_spur_incident_stays_on_its_branch() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMR-1", "Points failure at Beeston", "Points failure causing delays to services at Beeston.", &["EM"], &["BEE"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-midland-main-line".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/emr-regional.toml` (Batch 7, Task 7.2): exclusive-segment check
    // on the Erewash Valley stretch (Alfreton), which - per that file's
    // Ruling 3 comment - is station-overlap-only with
    // `emr-midland-main-line.toml`'s Nottingham spur, not a shared segment.
    #[test]
    fn emr_regional_erewash_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMRR-1", "Signal failure at Alfreton", "Signal failure causing delays to services at Alfreton.", &["EM"], &["ALF"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-regional".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Same file, Ruling 2: Chesterfield-Sheffield genuinely shares Midland
    // Main Line trackage with `emr-midland-main-line.toml`'s `emr-mml-north`
    // segment, so an incident there should propagate to both lines as
    // SharedSegment. `cross-country.toml` also lists CHD (its own
    // `xc-yorkshire` segment, per that Sheffield/Chesterfield stretch's
    // established station-overlap-only precedent) so it legitimately
    // appears too, as ExclusiveSegment - not asserted away, just not the
    // focus of this test.
    #[test]
    fn emr_regional_chesterfield_incident_shared_with_midland_main_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMRR-2", "Points failure at Chesterfield", "Points failure causing delays to services at Chesterfield.", &["EM"], &["CHD"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
        assert_eq!(by_id.get("emr-regional"), Some(&MatchScope::SharedSegment));
        assert_eq!(by_id.get("emr-midland-main-line"), Some(&MatchScope::SharedSegment));
    }

    // Same file, Ruling 1 (revised after final review): Manchester
    // Piccadilly-Stockport-Sheffield genuinely shares Hope Valley Line
    // *track* with `northern-hope-valley.toml`, but NOT a shared *segment
    // name* (see that file's Ruling 1 comment for why - a coarse-
    // granularity mismatch, the same shape as the Grantham test below).
    // This test confirms the intended, narrower outcome: an incident at
    // Stockport matches both lines independently, each still classified
    // within its own file (`emr-regional` as ExclusiveSegment on its own
    // `emr-regional-hope-valley` segment, `northern-hope-valley` as
    // ExclusiveSegment on its own `northern-hope-valley` segment - neither
    // reports SharedSegment for the other). `xc-manchester.toml` also
    // lists SPT (its own `xc-manchester` segment, an unrelated WCML route
    // that merely passes through the same station) so it legitimately
    // appears too, as ExclusiveSegment - not asserted away, just not the
    // focus of this test.
    #[test]
    fn emr_regional_stockport_and_hope_valley_both_match_without_over_propagating() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMRR-3", "Overhead line damage at Stockport", "Overhead line damage causing delays to services at Stockport.", &["EM"], &["SPT"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
        assert_eq!(by_id.get("emr-regional"), Some(&MatchScope::ExclusiveSegment));
        assert_eq!(by_id.get("northern-hope-valley"), Some(&MatchScope::ExclusiveSegment));
    }

    // `lines/emr-connect.toml` (Batch 7, Task 7.3): the real EMR Connect
    // route runs St Pancras - Corby, not just to Luton Airport Parkway, so
    // it shares `emr-midland-main-line.toml`'s `emr-mml-south` (St Pancras
    // - Bedford) and `emr-mml-trunk` (Wellingborough - Kettering) segments
    // for its entire route bar the final station. An incident anywhere on
    // that shared stretch should propagate to both lines as SharedSegment -
    // mirrors `swr_shared_trunk_incident_propagates`.
    #[test]
    fn emr_connect_shared_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMC-1", "Signal failure at Wellingborough", "Signal failure causing delays to services at Wellingborough.", &["EM"], &["WEL"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
        assert_eq!(by_id.get("emr-connect"), Some(&MatchScope::SharedSegment));
        assert_eq!(by_id.get("emr-midland-main-line"), Some(&MatchScope::SharedSegment));
    }

    // Same file: Corby is this line's only exclusive station (not on the
    // shared St Pancras-Kettering trunk, and not present in
    // `emr-midland-main-line.toml` at all - see that file's route-scope
    // ruling comment), so an incident there should stay local to
    // `emr-connect` as ExclusiveSegment.
    #[test]
    fn emr_connect_corby_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMC-2", "Points failure at Corby", "Points failure causing delays to services at Corby.", &["EM"], &["COR"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-connect".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/emr-rural-branches.toml` (Batch 7, Task 7.4): Worksop is this
    // bundled line's Robin Hood Line branch's own exclusive territory - no
    // other file in this catalogue lists WRK, and that file's own ruling
    // documents confirming (rather than assuming) no genuine shared trunk
    // exists for this specific branch beyond the Nottingham station itself.
    #[test]
    fn emr_rural_branches_worksop_incident_stays_on_its_own_branch() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMRB-1", "Signal failure at Worksop", "Signal failure causing delays to services at Worksop.", &["EM"], &["WRK"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-rural-branches".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Same file: the brief anticipated no genuine shared-trunk stretch for
    // any of these three branches beyond station-level overlap. Research
    // found genuine shared *track* between the Poacher Line (Nottingham-
    // Skegness) and `emr-regional.toml`'s Liverpool-Norwich service, both of
    // which run over the same Nottingham-Grantham line metals (the dedicated
    // "Nottingham-Grantham line" Wikipedia article confirms this) - but this
    // file's Branch 2 ruling comment explains why the segment *name* is
    // deliberately NOT shared regardless: `emr-regional.toml`'s
    // `emr-regional-east` segment is coarser than the genuine overlap (it
    // also spans that file's deliberately-exclusive Alfreton station and its
    // Peterborough-Ely-Norwich continuation), so reusing it here would
    // incorrectly promote those unrelated stations to "shared" too - reusing
    // it in an earlier draft of this file broke the pre-existing
    // `emr_regional_erewash_incident_stays_on_its_own_line` test below by
    // doing exactly that. This test instead confirms the intended, narrower
    // outcome: an incident at Grantham matches both lines independently,
    // each still classified within its own file (`emr-rural-branches` as
    // ExclusiveSegment on its own `emr-poacher-skegness` segment,
    // `emr-regional` as ExclusiveSegment on its own `emr-regional-east`
    // segment - neither reports SharedSegment for the other).
    #[test]
    fn emr_rural_branches_poacher_line_and_emr_regional_both_match_grantham_without_over_propagating() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMRB-2", "Points failure at Grantham", "Points failure causing delays to services at Grantham.", &["EM"], &["GRA"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
        assert_eq!(by_id.get("emr-rural-branches"), Some(&MatchScope::ExclusiveSegment));
        assert_eq!(by_id.get("emr-regional"), Some(&MatchScope::ExclusiveSegment));
    }

    // Same file: the second confirmed shared-trunk exception, and this one
    // DOES reuse a sibling file's segment name (a clean subset, unlike the
    // Poacher Line case above - see the Branch 3 ruling comment for why the
    // two cases are treated differently). The Derwent Valley Line (Derby-
    // Matlock) diverges from the Midland Main Line at Ambergate Junction,
    // just south of Ambergate station (Wikipedia's "Ambergate railway
    // station" article), so Derby-Ambergate is genuine shared trunk with
    // `emr-midland-main-line.toml`'s `emr-mml-derby` segment, reused
    // verbatim in this file's Branch 3. Derby (DBY) is the only station
    // common to both files' own station lists (the intercity MML service
    // skips Duffield/Belper/Ambergate entirely), so it is the only station
    // where an incident can demonstrate both lines matching together as
    // SharedSegment.
    #[test]
    fn emr_rural_branches_matlock_branch_shared_with_midland_main_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMRB-3", "Overhead line damage at Derby", "Overhead line damage causing delays to services at Derby.", &["EM"], &["DBY"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
        assert_eq!(by_id.get("emr-rural-branches"), Some(&MatchScope::SharedSegment));
        assert_eq!(by_id.get("emr-midland-main-line"), Some(&MatchScope::SharedSegment));
    }

    // Same file: Matlock itself is this branch's terminus, on the exclusive
    // `emr-matlock-branch` segment (starts at Whatstandwell, the station
    // after Ambergate Junction) - confirms the exclusive tail behaves
    // correctly alongside the shared-trunk stretch tested above.
    #[test]
    fn emr_rural_branches_matlock_incident_stays_on_its_own_branch() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMRB-4", "Trespass at Matlock", "Trespass incident causing delays at Matlock.", &["EM"], &["MAT"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-rural-branches".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }
}
