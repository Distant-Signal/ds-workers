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
    fn wcml_operators_use_avantis_real_code_not_transport_for_wales() {
        // Regression guard: this file's operators list once had "AW"
        // (Transport for Wales' code) where "VT" (Avanti West Coast's
        // real code, inherited from Virgin Trains) belonged. See
        // lines/west-coast-main-line.toml's `operators` comment for the
        // sourcing behind this fix.
        let lines = load_line("wcml");
        let wcml = lines.get("wcml").expect("wcml line should exist");
        assert!(wcml.operators.iter().any(|op| op == "VT"), "wcml operators should contain VT (Avanti West Coast)");
        assert!(!wcml.operators.iter().any(|op| op == "AW"), "wcml operators should not contain AW (Transport for Wales)");
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
        // `wcml-birmingham.toml` (added after this test was first written) also
        // terminates at Birmingham New Street, on its own exclusive
        // `wcml-birmingham-branch` segment -- station-level overlap with the
        // CrossCountry hub, same precedent xc-south-coast.toml/xc-manchester.toml
        // already documented for Coventry/Stafford/Crewe. It's a real sixth
        // line affected by this incident, just with a different scope.
        //
        // `wmr-cross-city.toml` (added after this test was first written) also
        // passes through Birmingham New Street, on its own exclusive
        // `wmr-cross-city-trunk` segment -- same station-level-overlap-only
        // pattern as `wcml-birmingham.toml` above (this task's own brief calls
        // out this exact precedent). It's a real seventh line affected by this
        // incident, still ExclusiveSegment.
        //
        // `lnwr-birmingham-crewe.toml` (added after this test was first
        // written, Task 1.8) also terminates at Birmingham New Street, on its
        // own exclusive `lnwr-birmingham` segment -- same station-overlap-only
        // pattern as the two lines above. It's a real eighth line affected by
        // this incident, still ExclusiveSegment.
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
                "wcml-birmingham".to_string(),
                "wmr-cross-city".to_string(),
                "lnwr-birmingham-crewe".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "wcml-birmingham" || m.line.id == "wmr-cross-city" || m.line.id == "lnwr-birmingham-crewe" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
            } else {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    #[test]
    fn elizabeth_branch_incident_stays_on_its_branch() {
        // Shenfield is a real Greater Anglia main-line station too (see
        // greater-anglia-main-line.toml's Shenfield-corridor decision), so
        // now that that line is catalogued, an incident here legitimately
        // matches both lines by station overlap. What this test still
        // guards: neither match escalates to MatchScope::SharedSegment (the
        // two files deliberately use distinct segment names at Shenfield —
        // station overlap, not a shared trunk), and the incident does not
        // leak to elizabeth-line or elizabeth-heathrow, the other two XR
        // branches.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("XR-1", "Trespass at Shenfield", "Trespass incident causing delays.", &["XR"], &["SNF"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["elizabeth-shenfield".to_string(), "greater-anglia-main-line".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn greater_anglia_shenfield_corridor_is_station_overlap_only() {
        // Romford is on both greater-anglia-main-line.toml (a genuine GA
        // main-line stop) and elizabeth-shenfield.toml (a metro stop on
        // dedicated Elizabeth line tracks). The two files deliberately do
        // NOT share a segment name for this corridor (see
        // greater-anglia-main-line.toml's decision comment), so an incident
        // here should match both lines independently, each still classified
        // as ExclusiveSegment rather than escalating to SharedSegment.
        //
        // Romford is also overground-liberty's own terminus (its file's own
        // comments already document this exact overlap, pre-dating this
        // batch's merge) -- its `overground-liberty` segment name is
        // exclusive catalogue-wide, so it joins the other two as a third
        // independent ExclusiveSegment match, not a shared trunk.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LE-1", "Points failure at Romford", "Points failure causing delays.", &["LE"], &["RMF"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "elizabeth-shenfield".to_string(),
                "overground-liberty".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn greater_anglia_exclusive_far_end_does_not_propagate() {
        // Diss is well beyond Shenfield, on Greater Anglia's exclusive
        // territory — the Elizabeth line goes no further than Shenfield —
        // and isn't a junction for any branch in this batch either (unlike
        // Colchester, which greater-anglia-essex-branches.toml's Sunshine
        // Coast branch also lists as its own real junction — but as
        // station-level overlap only, each independently ExclusiveSegment,
        // not a shared `geml-mainline` segment; see
        // essex_branches_colchester_is_station_overlap_only_with_main_line
        // below). An incident here should stay scoped to
        // greater-anglia-main-line only.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LE-2", "Signal failure at Diss", "Signal failure causing delays.", &["LE"], &["DIS"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["greater-anglia-main-line".to_string()]));
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
        // Upminster is also c2c's own Ockendon-loop junction (lines/c2c.toml,
        // segment `c2c-main-line` -- confirmed exclusive catalogue-wide, same
        // as `overground-liberty`'s own segment name). Station-level overlap,
        // distinct segment names -- same pattern as the Halifax/Berwick
        // precedents: both lines match by station, both stay ExclusiveSegment.
        assert_eq!(
            matched_ids,
            HashSet::from(["overground-liberty".to_string(), "c2c".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
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

    #[test]
    fn cumbrian_coast_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("NT-1", "Signal failure at Whitehaven", "Signal failure causing delays on the Cumbrian Coast.", &["NT"], &["WTH"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-cumbrian-coast".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn cumbrian_coast_shared_trunk_incident_propagates_to_furness() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("NT-2", "Points failure at Barrow-in-Furness", "Points failure causing delays at Barrow.", &["NT"], &["BIF"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("northern-cumbrian-coast"));
        assert!(matched_ids.contains("northern-furness"));
        for m in &matches {
            if m.line.id == "northern-cumbrian-coast" || m.line.id == "northern-furness" {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    // Regression test for the segment-split fix applied when
    // `lines/northern-cumbrian-coast.toml` was merged: `northern-furness.toml`
    // originally tagged ALL FOUR of its stations (not just the shared
    // junction, BIF) with `segment = "northern-furness"`, which would have
    // silently reclassified a Lancaster/Carnforth/Ulverston incident as
    // SharedSegment purely because a second file also used that literal
    // segment name for BIF. LAN/CNF/ULV now sit on their own exclusive
    // `northern-furness-branch` segment -- this asserts that split holds.
    #[test]
    fn furness_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("NT-FUR", "Signal failure at Ulverston", "Signal failure causing delays at Ulverston.", &["NT"], &["ULV"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-furness".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn calder_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-3",
            "Signal failure at Todmorden",
            "Signal failure causing delays on the Calder Valley Line at Todmorden.",
            &["NT"],
            &["TOD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-calder-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn airedale_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-4",
            "Signal failure at Keighley",
            "Signal failure causing delays on the Airedale Line at Keighley.",
            &["NT"],
            &["KEI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-airedale".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn wharfedale_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-5",
            "Signal failure at Ilkley",
            "Signal failure causing delays on the Wharfedale Line at Ilkley.",
            &["NT"],
            &["ILK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-wharfedale".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // The `northern-shipley-trunk` shared-trunk regression test, owned by
    // Task 8.4 per the plan (added once `lines/northern-wharfedale.toml`
    // also exists and shares that segment name - see the shared-trunk
    // naming comment at the top of `lines/northern-airedale.toml`).
    #[test]
    fn shipley_trunk_shared_incident_propagates_to_airedale_and_wharfedale() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-6",
            "Signal failure at Shipley",
            "Signal failure causing delays to Northern services at Shipley.",
            &["NT"],
            &["SHY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("northern-airedale"));
        assert!(matched_ids.contains("northern-wharfedale"));
        for m in &matches {
            if m.line.id == "northern-airedale" || m.line.id == "northern-wharfedale" {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    // `lines/northern-esk-valley.toml` (Task 8.5) is a genuinely standalone
    // line per the gap analysis ("entirely separate from anything currently
    // modelled") - no other line in this catalogue shares any track with
    // it, so there is no shared-trunk regression test to write, only the
    // exclusive-segment one below.
    #[test]
    fn esk_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-7",
            "Signal failure at Glaisdale",
            "Signal failure causing delays on the Esk Valley Line at Glaisdale.",
            &["NT"],
            &["GLS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-esk-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn clitheroe_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-8",
            "Signal failure at Blackburn",
            "Signal failure causing delays on the Ribble Valley Line at Blackburn.",
            &["NT"],
            &["BBN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-clitheroe".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `northern-clitheroe.toml`'s MCV entry joins the existing
    // `northern-manchester` shared segment already used by `northern.toml`
    // and `northern-blackpool.toml` (confirmed genuine track-sharing through
    // Bolton, not just both routes touching Manchester - see the
    // top-of-file comment in `lines/northern-clitheroe.toml`), making it a
    // three-way shared trunk.
    #[test]
    fn manchester_victoria_hub_incident_propagates_to_northern_blackpool_and_clitheroe() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-9",
            "Signal failure at Manchester Victoria",
            "Signal failure causing delays to Northern services at Manchester Victoria.",
            &["NT"],
            &["MCV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("northern"));
        assert!(matched_ids.contains("northern-blackpool"));
        assert!(matched_ids.contains("northern-clitheroe"));
        for m in &matches {
            if m.line.id == "northern" || m.line.id == "northern-blackpool" || m.line.id == "northern-clitheroe" {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
        // `northern-calder-valley.toml` also has an MCV entry, but tagged
        // with its own exclusive `northern-calder-valley` segment rather
        // than `northern-manchester` (see that file's own comment on why
        // the two termini are unrelated track). Guard against a future
        // regression where Calder Valley's MCV entry gets accidentally
        // merged into the shared `northern-manchester` segment.
        if matched_ids.contains("northern-calder-valley") {
            let calder_valley_match = matches.iter().find(|m| m.line.id == "northern-calder-valley").unwrap();
            assert_eq!(
                calder_valley_match.scope,
                MatchScope::ExclusiveSegment,
                "northern-calder-valley should be ExclusiveSegment"
            );
        }
    }

    #[test]
    fn lner_ecml_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Aberdeen sits on `ecml-aberdeen`, north of the Doncaster/Newark
        // junctions that Tasks 6.2-6.4's not-yet-written Leeds/Hull/Lincoln
        // branches will share `ecml-doncaster`/`ecml-fenland` with — no
        // other line touches `ecml-aberdeen` today, so this should stay
        // exclusive to `lner-ecml` and not propagate anywhere else.
        let inc = incident("LNER-1", "Points failure at Aberdeen", "Points failure causing delays at Aberdeen.", &["GR"], &["ABD"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["lner-ecml".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lner_leeds_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Harrogate sits on `lner-leeds-harrogate`, exclusive to this file
        // (LNER's Skipton working diverges at Leeds onto a different physical
        // line and isn't modeled as stations — see the file's comments).
        let inc = incident(
            "LNER-2",
            "Signal failure at Harrogate",
            "Signal failure causing delays to services at Harrogate.",
            &["GR"],
            &["HGT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["lner-leeds".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lner_leeds_doncaster_shared_trunk_propagates_to_ecml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Doncaster is `ecml-doncaster`, shared between `lner-ecml` and
        // `lner-leeds` (both run over the same ECML trunk to Doncaster
        // before the Leeds branch peels off onto the Wakefield Line).
        // `cross-country.toml` also has a station at Doncaster, but on its
        // own exclusive `xc-yorkshire` segment, so it's not asserted here.
        let inc = incident(
            "LNER-3",
            "Points failure at Doncaster",
            "Points failure causing disruption to services through Doncaster.",
            &["GR"],
            &["DON"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("lner-ecml"));
        assert!(matched_ids.contains("lner-leeds"));
        for m in &matches {
            if m.line.id.starts_with("lner-") {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    #[test]
    fn lner_hull_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Selby sits on `lner-hull`, exclusive to this file — the real
        // physical divergence from the ECML is Temple Hirst Junction
        // (north of Doncaster, no CRS code), but the shared trunk still
        // ends at Doncaster per `lner-ecml.toml`'s own instruction (see
        // that file's DON entry and this file's comments), so Selby is
        // this branch's first exclusive station.
        //
        // Since Task 6.6 added `hull-trains.toml` (a different operator,
        // `HT`, that also genuinely calls at Selby en route to Hull
        // Paragon — see that file's own research comments), Selby now also
        // matches `hull-trains` by station. This mirrors
        // `lner_leeds_station_overlap_at_leeds_does_not_share_northern_segment`'s
        // precedent: both lines match, but each stays `ExclusiveSegment`
        // because neither's own segment name (`lner-hull` vs.
        // `ht-kings-cross-hull`) is literally shared with the other — this
        // is real station-level overlap between two different operators'
        // files, not a shared-trunk relationship the matcher recognizes by
        // segment name.
        let inc = incident(
            "LNER-4",
            "Signal failure at Selby",
            "Signal failure causing delays to services at Selby.",
            &["GR"],
            &["SBY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["lner-hull".to_string(), "hull-trains".to_string()]));
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    #[test]
    fn lner_hull_doncaster_shared_trunk_propagates_to_ecml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Doncaster is `ecml-doncaster`, shared between `lner-ecml` and
        // `lner-hull` (both run over the same ECML trunk to Doncaster
        // before the Hull branch peels off toward Selby/Brough). Mirrors
        // `lner_leeds_doncaster_shared_trunk_propagates_to_ecml` above;
        // `lner-leeds` also shares this segment, so it's expected to show
        // up here too, but only `lner-ecml`/`lner-hull` are asserted since
        // that's what this test is about.
        let inc = incident(
            "LNER-5",
            "Points failure at Doncaster",
            "Points failure causing disruption to services through Doncaster.",
            &["GR"],
            &["DON"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("lner-ecml"));
        assert!(matched_ids.contains("lner-hull"));
        for m in &matches {
            if m.line.id.starts_with("lner-") {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    #[test]
    fn lner_lincoln_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Lincoln sits on `lner-lincoln`, exclusive to this file — the real
        // physical divergence from the ECML is the Newark flat crossing,
        // just north of Newark Northgate (no CRS code), but the shared
        // trunk still ends at Newark Northgate per `lner-ecml.toml`'s own
        // instruction (see that file's NNG entry and this file's
        // comments), so Lincoln is this branch's first exclusive station.
        let inc = incident(
            "LNER-6",
            "Signal failure at Lincoln",
            "Signal failure causing delays to services at Lincoln.",
            &["GR"],
            &["LCN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["lner-lincoln".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lner_lincoln_newark_northgate_shared_trunk_propagates_to_ecml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Newark Northgate is `ecml-fenland`, shared between ALL FOUR of
        // `lner-ecml`, `lner-hull`, `lner-leeds` and `lner-lincoln` today
        // (all run over the same ECML trunk to/through Newark Northgate
        // before their own branches peel off further north or, for
        // Lincoln, at the Newark flat crossing right here) -- confirmed by
        // grepping each `lner-*.toml` file's own NNG entry. Final review,
        // Fix #5: tightened from the previous `contains`-based partial
        // assertion (which only checked `lner-ecml`/`lner-lincoln`) to
        // exact `HashSet` equality, matching the Global Constraint's own
        // wording ("matches every line sharing it") more literally, mirrors
        // `xc_hub_incident_propagates_to_every_cross_country_arm`'s style
        // above.
        let inc = incident(
            "LNER-7",
            "Points failure at Newark Northgate",
            "Points failure causing disruption to services through Newark Northgate.",
            &["GR"],
            &["NNG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lner-ecml".to_string(),
                "lner-hull".to_string(),
                "lner-leeds".to_string(),
                "lner-lincoln".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    #[test]
    fn lner_leeds_station_overlap_at_leeds_does_not_share_northern_segment() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Leeds (LDS) is a station on all three of `lner-leeds`, `northern`
        // and `northern-yorkshire-coast` — but research found no LNER
        // service running the physical Leeds<->York trunk that Northern's
        // `northern-yorkshire` segment represents (see the file comment on
        // `lner-leeds.toml`'s LDS entry), so `lner-leeds` deliberately does
        // NOT reuse that segment name. This mirrors `xc-south-coast.toml`'s
        // "station overlap is fine, segment-sharing is a deliberate choice"
        // precedent: all three lines match this incident by station, but
        // `lner-leeds` stays ExclusiveSegment (its own `lner-leeds` segment
        // isn't shared with anyone) while Northern's two lines are
        // SharedSegment between themselves via `northern-yorkshire`.
        let inc = incident(
            "LNER-4",
            "Overhead line damage at Leeds",
            "Overhead line damage causing disruption to services at Leeds.",
            &["GR"],
            &["LDS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        // Leeds is now this catalogue's most-contested station, per Batch 8's
        // own final review: five more Northern-family files (all landed on
        // `main` after this test was originally written) also stop at LDS,
        // and Batch 9 (TransPennine Express) added a sixth line. Each line's
        // scope here is a genuine, individually-derived fact about that
        // line's OWN segment name at LDS -- not a blanket
        // "Northern shares, LNER doesn't" rule:
        //   - lner-leeds: its own `lner-leeds` segment, used nowhere else.
        //   - northern / northern-yorkshire-coast: both on `northern-yorkshire`,
        //     shared between exactly those two.
        //   - northern-airedale: on `northern-shipley-trunk` -- shared
        //     catalogue-wide with northern-wharfedale.toml's own BDQ/SHY
        //     entries (even though Wharfedale's own LDS entry uses a
        //     different, exclusive segment -- sharing is evaluated per
        //     segment NAME across the whole catalogue, not per station).
        //   - northern-wharfedale: its own `northern-wharfedale` segment at
        //     LDS specifically, used nowhere else -- exclusive despite
        //     sharing `northern-shipley-trunk` with Airedale at BDQ/SHY.
        //   - northern-calder-valley: its own `northern-calder-valley`
        //     segment, used nowhere else.
        //   - tpe-north: its own `tpe-north` segment, used nowhere else in
        //     the catalogue (confirmed via exact-match grep, not the
        //     substring search that once wrongly suggested 4 files used it).
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lner-leeds".to_string(),
                "northern".to_string(),
                "northern-yorkshire-coast".to_string(),
                "northern-airedale".to_string(),
                "northern-wharfedale".to_string(),
                "northern-calder-valley".to_string(),
                "tpe-north".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "lner-leeds" | "northern-wharfedale" | "northern-calder-valley" | "tpe-north" => {
                    MatchScope::ExclusiveSegment
                }
                "northern" | "northern-yorkshire-coast" | "northern-airedale" => MatchScope::SharedSegment,
                other => panic!("unexpected line in Leeds overlap test: {other}"),
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    #[test]
    fn grand_central_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Sunderland sits on `gc-sunderland`, exclusive to `grand-central` —
        // no other line in this catalogue reaches Sunderland, so this should
        // stay exclusive and not propagate anywhere else. Per the task
        // brief, no shared-trunk test against `lner-ecml.toml` (or any other
        // LNER file) is required for Grand Central: the plan is explicit
        // that Grand Central's relationship to LNER is station-overlap-only
        // (shared at King's Cross/Peterborough/Doncaster/York, none of which
        // this test touches), not a forced shared segment.
        let inc = incident(
            "GC-1",
            "Signal failure at Sunderland",
            "Signal failure causing delays to services at Sunderland.",
            &["GC"],
            &["SUN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["grand-central".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn grand_central_kings_cross_trunk_shared_trunk_propagates_to_bradford() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Final review, Fix #1: `grand-central.toml` used to model BOTH the
        // King's Cross-Sunderland and King's Cross-Bradford Interchange
        // routes as one non-linear file, with a `gc-trunk-kings-cross`
        // segment that could never be a real shared trunk (only one *line*
        // used the name). Split into `grand-central.toml` (Sunderland) and
        // `grand-central-bradford.toml` (Bradford Interchange), which now
        // genuinely share `gc-trunk-kings-cross` across King's Cross,
        // Peterborough and Doncaster — mirroring
        // `swr_shared_trunk_incident_propagates` above. An incident at
        // Doncaster should now match both files, each SharedSegment.
        let inc = incident(
            "GC-2",
            "Points failure at Doncaster",
            "Points failure causing disruption to services through Doncaster.",
            &["GC"],
            &["DON"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("grand-central"));
        assert!(matched_ids.contains("grand-central-bradford"));
        for m in &matches {
            if m.line.id.starts_with("grand-central") {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    #[test]
    fn grand_central_bradford_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Halifax sits on `gc-bradford`, exclusive to `grand-central-bradford`
        // — this was written when Batch 8's `lines/northern-calder-valley.toml`
        // (also a real Halifax stop, on its own distinctly-named
        // `northern-calder-valley` segment) didn't exist yet in this worktree.
        // Real station-level overlap, no shared segment name between the two
        // files, so both independently classify as ExclusiveSegment — this is
        // the correct, unchanged matcher behaviour; only the expected match
        // set needed updating once both batches landed on `main` together.
        let inc = incident(
            "GC-3",
            "Signal failure at Halifax",
            "Signal failure causing delays to services at Halifax.",
            &["GC"],
            &["HFX"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["grand-central-bradford".to_string(), "northern-calder-valley".to_string()]));
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    #[test]
    fn grand_central_birmingham_shopping_centre_mention_vetoes_match() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Final review, Fix #3: `excluded_keywords` was narrowed from a bare
        // "Birmingham" to "Grand Central, Birmingham" (the shopping
        // centre's own name+city, per Wikipedia's "Grand Central,
        // Birmingham" article), so it should still veto an incident that
        // genuinely mentions the shopping centre. Mirrors
        // `excluded_keyword_vetoes_match`'s style above.
        let inc = incident(
            "GC-4",
            "Fire alarm at Grand Central, Birmingham",
            "A fire alarm was activated at the Grand Central, Birmingham shopping centre, next to New Street station.",
            &[],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(!matched_ids.contains("grand-central"), "shopping-centre mention should still veto grand-central");
        assert!(!matched_ids.contains("grand-central-bradford"), "shopping-centre mention should still veto grand-central-bradford");
    }

    #[test]
    fn grand_central_unrelated_birmingham_mention_does_not_veto_match() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Final review, Fix #3: the old bare "Birmingham" exclusion would
        // have wrongly vetoed a genuine Grand Central incident that happens
        // to mention Birmingham for an unrelated reason (e.g. a diversion
        // routed via Birmingham). The narrowed "Grand Central, Birmingham"
        // phrase should NOT fire here, so the incident matches via the
        // `match_keywords` "Grand Central" phrase instead.
        let inc = incident(
            "GC-5",
            "Grand Central service diverted",
            "A Grand Central service was diverted via Birmingham due to engineering works.",
            &[],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("grand-central"), "unrelated Birmingham mention should not veto grand-central");
        assert!(matched_ids.contains("grand-central-bradford"), "unrelated Birmingham mention should not veto grand-central-bradford");
        for m in &matches {
            if m.line.id.starts_with("grand-central") {
                assert_eq!(m.scope, MatchScope::KeywordOnly, "{} should match via keyword only", m.line.id);
            }
        }
    }

    #[test]
    fn hull_trains_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Howden sits on `ht-kings-cross-hull`, exclusive to `hull-trains` —
        // no other line in this catalogue has a station at Howden, so this
        // should stay exclusive and not propagate anywhere else. Per the
        // task brief (the same standalone-operator exception
        // `grand-central.toml` already established for its own relationship
        // to LNER), no shared-trunk test against any `lner-*.toml` file is
        // required for Hull Trains: `hull-trains.toml`'s station-level
        // overlap with `lner-hull.toml` (Stevenage, Grantham, Retford,
        // Doncaster, Selby, Brough, Hull Paragon) is deliberate and
        // documented, not a forced shared segment.
        let inc = incident(
            "HT-1",
            "Signal failure at Howden",
            "Signal failure causing delays to services at Howden.",
            &["HT"],
            &["HOW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["hull-trains".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lumo_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Falkirk High sits on `lumo-glasgow`, exclusive to `lumo` — no
        // other line in this catalogue has a station at Falkirk High, so
        // this should stay exclusive and not propagate anywhere else. Per
        // the task brief (the same standalone-operator exception
        // `grand-central.toml` and `hull-trains.toml` already established
        // for their own relationship to LNER), no shared-trunk test against
        // any `lner-*.toml` file is required for Lumo: `lumo.toml`'s
        // station-level overlap with `lner-ecml.toml` (King's Cross,
        // Stevenage, Newcastle, Morpeth, Edinburgh Waverley, Haymarket) is
        // deliberate and documented, not a forced shared segment. None of
        // those shared stations are used here, so no pre-existing test
        // needed updating for this task (unlike Task 6.6's Selby situation
        // with `lner-hull.toml`) — checked: no other test in this file
        // references King's Cross, Stevenage, Newcastle, Morpeth, Edinburgh
        // Waverley or Haymarket.
        let inc = incident(
            "LD-1",
            "Signal failure at Falkirk High",
            "Signal failure causing delays to services at Falkirk High.",
            &["LD"],
            &["FKK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["lumo".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }


    // No shared-segment-propagation test for tpe-anglo-scottish: per the
    // batch's pre-flight scan it only overlaps sibling TPE lines at
    // station level (Liverpool Lime Street / Manchester Piccadilly, and
    // Edinburgh Waverley with tpe-borders), and has no shared segment with
    // wcml/xc-manchester/northern by design (station-overlap-only, same
    // precedent as xc-manchester.toml). It's a genuinely standalone line
    // for this assertion.
    #[test]
    fn tpe_anglo_scottish_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-1",
            "Points failure at Motherwell",
            "Points failure causing delays to TransPennine Express services at Motherwell.",
            &["TP"],
            &["MTH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tpe-anglo-scottish".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // No shared-segment-propagation test for tpe-south either: per this
    // task's own pre-flight scan it only overlaps sibling TPE lines at
    // station level (Liverpool Lime Street / Manchester Piccadilly with
    // tpe-anglo-scottish and tpe-north; no overlap at all with
    // tpe-borders), and has no shared segment with xc-manchester/northern
    // by design (station-overlap-only, same precedent as
    // xc-manchester.toml and tpe-anglo-scottish.toml). Genuinely standalone
    // for this assertion.
    #[test]
    fn tpe_south_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-2",
            "Signal failure at Grimsby Town",
            "Signal failure causing delays to TransPennine Express services at Grimsby Town.",
            &["TP"],
            &["GMB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tpe-south".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // No shared-SEGMENT-propagation test for tpe-borders: per this task's
    // own pre-flight scan its own segment name `tpe-borders` has no real
    // overlap with anything else in the catalogue, including this batch's
    // own tpe-north — the Newcastle boundary between them is ruled a
    // terminus-to-terminus handoff, not a shared trunk (mirrors how
    // west-coast-main-line.toml and xc-manchester.toml treat their own
    // Crewe overlap). What this task's own pre-flight scan didn't (and
    // couldn't) anticipate: `lner-ecml.toml` (merged separately, in an
    // earlier batch, and absent from this batch's own isolated worktree)
    // also stops at Berwick-upon-Tweed, via its own distinct `ecml-borders`
    // segment (confirmed exclusive to `lner-ecml.toml` via exact-match
    // grep). Station-level overlap, different segment names — same pattern
    // as the Halifax/Grand-Central-Bradford precedent: both lines match by
    // station, both stay ExclusiveSegment on their own segment names.
    #[test]
    fn tpe_borders_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-3",
            "Signal failure at Berwick-upon-Tweed",
            "Signal failure causing delays to TransPennine Express services at Berwick-upon-Tweed.",
            &["TP"],
            &["BWK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tpe-borders".to_string(), "lner-ecml".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    // No shared-segment-propagation test for tpe-north: no segment name it
    // uses is shared with any other line, including this batch's own
    // tpe-borders (the Newcastle boundary between them is a
    // terminus-to-terminus handoff, not a shared trunk — see tpe-north's
    // own file comments, consistent with tpe-borders's). Genuinely
    // standalone for that assertion, despite unusually heavy *station*-level
    // overlap with northern/cross-country/northern-tyne-valley (station
    // hits alone still produce a `Match` per overlapping line — see below).
    //
    // Station choice for the exclusive-segment test below: almost every
    // principal station on tpe-north's route also appears in another line
    // file (LIV/NLW/MCV/HUD/LDS/YRK in `northern.toml`, DAR/DHM/YRK also in
    // `cross-country.toml`, NCL in `cross-country.toml`/
    // `northern-tyne-valley.toml`/`tpe-borders.toml`), and `match_one`
    // matches per-line on raw station hits before segment classification —
    // so an incident at any of those stations would also match those other
    // lines (each independently as their own ExclusiveSegment, since no
    // segment *name* collides), failing a "matches only tpe-north"
    // assertion. Verified by grepping `lines/*.toml`: Chester-le-Street
    // (CLS) is the one tpe-north station that appears in no other line
    // file, so it's used here instead of the Darlington/Newcastle choice
    // this task's brief originally suggested.
    #[test]
    fn tpe_north_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-4",
            "Signal failure at Chester-le-Street",
            "Signal failure causing delays to TransPennine Express services at Chester-le-Street.",
            &["TP"],
            &["CLS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tpe-north".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn chiltern_stratford_branch_incident_does_not_propagate() {
        // Strict-equality on the whole match set would break once a future
        // WMR Snow Hill lines entry (station-overlap-only, per
        // chiltern-main-line.toml's own comments) plausibly also lists
        // Wilmcote on the North Warwickshire line to Stratford - so, like
        // `chiltern_banbury_incident_matches_by_station_not_shared_segment`,
        // this only asserts chiltern-main-line is among the matches and is
        // classified ExclusiveSegment, not that nothing else could match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-1",
            "Trespass incident at Wilmcote",
            "Trespass incident causing delays to Chiltern Railways services.",
            &["CH"],
            &["WMC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("chiltern-main-line"));
        for m in &matches {
            if m.line.id == "chiltern-main-line" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment);
            }
        }
    }

    #[test]
    fn chiltern_birmingham_approach_incident_does_not_propagate() {
        // Strict-equality on the whole match set would break once a future
        // WMR Snow Hill lines entry (station-overlap-only, per
        // chiltern-main-line.toml's own comments) plausibly also lists
        // Solihull on the Dorridge approach - so, like
        // `chiltern_banbury_incident_matches_by_station_not_shared_segment`,
        // this only asserts chiltern-main-line is among the matches and is
        // classified ExclusiveSegment, not that nothing else could match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-2",
            "Signal failure at Solihull",
            "Signal failure causing delays to Chiltern Railways services.",
            &["CH"],
            &["SOL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("chiltern-main-line"));
        for m in &matches {
            if m.line.id == "chiltern-main-line" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment);
            }
        }

        // Station-level overlap with Birmingham only, not a shared segment:
        // an incident at Birmingham New Street (XC's hub) must not match
        // this line, and this line's Birmingham approach (Snow Hill/Moor
        // Street) is a different station entirely.
        let bhm_inc = incident("XC-BHM", "Points failure at Birmingham New Street", "Points failure causing delays.", &["XC"], &["BHM"]);
        let bhm_matches = lines_affected_by(&bhm_inc, &lines, &registry);
        let bhm_matched_ids: HashSet<String> = bhm_matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(!bhm_matched_ids.contains("chiltern-main-line"));
    }

    #[test]
    fn chiltern_banbury_incident_matches_by_station_not_shared_segment() {
        // Banbury sits on both chiltern-main-line and xc-south-coast's
        // physical trunk, but the two files deliberately don't share a
        // segment name there (see chiltern-main-line.toml's own comment).
        // An incident should therefore match both lines individually by
        // station, each classified against its own (exclusive) segment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("CH-3", "Overhead line damage at Banbury", "Overhead line damage causing delays.", &[], &["BAN"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("chiltern-main-line"));
        assert!(matched_ids.contains("xc-south-coast"));
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment (station overlap, not segment-sharing)", m.line.id);
        }
    }

    #[test]
    fn chiltern_marylebone_shared_trunk_incident_propagates_to_both_files() {
        // chiltern-aylesbury.toml reuses chiltern-main-line.toml's
        // "chiltern-marylebone" segment tag for Marylebone itself (Task
        // 12.1's comment invited this): both files' services genuinely
        // originate there before diverging at Neasden Junction.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-4",
            "Points failure at Marylebone",
            "Points failure causing delays to Chiltern Railways services.",
            &["CH"],
            &["MYB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("chiltern-main-line"));
        assert!(matched_ids.contains("chiltern-aylesbury"));
        for m in &matches {
            if m.line.id.starts_with("chiltern-") {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    #[test]
    fn chiltern_aylesbury_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-5",
            "Signal failure at Amersham",
            "Signal failure causing delays to Chiltern Railways services.",
            &["CH"],
            &["AMR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["chiltern-aylesbury".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn chiltern_oxford_branch_incident_does_not_propagate() {
        // The Oxford branch (folded into chiltern-aylesbury.toml) is a
        // physically distinct corridor from both this file's own Amersham
        // branch and chiltern-main-line.toml's Birmingham route (it only
        // shares Marylebone itself, per the file's own comments) - an
        // incident here should stay exclusive to chiltern-aylesbury and not
        // leak onto chiltern-main-line or xc-south-coast (which also calls
        // at Oxford, station-overlap only).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-6",
            "Trespass incident at Bicester Village",
            "Trespass incident causing delays to Chiltern Railways services.",
            &["CH"],
            &["BIT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["chiltern-aylesbury".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn c2c_exclusive_segment_incident_does_not_propagate() {
        // c2c is a standalone line with no shared segment anywhere in the
        // catalogue (per the 2026-08-29 line-coverage gap analysis and
        // lines/c2c.toml's own comment) - only the exclusive-segment
        // assertion is required here, no shared-trunk test.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CC-1",
            "Signal failure at Basildon",
            "Signal failure causing delays to c2c services.",
            &["CC"],
            &["BSO"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["c2c".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn merseyrail_northern_kirkby_branch_incident_does_not_propagate() {
        // The Kirkby/Headbolt Lane branch (merseyrail-northern-kirkby) is
        // exclusive to merseyrail-northern.toml - it doesn't touch the
        // Southport or Ormskirk branches, nor (per that file's own
        // central-Liverpool research comment, honored by
        // merseyrail-wirral.toml) the Wirral Line. See
        // `merseyrail_central_liverpool_incident_matches_by_station_not_shared_segment`
        // below for the complementary "station overlap, not shared segment"
        // test at Liverpool Central.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "ME-1",
            "Signal failure at Fazakerley",
            "Signal failure causing delays to Merseyrail Northern Line services.",
            &["ME"],
            &["FAZ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["merseyrail-northern".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn merseyrail_wirral_new_brighton_branch_incident_does_not_propagate() {
        // The New Brighton branch (merseyrail-wirral-new-brighton) is
        // exclusive to merseyrail-wirral.toml - it doesn't touch the West
        // Kirby, Chester or Ellesmere Port branches, nor does it touch
        // merseyrail-northern.toml at all (that file has no stations on the
        // Wirral peninsula).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "ME-2",
            "Signal failure at Wallasey Grove Road",
            "Signal failure causing delays to Merseyrail Wirral Line services.",
            &["ME"],
            &["WLG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["merseyrail-wirral".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn merseyrail_central_liverpool_incident_matches_by_station_not_shared_segment() {
        // Liverpool Central sits on both merseyrail-northern.toml and
        // merseyrail-wirral.toml, but the two files deliberately don't
        // share a segment name there (see merseyrail-northern.toml's own
        // central-Liverpool research comment, honored unchanged by
        // merseyrail-wirral.toml): the Northern Line's Link tunnel and the
        // Wirral Line's Loop tunnel are physically distinct, meeting only
        // at the station buildings. An incident there should therefore
        // match both lines individually by station, each classified
        // against its own (exclusive) segment - mirrors
        // `chiltern_banbury_incident_matches_by_station_not_shared_segment`.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "ME-3",
            "Points failure at Liverpool Central",
            "Points failure causing delays to Merseyrail services.",
            &[],
            &["LVC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("merseyrail-northern"));
        assert!(matched_ids.contains("merseyrail-wirral"));
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment (station overlap, not segment-sharing)", m.line.id);
        }
    }
    #[test]
    fn west_anglia_exclusive_segment_incident_does_not_propagate() {
        // Newport is on `waml-mainline` (Elsenham-Cambridge), well beyond
        // Stansted Mountfitchet where greater-anglia-stansted-express.toml
        // (Task 2.3) diverges onto its own airport branch, and beyond
        // Cambridge is the only other overlap this line has with any other
        // committed line (see the next test). No other `lines/*.toml` file
        // touches this station, so this should stay scoped to
        // greater-anglia-west-anglia only.
        //
        // NOTE: this test used to use Bishop's Stortford (BIS), but that
        // station is on `waml-trunk-london`, which greater-anglia-
        // stansted-express.toml now genuinely shares (see that file's
        // segment-decision comment and the
        // stansted_express_shared_trunk_incident_propagates test below) — an
        // incident there now correctly escalates to SharedSegment and
        // matches both lines, so it's no longer a valid "stays exclusive"
        // example.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-3",
            "Signal failure at Newport",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["NWE"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["greater-anglia-west-anglia".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn stansted_express_shared_trunk_incident_propagates() {
        // Tottenham Hale is on `waml-trunk-london`, genuinely shared between
        // greater-anglia-west-anglia.toml (Task 2.2) and
        // greater-anglia-stansted-express.toml (Task 2.3) per the latter's
        // segment-decision comment (both routes run over the same physical
        // West Anglia Main Line tracks from Liverpool Street through
        // Stansted Mountfitchet). An incident here should propagate to both
        // lines as SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-5",
            "Signal failure at Tottenham Hale",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["TOM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-west-anglia".to_string(), "greater-anglia-stansted-express".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    #[test]
    fn stansted_express_airport_is_station_overlap_only_with_xc_stansted() {
        // Stansted Airport (SSD) is the terminus of
        // greater-anglia-stansted-express.toml's own exclusive
        // `stansted-express-branch` segment, but is also the terminus of
        // xc-stansted.toml's whole-route `xc-stansted` segment (CrossCountry's
        // Birmingham-Stansted service, approaching via a different leg of the
        // triangular junction north of Stansted Mountfitchet — see the
        // segment-decision comment in greater-anglia-stansted-express.toml).
        // The two files deliberately do NOT share a segment name here, so an
        // incident should match both lines independently, each still
        // classified as ExclusiveSegment rather than escalating to
        // SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-6",
            "Points failure at Stansted Airport",
            "Points failure causing delays.",
            &["LE"],
            &["SSD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-stansted-express".to_string(), "xc-stansted".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn west_anglia_cambridge_is_station_overlap_only_with_xc_stansted() {
        // Cambridge (CBG) is on greater-anglia-west-anglia.toml's
        // `waml-mainline` segment, xc-stansted.toml's `xc-stansted` segment
        // (CrossCountry's Birmingham-Stansted service also calls there) and,
        // since Task 2.6, greater-anglia-norfolk-branches.toml's own
        // `breckland-line` segment (the Breckland Line's Cambridge terminus,
        // reached via an entirely different physical corridor — Ely and
        // Cambridge North, not Elsenham/Audley End — that only converges
        // with the other two at this station). None of the three files
        // share a segment name for this station (see
        // greater-anglia-west-anglia.toml's and
        // greater-anglia-norfolk-branches.toml's decision comments —
        // reusing another file's segment name here would incorrectly mark
        // its whole trunk as shared with this line), so an incident here
        // should match all three lines independently, each still classified
        // as ExclusiveSegment rather than escalating to SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("LE-4", "Points failure at Cambridge", "Points failure causing delays.", &["LE"], &["CBG"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-west-anglia".to_string(),
                "xc-stansted".to_string(),
                "greater-anglia-norfolk-branches".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn essex_branches_witham_is_station_overlap_only_with_main_line() {
        // Witham is on both greater-anglia-main-line.toml's `geml-mainline`
        // segment and greater-anglia-essex-branches.toml's own
        // `braintree-branch` segment (the Braintree branch's real junction).
        // Per that file's segment-decision note, the two files deliberately
        // do NOT share a segment name here — reusing `geml-mainline` verbatim
        // would (confirmed empirically while drafting that file) incorrectly
        // reclassify unrelated far-flung `geml-mainline` stations (e.g. Diss,
        // Norwich) as shared trunk too, since SegmentRegistry::is_shared marks
        // a segment name shared globally, not per overlapping station. So an
        // incident here should match both lines independently, each still
        // classified as ExclusiveSegment rather than escalating to
        // SharedSegment — mirroring the Romford/Shenfield precedent in
        // `elizabeth_branch_incident_stays_on_its_branch` above.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-7",
            "Points failure at Witham",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["WTM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-main-line".to_string(), "greater-anglia-essex-branches".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn essex_branches_colchester_is_station_overlap_only_with_main_line() {
        // Colchester is on both greater-anglia-main-line.toml's
        // `geml-mainline` segment and greater-anglia-essex-branches.toml's
        // own `sunshine-coast-main` segment (the Sunshine Coast line's real
        // junction). Same reasoning and same non-sharing decision as Witham
        // above: station-level overlap only, each line classified
        // independently as ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-9",
            "Points failure at Colchester",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["COL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-main-line".to_string(), "greater-anglia-essex-branches".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn essex_branches_exclusive_segment_incident_does_not_propagate() {
        // Southminster is on `crouch-valley-line`, exclusive to
        // greater-anglia-essex-branches.toml. Per that file's Southminster-
        // branch deviation note: the branch's real, verified junction is
        // Wickford on the Shenfield-Southend line, two hops from the GEML
        // via a line not covered by any file in this catalogue — so unlike
        // the Braintree/Sunshine Coast branches above, this segment is not
        // tagged as shared with greater-anglia-main-line or any other line.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-8",
            "Signal failure at Southminster",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["SMN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["greater-anglia-essex-branches".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn suffolk_branches_exclusive_segment_incident_does_not_propagate() {
        // Sudbury is the terminus of `gainsborough-line`, exclusive to
        // greater-anglia-suffolk-branches.toml. It isn't a junction or
        // overlap point for any other committed line, so an incident here
        // should stay scoped to greater-anglia-suffolk-branches only.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-10",
            "Signal failure at Sudbury",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["SUY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["greater-anglia-suffolk-branches".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn suffolk_branches_marks_tey_is_station_overlap_only_with_main_line() {
        // Marks Tey is on both greater-anglia-main-line.toml's
        // `geml-mainline` segment and greater-anglia-suffolk-branches.toml's
        // own `gainsborough-line` segment (the Sudbury branch's real
        // junction). Per that file's segment-decision note (mirroring
        // Task 2.4's Witham/Colchester precedent), the two files
        // deliberately do NOT share a segment name here — reusing
        // `geml-mainline` verbatim would incorrectly reclassify unrelated
        // far-flung `geml-mainline` stations (e.g. Diss, Norwich) as shared
        // trunk too, since SegmentRegistry::is_shared marks a segment name
        // shared globally, not per overlapping station. So an incident here
        // should match both lines independently, each still classified as
        // ExclusiveSegment rather than escalating to SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-11",
            "Points failure at Marks Tey",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["MKT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-main-line".to_string(), "greater-anglia-suffolk-branches".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn suffolk_branches_ipswich_is_station_overlap_only_with_main_line() {
        // Ipswich is on both greater-anglia-main-line.toml's `geml-mainline`
        // segment and greater-anglia-suffolk-branches.toml's own
        // `felixstowe-branch` segment (where Felixstowe branch passenger
        // services originate; the branch's true physical fork is one stop
        // further out at Westerfield). Same non-sharing decision as Marks
        // Tey above: station-level overlap only, each line classified
        // independently as ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-12",
            "Points failure at Ipswich",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["IPS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-main-line".to_string(), "greater-anglia-suffolk-branches".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn suffolk_branches_manningtree_is_station_overlap_only_with_main_line() {
        // Manningtree is on both greater-anglia-main-line.toml's
        // `geml-mainline` segment and greater-anglia-suffolk-branches.toml's
        // own `mayflower-line` segment (the Mayflower line's real junction).
        // Same non-sharing decision as Marks Tey and Ipswich above:
        // station-level overlap only, each line classified independently as
        // ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-13",
            "Points failure at Manningtree",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["MNG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-main-line".to_string(), "greater-anglia-suffolk-branches".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn norfolk_branches_exclusive_segment_incident_does_not_propagate() {
        // Sheringham is the terminus of `bittern-line`, exclusive to
        // greater-anglia-norfolk-branches.toml. It isn't a junction or
        // overlap point for any other committed line, so an incident here
        // should stay scoped to greater-anglia-norfolk-branches only.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-14",
            "Signal failure at Sheringham",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["SHM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["greater-anglia-norfolk-branches".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn norfolk_branches_great_yarmouth_exclusive_segment_incident_does_not_propagate() {
        // Great Yarmouth (the Acle route's terminus, and also the physical
        // terminus of the separate, much lower-frequency Berney Arms route —
        // see greater-anglia-norfolk-branches.toml's Wherry Lines segment
        // note for why GYM is listed once, under `wherry-acle-branch`) isn't
        // a junction or overlap point for any other committed line, so an
        // incident here should stay scoped to greater-anglia-norfolk-branches
        // only.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-15",
            "Signal failure at Great Yarmouth",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["GYM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["greater-anglia-norfolk-branches".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn norfolk_branches_norwich_is_station_overlap_only_with_main_line() {
        // Norwich is on both greater-anglia-main-line.toml's `geml-mainline`
        // segment (as GEML's terminus) and
        // greater-anglia-norfolk-branches.toml's own
        // `norfolk-branches-norwich` segment (the shared origin of the
        // Bittern, Wherry and Breckland lines). Per that file's
        // segment-decision note (mirroring Task 2.4's Witham/Colchester and
        // Task 2.5's Marks Tey/Ipswich/Manningtree precedent — and a
        // deliberate departure from this task's own brief, which suggested a
        // SharedSegment-asserting test here), the two files deliberately do
        // NOT share a segment name — reusing `geml-mainline` verbatim would
        // incorrectly reclassify unrelated far-flung `geml-mainline`
        // stations (e.g. Diss, Ingatestone) as shared trunk too, since
        // SegmentRegistry::is_shared marks a segment name shared globally,
        // not per overlapping station, and there is no track beyond Norwich
        // that GEML and these three branches jointly occupy. So an incident
        // here should match both lines independently, each still classified
        // as ExclusiveSegment rather than escalating to SharedSegment.
        //
        // Norwich is also emr-regional.toml's own terminus (its own
        // `emr-regional-east` segment, exclusive catalogue-wide, merged
        // separately from this batch) -- a third independent ExclusiveSegment
        // match by the same station-overlap pattern.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-16",
            "Points failure at Norwich",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["NRW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "greater-anglia-norfolk-branches".to_string(),
                "emr-regional".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn norfolk_branches_ely_is_station_overlap_only_with_xc_stansted() {
        // Ely is on both greater-anglia-norfolk-branches.toml's own
        // `breckland-line` segment (the Breckland Line's route to Cambridge)
        // and xc-stansted.toml's whole-route `xc-stansted` segment
        // (CrossCountry's Birmingham-Stansted Airport route also approaches
        // Cambridge via Peterborough and Ely). This overlap wasn't
        // previously exercised by any regression test, since no other
        // committed line touched Ely before this file existed. The two
        // files deliberately do NOT share a segment name here (see
        // greater-anglia-norfolk-branches.toml's Ely/Cambridge decision
        // note — reusing `xc-stansted` verbatim would incorrectly mark
        // xc-stansted.toml's entire Midlands trunk as shared with this
        // line), so an incident here should match both lines independently,
        // each still classified as ExclusiveSegment rather than escalating
        // to SharedSegment.
        //
        // Ely is also emr-regional.toml's own `emr-regional-east` segment
        // (exclusive catalogue-wide, merged separately from this batch) --
        // a third independent ExclusiveSegment match by the same
        // station-overlap pattern.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-17",
            "Points failure at Ely",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["ELY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-norfolk-branches".to_string(),
                "xc-stansted".to_string(),
                "emr-regional".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    #[test]
    fn wcml_birmingham_exclusive_segment_incident_does_not_propagate() {
        // `lnwr-birmingham-crewe.toml` (added after this test was first
        // written, Task 1.8) also calls at Birmingham International, on its
        // own exclusive `lnwr-birmingham` segment -- station-level overlap
        // with the Avanti branch, same "overlap is fine, segment-sharing is
        // a deliberate choice" precedent already exercised elsewhere in this
        // file (e.g. `xc_hub_incident_propagates_to_every_cross_country_arm`).
        // It's a real second line affected by this incident, still
        // ExclusiveSegment (different segment name, no sharing).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-1",
            "Points failure at Birmingham International",
            "Points failure causing delays to services at Birmingham International.",
            &["VT"],
            &["BHI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["wcml-birmingham".to_string(), "lnwr-birmingham-crewe".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    #[test]
    fn wcml_birmingham_shared_trunk_incident_propagates_to_wcml_spine() {
        // Rugby is the diverging junction: `wcml-birmingham.toml` reuses
        // `west-coast-main-line.toml`'s own `wcml-midlands` segment tag there
        // (see that file's comment), so an incident at Rugby should be a
        // SharedSegment match for both lines, not exclusive to either.
        //
        // `wcml-manchester.toml` (added after this test was first written)
        // also reuses the same `wcml-midlands` tag at Rugby -- both of its
        // branches (via Stoke-on-Trent and via Crewe/Wilmslow) travel over
        // this same shared spine before diverging further north, so it's a
        // real third line affected by this incident, all three SharedSegment.
        //
        // `wcml-liverpool.toml` (added after this test was first written)
        // also reuses `wcml-midlands` at Rugby -- it doesn't diverge from
        // the spine until Crewe, further north -- so it's a real fourth
        // line affected by this incident, still SharedSegment.
        //
        // `wcml-north-wales.toml` (added after this test was first written)
        // also reuses `wcml-midlands` at Rugby -- like `wcml-liverpool.toml`
        // it doesn't diverge from the spine until Crewe -- so it's a real
        // fifth line affected by this incident, still SharedSegment.
        //
        // `lnwr-birmingham-crewe.toml` (added after this test was first
        // written, Task 1.8) also calls at Rugby -- it's the genuine physical
        // reconvergence point between that file's two internal branches
        // (Northampton Loop and Trent Valley Line), tagged there with its
        // own exclusive `lnwr-rugby` segment, deliberately NOT sharing
        // `wcml-midlands` (same station-overlap-not-segment-sharing
        // precedent `xc-manchester.toml` already set, per that task's own
        // brief). It's a real sixth line affected by this incident, but
        // ExclusiveSegment rather than SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-2",
            "Signal failure at Rugby",
            "Signal failure causing delays to services at Rugby.",
            &["VT"],
            &["RUG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "wcml".to_string(),
                "wcml-birmingham".to_string(),
                "wcml-manchester".to_string(),
                "wcml-liverpool".to_string(),
                "wcml-north-wales".to_string(),
                "lnwr-birmingham-crewe".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "lnwr-birmingham-crewe" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
            } else {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    #[test]
    fn wcml_manchester_exclusive_segment_incident_does_not_propagate() {
        // Stoke-on-Trent is on the exclusive `wcml-manchester-stoke` branch
        // segment, not shared with any other line's segment tag.
        //
        // Uses SOT, Stoke-on-Trent's real CRS code (confirmed via Wikipedia)
        // -- this file originally had it wrong as "STO", which is actually
        // South Tottenham's own real code and collided with
        // overground-suffragette.toml once that file merged. Fixed at the
        // data level (lines/wcml-manchester.toml), not just here.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-5",
            "Points failure at Stoke-on-Trent",
            "Points failure causing delays to services at Stoke-on-Trent.",
            &["VT"],
            &["SOT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["wcml-manchester".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn wcml_liverpool_exclusive_segment_incident_does_not_propagate() {
        // Runcorn is on the exclusive `wcml-liverpool-branch` segment,
        // starting immediately after the Crewe junction (per the
        // shared-trunk rule of thumb) -- not shared with any other line's
        // segment tag, even though `wcml-manchester.toml` also diverges at
        // Crewe (onto a different physical branch, via Wilmslow).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-6",
            "Overhead line damage at Runcorn",
            "Overhead line damage causing delays to services at Runcorn.",
            &["VT"],
            &["RUN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["wcml-liverpool".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn wcml_north_wales_exclusive_segment_incident_does_not_propagate() {
        // Rhyl is on the exclusive `wcml-north-wales-branch` segment,
        // starting immediately after the Crewe junction (per the
        // shared-trunk rule of thumb) -- not shared with any other line's
        // segment tag, even though `wcml-manchester.toml` and
        // `wcml-liverpool.toml` also diverge at Crewe (onto different
        // physical branches, via Wilmslow and Runcorn respectively).
        //
        // Rhyl is also tfw-north-wales-coast.toml's own station (merged
        // separately, Batch 11), on its exclusive `tfw-north-wales-coast`
        // segment -- station-level overlap, distinct segment names, both
        // stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-7",
            "Overhead line damage at Rhyl",
            "Overhead line damage causing delays to services at Rhyl.",
            &["VT"],
            &["RHL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["wcml-north-wales".to_string(), "tfw-north-wales-coast".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    #[test]
    fn wmr_snow_hill_dorridge_branch_exclusive_segment_incident_does_not_propagate() {
        // Dorridge is on the exclusive `wmr-snow-hill-dorridge` segment,
        // starting after the Tyseley junction (per the shared-trunk rule of
        // thumb) -- this line has no meaningful overlap with any existing
        // WCML/XC file (Snow Hill/Moor Street, not Birmingham New Street).
        //
        // Dorridge is also chiltern-main-line.toml's own station (merged
        // separately, after this test was first written), on its exclusive
        // `chiltern-birmingham-approach` segment -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-1",
            "Signal failure at Dorridge",
            "Signal failure causing delays to services at Dorridge.",
            &["LM"],
            &["DDG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["wmr-snow-hill".to_string(), "chiltern-main-line".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    #[test]
    fn wmr_snow_hill_stratford_branch_exclusive_segment_incident_does_not_propagate() {
        // Stratford-upon-Avon is on the exclusive `wmr-snow-hill-stratford`
        // segment (the North Warwickshire Line), starting after the same
        // Tyseley junction as the Dorridge branch above, but tagged with a
        // distinct segment name since it's a different physical branch.
        //
        // Stratford-upon-Avon is also chiltern-main-line.toml's own terminus
        // (merged separately, after this test was first written), on its
        // exclusive `chiltern-stratford-branch` segment -- station-level
        // overlap, distinct segment names, both stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-2",
            "Points failure at Stratford-upon-Avon",
            "Points failure causing delays to services at Stratford-upon-Avon.",
            &["LM"],
            &["SAV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["wmr-snow-hill".to_string(), "chiltern-main-line".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    #[test]
    fn wmr_cross_city_redditch_branch_exclusive_segment_incident_does_not_propagate() {
        // Redditch is on the exclusive `wmr-cross-city-redditch` segment,
        // starting after the Barnt Green junction (per the shared-trunk rule
        // of thumb) -- this line's only real station-level overlaps with
        // other catalogue files are at Lichfield Trent Valley, University and
        // Birmingham New Street, all on the trunk, and none of those are
        // shared-segment (see `wmr-cross-city.toml`'s own comment) -- so
        // there's no shared-segment counterpart to assert for this line.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-3",
            "Signal failure at Redditch",
            "Signal failure causing delays to services at Redditch.",
            &["LM"],
            &["RDC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["wmr-cross-city".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn wmr_cross_city_bromsgrove_branch_exclusive_segment_incident_does_not_propagate() {
        // Bromsgrove is on the exclusive `wmr-cross-city-bromsgrove` segment,
        // starting after the same Barnt Green junction as the Redditch branch
        // above, but tagged with a distinct segment name since it's a
        // different physical branch (added as a second southern terminus once
        // electrification reached it in 2018).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-4",
            "Points failure at Bromsgrove",
            "Points failure causing delays to services at Bromsgrove.",
            &["LM"],
            &["BMV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["wmr-cross-city".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lnwr_northampton_shared_segment_incident_propagates_to_both_lnwr_lines() {
        // Task 1.8 resolved the WOL/NMP question `lnwr-euston-commuter.toml`
        // (Task 1.7) deliberately left open: Northampton is on the
        // `lnwr-northampton` segment, which both LNWR files now use, because
        // Task 1.8's research found this isn't really two distinct
        // real-world workings -- every LNWR train calling at Northampton
        // continues on to Birmingham New Street (see
        // `lines/lnwr-birmingham-crewe.toml`'s own comment for the full
        // sourcing). This test replaces the old
        // `lnwr_euston_commuter_exclusive_segment_incident_does_not_propagate`
        // test, which asserted ExclusiveSegment here before that resolution.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-5",
            "Signal failure at Northampton",
            "Signal failure causing delays to services at Northampton.",
            &["LM"],
            &["NMP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lnwr-euston-commuter".to_string(), "lnwr-birmingham-crewe".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    #[test]
    fn lnwr_euston_trunk_shared_segment_incident_propagates_to_both_lnwr_lines() {
        // Leighton Buzzard is on `lnwr-euston-trunk`, the confirmed
        // LNWR-internal shared-trunk segment (Euston through Milton Keynes
        // Central) that `lnwr-birmingham-crewe.toml` (Task 1.8) reuses
        // verbatim from `lnwr-euston-commuter.toml` (Task 1.7) -- see that
        // file's own comment for the full derivation. Leighton Buzzard
        // appears in no other catalogued line, so this is a clean two-line
        // assertion, mirroring `swr_shared_trunk_incident_propagates` and
        // `xc_hub_incident_propagates_to_every_cross_country_arm`.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-6",
            "Overhead line damage at Leighton Buzzard",
            "Overhead line damage causing delays to services at Leighton Buzzard.",
            &["LM"],
            &["LBZ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lnwr-euston-commuter".to_string(), "lnwr-birmingham-crewe".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    #[test]
    fn lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate() {
        // Canley is on the exclusive `lnwr-birmingham` segment (the
        // Birmingham branch, beyond the Rugby reconvergence point) -- not
        // shared with any other catalogued line's segment tag, and not
        // otherwise a station on any other line in the catalogue.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-7",
            "Signal failure at Canley",
            "Signal failure causing delays to services at Canley.",
            &["LM"],
            &["CNL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["lnwr-birmingham-crewe".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `gwr-main-line`'s Bristol-bound exclusive segment starts at Chippenham
    // (CPM), the first station beyond Swindon reached only by GWML-via-Bath
    // services (South Wales Main Line diverges at Wootton Bassett Junction,
    // just west of Swindon), mirroring
    // `swr_exclusive_segment_incident_does_not_propagate`. See
    // `gwr_trunk_paddington_incident_propagates_to_cotswold` below for the
    // shared-trunk case, now that `gwr-cotswold` (Task 4.2) also shares
    // `gwr-trunk-paddington`.
    #[test]
    fn gwr_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-1", "Points failure at Chippenham", "Points failure causing delays at Chippenham.", &["GW"], &["CPM"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-main-line".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `gwr-cotswold`'s (Task 4.2) own exclusive segment starts at Oxford
    // (OXF), the first station beyond Didcot reached only by Cotswold Line
    // services (South Wales/Bristol-bound gwr-main-line services continue
    // west towards Swindon at Didcot instead). Mirrors
    // `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn gwr_cotswold_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-2", "Signal failure at Moreton-in-Marsh", "Signal failure causing delays at Moreton-in-Marsh.", &["GW"], &["MIM"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-cotswold".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `gwr-main-line` and `gwr-cotswold` both share the `gwr-trunk-paddington`
    // segment (PAD/RDG/DID) established by Task 4.1 and reused verbatim by
    // Task 4.2. An incident at Didcot (a station on that shared segment)
    // should propagate to both lines as a shared-trunk event. Mirrors
    // `swr_shared_trunk_incident_propagates`'s shape.
    //
    // Task 4.5 (`gwr-thames-valley`) also has a DID station, but on its own
    // `gwr-thames-valley` segment (deliberately not sharing
    // `gwr-trunk-paddington` — see that file's own segment-naming comment),
    // so it's excluded from the SharedSegment check below even though its ID
    // also starts with "gwr-".
    #[test]
    fn gwr_trunk_paddington_incident_propagates_to_cotswold() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-3", "Signal failure at Didcot Parkway", "Signal failure causing delays to GWR services.", &["GW"], &["DID"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("gwr-main-line"));
        assert!(matched_ids.contains("gwr-cotswold"));
        for m in &matches {
            if m.line.id.starts_with("gwr-") && m.line.id != "gwr-thames-valley" {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    // `gwr-south-wales`'s (Task 4.3) own exclusive segment covers Bristol
    // Parkway through Swansea. Bridgend (BGN) is not shared with any other
    // line's own station list in this catalogue, so this is a clean
    // ExclusiveSegment case, mirroring
    // `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_cotswold_exclusive_segment_incident_does_not_propagate`. See
    // `gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line`
    // below for the deliberately-not-shared overlap case at Newport/Cardiff
    // (task-4.3-brief.md's plan-mandated "don't force a shared segment"
    // decision), and `gwr_trunk_paddington_incident_propagates_to_south_wales`
    // for the shared-trunk case.
    #[test]
    fn gwr_south_wales_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-4", "Overhead line damage at Bridgend", "Overhead line damage causing delays at Bridgend.", &["GW"], &["BGN"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-south-wales".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `gwr-main-line`, `gwr-cotswold` and `gwr-south-wales` all share the
    // `gwr-trunk-paddington` segment, but only at the stations each file
    // actually lists on it: gwr-cotswold.toml stops at DID (Cotswold
    // services diverge onto the Cherwell Valley line there, before Swindon),
    // while gwr-main-line.toml and gwr-south-wales.toml both continue
    // through SWI. Didcot Parkway (DID) is therefore the one station all
    // three files genuinely share, so an incident there should propagate to
    // all three as a shared-trunk event. Mirrors
    // `swr_shared_trunk_incident_propagates`'s full-set-assertion shape, now
    // extended to a third sibling per task-4.3-brief.md's test requirement
    // #2 (that requirement names Swindon as the example station, but Swindon
    // is not actually on gwr-cotswold.toml's own station list — see that
    // file's own segment-naming comment — so Didcot is used here instead to
    // get a real three-way match rather than a two-way one).
    //
    // Task 4.5 (`gwr-thames-valley`) also stops at DID, but on its own
    // exclusive `gwr-thames-valley` segment (deliberately not sharing
    // `gwr-trunk-paddington` — see that file's own segment-naming comment),
    // so it's included in the matched set (a real station overlap) but stays
    // ExclusiveSegment rather than SharedSegment.
    #[test]
    fn gwr_trunk_paddington_incident_propagates_to_south_wales() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-5", "Signal failure at Didcot Parkway", "Signal failure causing delays to GWR services.", &["GW"], &["DID"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-main-line".to_string(),
                "gwr-cotswold".to_string(),
                "gwr-south-wales".to_string(),
                "gwr-thames-valley".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "gwr-thames-valley" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment, "gwr-thames-valley should stay ExclusiveSegment");
            } else {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    // task-4.3-brief.md's plan-mandated regression guard: Cardiff Central is
    // a real station overlap between `gwr-south-wales` (this task) and
    // `xc-cardiff.toml` (already committed) — both lines call there, but via
    // physically different corridors for most of their length (South Wales
    // Main Line via Bristol Parkway/Severn Tunnel vs. xc-cardiff's Gloucester
    // to Newport Line via Chepstow), and per this task's file-scope
    // restriction (lines/gwr-south-wales.toml + matcher.rs only, not
    // xc-cardiff.toml) neither file reuses the other's segment name for
    // NWP/CDF. So an incident at Cardiff Central should match BOTH lines
    // (real station overlap — each notified about an incident at "their"
    // station) but EACH must stay `MatchScope::ExclusiveSegment` for its own
    // segment, never `SharedSegment` — confirming the two lines' overlap
    // here stays a station-level thing, not a segment-level one. See
    // gwr-south-wales.toml's own segment-naming comment for the research
    // this decision is based on (a genuine, not just assumed, finding that
    // physical track sharing exists further up the corridor at Severn Tunnel
    // Junction, deliberately not modelled as a shared segment given this
    // task's file-scope limits).
    #[test]
    fn gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line() {
        // Cardiff Central is also both tfw-valley-lines-north.toml's and
        // tfw-valley-lines-south.toml's own terminus (merged separately,
        // Batch 11), tagged on both sides with their genuinely shared
        // `tfw-valley-cardiff-hub` segment -- those two resolve
        // SharedSegment *with each other*, while gwr-south-wales/xc-cardiff
        // stay ExclusiveSegment on their own distinct segment names, same
        // station-overlap-only pattern as this test already established.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-6", "Points failure at Cardiff Central", "Points failure causing delays at Cardiff Central.", &["GW"], &["CDF"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-south-wales".to_string(),
                "xc-cardiff".to_string(),
                "tfw-valley-lines-north".to_string(),
                "tfw-valley-lines-south".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "tfw-valley-lines-north" | "tfw-valley-lines-south" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // Task 4.4 split `gwr-west-of-england` (Reading-Taunton line) into its own
    // file. Originally its exclusive segment (`gwr-west-of-england`) covered
    // Newbury through Castle Cary with no *cross-file segment-name* sharing
    // at all. Task 4.6 (gwr-bristol-suburban.toml) found genuine physical
    // track sharing at Westbury/Castle Cary, but an early draft of that fix
    // reused the whole `gwr-west-of-england` segment name (including Newbury,
    // which gwr-bristol-suburban's own service never reaches, and Frome/
    // Bruton, which are gwr-bristol-suburban's own exclusive territory) —
    // wrong, since segment sharing is tracked per segment *name*, not per
    // individual station, so that draft mislabelled all three as "shared"
    // catalogue-wide. The final-review fix wave introduced a new, narrower
    // segment name, `gwr-westbury-castle-cary`, covering ONLY Westbury (WSB)
    // and Castle Cary (CLC) — the two stations both files' own cited sources
    // actually name as shared. Newbury (NBY) reverts to being a genuinely
    // exclusive station on this line's own `gwr-west-of-england` segment
    // (gwr-bristol-suburban.toml never reaches it), and Frome/Bruton move
    // onto gwr-bristol-suburban.toml's own `gwr-bristol-weymouth` segment.
    // See `gwr_westbury_castle_cary_trunk_incident_propagates_to_bristol_
    // suburban` below for the corrected shared-segment case, and
    // `gwr_thames_valley_station_overlap_with_gwr_west_of_england_stays_
    // exclusive_each_line` below for the Newbury case, now back to
    // ExclusiveSegment on both sides (a real station overlap, not a segment
    // share).

    // Task 4.4's second file, `gwr-cornish-main-line`, picks up its own
    // exclusive segment (also named `gwr-cornish-main-line`) at Liskeard,
    // the first station west of Plymouth not already claimed by
    // cross-country.toml's `xc-south-west` segment. Truro is deep inside
    // that exclusive stretch, so this should stay a clean ExclusiveSegment
    // case, mirroring `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_cotswold_exclusive_segment_incident_does_not_propagate` above.
    #[test]
    fn gwr_cornish_main_line_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-8", "Signal failure at Truro", "Signal failure causing delays at Truro.", &["GW"], &["TRU"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-cornish-main-line".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 4.4's research found a genuine multi-station shared trunk with
    // cross-country.toml's own `xc-south-west` segment (Taunton-Exeter St
    // Davids-Newton Abbot-Plymouth, on the Bristol to Exeter line / South
    // Devon Main Line) — unlike gwr-south-wales.toml's single-waypoint
    // overlaps with cross-country.toml/xc-cardiff.toml, which deliberately
    // stayed station-only (see
    // `gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line`
    // above). Exeter St Davids (EXD) is the station both of Task 4.4's own
    // files share with each other (the west-of-england/Cornish Main Line
    // split boundary) *and* with cross-country.toml, so an incident there
    // should propagate to all three as a shared-trunk event, mirroring
    // `swr_shared_trunk_incident_propagates`'s / `xc_hub_incident_propagates_
    // to_every_cross_country_arm`'s full-set-assertion shape.
    #[test]
    fn gwr_trunk_xc_south_west_incident_propagates_across_west_of_england_and_cornish_main_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-9", "Flooding at Exeter St Davids", "Flooding causing delays to GWR and CrossCountry services.", &["GW"], &["EXD"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-west-of-england".to_string(),
                "gwr-cornish-main-line".to_string(),
                "cross-country".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Task 4.5's research found this line's real local/stopping service runs
    // on the physically separate Relief lines (Reading railway station's own
    // Wikipedia article, independently corroborated at Southall), not the
    // express Main lines gwr-trunk-paddington represents — so, unlike its
    // GWR siblings, `gwr-thames-valley` does NOT reuse `gwr-trunk-paddington`
    // anywhere, including at PAD/RDG. The whole line (both the Didcot-Oxford
    // and Reading-Newbury branches) uses one exclusive segment,
    // `gwr-thames-valley`, not shared with any other catalogued line. Culham
    // (CUM), on the Oxford branch, is a clean ExclusiveSegment case, mirroring
    // `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_cotswold_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn gwr_thames_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-10", "Signal failure at Culham", "Signal failure causing delays at Culham.", &["GW"], &["CUM"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-thames-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // task-4.5-brief.md's plan-mandated regression guard: Maidenhead (MAI) is
    // a real station overlap between `gwr-thames-valley` (this task) and
    // elizabeth-line.toml's own `elizabeth-west` segment — both lines call
    // there (different service classes on the same physical station: a 2tph
    // GWR semi-fast continuing past Reading to Didcot/Oxford/Newbury, versus
    // the Elizabeth line's high-frequency metro stopper terminating at
    // Reading) but this task's research deliberately did not force
    // segment-sharing (see gwr-thames-valley.toml's own segment-naming
    // comment). So an incident at Maidenhead should match BOTH lines (real
    // station overlap) but EACH must stay `MatchScope::ExclusiveSegment` for
    // its own segment, never `SharedSegment` — mirrors
    // `gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line`.
    #[test]
    fn gwr_thames_valley_station_overlap_with_elizabeth_west_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-11", "Points failure at Maidenhead", "Points failure causing delays at Maidenhead.", &["GW"], &["MAI"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-thames-valley".to_string(), "elizabeth-line".to_string()]));
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // A second, genuine (not just assumed) overlap this task's own research
    // found beyond what task-4.5-brief.md names: Oxford (OXF) is also
    // gwr-cotswold.toml's own exclusive segment's starting station, and the
    // Didcot-Oxford stretch both lines use is real single-track shared
    // infrastructure (see gwr-thames-valley.toml's own segment-naming
    // comment). Kept as station overlap only for this task's file-scope
    // reasons, so both lines should stay ExclusiveSegment, never
    // SharedSegment, mirroring the Maidenhead/elizabeth-line case above.
    // xc-south-coast.toml also calls at OXF (already documented, by
    // gwr-cotswold.toml's own comment, as a pre-existing station overlap
    // with that line) — included here too, also staying ExclusiveSegment.
    #[test]
    fn gwr_thames_valley_station_overlap_with_gwr_cotswold_stays_exclusive_each_line() {
        // Oxford is also chiltern-aylesbury.toml's own terminus (merged
        // separately, Batch 12), on its exclusive `chiltern-oxford-branch`
        // segment -- a fourth independent ExclusiveSegment match by the same
        // station-overlap pattern the other three already establish.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-12", "Overhead line damage at Oxford", "Overhead line damage causing delays at Oxford.", &["GW"], &["OXF"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-thames-valley".to_string(),
                "gwr-cotswold".to_string(),
                "xc-south-coast".to_string(),
                "chiltern-aylesbury".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // A third genuine overlap this task's own research found: Newbury (NBY)
    // is also gwr-west-of-england.toml's own exclusive segment's starting
    // station, and the Reading-Newbury stretch both lines use (via Southcote
    // Junction) is real shared Berks and Hants line track (see
    // gwr-thames-valley.toml's own segment-naming comment). Kept as station
    // overlap only for this task's file-scope reasons, mirroring the Oxford/
    // gwr-cotswold.toml case above. An earlier draft of gwr-bristol-
    // suburban.toml's own Westbury/Castle Cary fix mistakenly reused the
    // whole `gwr-west-of-england` segment name (not just WSB/CLC), which
    // pulled NBY into SharedSegment status too even though
    // gwr-bristol-suburban's own service never reaches it. The final-review
    // fix wave narrowed that shared segment to a new name,
    // `gwr-westbury-castle-cary` (WSB/CLC only — see
    // `gwr_westbury_castle_cary_trunk_incident_propagates_to_bristol_
    // suburban` below), so NBY is once again a genuinely exclusive station on
    // gwr-west-of-england's own `gwr-west-of-england` segment: both lines
    // should now stay `MatchScope::ExclusiveSegment` for their own segment,
    // confirming this is a real station overlap, not a segment-level share.
    #[test]
    fn gwr_thames_valley_station_overlap_with_gwr_west_of_england_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-13", "Points failure at Newbury", "Points failure causing delays at Newbury.", &["GW"], &["NBY"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-thames-valley".to_string(), "gwr-west-of-england".to_string()]));
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // Task 4.6's own exclusive segment (`gwr-severn-beach`) covers the whole
    // Severn Beach branch — Severn Beach itself (SVB) is not shared with any
    // other catalogued line, so this should stay a clean ExclusiveSegment
    // case, mirroring `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_cotswold_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn gwr_bristol_suburban_severn_beach_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-14", "Trespass incident at Severn Beach", "Trespass incident causing delays at Severn Beach.", &["GW"], &["SVB"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-bristol-suburban".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 4.6's research found a genuine, multi-station shared trunk with
    // gwr-west-of-england.toml between Westbury and Castle Cary — both
    // stations' own Wikipedia pages independently confirm this ("the Reading
    // to Taunton Line and the Heart of Wessex Line... share tracks between
    // Westbury and Castle Cary stations"), the same shape as `xc-south-west`/
    // `gwr-trunk-paddington` earlier in this batch. An earlier draft reused
    // gwr-west-of-england.toml's own `gwr-west-of-england` segment name
    // verbatim for this — wrong, because that also pulled Newbury (not
    // reached by gwr-bristol-suburban.toml's service) and Frome/Bruton
    // (gwr-bristol-suburban.toml's own exclusive territory, not actually
    // shared) into "shared" status. The final-review fix wave introduced a
    // new, narrower segment name, `gwr-westbury-castle-cary`, covering ONLY
    // Westbury (WSB) and Castle Cary (CLC) — the two stations both files'
    // own cited sources actually name as shared. An incident at either
    // should still propagate to both lines as a shared-trunk event, mirroring
    // `swr_shared_trunk_incident_propagates`'s / `gwr_trunk_xc_south_west_
    // incident_propagates_across_west_of_england_and_cornish_main_line`'s
    // shape.
    #[test]
    fn gwr_westbury_castle_cary_trunk_incident_propagates_to_bristol_suburban() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-15", "Points failure at Westbury", "Points failure causing delays at Westbury.", &["GW"], &["WSB"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["gwr-west-of-england".to_string(), "gwr-bristol-suburban".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // A second, genuine (not just assumed) overlap this task's own research
    // found beyond the Westbury/Castle Cary one the brief itself names: Bath
    // Spa (BTH) and Bristol Temple Meads (BRI) are also gwr-main-line.toml's
    // own exclusive-segment stations (its `gwr-main-line` segment). This
    // task's research confirms genuine physical track sharing (there is only
    // one railway between Bristol Temple Meads and Bath Spa), but
    // `gwr-main-line` also covers Chippenham (CPM), which this line's own
    // Bristol-Weymouth service does not reach — the same file-scope reason
    // gwr-thames-valley.toml's Oxford/Newbury cases stayed station-overlap
    // only (see `gwr_thames_valley_station_overlap_with_gwr_cotswold_stays_
    // exclusive_each_line` above). So an incident at Bath Spa should match
    // both lines (real station overlap) but each must stay
    // `MatchScope::ExclusiveSegment`, never `SharedSegment`.
    #[test]
    fn gwr_bristol_suburban_station_overlap_with_gwr_main_line_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-16", "Overhead line damage at Bath Spa", "Overhead line damage causing delays at Bath Spa.", &["GW"], &["BTH"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-bristol-suburban".to_string(), "gwr-main-line".to_string()]));
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // A third genuine overlap this task's own research found, caught during
    // review after an earlier draft of gwr-bristol-suburban.toml's own WEY
    // comment wrongly claimed SWR's route "is not otherwise catalogued yet":
    // Weymouth (WEY) is also swr-south-west-main.toml's own terminus (its
    // own exclusive `swr-swml-south` segment). gwr-bristol-suburban's own
    // Bristol-Weymouth service never runs over any of swr-south-west-
    // main.toml's own claimed stations except WEY itself, so this stays
    // station overlap only, not a shared segment — different segment names
    // (`gwr-bristol-weymouth` vs `swr-swml-south`) mean no incorrect
    // `SharedSegment` cross-propagation. Mirrors
    // `gwr_bristol_suburban_station_overlap_with_gwr_main_line_stays_exclusive_each_line`
    // above.
    #[test]
    fn gwr_bristol_suburban_station_overlap_with_swr_south_west_main_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-17", "Flooding at Weymouth", "Flooding causing delays at Weymouth.", &["GW"], &["WEY"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-bristol-suburban".to_string(), "swr-south-west-main".to_string()]));
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // `tfw-cambrian`'s own trunk segment (`tfw-cambrian-trunk`) is still
    // unique to Cambrian -- no other file shares that exact segment name,
    // so an incident away from Shrewsbury only ever matches Cambrian as
    // `MatchScope::ExclusiveSegment`, which is what this test covers.
    // Shrewsbury itself, however, is now a genuine three-way overlap point:
    // `tfw-marches.toml` (Task 11.5) and `tfw-heart-of-wales.toml` both tag
    // SHR with the shared `tfw-heart-of-wales-shrewsbury` segment, while
    // Cambrian keeps SHR on its own exclusive `tfw-cambrian-trunk` segment
    // (the Cambrian Line diverges west immediately, still station-overlap
    // only there -- see `lines/tfw-cambrian.toml`'s own comment). So an
    // incident specifically at Shrewsbury resolves three ways at once: see
    // `shrewsbury_three_way_overlap_resolves_per_line` below for that case.
    #[test]
    fn cambrian_coast_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-1",
            "Signal failure at Barmouth",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["BRM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-cambrian".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn cambrian_aberystwyth_branch_incident_does_not_propagate_to_coast_branch() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-2",
            "Trespass incident at Aberystwyth",
            "Trespass incident causing delays to Transport for Wales services.",
            &["AW"],
            &["AYW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-cambrian".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-heart-of-wales` is standalone on its own southern stretch
    // (Craven Arms southwards) -- `tfw-marches.toml` (Task 11.5) now exists
    // but only shares the Shrewsbury-Craven Arms stretch, not the branch
    // south of Craven Arms towards Llandrindod/Swansea, so an incident well
    // south of Craven Arms should still match only Heart of Wales, as
    // `MatchScope::ExclusiveSegment`. See
    // `heart_of_wales_shrewsbury_shared_trunk_propagates` below for the
    // shared-trunk case Task 11.5 decided on.
    #[test]
    fn heart_of_wales_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-3",
            "Signal failure at Llandrindod",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["LLO"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-heart-of-wales".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-conwy-valley` is, at this point in the line catalogue, a
    // genuinely standalone line: it uses a single whole-line segment name
    // and no other file in the catalogue shares it yet.
    // `tfw-north-wales-coast.toml` (Task 11.4) now exists and also reaches
    // Llandudno Junction, but Task 11.4 deliberately ruled that overlap
    // "station-overlap-only" rather than a genuine shared trunk (the Conwy
    // Valley Line diverges south immediately at the junction, with no track
    // actually shared by both lines' services beyond that one calling
    // point) -- so the two files use distinct segment names there and this
    // remains an exclusive-segment case for Conwy Valley. See
    // `lines/tfw-north-wales-coast.toml`'s comments for the full reasoning,
    // and `llj_station_overlap_matches_both_lines_as_exclusive` below for
    // the assertion that exercises the overlap itself.
    #[test]
    fn conwy_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-4",
            "Signal failure at Betws-y-Coed",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["BYC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-conwy-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-north-wales-coast` (Task 11.4). An incident on a station well
    // away from the Llandudno Junction overlap should match only this line,
    // as `MatchScope::ExclusiveSegment` -- e.g. Rhyl.
    #[test]
    fn north_wales_coast_exclusive_segment_incident_does_not_propagate() {
        // Rhyl is also wcml-north-wales.toml's own station (merged
        // separately, Batch 1), on its exclusive `wcml-north-wales-branch`
        // segment -- station-level overlap, distinct segment names, both
        // stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-1",
            "Signal failure at Rhyl",
            "Signal failure causing delays to services on the North Wales Coast Line.",
            &["AW"],
            &["RHL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-north-wales-coast".to_string(), "wcml-north-wales".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    // Llandudno Junction is on both `tfw-conwy-valley` and
    // `tfw-north-wales-coast`, but Task 11.4 ruled that overlap
    // station-overlap-only rather than a shared trunk (see the comment
    // above `conwy_valley_exclusive_segment_incident_does_not_propagate`
    // and the comments in `lines/tfw-north-wales-coast.toml`). So an
    // incident there should match both lines independently, each still
    // classified as `MatchScope::ExclusiveSegment` (not `SharedSegment` --
    // that scope only applies when a segment name is genuinely shared
    // across line files, which is deliberately not the case here).
    #[test]
    fn llj_station_overlap_matches_both_lines_as_exclusive() {
        // Llandudno Junction is also wcml-north-wales.toml's own station
        // (merged separately, Batch 1), on its exclusive
        // `wcml-north-wales-branch` segment -- a third independent
        // ExclusiveSegment match by the same station-overlap pattern.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-5",
            "Points failure at Llandudno Junction",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["LLJ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tfw-conwy-valley".to_string(),
                "tfw-north-wales-coast".to_string(),
                "wcml-north-wales".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    // `tfw-marches` (Task 11.5). An incident on a station well away from
    // both coordination points (the Shrewsbury-Craven Arms shared trunk
    // with Heart of Wales, and the Chester station-overlap with North
    // Wales Coast) should match only this line, as
    // `MatchScope::ExclusiveSegment` -- e.g. Hereford, which sits on
    // `tfw-marches-south`, a segment no other line in the catalogue uses.
    #[test]
    fn marches_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-6",
            "Signal failure at Hereford",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["HFD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-marches".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Craven Arms is on both `tfw-marches` and `tfw-heart-of-wales`, and
    // Task 11.5 ruled -- after independently verifying the physical
    // track-sharing claim against three separate Wikipedia articles (see
    // the comments above Craven Arms in `lines/tfw-marches.toml`) -- that
    // this is a genuine shared trunk, not mere station overlap: Heart of
    // Wales services physically run over Marches Line metals between
    // Craven Arms and Shrewsbury, calling at the intermediate station
    // Church Stretton along the way. Both files tag Craven Arms (and
    // Church Stretton, and Shrewsbury) with the same segment name,
    // `tfw-heart-of-wales-shrewsbury` (reusing the forward-bet name
    // `tfw-heart-of-wales.toml` left for this file to pick up), so an
    // incident there should propagate to both lines, each classified
    // `MatchScope::SharedSegment`.
    #[test]
    fn heart_of_wales_shrewsbury_shared_trunk_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-7",
            "Points failure at Craven Arms",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CRV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-marches".to_string(), "tfw-heart-of-wales".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Church Stretton (CTT) is an ordinary intermediate stop on the same
    // Shrewsbury-Craven Arms shared trunk exercised above, added to
    // `tfw-heart-of-wales.toml` during the final whole-branch review's fix
    // wave (Important #2) -- previously that file's station list jumped
    // straight from Shrewsbury to Craven Arms, omitting Church Stretton
    // entirely, which silently broke this exact propagation guarantee for
    // an incident reported there specifically (it would have matched
    // `tfw-marches` correctly but had no way to also match
    // `tfw-heart-of-wales`, since CTT wasn't in that file's station list at
    // all). Both files now tag Church Stretton with the same segment name,
    // `tfw-heart-of-wales-shrewsbury` (see the sourcing/decision note above
    // Craven Arms in `lines/tfw-marches.toml`, and the comment above CTT in
    // `lines/tfw-heart-of-wales.toml`), so an incident there should
    // propagate to both lines, each classified `MatchScope::SharedSegment`
    // -- mirroring `heart_of_wales_shrewsbury_shared_trunk_propagates`
    // above exactly, just at a different station on the same shared trunk.
    #[test]
    fn church_stretton_shared_trunk_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-15",
            "Signal failure at Church Stretton",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CTT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-marches".to_string(), "tfw-heart-of-wales".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Chester is on both `tfw-marches` and `tfw-north-wales-coast`, but
    // Task 11.5 ruled that overlap station-overlap-only rather than a
    // shared trunk, mirroring `tfw-north-wales-coast.toml`'s own Llandudno
    // Junction decision against `tfw-conwy-valley.toml`: despite genuine
    // physical track sharing existing at Saltney Junction on the final
    // approach into Chester (see the comment above Chester in
    // `lines/tfw-marches.toml`), `tfw-north-wales-coast.toml` uses one
    // single whole-line segment name for its entire route, so reusing it
    // here would incorrectly mark that line's whole route (Rhyl, Bangor,
    // Holyhead, etc.) as shared with Marches. So the two files use distinct
    // segment names at Chester, and an incident there should match both
    // lines independently, each still classified as
    // `MatchScope::ExclusiveSegment` (not `SharedSegment`).
    #[test]
    fn chester_station_overlap_matches_both_lines_as_exclusive() {
        // Chester is also wcml-north-wales.toml's (Batch 1) and
        // merseyrail-wirral.toml's (Batch 12) own station, each on its own
        // exclusive segment (`wcml-north-wales-branch`,
        // `merseyrail-wirral-chester`) -- two more independent
        // ExclusiveSegment matches by the same station-overlap pattern.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-8",
            "Points failure at Chester",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CTR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tfw-marches".to_string(),
                "tfw-north-wales-coast".to_string(),
                "wcml-north-wales".to_string(),
                "merseyrail-wirral".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    // Shrewsbury is a genuine three-way overlap point, introduced by Task
    // 11.5: `tfw-cambrian.toml` tags SHR with its own exclusive
    // `tfw-cambrian-trunk` segment (the Cambrian Line diverges west
    // immediately -- station-overlap only there, per that file's own
    // comment), while `tfw-marches.toml` and `tfw-heart-of-wales.toml` both
    // tag SHR with the shared `tfw-heart-of-wales-shrewsbury` segment (see
    // the comment above Craven Arms in `lines/tfw-marches.toml` for the
    // shared-trunk sourcing). So a single incident at SHR should resolve
    // differently per line, all at once: Cambrian stays
    // `MatchScope::ExclusiveSegment`, while Marches and Heart of Wales are
    // both `MatchScope::SharedSegment`.
    #[test]
    fn shrewsbury_three_way_overlap_resolves_per_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-9",
            "Signal failure at Shrewsbury",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["SHR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let scopes: HashMap<String, MatchScope> =
            matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
        assert_eq!(
            scopes.keys().cloned().collect::<HashSet<String>>(),
            HashSet::from(["tfw-cambrian".to_string(), "tfw-marches".to_string(), "tfw-heart-of-wales".to_string()])
        );
        assert_eq!(scopes["tfw-cambrian"], MatchScope::ExclusiveSegment);
        assert_eq!(scopes["tfw-marches"], MatchScope::SharedSegment);
        assert_eq!(scopes["tfw-heart-of-wales"], MatchScope::SharedSegment);
    }

    // `tfw-valley-lines-north` (Task 11.6). An incident on a station well
    // into the Rhymney Line's own exclusive corridor (its own segment,
    // `tfw-valley-rhymney`, used by no other branch in this file and no
    // other file in the catalogue) should match only this line, as
    // `MatchScope::ExclusiveSegment` -- e.g. Caerphilly.
    #[test]
    fn valley_lines_north_exclusive_rhymney_segment_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-10",
            "Signal failure at Caerphilly",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CPH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-valley-lines-north".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-valley-lines-north` (Task 11.6) x `tfw-valley-lines-south`
    // (Task 11.7): the Cardiff hub segment-sharing decision. This test
    // supersedes the batch's earlier
    // `valley_lines_north_cardiff_hub_is_exclusive_pending_task_11_7`, which
    // documented the interim state before Task 11.7 existed (back then only
    // one line file used the `tfw-valley-cardiff-hub` segment name, so the
    // registry resolved it as `ExclusiveSegment`). Task 11.7 independently
    // verified genuine same-platform sharing (both files' routes call at
    // Cardiff Central and/or Cardiff Queen Street) and deliberately reused
    // `tfw-valley-lines-north.toml`'s `tfw-valley-cardiff-hub` segment name
    // in `tfw-valley-lines-south.toml` -- see that file's own Cardiff hub
    // segment-sharing decision comment. With two line files now sharing the
    // name, an incident at Cardiff Queen Street correctly propagates to both
    // as `MatchScope::SharedSegment`, mirroring
    // `xc_hub_incident_propagates_to_every_cross_country_arm`.
    // `tfw-valley-lines-north.toml` itself was not edited to make this
    // happen -- only this test (whose docstring always said the outcome was
    // pending Task 11.7) and the new `tfw-valley-lines-south.toml` file.
    #[test]
    fn valley_lines_cardiff_hub_shared_segment_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-11",
            "Points failure at Cardiff Queen Street",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CDQ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-valley-lines-north".to_string(), "tfw-valley-lines-south".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // `tfw-valley-lines-south` (Task 11.7). An incident on a station well
    // into the Coryton Line's own exclusive corridor (its own segment,
    // `tfw-valley-coryton`, used by no other branch in this file and no
    // other file in the catalogue) should match only this line, as
    // `MatchScope::ExclusiveSegment` -- e.g. Birchgrove.
    #[test]
    fn valley_lines_south_exclusive_coryton_segment_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-12",
            "Signal failure at Birchgrove",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["BCG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-valley-lines-south".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-valley-lines-north` (Task 11.6) x `tfw-valley-lines-south`
    // (Task 11.7): the Radyr junction-sharing decision (see
    // `tfw-valley-lines-south.toml`'s own comment). Radyr carries its own
    // dedicated, Radyr-only segment name, `tfw-valley-radyr-junction`,
    // minted in both files (fix round 1: this used to reuse
    // `tfw-valley-lines-north.toml`'s `tfw-valley-taff-trunk` segment name
    // for Radyr alone, which incorrectly made every other station on that
    // segment register as shared too -- see
    // `valley_lines_north_exclusive_pontypridd_segment_does_not_propagate`
    // below for the regression test guarding against that) -- so an
    // incident at Radyr itself should still propagate to both files, both
    // `MatchScope::SharedSegment`.
    #[test]
    fn valley_lines_radyr_junction_shared_segment_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-13",
            "Points failure at Radyr",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["RDR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-valley-lines-north".to_string(), "tfw-valley-lines-south".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // `tfw-valley-lines-north` (Task 11.6), fix round 1 regression test.
    // Pontypridd (PPD) sits on `tfw-valley-taff-trunk`, the segment shared
    // *within this file* by the Merthyr, Aberdare and Rhondda/Treherbert
    // branches -- but it is not, and must never become, shared with
    // `tfw-valley-lines-south.toml`: the City Line (that file) only touches
    // this trunk at Radyr itself, via its own dedicated
    // `tfw-valley-radyr-junction` segment, not at Pontypridd or any of the
    // other five stations on `tfw-valley-taff-trunk` (Cathays, Llandaf,
    // Taffs Well, Treforest, Treforest Estate). This guards against the
    // original Task 11.7 bug, where south's Radyr entry reused
    // `tfw-valley-taff-trunk` verbatim and made `SegmentRegistry` (which
    // indexes sharing by segment-name string across the whole catalogue,
    // not per-station) incorrectly resolve Pontypridd as `SharedSegment`
    // too.
    #[test]
    fn valley_lines_north_exclusive_pontypridd_segment_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-14",
            "Signal failure at Pontypridd",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["PPD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-valley-lines-north".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }
}
