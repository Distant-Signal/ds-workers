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

    // Same file, Task 7.4 (Batch 7): fills the five previously-omitted
    // Nottingham-Grantham intermediate stations named directly in that
    // task's spec (Netherfield & Colwick / NET, Radcliffe-on-Trent / RDF,
    // Aslockton & Whatton / ALK, Elton & Orston / ELO, Bottesford / BTF),
    // now two-source confirmed and inserted at their true geographic
    // position around the pre-existing Bingham (BIN) entry - see the
    // updated comment above `[[stations]] crs = "BIN"` in
    // `lines/emr-rural-branches.toml` for the full sourcing. All five sit
    // on `emr-poacher-skegness`, the same segment as their BIN/GRA
    // neighbours, and (per that file's own Branch 2 ruling) that segment
    // name is deliberately NOT shared with any sibling line's segment name
    // even though genuine Nottingham-Grantham track-sharing exists with
    // `emr-regional` - so unlike
    // `emr_rural_branches_matlock_branch_shared_with_midland_main_line`
    // above, there is no cross-file SharedSegment assertion to add here;
    // see `emr_rural_branches_poacher_line_and_emr_regional_both_match_grantham_without_over_propagating`
    // for why that's already covered at Grantham itself.
    #[test]
    fn emr_rural_branches_poacher_line_infill_stations_present() {
        let lines = load_line("emr-rural-branches");
        let line = lines.get("emr-rural-branches").expect("emr-rural-branches line should exist");
        for crs in ["NET", "RDF", "ALK", "ELO", "BTF"] {
            assert!(line.has_station(crs), "emr-rural-branches should now list {crs}");
        }
    }

    // Same file, same task: an incident at one of the newly-added stations
    // (Bottesford) should behave exactly like the pre-existing Worksop
    // exclusive-segment case above - matches only this bundled line, as
    // ExclusiveSegment on `emr-poacher-skegness`.
    #[test]
    fn emr_rural_branches_bottesford_incident_stays_on_its_own_branch() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("EMRB-5", "Signal failure at Bottesford", "Signal failure causing delays to services at Bottesford.", &["EM"], &["BTF"]);
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

    // Task 2.1 (updated by Task 2.10): `lines/northern-airedale.toml` and
    // `lines/northern-wharfedale.toml` both gained Frizinghall (FZH), which
    // sits between BDQ and SHY on the shared `northern-shipley-trunk`
    // segment (see `northern-airedale.toml`'s "RESOLUTION for Task 2.10"
    // comment, and `northern-wharfedale.toml`'s own "[FILL-IN] resolved
    // (Task 2.10)" comment). Confirms `has_station` picks it up on both
    // files and that an incident there resolves as SharedSegment for both,
    // mirroring the existing SHY-based shared-trunk test above (now that
    // both files list FZH directly, this is a direct station-hit match on
    // each line, not just segment-name propagation).
    #[test]
    fn airedale_and_wharfedale_frizinghall_has_station_and_is_shared_trunk() {
        let lines = load_line("northern-airedale");
        let airedale = lines.get("northern-airedale").expect("northern-airedale should load");
        assert!(airedale.has_station("FZH"), "northern-airedale should now list Frizinghall (FZH)");
        assert_eq!(airedale.segment_for("FZH"), Some("northern-shipley-trunk"));

        let all_lines = load_all_lines();
        let wharfedale = all_lines.get("northern-wharfedale").expect("northern-wharfedale should load");
        assert!(wharfedale.has_station("FZH"), "northern-wharfedale should now list Frizinghall (FZH)");
        assert_eq!(wharfedale.segment_for("FZH"), Some("northern-shipley-trunk"));

        let registry = SegmentRegistry::new(&all_lines);
        assert!(
            registry.is_shared("northern-shipley-trunk"),
            "northern-shipley-trunk should still be a shared segment"
        );
        let inc = incident(
            "NT-8",
            "Signal failure at Frizinghall",
            "Signal failure causing delays to Northern services at Frizinghall.",
            &["NT"],
            &["FZH"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
        assert_eq!(by_id.get("northern-airedale"), Some(&MatchScope::SharedSegment));
        assert_eq!(by_id.get("northern-wharfedale"), Some(&MatchScope::SharedSegment));
    }

    // Task 2.1: SAE (Saltaire), by contrast, is the first station on this
    // file's own exclusive `northern-airedale` segment after the Shipley
    // junction - confirms it doesn't propagate to Wharfedale, mirroring
    // `airedale_exclusive_segment_incident_does_not_propagate` above.
    #[test]
    fn airedale_saltaire_has_station_and_stays_exclusive() {
        let lines = load_line("northern-airedale");
        let airedale = lines.get("northern-airedale").expect("northern-airedale should load");
        assert!(airedale.has_station("SAE"), "northern-airedale should now list Saltaire (SAE)");
        assert_eq!(airedale.segment_for("SAE"), Some("northern-airedale"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-9",
            "Signal failure at Saltaire",
            "Signal failure causing delays on the Airedale Line at Saltaire.",
            &["NT"],
            &["SAE"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-airedale".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
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

    // Task 2.2: `lines/northern-blackpool.toml` gained the full Bolton to
    // Preston (via Chorley) local calling pattern, previously omitted. CRL
    // (Chorley) is strictly between the file's existing BON and PRE entries
    // and inherits the file's own exclusive `northern-blackpool` segment
    // (Salford Crescent to Bolton is run non-stop by this service, so no
    // new station landed on the shared `northern-manchester` segment -
    // see the file's own top-of-file comment). Mirrors
    // `airedale_exclusive_segment_incident_does_not_propagate` above.
    #[test]
    fn northern_blackpool_chorley_has_station_and_stays_exclusive() {
        let lines = load_line("northern-blackpool");
        let blackpool = lines.get("northern-blackpool").expect("northern-blackpool should load");
        assert!(blackpool.has_station("CRL"), "northern-blackpool should now list Chorley (CRL)");
        assert_eq!(blackpool.segment_for("CRL"), Some("northern-blackpool"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-10",
            "Signal failure at Chorley",
            "Signal failure causing delays on the Blackpool Line at Chorley.",
            &["NT"],
            &["CRL"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-blackpool".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
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
        //
        // Aberdeen is also scotrail-aberdeen-inverness.toml's own terminus
        // (merged separately, Batch 10), on its exclusive
        // `scotrail-aberdeen-inverness` segment -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        let inc = incident("LNER-1", "Points failure at Aberdeen", "Points failure causing delays at Aberdeen.", &["GR"], &["ABD"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lner-ecml".to_string(), "scotrail-aberdeen-inverness".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
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
        //
        // Falkirk High is also scotrail-central-belt.toml's own station
        // (merged separately, Batch 10), on its exclusive
        // `scotrail-central-belt` segment -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        let inc = incident(
            "LD-1",
            "Signal failure at Falkirk High",
            "Signal failure causing delays to services at Falkirk High.",
            &["LD"],
            &["FKK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lumo".to_string(), "scotrail-central-belt".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
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
        // Motherwell is also scotrail-glasgow-suburban.toml's own junction
        // (its own `scotrail-glasgow-suburban-argyle-east` segment, merged
        // separately, Batch 10) -- station-level overlap, distinct segment
        // names, both stay ExclusiveSegment.
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
        assert_eq!(
            matched_ids,
            HashSet::from(["tpe-anglo-scottish".to_string(), "scotrail-glasgow-suburban".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    // Task 8.1 (Batch 8): fills genuinely missing intermediate stations
    // that TPE's own May-2026 Anglo-Scottish timetable PDF confirms this
    // route calls at but which the file's prior "principal stations only"
    // listing omitted - St Helens Central (SNH) and Wigan North Western
    // (WGN) on the Liverpool leg, Manchester Oxford Road (MCO) and Bolton
    // (BON) on the Manchester leg, and Carstairs (CRS, literally that
    // three-letter code) - the route's actual Glasgow/Edinburgh split
    // point, previously misattributed to Lockerbie. See the updated
    // comments in lines/tpe-anglo-scottish.toml for full sourcing.
    #[test]
    fn tpe_anglo_scottish_batch8_infill_stations_present() {
        let lines = load_line("tpe-anglo-scottish");
        let line = lines.get("tpe-anglo-scottish").expect("tpe-anglo-scottish line should exist");
        for crs in ["SNH", "WGN", "MCO", "BON", "CRS"] {
            assert!(line.has_station(crs), "tpe-anglo-scottish should now list {crs}");
        }
    }

    // Same task: St Helens Central sits on the newly-filled
    // `tpe-anglo-scottish-nw` segment, which (per this batch's own
    // pre-flight scan, confirmed unchanged by this task's research) is not
    // shared with any sibling line - an incident here should match only
    // tpe-anglo-scottish, as ExclusiveSegment, same shape as
    // emr_rural_branches_bottesford_incident_stays_on_its_own_branch
    // above. St Helens Central was chosen over Wigan North Western /
    // Manchester Oxford Road / Bolton because those three also appear
    // (station-level only, via wcml / emr-regional / northern-clitheroe /
    // northern-blackpool) in other line files, which would otherwise
    // widen this assertion's matched-line set without adding anything
    // tpe_anglo_scottish_exclusive_segment_incident_does_not_propagate
    // above doesn't already cover.
    #[test]
    fn tpe_anglo_scottish_st_helens_central_incident_stays_exclusive() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-3",
            "Signal failure at St Helens Central",
            "Signal failure causing delays to TransPennine Express services at St Helens Central.",
            &["TP"],
            &["SNH"],
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

    // Task 8.4 (Batch 8): fills the 9 genuinely missing, currently-open,
    // currently-served intermediate calls on this route's Liverpool leg
    // (Liverpool South Parkway, Warrington West, Warrington Central,
    // Birchwood, Irlam, Urmston) and Manchester Piccadilly-Cleethorpes leg
    // (Manchester Oxford Road, Meadowhall, Barnetby, Habrough), confirmed
    // via Wikipedia's TransPennine Express route table. See the updated
    // comments in lines/tpe-south.toml for full sourcing. All nine sit on
    // the file's own single `tpe-south` segment, which (per
    // tpe_south_exclusive_segment_incident_does_not_propagate above) has no
    // shared-segment overlap with any sibling line, so no second,
    // MatchScope-asserting test is added for this task.
    #[test]
    fn tpe_south_batch8_infill_stations_present() {
        let lines = load_line("tpe-south");
        let line = lines.get("tpe-south").expect("tpe-south line should exist");
        for crs in ["LPY", "WAW", "WAC", "BWD", "IRL", "URM", "MCO", "MHS", "BTB", "HAB"] {
            assert!(line.has_station(crs), "tpe-south should now list {crs}");
        }
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

    // Task 8.2 (Batch 8): fills two genuinely missing, currently-served,
    // very-low-frequency intermediate stations Cramlington (CRM, one train
    // per day, between Newcastle and Morpeth) and East Linton (ELT, reopened
    // 13 December 2023, between Dunbar and Edinburgh Waverley) - both
    // confirmed via Wikipedia and nationalrail.co.uk. See the updated
    // comments in lines/tpe-borders.toml for full sourcing. Both stations
    // sit on the file's own single `tpe-borders` segment, which (per
    // tpe_borders_exclusive_segment_incident_does_not_propagate above) has
    // no shared-segment overlap with any sibling line, so no second,
    // MatchScope-asserting test is added for this task.
    #[test]
    fn tpe_borders_batch8_infill_stations_present() {
        let lines = load_line("tpe-borders");
        let line = lines.get("tpe-borders").expect("tpe-borders line should exist");
        for crs in ["CRM", "ELT"] {
            assert!(line.has_station(crs), "tpe-borders should now list {crs}");
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
    fn c2c_rainham_branch_stations_are_in_the_catalogue() {
        // Regression guard for the Rainham-branch research pass: lines/c2c.toml
        // used to scope the Barking - Dagenham Dock - Rainham - Purfleet -
        // Grays route out entirely ("omitted rather than guessed at"), so
        // incidents on c2c's third route matched nothing by station. All three
        // new stations are c2c-managed and currently passenger-served (see that
        // file's `c2c-rainham-branch` comment for the two-source citations).
        let lines = load_line("c2c");
        let c2c = lines.get("c2c").expect("c2c line should exist");
        for crs in ["DDK", "RNM", "PFL"] {
            assert!(c2c.has_station(crs), "c2c should include {crs} on the Rainham branch");
        }
        // Rainham (Essex) is RNM; RAI is Rainham (Kent) on Southeastern's
        // Chatham main line and must never appear here.
        assert!(!c2c.has_station("RAI"), "RAI is Rainham (Kent), not c2c's Rainham (Essex)");
    }

    #[test]
    fn c2c_rainham_branch_incident_does_not_propagate() {
        // Same standalone-line exception class as
        // `c2c_exclusive_segment_incident_does_not_propagate` above: `DDK`,
        // `RNM` and `PFL` each appear only in lines/c2c.toml (re-checked
        // catalogue-wide when they were added), so an incident on the new
        // `c2c-rainham-branch` segment stays exclusive to c2c.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CC-2",
            "Points failure at Rainham",
            "Points failure causing delays to c2c services between Barking and Grays.",
            &["CC"],
            &["RNM"],
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
        //
        // UPDATED (Task 9.7, 2026-09-01): `xc-stansted.toml`'s own fresh
        // route-diagram pass reused NWE verbatim too (its own
        // Cambridge-Stansted Mountfitchet stretch runs over this same
        // physical West Anglia Main Line trunk, per that file's own
        // comment) on its own exclusive `xc-stansted` segment — a second
        // genuine station overlap, so "no other file touches this station"
        // no longer holds. Updated rather than left stale, mirroring Task
        // 9.3's own precedent.
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
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-west-anglia".to_string(), "xc-stansted".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should stay ExclusiveSegment", m.line.id);
        }
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
        //
        // Cambridge is also great-northern-kings-lynn.toml's (its own
        // `gn-cambridge-kings-lynn-branch` segment) and
        // thameslink-cambridge.toml's (its own `thameslink-cambridge-branch`
        // segment) terminus, both merged separately (Batch 5) -- two more
        // independent ExclusiveSegment matches by the same pattern.
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
                "greater-anglia-norfolk-branches".to_string(),
                "great-northern-kings-lynn".to_string(),
                "thameslink-cambridge".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 9.7 (2026-09-01) fresh route-diagram pass on `xc-stansted.toml`
    // added 20 real, currently-open, currently-served intermediate stations
    // across the whole Birmingham-Stansted Airport corridor's five named
    // CRS-pending stations plus several more this task's own research
    // found. All inherit the existing exclusive `xc-stansted` segment.
    // Regression guard that `has_station` now recognises a representative
    // spread across every leg researched (see
    // `west_anglia_exclusive_segment_incident_does_not_propagate` above for
    // the updated Newport overlap case, and the new Audley End overlap test
    // below for another West Anglia Main Line station-overlap case).
    #[test]
    fn xc_stansted_recognises_newly_added_stations() {
        let lines = load_line("xc-stansted");
        let line = lines.get("xc-stansted").expect("xc-stansted should load");
        for crs in [
            "WTO", "CEH", "HNK", "NBR", "SWS", "MMO", "OKM", "SMD", "WLE", "MCH", "MNE", "CMS",
            "SED", "WLF", "GRC", "AUD", "NWE", "ESM", "SST",
        ] {
            assert!(line.has_station(crs), "{crs} should now be recognised on xc-stansted");
        }
    }

    // Audley End (AUD) is a second genuine West Anglia Main Line station
    // overlap this task's own research found with
    // `greater-anglia-west-anglia.toml` (beyond the Newport case already
    // updated above), reusing its CRS verbatim. Station-level overlap only,
    // no segment shared, so both lines should stay ExclusiveSegment.
    #[test]
    fn xc_stansted_station_overlap_with_west_anglia_at_audley_end_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "XC-AUD-1",
            "Signal failure at Audley End",
            "Signal failure causing delays at Audley End.",
            &["LE"],
            &["AUD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-west-anglia".to_string(), "xc-stansted".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should stay ExclusiveSegment", m.line.id);
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
        //
        // Ely is also great-northern-kings-lynn.toml's own junction (its
        // own `gn-cambridge-kings-lynn-branch` segment, merged separately,
        // Batch 5) -- a fourth independent ExclusiveSegment match.
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
                "great-northern-kings-lynn".to_string(),
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
    fn wcml_birmingham_has_station_marston_green() {
        // Task 9.3: eight West Midlands Trains local stops (Canley, Tile
        // Hill, Berkswell, Hampton-in-Arden, Marston Green, Lea Hall,
        // Stechford, Adderley Park) were added to `lines/wcml-birmingham.toml`
        // as real, currently-open, currently-served stations between
        // Coventry and Birmingham New Street -- previously omitted on the
        // (rejected, per this plan's full-coverage mandate) reasoning that
        // Avanti's own service doesn't call there. Marston Green (MGN) here
        // stands in for all eight as the `has_station` regression this
        // task's testing convention requires; each was independently
        // two-source verified (Wikipedia's own station article
        // cross-checked against nationalrail.co.uk's live
        // /stations/<crs>/details.html page) per the file's own header
        // comment.
        //
        // All eight sit on `wcml-birmingham-branch`, which -- per a grep of
        // every `lines/*.toml` file -- remains exclusive to this file: no
        // sibling line reuses that segment name. (`lnwr-birmingham-crewe.toml`
        // covers the same physical Coventry-Birmingham track under its own,
        // differently-named `lnwr-birmingham` segment, and doesn't itself
        // model Marston Green/Lea Hall/Stechford/Adderley Park at all -- see
        // that file's Task 9.2 note.) So per this task's testing convention,
        // there's no sibling `MatchScope` assertion to add here; skipped.
        let lines = load_line("wcml-birmingham");
        let line = lines.get("wcml-birmingham").expect("wcml-birmingham line should exist");
        assert!(line.has_station("MGN"), "wcml-birmingham should now include Marston Green (MGN)");
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
    fn lnwr_northampton_incident_matches_single_remaining_lnwr_line() {
        // Was `lnwr_northampton_shared_segment_incident_propagates_to_both_lnwr_lines`,
        // asserting a two-line SharedSegment match between this file and
        // Task 1.7's `lnwr-euston-commuter.toml`. That file was deleted
        // (2026-08-31 line-catalogue-coverage follow-up): fresh research
        // reconfirmed it modelled a service that doesn't exist as a distinct
        // real-world working, and its entire station list was already a
        // strict subset of this file's own -- see the FOLD-IN NOTE in
        // `lines/lnwr-birmingham-crewe.toml`. With only one catalogued LNWR
        // line left, `lnwr-northampton` is no longer a name shared across
        // multiple files, so an incident here is now ExclusiveSegment, not
        // SharedSegment -- same shape as
        // `lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate`
        // below.
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
        assert_eq!(matched_ids, HashSet::from(["lnwr-birmingham-crewe".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lnwr_euston_trunk_incident_matches_single_remaining_lnwr_line() {
        // Was `lnwr_euston_trunk_shared_segment_incident_propagates_to_both_lnwr_lines`,
        // asserting a two-line SharedSegment match between this file and
        // Task 1.7's `lnwr-euston-commuter.toml`. That file was deleted
        // (2026-08-31 line-catalogue-coverage follow-up) -- see the FOLD-IN
        // NOTE in `lines/lnwr-birmingham-crewe.toml` for the full research
        // and sourcing. Leighton Buzzard appears in no other catalogued
        // line, and with only one catalogued LNWR line left,
        // `lnwr-euston-trunk` is no longer shared across multiple files, so
        // this is now a plain single-line ExclusiveSegment match.
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
        assert_eq!(matched_ids, HashSet::from(["lnwr-birmingham-crewe".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate() {
        // Canley is on the exclusive `lnwr-birmingham` segment (the
        // Birmingham branch, beyond the Rugby reconvergence point) -- not
        // shared with any other catalogued line's segment tag.
        //
        // `wcml-birmingham.toml` (Task 9.3, added after this test was first
        // written) now also calls at Canley -- previously omitted there as
        // "not called at by Avanti", but per this plan's full-coverage
        // mandate a real, currently-served station belongs in `stations`
        // regardless of which of this file's own operators calls there. It's
        // a real second line affected by this incident, on its own exclusive
        // `wcml-birmingham-branch` segment -- station-level overlap only,
        // same "overlap is fine, segment-sharing is a deliberate choice"
        // precedent this file already exercises elsewhere in this test
        // module (e.g. `wcml_birmingham_exclusive_segment_incident_does_not_propagate`
        // at Birmingham International), so still ExclusiveSegment for both.
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
        assert_eq!(
            matched_ids,
            HashSet::from(["lnwr-birmingham-crewe".to_string(), "wcml-birmingham".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
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

    // Station Catalogue Completeness Task 1.2: fills in five previously
    // missing minor Cotswold Line halts on gwr-cotswold.toml (Hanborough,
    // Combe, Finstock, Ascott-under-Wychwood, Shipton), two-source confirmed
    // and inserted at their true geographic position between OXF and KGM.
    // Hanborough was not in the task's original starting list (Combe,
    // Finstock, Ascott-under-Wychwood, Shipton) but was surfaced by a fresh
    // route-diagram read required by that task. See that file's own
    // "Ordered London to Worcester" comment for the full sourcing/rationale.
    #[test]
    fn gwr_cotswold_minor_halts_infill_stations_present() {
        let lines = load_line("gwr-cotswold");
        let line = lines.get("gwr-cotswold").expect("gwr-cotswold line should exist");
        for crs in ["HND", "CME", "FIN", "AUW", "SIP"] {
            assert!(line.has_station(crs), "gwr-cotswold should now list {crs}");
        }
    }

    // Same task: an incident at one of the newly-added stations (Hanborough)
    // should behave exactly like the pre-existing Moreton-in-Marsh
    // exclusive-segment case above -- `gwr-cotswold` is not shared with any
    // sibling line's segment (confirmed by grepping the catalogue: only
    // comments in gwr-thames-valley.toml reference the name, no other file's
    // `[[stations]]` entries actually set `segment = "gwr-cotswold"`), so
    // this stays a clean ExclusiveSegment match with no shared-segment
    // propagation to assert, mirroring
    // `gwr_cornish_main_line_saltash_incident_stays_on_its_own_line`'s
    // identical judgment call for that file's own infill task.
    #[test]
    fn gwr_cotswold_hanborough_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-18", "Signal failure at Hanborough", "Signal failure causing delays at Hanborough.", &["GW"], &["HND"]);
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

    // Station Catalogue Completeness Task 1.3: fills in three previously
    // missing intermediate stations on gwr-south-wales.toml (Patchway,
    // Pilning, Severn Tunnel Junction), two-source confirmed and inserted at
    // their true geographic position between BPW and NWP. See that file's
    // own segment-naming comment for the full sourcing/rationale.
    #[test]
    fn gwr_south_wales_infill_stations_present() {
        let lines = load_line("gwr-south-wales");
        let line = lines.get("gwr-south-wales").expect("gwr-south-wales line should exist");
        for crs in ["PWY", "PIL", "STJ"] {
            assert!(line.has_station(crs), "gwr-south-wales should now list {crs}");
        }
    }

    // Same task: an incident at one of the newly-added stations (Severn
    // Tunnel Junction) should behave exactly like the pre-existing Bridgend
    // exclusive-segment case above -- `gwr-south-wales` is not shared with
    // any sibling line's own segment at this station (grepping the
    // catalogue confirms no other line file lists PWY/PIL/STJ as a station
    // at all, let alone shares the `gwr-south-wales` segment name -- see
    // that file's own research comment on the deliberate decision not to
    // reuse `xc-cardiff`'s segment name despite the genuine physical track
    // convergence there), so this stays a clean ExclusiveSegment match with
    // no shared-segment propagation to assert, mirroring
    // `gwr_cornish_main_line_saltash_incident_stays_on_its_own_line`'s /
    // `gwr_cotswold_hanborough_incident_stays_on_its_own_line`'s identical
    // judgment call for those files' own infill tasks.
    #[test]
    fn gwr_south_wales_severn_tunnel_junction_incident_stays_on_its_own_line() {
        // `xc-cardiff.toml` (Task 9.4, added after this test was first
        // written) independently added Severn Tunnel Junction too -- a real
        // second line affected by this incident, on its own exclusive
        // `xc-cardiff` segment, not `gwr-south-wales`'s. Station-level
        // overlap only, same precedent as
        // `lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate`
        // above, so still ExclusiveSegment for both.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-19",
            "Flooding at Severn Tunnel Junction",
            "Flooding causing delays at Severn Tunnel Junction.",
            &["GW"],
            &["STJ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["gwr-south-wales".to_string(), "xc-cardiff".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
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
    //
    // UPDATED (Task 9.6, 2026-09-01): `xc-south-coast.toml`'s own fresh
    // route-diagram pass added DID too (its own OXF-RDG stretch runs via
    // Didcot Parkway, the only physical route), on its own exclusive
    // `xc-south-coast` segment — a fifth genuine station overlap, same
    // ExclusiveSegment treatment as gwr-thames-valley above. This assertion
    // is updated (not left to silently go stale) because the previous
    // four-line exact-set assertion is now factually false with DID added
    // to a fifth file, mirroring Task 9.3's own precedent for updating a
    // pre-existing test a new station addition invalidates.
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
                "xc-south-coast".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "gwr-thames-valley" || m.line.id == "xc-south-coast" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should stay ExclusiveSegment", m.line.id);
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

    // Task 9.4's fresh route-diagram pass on xc-cardiff.toml added two
    // previously-missing, real, currently-open, currently-served stations to
    // its exclusive `xc-cardiff` segment between Chepstow and Newport:
    // Caldicot (CDT) and Severn Tunnel Junction (STJ). Regression guard that
    // `has_station` now recognises one of them. `xc-cardiff` is not shared
    // with any other line file (grepped `lines/*.toml` for
    // `segment = "xc-cardiff"`: only this file uses it, and neither CDT nor
    // STJ appears on any other file's station list -- gwr-south-wales.toml's
    // own comment documents the genuine physical track sharing near Severn
    // Tunnel Junction but deliberately doesn't model it as a shared segment),
    // so per the testing convention only the has_station assertion applies
    // here -- no sibling-line MatchScope assertion to add.
    #[test]
    fn xc_cardiff_has_station_severn_tunnel_junction() {
        let lines = load_line("xc-cardiff");
        let xc_cardiff = lines.get("xc-cardiff").expect("xc-cardiff line should exist");
        assert!(xc_cardiff.has_station("STJ"), "xc-cardiff should now recognise Severn Tunnel Junction (STJ)");
        assert!(xc_cardiff.has_station("CDT"), "xc-cardiff should now recognise Caldicot (CDT)");
    }

    // Task 9.5 (2026-09-01) fresh route-diagram pass on `xc-manchester.toml`
    // added the real, currently-open, currently-served intermediate stations
    // this file's own "minor intermediate calls are omitted" boilerplate had
    // left out end-to-end: Levenshulme/Heaton Chapel (Manchester-Stockport),
    // Cheadle Hulme/Handforth (Stockport-Wilmslow), Alderley Edge/Chelford/
    // Goostrey/Holmes Chapel/Sandbach (Wilmslow-Crewe), Penkridge
    // (Stafford-Wolverhampton), and Coseley/Tipton/Dudley Port/Sandwell &
    // Dudley/Smethwick Galton Bridge/Smethwick Rolfe Street
    // (Wolverhampton-Birmingham New Street). All sixteen inherit this file's
    // own exclusive `xc-manchester` segment (no sibling line shares that
    // segment name -- grepped `lines/*.toml`), so per the testing convention
    // only the has_station assertion applies for most of them; see the
    // separate overlap test below for Smethwick Galton Bridge specifically,
    // which is also a station (not segment) overlap with
    // `wmr-snow-hill.toml`.
    #[test]
    fn xc_manchester_recognises_newly_added_stations() {
        let lines = load_line("xc-manchester");
        let line = lines.get("xc-manchester").expect("xc-manchester should load");
        for crs in [
            "LVM", "HTC", "CHU", "HTH", "ALD", "CEL", "GTR", "HCH", "SDB", "PKG", "CSY", "TIP",
            "DDP", "SAD", "SGB", "SMR",
        ] {
            assert!(line.has_station(crs), "{crs} should now be recognised on xc-manchester");
        }
    }

    // Smethwick Galton Bridge (SGB) is a genuine split-level interchange:
    // its high-level platforms carry `wmr-snow-hill.toml`'s Snow Hill route
    // (segment `wmr-snow-hill-trunk`), its low-level platforms carry this
    // file's Stour Valley stretch (segment `xc-manchester`) -- station-level
    // overlap only, the two segment names are genuinely distinct strings, so
    // an incident there should stay ExclusiveSegment on both lines rather
    // than propagate as a shared trunk. Mirrors
    // `gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line`.
    #[test]
    fn xc_manchester_station_overlap_with_wmr_snow_hill_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "XC-SGB-1",
            "Signalling fault at Smethwick Galton Bridge",
            "Signalling fault causing delays at Smethwick Galton Bridge.",
            &["XC"],
            &["SGB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["xc-manchester".to_string(), "wmr-snow-hill".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} scope mismatch", m.line.id);
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

    // Station Catalogue Completeness Task 1.1: fills in seven previously
    // missing Plymouth-area suburban stations on gwr-cornish-main-line.toml
    // (Devonport, Dockyard, Keyham, St Budeaux Ferry Road, Saltash, St
    // Germans, Menheniot), two-source confirmed and inserted at their true
    // geographic position between PLY and LSK. See that file's own
    // segment-naming comment for the full sourcing/rationale.
    #[test]
    fn gwr_cornish_main_line_plymouth_area_infill_stations_present() {
        let lines = load_line("gwr-cornish-main-line");
        let line = lines.get("gwr-cornish-main-line").expect("gwr-cornish-main-line line should exist");
        for crs in ["DPT", "DOC", "KEY", "SBF", "STS", "SGM", "MEN"] {
            assert!(line.has_station(crs), "gwr-cornish-main-line should now list {crs}");
        }
    }

    // Same task: an incident at one of the newly-added stations (Saltash)
    // should behave exactly like the pre-existing Truro exclusive-segment
    // case above -- `gwr-cornish-main-line` is not shared with any sibling
    // line's segment (confirmed by grepping the catalogue: the segment name
    // is exclusive to this one file), so this stays a clean ExclusiveSegment
    // match with no shared-segment propagation to assert, mirroring
    // `emr_rural_branches_bottesford_incident_stays_on_its_own_branch`'s
    // identical judgment call for that file's own infill task.
    #[test]
    fn gwr_cornish_main_line_saltash_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-10", "Points failure at Saltash", "Points failure causing delays at Saltash.", &["GW"], &["STS"]);
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
    //
    // UPDATED (Task 9.6, 2026-09-01): `xc-south-coast.toml`'s own fresh
    // route-diagram pass reused CUM verbatim (its own Oxford-Reading
    // stretch runs via Didcot Parkway, the same physical Oxford branch this
    // file curates) on its own exclusive `xc-south-coast` segment — a
    // genuine second station overlap, so this incident now also matches
    // xc-south-coast, staying ExclusiveSegment there too. Updated rather
    // than left stale, mirroring Task 9.3's own precedent.
    #[test]
    fn gwr_thames_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-10", "Signal failure at Culham", "Signal failure causing delays at Culham.", &["GW"], &["CUM"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["gwr-thames-valley".to_string(), "xc-south-coast".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should stay ExclusiveSegment", m.line.id);
        }
    }

    // Task 9.6 (2026-09-01) fresh route-diagram pass on `xc-south-coast.toml`
    // added 30 real, currently-open, currently-served intermediate stations
    // across this whole Birmingham-Bournemouth corridor's "minor calls
    // omitted" boilerplate. All inherit the existing exclusive
    // `xc-south-coast` segment. Regression guard that `has_station` now
    // recognises a representative spread across every leg researched (see
    // the overlap-specific tests below for the DID/CUM/RDW station-overlap
    // cases with `gwr-thames-valley.toml`, already updated above).
    #[test]
    fn xc_south_coast_recognises_newly_added_stations() {
        let lines = load_line("xc-south-coast");
        let line = lines.get("xc-south-coast").expect("xc-south-coast should load");
        for crs in [
            "KNW", "KGS", "HYD", "TAC", "RAD", "APF", "CHO", "GOR", "PAN", "TLH", "RDW", "RGP",
            "MOR", "BMY", "MIC", "SHW", "ESL", "SOA", "SWG", "SDN", "MBK", "RDB", "TTN", "ANF",
            "BEU", "BCU", "SWY", "NWM", "HNA", "CHR", "POK",
        ] {
            assert!(line.has_station(crs), "{crs} should now be recognised on xc-south-coast");
        }
    }

    // Reading West (RDW) is a genuine second overlap this task's own
    // research found with `gwr-thames-valley.toml`: both files reach it via
    // shared trackage as far as Southcote Junction (that file's own
    // Reading-Newbury branch, this file's Reading-Basingstoke stretch),
    // reusing RDW's CRS/TIPLOC verbatim. Station-level overlap only, no
    // segment shared, so both lines should stay ExclusiveSegment.
    #[test]
    fn xc_south_coast_station_overlap_with_gwr_thames_valley_at_reading_west_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "XC-RDW-1",
            "Points failure at Reading West",
            "Points failure causing delays at Reading West.",
            &["GW"],
            &["RDW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["gwr-thames-valley".to_string(), "xc-south-coast".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should stay ExclusiveSegment", m.line.id);
        }
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

    // Station Catalogue Completeness Task 1.4: fills in the previously
    // missing Paddington-area inner-suburban stations on
    // gwr-thames-valley.toml (Southall, Hayes & Harlington, West Drayton,
    // Iver, Langley, Burnham, Taplow), two-source confirmed and inserted at
    // their true geographic position between PAD and MAI. Ealing Broadway,
    // the eighth station in that task's starting candidate list, is
    // deliberately NOT added — its own Wikipedia "Services" section and
    // nationalrail.co.uk's own details page both show no current GWR
    // calling service there (Elizabeth line/Underground only) — see that
    // file's own segment-naming comment for the full sourcing/rationale.
    #[test]
    fn gwr_thames_valley_paddington_area_infill_stations_present() {
        let lines = load_line("gwr-thames-valley");
        let line = lines.get("gwr-thames-valley").expect("gwr-thames-valley line should exist");
        for crs in ["STL", "HAY", "WDT", "IVR", "LNY", "BNM", "TAP"] {
            assert!(line.has_station(crs), "gwr-thames-valley should now list {crs}");
        }
        assert!(
            !line.has_station("EAL"),
            "gwr-thames-valley should NOT list EAL (Ealing Broadway) — GWR does not currently call there, see the file's own Task 1.4 comment"
        );
    }

    // Same task: Southall (STL) is a real, additional station overlap with
    // elizabeth-line.toml's own `elizabeth-trunk-west` segment. Unlike the
    // `elizabeth-west` overlap at MAI/SLO/TWY/WDT, `elizabeth-trunk-west` is
    // itself a genuine shared trunk between elizabeth-line.toml and
    // elizabeth-heathrow.toml (both list HAY/STL/EAL on that exact segment
    // name — checked directly), so an incident at Southall propagates as
    // SharedSegment between those two Elizabeth-line arms, while
    // gwr-thames-valley — on its own unrelated `gwr-thames-valley` segment —
    // still only sees a station-level hit and stays ExclusiveSegment,
    // mirroring the MAI/SLO/TWY station-overlap precedent for this line's
    // own segment even though the *other* two lines here happen to share a
    // trunk with each other.
    #[test]
    fn gwr_thames_valley_station_overlap_with_elizabeth_trunk_west_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-20", "Signal failure at Southall", "Signal failure causing delays at Southall.", &["GW"], &["STL"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
        assert_eq!(
            by_id.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from(["gwr-thames-valley".to_string(), "elizabeth-line".to_string(), "elizabeth-heathrow".to_string()])
        );
        assert_eq!(
            by_id.get("gwr-thames-valley"),
            Some(&MatchScope::ExclusiveSegment),
            "gwr-thames-valley should stay ExclusiveSegment (station overlap only, not a shared segment, for its own segment)"
        );
        assert_eq!(by_id.get("elizabeth-line"), Some(&MatchScope::SharedSegment), "elizabeth-line should be SharedSegment (elizabeth-trunk-west)");
        assert_eq!(
            by_id.get("elizabeth-heathrow"),
            Some(&MatchScope::SharedSegment),
            "elizabeth-heathrow should be SharedSegment (elizabeth-trunk-west)"
        );
    }

    // Same task: Iver (IVR) is not currently in elizabeth-line.toml's own
    // station list at all, so an incident there has no sibling segment to
    // stay off of — a clean ExclusiveSegment case, mirroring
    // `gwr_cornish_main_line_saltash_incident_stays_on_its_own_line`'s
    // identical judgment call for that file's own infill task.
    #[test]
    fn gwr_thames_valley_iver_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-21", "Points failure at Iver", "Points failure causing delays at Iver.", &["GW"], &["IVR"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-thames-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Station Catalogue Completeness Task 1.5: fills in four previously
    // missing stations on gwr-west-of-england.toml's own exclusive stretch
    // west of Newbury — Kintbury (KIT), Hungerford (HGD), Bedwyn (BDW) and
    // Pewsey (PEW), two-source confirmed (Wikipedia + nationalrail.co.uk,
    // TIPLOCs additionally cross-checked against railwaycodes.org.uk) and
    // inserted at their true geographic position between NBY and WSB, all
    // tagged this line's own exclusive `gwr-west-of-england` segment (not
    // `gwr-westbury-castle-cary`, which starts at the next station, WSB, per
    // the shared-trunk rule of thumb). Six other named candidates from this
    // task's starting list are deliberately NOT added here: Reading West,
    // Theale, Aldermaston, Midgham, Thatcham and Newbury Racecourse already
    // live on gwr-thames-valley.toml's own "Branch 2: Reading-Newbury"
    // section (a different, Reading-based local service, not this file's
    // Reading-Taunton express) and Frome already lives on
    // gwr-bristol-suburban.toml (reached only via a branch off this line's
    // direct route) — both untouched by this task. A further five named
    // candidates — Savernake (Low Level), Woodborough, Patney and Chirton,
    // Lavington, and Edington and Bratton — were checked to the same
    // two-source bar and found closed (all closed to passengers between
    // 1952 and 1966; railwaycodes.org.uk shows no CRS code for any of them)
    // so they stay out too. See that file's own segment-naming comment for
    // the full sourcing/rationale.
    #[test]
    fn gwr_west_of_england_berks_and_hants_infill_stations_present() {
        let lines = load_line("gwr-west-of-england");
        let line = lines.get("gwr-west-of-england").expect("gwr-west-of-england line should exist");
        for crs in ["KIT", "HGD", "BDW", "PEW"] {
            assert!(line.has_station(crs), "gwr-west-of-england should now list {crs}");
        }
        for crs in ["RDW", "THE", "AMT", "MDG", "THA", "NRC", "FRO"] {
            assert!(
                !line.has_station(crs),
                "gwr-west-of-england should NOT list {crs} — it belongs to a sibling file, see the file's own Task 1.5 comment"
            );
        }
    }

    // Same task: none of the four newly-added stations sit on a segment name
    // shared with any other catalogued line (`gwr-west-of-england` is this
    // line's own exclusive segment throughout, confirmed by grepping the
    // catalogue), so an incident at one of them — Kintbury, picked as a
    // representative example — should stay a clean ExclusiveSegment case,
    // mirroring `gwr_thames_valley_iver_incident_stays_on_its_own_line`'s /
    // `gwr_cornish_main_line_saltash_incident_stays_on_its_own_line`'s
    // identical judgment call for their own infill tasks.
    #[test]
    fn gwr_west_of_england_kintbury_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("GW-22", "Signal failure at Kintbury", "Signal failure causing delays at Kintbury.", &["GW"], &["KIT"]);
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-west-of-england".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
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

    // southeastern-main-line was the first Southeastern file in this
    // catalogue (Batch 5, Task 5.1). At the time it shared no segment with
    // any already-curated line — thameslink-core.toml overlaps at LBG by
    // station only, not by segment. That's still true of `seml-weald`
    // specifically (Task 5.3's southeastern-highspeed.toml overlaps this
    // line only at AFK, by station, on its own `hs1-ashford` segment - see
    // afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments
    // below), so an incident on a `seml-weald` station untouched by any
    // other file should still stay exclusive to this line alone.
    #[test]
    fn seml_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-1",
            "Signal failure at Tonbridge",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["TON"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southeastern-main-line".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // southeastern-chatham (Batch 5, Task 5.2) shares no segment with any
    // other already-curated line. Its DVP overlap with
    // southeastern-main-line.toml is station-only (the two lines approach
    // Dover Priory from physically different directions), documented in
    // southeastern-chatham.toml's own header comment. Since Task 5.3
    // (southeastern-highspeed.toml) reused `chatham-medway`/
    // `chatham-coastal` verbatim as a genuine shared trunk (see
    // hs1_chatham_medway_shared_segment_incident_propagates_to_both_lines
    // below), this test now exercises a `chatham-medway` station the new
    // file doesn't touch (Meopham, between Longfield and Sole Street -
    // west of Strood, where the Javelin's North Kent pattern joins), to
    // confirm the untouched part of chatham-medway still stays exclusive.
    #[test]
    fn chatham_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-2",
            "Signal failure at Meopham",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["MEP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southeastern-chatham".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // The two branches within this one file (chatham-coastal via Ramsgate,
    // chatham-dover via Canterbury East) are both exclusive to
    // southeastern-chatham - an incident on the Dover branch shouldn't
    // pull in southeastern-main-line even though both lines terminate at
    // DVP (station overlap only, not a shared segment; see the file's own
    // header comment).
    #[test]
    fn chatham_dover_branch_incident_does_not_propagate_to_seml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-3",
            "Points failure at Adisham",
            "Points failure causing delays to Southeastern services.",
            &["SE"],
            &["ADM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southeastern-chatham".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // southeastern-highspeed (Batch 5, Task 5.3) is the domestic "Javelin"
    // HS1 service. Its St Pancras - Stratford International - Ebbsfleet
    // International trunk (`hs1-domestic`) is purpose-built high-speed
    // infrastructure no other curated line touches, so an incident there
    // should stay exclusive to this file - mirrors
    // seml_exclusive_segment_incident_does_not_propagate above.
    #[test]
    fn hs1_domestic_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-4",
            "Overhead line problem at Stratford International",
            "An overhead line problem is causing delays to Southeastern high speed services.",
            &["SE"],
            &["SFA"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southeastern-highspeed".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Ashford International (AFK) is on both southeastern-main-line.toml
    // (`seml-weald`) and southeastern-highspeed.toml (`hs1-ashford`) -
    // deliberately different segment names, because HS1 reaches Ashford via
    // its own purpose-built alignment through the North Downs Tunnel, not
    // via SEML's classic Sevenoaks/Tonbridge route. Per this task's brief
    // and both files' header comments, that's station overlap, not a
    // shared trunk: an AFK incident should match both lines independently,
    // each still scoped ExclusiveSegment, never SharedSegment.
    #[test]
    fn afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-5",
            "Signal failure at Ashford International",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["AFK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-main-line".to_string(), "southeastern-highspeed".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Past Ashford, the Javelin runs over the same physical track
    // southeastern-main-line.toml documents as `seml-coast` (there is only
    // one route from Ashford to Dover via Folkestone). This file
    // deliberately does NOT reuse that segment name, though: `is_shared`
    // treats an entire segment name as shared the moment two files use it,
    // and this file doesn't call at every `seml-coast` station (e.g. it
    // skips Westenhanger/Sandling) - reusing the name verbatim would
    // therefore also mark those untouched stations SharedSegment. So this
    // is kept as station overlap on this file's own `hs1-ashford` segment
    // (see the file's header comment) - both lines still match a
    // Folkestone Central incident, but each independently as
    // ExclusiveSegment.
    #[test]
    fn hs1_ashford_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-6",
            "Points failure at Folkestone Central",
            "Points failure causing delays to Southeastern services.",
            &["SE"],
            &["FKC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-main-line".to_string(), "southeastern-highspeed".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Same reasoning again for the North Kent pattern: from Strood onward
    // this file's `hs1-northkent` segment runs over the same physical
    // track as southeastern-chatham.toml's `chatham-medway`/
    // `chatham-coastal`, but the name isn't reused (this file doesn't
    // touch e.g. Longfield/Meopham/Sole Street), so it's station overlap,
    // not a shared trunk - both lines match a Ramsgate incident
    // independently, each ExclusiveSegment.
    #[test]
    fn hs1_northkent_station_overlap_matches_both_chatham_and_hs1_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-7",
            "Signal failure at Ramsgate",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["RAM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-chatham".to_string(), "southeastern-highspeed".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // southeastern-metro-north-kent (Batch 5, Task 5.4) covers the
    // Bexleyheath line and Dartford Loop line, both diverging from a
    // shared London Bridge-Lewisham trunk (`southeastern-lewisham-
    // corridor`). Per this file's own header comment (FINDING 2), research
    // for this task could NOT confirm the gap analysis's premise that
    // Thameslink genuinely shares that trunk under normal service - so no
    // sibling file uses `southeastern-lewisham-corridor` yet, and an
    // incident on this line's own exclusive Bexleyheath branch (past the
    // Lewisham junction) should stay exclusive to this line alone. Mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn senk_bexleyheath_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-8",
            "Points failure at Bexleyheath",
            "Points failure causing delays to Southeastern services.",
            &["SE"],
            &["BXH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southeastern-metro-north-kent".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Same for the Dartford Loop branch (the other branch in this one
    // file, diverging from the shared trunk at Hither Green rather than at
    // Lewisham itself) - an incident on it should also stay exclusive to
    // this line, and shouldn't spuriously pull in the Bexleyheath branch's
    // own segment name either (the two branches use different segment
    // names, `senk-bexleyheath` vs `senk-dartford-loop`, despite being the
    // same file/line).
    #[test]
    fn senk_dartford_loop_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-9",
            "Signal failure at Sidcup",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["SID"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southeastern-metro-north-kent".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // LBG is also thameslink-core.toml's own terminus (its own segment
    // ends there too) and southeastern-main-line.toml's own `seml-london`
    // station, but per this file's header comment that's station overlap
    // only, not a shared trunk - same judgment southeastern-main-line.toml
    // already made for LBG/Thameslink. Confirms an LBG incident matches
    // all three lines independently, each still scoped ExclusiveSegment,
    // never SharedSegment - mirrors
    // afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments
    // above.
    //
    // Task 5.5 (southeastern-hayes-line.toml) added a fourth LBG overlap:
    // its own `hayes-london` segment also calls at LBG (see that file's own
    // header comment for why it does NOT reuse senk's
    // `southeastern-lewisham-corridor` name despite passing through the
    // same station - the Hayes line's own calling pattern skips New
    // Cross/St Johns, so the two runs aren't confirmed to share physical
    // track for that stretch).
    //
    // NOTE for Task 5.14 (lines/thameslink-southern.toml, not yet
    // written): this task could not add the shared-segment propagation
    // test the batch's testing convention otherwise requires (mirrors
    // swr_shared_trunk_incident_propagates /
    // xc_hub_incident_propagates_to_every_cross_country_arm) because that
    // sibling file doesn't exist yet. If Task 5.14's own research
    // independently confirms genuine Thameslink running over the London
    // Bridge-Lewisham stretch and it reuses `southeastern-lewisham-
    // corridor` verbatim, its implementer should add a test here (or in
    // that task's own matcher tests) asserting an incident on that shared
    // segment matches BOTH `southeastern-metro-north-kent` and
    // `thameslink-southern` with `MatchScope::SharedSegment`.
    // Updated by Task 5.6 (southern-brighton-main-line.toml): that file also
    // has a station at LBG (its own `southern-bml-north` segment, named as a
    // courtesy hand-off for Task 5.14's thameslink-southern.toml, not yet a
    // real cross-file shared trunk - see that file's own header comment), so
    // it now joins this set as a fifth independent exclusive-segment match.
    //
    // Updated by Task 5.9 (southern-oxted-uckfield.toml): that file also has
    // a station at LBG (its own `oxted-london-bridge-approach` segment - the
    // usual London terminus for its Uckfield branch service, see that
    // file's own header comment for why this is station overlap, not a
    // shared trunk, with every other line here), so it now joins this set
    // as a sixth independent exclusive-segment match.
    // Updated by Task 5.14 (thameslink-southern.toml): that file's own
    // Brighton branch also meets London Bridge here. An earlier draft
    // reused southern-brighton-main-line.toml's own `southern-bml-north`
    // segment name here, asserting a genuine SharedSegment pair - on
    // review this was withdrawn (thetrainline.com, the source relied on to
    // clear COMMON.md's bar, is not one of its four approved second-source
    // categories and doesn't attest physical track sharing anyway - see
    // thameslink-southern.toml's own BRIGHTON BRANCH header comment for the
    // full writeup). thameslink-southern now uses its own segment name
    // here (`thameslink-brighton`), so it joins this set as a seventh
    // independent ExclusiveSegment station-overlap match, same treatment as
    // every other line in this set.
    #[test]
    fn lbg_station_overlap_matches_senk_thameslink_core_seml_and_hayes_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-10",
            "Signal failure at London Bridge",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["LBG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-metro-north-kent".to_string(),
                "thameslink-core".to_string(),
                "southeastern-main-line".to_string(),
                "southeastern-hayes-line".to_string(),
                "southern-brighton-main-line".to_string(),
                "southern-oxted-uckfield".to_string(),
                "thameslink-southern".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.5 (southeastern-hayes-line.toml). An incident on the Hayes
    // line's own exclusive branch (past Lewisham, e.g. West Wickham) should
    // stay exclusive to this line - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn hayes_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-11",
            "Signal failure at West Wickham",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["WWI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southeastern-hayes-line".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.5 (southeastern-hayes-line.toml). Lewisham (LEW) is a station
    // overlap between this file's own `hayes-london` segment and
    // southeastern-metro-north-kent.toml's `southeastern-lewisham-corridor`
    // - two different segment names for the same station, per this file's
    // own header comment (not a shared trunk, since the Hayes line's own
    // calling pattern diverges from senk's before Lewisham). Confirms an
    // incident there matches both lines independently, each still scoped
    // ExclusiveSegment, never SharedSegment.
    //
    // Updated by Task 5.3 (southeastern-main-line.toml, station-catalogue-
    // completeness plan): that file's own research confirmed New Cross, St
    // Johns, Lewisham and Hither Green all sit on the South Eastern Main
    // Line's own physical alignment toward Orpington (not just on the
    // Dartford Loop/Bexleyheath corridor's distinct tracks), so it now adds
    // Lewisham too, on its own `seml-london` segment - a third independent
    // exclusive-segment station overlap here, same treatment as every other
    // line in this set.
    #[test]
    fn lew_station_overlap_matches_hayes_line_and_senk_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-12",
            "Signal failure at Lewisham",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["LEW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-hayes-line".to_string(),
                "southeastern-metro-north-kent".to_string(),
                "southeastern-main-line".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // REGRESSION TEST for this batch's final-review fix: sibling-line names
    // in an incident's text must not veto a genuine station hit.
    //
    // `is_excluded` is a HARD VETO evaluated in `lines_affected_by` BEFORE
    // `match_one` ever looks at `affected_stations`, so an
    // `excluded_keywords` entry naming a sibling line suppresses that file
    // even when the incident lists a CRS genuinely on it. Before this fix,
    // southeastern-hayes-line.toml excluded "Dartford Loop line" and
    // southeastern-metro-north-kent.toml excluded "Hayes line", so a real
    // incident naming BOTH routes and listing a station both files list
    // (LEW - Lewisham, where the two corridors diverge, and also CHX/LBG)
    // vetoed BOTH files at once and returned zero Southeastern matches - the
    // exact multi-line incident these two files were written to model. The
    // vetoes have been removed from both files' `excluded_keywords`; the
    // station-CRS path already disambiguates this correctly, as
    // lew_station_overlap_matches_hayes_line_and_senk_as_independent_exclusive_segments
    // above shows for the no-line-names-in-text case.
    //
    // The veto MECHANISM itself is unchanged and still proven by
    // excluded_keyword_vetoes_match above (a genuinely foreign service on a
    // line that shares no station with the excluding file) - only the
    // specific data entries that misapplied it to station-sharing siblings
    // were removed.
    //
    // Updated by Task 5.3 (southeastern-main-line.toml): that file now also
    // lists LEW (see the lew_station_overlap... update above) and its own
    // `excluded_keywords` is just ["Hastings line"], which this incident's
    // text doesn't contain, so it joins this set as a third match with no
    // veto risk.
    #[test]
    fn sibling_line_names_no_longer_veto_a_shared_station_hit() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-13",
            "Disruption between London Bridge and Lewisham",
            "Disruption between London Bridge and Lewisham affecting the Hayes line and the Dartford Loop line.",
            &["SE"],
            &["LEW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-hayes-line".to_string(),
                "southeastern-metro-north-kent".to_string(),
                "southeastern-main-line".to_string(),
            ]),
            "both named lines list LEW and must both match; before the fix each vetoed the other and this was empty"
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.6 (southern-brighton-main-line.toml). An incident on this
    // line's own exclusive `southern-bml-south` segment (past the ECR
    // junction) should stay exclusive to this line alone - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above. Uses Hassocks
    // (HSK) rather than Brighton (BTN) itself: Task 5.7
    // (southern-coastway-east.toml) added its own station at BTN too (a
    // real station overlap, since Coastway East also originates there -
    // see that file's own header comment), so BTN alone no longer proves
    // "no other already-curated line touches this station" the way it did
    // when this test was first written - that overlap is now covered
    // separately by
    // btn_station_overlap_matches_coastway_east_and_brighton_main_line_as_independent_exclusive_segments
    // below. HSK remains exclusive to `southern-bml-south`.
    //
    // Updated by Task 5.14 (thameslink-southern.toml): that file's own
    // Brighton branch also calls at Hassocks, on its own `thameslink-
    // brighton` segment - deliberately NOT the same name as this line's own
    // `southern-bml-south` (see thameslink-southern.toml's own PAST EAST
    // CROYDON header comment for why that lead was documented but not
    // acted on), so this is a second independent ExclusiveSegment match,
    // not a SharedSegment one.
    #[test]
    fn southern_bml_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-1",
            "Signal failure at Hassocks",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["HSK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southern-brighton-main-line".to_string(), "thameslink-southern".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.6. NOTE ON WHAT THIS ACTUALLY PROVES (corrected after review -
    // the original comment here overclaimed): an incident tagged with the
    // `GX` operator at Preston Park - one of Gatwick Express's own
    // (Wikipedia-only-sourced, see southern-brighton-main-line.toml's own
    // header comment) peak-only calling points - still resolves to
    // `southern-brighton-main-line`, ExclusiveSegment. But this is a
    // station-hit match: `match_one`'s Tier 1 path matches on
    // `line.has_station(crs)` alone (see `common::LineDefinition::
    // has_station`) and never consults `operators`, so this test would pass
    // identically even if `GX` were never added to this line's `operators`
    // list - PRP is already this line's own station regardless of who's
    // asking. It does NOT exercise the `operators` field or prove the
    // fold-in decision "works" in the sense the file's own comment claims.
    // What it does confirm: a `GX`-tagged incident at a real Brighton Main
    // Line station isn't accidentally excluded or misrouted by this line's
    // matching logic - a narrower but still real assurance.
    //
    // The `operators` field's actual role - LDBWS sample classification via
    // `belongs_to_line` - is exercised by a separate test,
    // `belongs_to_line_gatwick_express_operator_folds_in_via_operators_list`
    // in `aggregation.rs`, which WOULD fail without `GX` in `operators`.
    //
    // Updated by Task 5.14 (thameslink-southern.toml): that file's own
    // Brighton branch also calls at Preston Park, on its own
    // `thameslink-brighton` segment - a second independent ExclusiveSegment
    // station-overlap match, same reasoning as
    // southern_bml_exclusive_segment_incident_does_not_propagate above.
    #[test]
    fn southern_bml_station_hit_matches_regardless_of_gx_or_sn_operator_tag() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GX-1",
            "Delays at Preston Park",
            "Gatwick Express services are delayed at Preston Park.",
            &["GX"],
            &["PRP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southern-brighton-main-line".to_string(), "thameslink-southern".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.7 (southern-coastway-east.toml). An incident on this line's
    // own exclusive `coastway-east-hastings` segment (past the Lewes
    // junction) should stay exclusive to this line alone - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above. This line is a
    // genuinely standalone route (no sibling file shares any of its
    // segment names - see that file's own header comment), so this also
    // confirms no accidental cross-file match: no shared-segment
    // propagation test is added for this line, per COMMON.md's own
    // "skip only for a genuinely standalone line" exception.
    #[test]
    fn coastway_east_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-2",
            "Signal failure at Eastbourne",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["EBN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southern-coastway-east".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.7, updated by Task 5.8. Brighton (BTN) is also
    // southern-brighton-main-line.toml's own terminus, but per this file's
    // own header comment that's station overlap only (the two lines
    // diverge immediately east of Brighton onto physically different
    // routes, and use different segment names - `coastway-east-brighton`
    // here vs `southern-bml-victoria`/`southern-bml-south` there) - same
    // judgment call as the LBG overlaps exercised above. Task 5.8
    // (southern-coastway-west.toml) added a third line at this same
    // station (its own `coastway-west-brighton` segment, diverging west
    // out of Brighton) - see that file's own header comment. Confirms an
    // incident at Brighton matches all three lines independently, each
    // still scoped ExclusiveSegment, never SharedSegment.
    // Updated by Task 5.14 (thameslink-southern.toml): that file's own
    // Brighton branch also terminates at Brighton, on its own
    // `thameslink-brighton` segment - deliberately NOT the same name as
    // southern-brighton-main-line.toml's own `southern-bml-south` here (see
    // that file's own PAST EAST CROYDON header comment for why the
    // southern-bml-south sharing lead found past East Croydon was
    // documented but not acted on), so this remains a fourth independent
    // ExclusiveSegment station-overlap match, not a SharedSegment one.
    #[test]
    fn btn_station_overlap_matches_coastway_east_and_brighton_main_line_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-3",
            "Signal failure at Brighton",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["BTN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southern-coastway-east".to_string(),
                "southern-brighton-main-line".to_string(),
                "southern-coastway-west".to_string(),
                "thameslink-southern".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.8 (southern-coastway-west.toml). An incident on this line's
    // own exclusive `coastway-west-brighton` segment (a station no other
    // curated file touches) should stay exclusive to this line alone -
    // mirrors swr_exclusive_segment_incident_does_not_propagate and
    // coastway_east_exclusive_segment_incident_does_not_propagate above.
    // This line shares no segment name with any sibling file (its two
    // real overlaps - Brighton with southern-brighton-main-line.toml/
    // southern-coastway-east.toml, and Havant/Portsmouth with
    // swr-portsmouth-direct.toml - are both deliberately station overlap
    // only, per this file's own header comment), so no SharedSegment
    // propagation test is added for this line, per COMMON.md's own "skip
    // only for a genuinely standalone line" exception - the two station-
    // overlap tests below exercise both real overlaps instead.
    #[test]
    fn coastway_west_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-4",
            "Signal failure at Chichester",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["CCH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southern-coastway-west".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.8. Havant (HAV) is also swr-portsmouth-direct.toml's own
    // station (on that file's `swr-portsmouth-direct` segment). Per this
    // file's own STATION-OVERLAP AT HAVANT/PORTSMOUTH header comment: this
    // is genuine physical track-sharing (Southern's West Coastway stopping
    // service and SWR's Portsmouth Direct service both run Havant-
    // Bedhampton-Hilsea-Fratton-Portsmouth into Portsmouth), but per the
    // cross-operator precedent xc-south-coast.toml/xc-manchester.toml
    // already set, segment names are only reused between sibling lines of
    // the SAME operator - SN and SW are different operators, so this is
    // treated as station overlap, not a shared trunk, same judgment call
    // as the AFK/Ramsgate overlaps between Southeastern and HS1 above.
    // Confirms an incident at Havant matches both lines independently,
    // each still scoped ExclusiveSegment, never SharedSegment.
    #[test]
    fn hav_station_overlap_matches_coastway_west_and_swr_portsmouth_direct_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-5",
            "Signal failure at Havant",
            "Signal failure causing delays to services.",
            &["SN"],
            &["HAV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southern-coastway-west".to_string(), "swr-portsmouth-direct".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.9 (southern-oxted-uckfield.toml). An incident on this line's
    // own exclusive `oxted-uckfield-branch` segment (past the Hurst Green
    // junction) should stay exclusive to this line alone - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above. This line shares
    // no segment name with any sibling file (its three real overlaps - VIC,
    // LBG and ECR, all station overlap only per this file's own header
    // comment - are exercised by the two tests below and by the updated LBG
    // test above), so no SharedSegment propagation test is added for this
    // line, per COMMON.md's own "skip only for a genuinely standalone line"
    // exception.
    #[test]
    fn oxted_uckfield_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-6",
            "Signal failure at Buxted",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["BXD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southern-oxted-uckfield".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.9. Same for the East Grinstead branch (the other branch past
    // Hurst Green Junction) - an incident there should also stay exclusive
    // to this line. Per this file's own header comment, East Grinstead is
    // confirmed as Southern's own Oxted line terminus (not Thameslink
    // territory, despite the gap analysis grouping it with Thameslink's
    // southern branches) - no Thameslink sibling file exists yet, so this
    // stays a plain exclusive-segment case for now; see that file's own
    // HAND-OFF NOTE for what a future Thameslink southern-branches file
    // should re-check.
    #[test]
    fn oxted_east_grinstead_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-7",
            "Points failure at Lingfield",
            "Points failure causing delays to Southern services.",
            &["SN"],
            &["LFD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southern-oxted-uckfield".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.9. Victoria (VIC) is also southern-brighton-main-line.toml's
    // own terminus and southeastern-chatham.toml's own station. Per this
    // file's own STATION OVERLAP AT VIC / LBG / ECR header comment: whether
    // Oxted line services physically share fast/slow tracks with those
    // other services out of Victoria was not confirmed to COMMON.md's bar,
    // so this is treated as station overlap only (this file's own
    // `oxted-victoria-approach` segment, not reusing either sibling's
    // segment name) - same judgment call as the AFK/Ramsgate/LBG overlaps
    // exercised elsewhere in this module. Confirms an incident at Victoria
    // matches all three lines independently, each still scoped
    // ExclusiveSegment, never SharedSegment.
    #[test]
    fn vic_station_overlap_matches_brighton_main_line_chatham_and_oxted_uckfield_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-8",
            "Signal failure at Victoria",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["VIC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southern-brighton-main-line".to_string(),
                "southeastern-chatham".to_string(),
                "southern-oxted-uckfield".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.10 (great-northern-kings-lynn.toml). No shared-segment test is
    // added for the LNER pairing documented in that file's own LNER HAND-OFF
    // comment (`gn-ecml-slow-lines`) - `lines/lner-ecml.toml` (Batch 6) is
    // being written in a separate, parallel git worktree and does not exist
    // here, so a test asserting it would fail. Add that test once both
    // files exist and Batch 6 confirms whether it reuses the segment name.
    //
    // Originally this test also covered the direct Peterborough branch
    // (using Huntingdon) to show it stayed exclusive to this line. Task 5.13
    // (thameslink-cambridge.toml) confirmed genuine track-sharing on that
    // exact branch instead (see gn_peterborough_branch_shared_trunk_incident_
    // propagates_to_thameslink_cambridge below) - every station on that
    // branch is now genuinely shared, so there is no longer an exclusive
    // proof point left on it. That half of this test is retired in favour of
    // the new shared-trunk test, which asserts the real current behaviour.
    //
    // Same reasoning for the Cambridge/King's Lynn branch (the other branch
    // past Hitchin): Baldock (BDK) is no longer exclusive to this line
    // either, since Task 5.13's own semi-fast Cambridge service also stops
    // there (station overlap only, not a shared trunk - see
    // bdk_station_overlap_matches_great_northern_kings_lynn_and_thameslink_cambridge_as_independent_exclusive_segments
    // below). Meldreth (MEL), which that file's own MEL/STH/FXN EXCLUSION
    // header comment confirms Thameslink's semi-fast service skips
    // entirely, remains genuinely untouched by any other curated line and
    // takes over as this test's proof point.
    #[test]
    fn gn_kings_lynn_cambridge_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-2",
            "Points failure at Meldreth",
            "Points failure causing delays to GN train services.",
            &["GN"],
            &["MEL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["great-northern-kings-lynn".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.10. Cambridge (CBG) is also xc-stansted.toml's own terminus,
    // reached via a physically distinct route (Ely/March, not Hitchin/
    // Royston) - per that file's own STATION OVERLAP comment, this is
    // station overlap only, not a shared trunk. Confirms an incident there
    // matches both lines independently, each still scoped ExclusiveSegment,
    // never SharedSegment - same pattern as
    // vic_station_overlap_matches_brighton_main_line_chatham_and_oxted_uckfield_as_independent_exclusive_segments
    // above.
    //
    // Updated by Task 5.13 (thameslink-cambridge.toml): that file's own
    // Cambridge Line branch also terminates at CBG (its own
    // `thameslink-cambridge-branch` segment - see that file's own OVERLAP
    // (d) header comment), so it now joins this set as a third independent
    // exclusive-segment match.
    #[test]
    fn cbg_station_overlap_matches_great_northern_kings_lynn_and_xc_stansted_as_independent_exclusive_segments() {
        // Cambridge is also greater-anglia-west-anglia.toml's (`waml-mainline`)
        // and greater-anglia-norfolk-branches.toml's (`breckland-line`) own
        // terminus (both Batch 2) -- two more independent ExclusiveSegment
        // matches by the same station-overlap pattern already established
        // by west_anglia_cambridge_is_station_overlap_only_with_xc_stansted.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-3",
            "Signal failure at Cambridge",
            "Signal failure causing delays to train services.",
            &["GN"],
            &["CBG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "great-northern-kings-lynn".to_string(),
                "xc-stansted".to_string(),
                "thameslink-cambridge".to_string(),
                "greater-anglia-west-anglia".to_string(),
                "greater-anglia-norfolk-branches".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.11 (great-northern-suburban.toml). This file's research
    // (documented in its own SHARED-TRUNK RESEARCH FINDING header comment)
    // confirmed the Moorgate suburban service physically joins the East
    // Coast Main Line at Finsbury Park and runs over the same
    // `gn-ecml-slow-lines` corridor `great-northern-kings-lynn.toml` already
    // documents as far as Welwyn Garden City - but with a different calling
    // pattern (Moorgate stops at extra local stations that file's semi-fast
    // service skips), so - mirroring `southeastern-highspeed.toml`'s own
    // decision for the same "same track, different calling pattern"
    // situation - the segment name is deliberately NOT reused. This file is
    // therefore genuinely standalone with respect to cross-file segment
    // sharing (no `SharedSegment` propagation test is added, per COMMON.md's
    // own exception for a standalone line). This test confirms an incident
    // on this line's own exclusive `gn-moorgate-hertford-branch` segment
    // (Winchmore Hill - not a station either sibling GN file touches) stays
    // exclusive to this line alone - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn gn_suburban_hertford_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-4",
            "Signal failure at Winchmore Hill",
            "Signal failure causing delays to GN train services.",
            &["GN"],
            &["WIH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["great-northern-suburban".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.11. Same for the Welwyn Garden City branch's own local-only
    // stops (the other branch past Alexandra Palace) - an incident at
    // Oakleigh Park, a station `great-northern-kings-lynn.toml`'s semi-fast
    // service never calls at, should also stay exclusive to this line, and
    // shouldn't spuriously pull in the Hertford Loop branch's own segment
    // name either (the two branches use different segment names despite
    // being the same file/line).
    #[test]
    fn gn_suburban_wgc_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-5",
            "Points failure at Oakleigh Park",
            "Points failure causing delays to GN train services.",
            &["GN"],
            &["OKL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["great-northern-suburban".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.11. Finsbury Park (FPK) is also `great-northern-kings-lynn.toml`'s
    // own `gn-ecml-slow-lines` station - per this file's own SHARED-TRUNK
    // RESEARCH FINDING header comment, this is a genuine physical overlap
    // (both services' trains run over the same East Coast Main Line slow
    // lines here) but with different calling patterns, so it's deliberately
    // kept as station overlap only, not a shared segment name - same
    // judgment call as the AFK/Ramsgate/LBG overlaps between Southeastern
    // and HS1 exercised above. Confirms an incident at Finsbury Park matches
    // both lines independently, each still scoped ExclusiveSegment, never
    // SharedSegment.
    //
    // Updated by Task 5.13 (thameslink-cambridge.toml): that file also has a
    // station at FPK (its own `thameslink-cambridge-peterborough-trunk`
    // segment - the Canal Tunnels' connection to the ECML, physically the
    // FAST lines here rather than `gn-ecml-slow-lines`, per that file's own
    // OVERLAP (a) header comment), so it now joins this set as a third
    // independent exclusive-segment match.
    #[test]
    fn fpk_station_overlap_matches_great_northern_suburban_and_kings_lynn_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-6",
            "Signal failure at Finsbury Park",
            "Signal failure causing delays to GN train services.",
            &["GN"],
            &["FPK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "great-northern-suburban".to_string(),
                "great-northern-kings-lynn".to_string(),
                "thameslink-cambridge".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.12 (thameslink-bedford.toml). This file's `mml-bedford-
    // st-pancras` segment (Kentish Town through Bedford) is named and
    // documented, per this file's own header comment, as the segment
    // Batch 7's EMR Midland Main Line file is required to cite and reuse
    // verbatim for its own Bedford-St Pancras section - but that sibling
    // file doesn't exist in this worktree yet, so no SharedSegment test for
    // that pairing is added here (would fail to compile/pass without the
    // sibling). Until that file exists and reuses the name, this segment is
    // exclusive to this line alone: an incident at Harpenden, a station on
    // that segment untouched by any other curated line, should stay
    // exclusive to this line - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn thameslink_bedford_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-1",
            "Signal failure at Harpenden",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["HPD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["thameslink-bedford".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.12. St Pancras International (STP) is where this file's
    // northern branch meets thameslink-core.toml's own terminus, and is
    // also southeastern-highspeed.toml's own terminus (`hs1-domestic`).
    // Per this file's own SEGMENT NAMING header comment, this is
    // deliberately kept as station overlap only in every direction: this
    // file's own `mml-bedford-st-pancras` segment name is NOT reused by
    // thameslink-core (whose `thameslink-core` segment also covers
    // Farringdon/City Thameslink/Blackfriars/London Bridge, none of which
    // this branch file touches - reusing the name verbatim would
    // incorrectly mark those untouched stations SharedSegment too, exactly
    // the trap southeastern-highspeed.toml's own header comment already
    // flags for `seml-coast`/`chatham-medway`). Confirms an incident at STP
    // matches all three lines independently, each still scoped
    // ExclusiveSegment, never SharedSegment - mirrors
    // afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments
    // above.
    //
    // Updated by Task 5.13 (thameslink-cambridge.toml): that file's own
    // Cambridge/Peterborough branch also meets the core at STP (its own
    // `thameslink-cambridge-peterborough-trunk` segment, diverging into the
    // Canal Tunnels towards the ECML rather than north up the Midland Main
    // Line like thameslink-bedford - see that file's own STP header
    // comment), so it now joins this set as a fourth independent
    // exclusive-segment match.
    #[test]
    fn stp_station_overlap_matches_thameslink_core_bedford_and_highspeed_as_independent_exclusive_segments() {
        // St Pancras is also emr-connect.toml's and emr-midland-main-line.toml's
        // own terminus (both Batch 7, merged separately), which genuinely
        // share track London-Bedford-ward and correspondingly share the
        // `emr-mml-south` segment name with EACH OTHER -- SharedSegment
        // between those two specifically, while staying independent
        // ExclusiveSegment matches relative to the four Thameslink/HS1
        // lines, which use entirely distinct segment names.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-2",
            "Signal failure at St Pancras International",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["STP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "thameslink-core".to_string(),
                "thameslink-bedford".to_string(),
                "southeastern-highspeed".to_string(),
                "thameslink-cambridge".to_string(),
                "emr-connect".to_string(),
                "emr-midland-main-line".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "emr-connect" | "emr-midland-main-line" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // Cross-batch follow-up (resolved): `lines/emr-midland-main-line.toml`'s
    // "Bedford-St Pancras: cross-batch dependency ruling" header comment
    // confirms Luton Airport Parkway, Luton and Bedford are genuine
    // station-overlap-only stations between the two EMR files
    // (`emr-midland-main-line` and `emr-connect`, which genuinely share
    // `emr-mml-south`'s calling pattern with EACH OTHER and so report
    // SharedSegment for one another) and `thameslink-bedford.toml`'s
    // `mml-bedford-st-pancras` segment (which calls at 13 additional local
    // stations neither EMR service stops at, so it does NOT share the
    // segment name and reports ExclusiveSegment). Mirrors the STP case
    // already exercised by
    // `stp_station_overlap_matches_thameslink_core_bedford_and_highspeed_as_independent_exclusive_segments`
    // above; this test covers the three remaining overlap stations.
    #[test]
    fn ltn_lut_bdm_station_overlap_between_emr_and_thameslink_bedford_stays_exclusive_for_thameslink() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        for (crs, station_name) in [
            ("LTN", "Luton Airport Parkway"),
            ("LUT", "Luton"),
            ("BDM", "Bedford"),
        ] {
            let inc = incident(
                &format!("EMR-TL-{crs}"),
                &format!("Signal failure at {station_name}"),
                &format!("Signal failure causing delays to train services at {station_name}."),
                &["EM"],
                &[crs],
            );
            let matches = lines_affected_by(&inc, &lines, &registry);
            let by_id: HashMap<String, MatchScope> = matches.iter().map(|m| (m.line.id.clone(), m.scope)).collect();
            assert_eq!(
                by_id.keys().cloned().collect::<HashSet<_>>(),
                HashSet::from([
                    "emr-midland-main-line".to_string(),
                    "emr-connect".to_string(),
                    "thameslink-bedford".to_string(),
                ]),
                "unexpected match set for {crs}"
            );
            assert_eq!(by_id.get("emr-midland-main-line"), Some(&MatchScope::SharedSegment), "{crs}");
            assert_eq!(by_id.get("emr-connect"), Some(&MatchScope::SharedSegment), "{crs}");
            assert_eq!(by_id.get("thameslink-bedford"), Some(&MatchScope::ExclusiveSegment), "{crs}");
        }
    }

    // Task 5.13 (thameslink-cambridge.toml). Per that file's own OVERLAP (c)
    // header comment: the direct Peterborough branch (Arlesey, Biggleswade,
    // Sandy, St Neots, Huntingdon, Peterborough) is a genuine shared trunk
    // with great-northern-kings-lynn.toml's own `gn-peterborough-branch`
    // segment - Stevenage station's own Wikipedia services section confirms
    // this line runs "2 tph to Peterborough (all stations)", an identical
    // calling pattern to that file's own complete station list for the
    // branch (unlike the Cambridge Line branch, where this line's semi-fast
    // pattern skips Meldreth/Shepreth/Foxton - see
    // bdk_station_overlap_matches_great_northern_kings_lynn_and_thameslink_cambridge_as_independent_exclusive_segments
    // below). This replaces the previous exclusive-segment expectation for
    // Huntingdon (now genuinely shared) - mirrors
    // swr_shared_trunk_incident_propagates above.
    #[test]
    fn gn_peterborough_branch_shared_trunk_incident_propagates_to_thameslink_cambridge() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-1",
            "Signal failure at Huntingdon",
            "Signal failure causing delays to GN train services.",
            &["GN"],
            &["HUN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["great-northern-kings-lynn".to_string(), "thameslink-cambridge".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Task 5.13. Baldock (BDK) is on this line's own Cambridge Line branch
    // (`thameslink-cambridge-branch`) and also
    // great-northern-kings-lynn.toml's own `gn-cambridge-kings-lynn-branch`
    // station - per that file's own OVERLAP (d) header comment, this is
    // station overlap only, not a shared trunk, because this line's
    // semi-fast Cambridge service skips Meldreth/Shepreth/Foxton that GN's
    // own segment treats as consecutive stops (the same "same track,
    // different calling pattern" situation as the FPK/Stevenage overlaps
    // above). Confirms an incident at Baldock matches both lines
    // independently, each still scoped ExclusiveSegment, never
    // SharedSegment - mirrors
    // cbg_station_overlap_matches_great_northern_kings_lynn_and_xc_stansted_as_independent_exclusive_segments
    // above.
    #[test]
    fn bdk_station_overlap_matches_great_northern_kings_lynn_and_thameslink_cambridge_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-3",
            "Points failure at Baldock",
            "Points failure causing delays to train services.",
            &["TL"],
            &["BDK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["great-northern-kings-lynn".to_string(), "thameslink-cambridge".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.14 (thameslink-southern.toml). An incident on this line's own
    // exclusive Catford Loop stretch (`thameslink-sevenoaks-catford`,
    // between Elephant & Castle and the rejoin with the Chatham Main Line
    // at Shortlands - see that segment below) should stay exclusive to
    // this line alone - mirrors swr_exclusive_segment_incident_does_not_propagate
    // and elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn thameslink_sevenoaks_catford_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-4",
            "Signal failure at Catford",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["CTF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["thameslink-southern".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.14. The Sutton Loop (`thameslink-sutton-loop`) is exclusive to
    // this line everywhere except the single Wimbledon station overlap
    // (see wim_station_overlap_matches_swr_trunk_and_thameslink_southern_
    // as_independent_segments below) - an incident elsewhere on the loop
    // should stay exclusive, mirroring the Catford Loop test above.
    #[test]
    fn thameslink_sutton_loop_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-5",
            "Signal failure at Sutton Common",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["SUC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["thameslink-southern".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.14. Genuine physical track sharing exists here (the Sevenoaks
    // branch's Catford Loop stretch rejoins the Chatham Main Line at
    // Shortlands and shares track with southeastern-chatham.toml's own
    // approach as far as Swanley - see thameslink-southern.toml's own
    // header comment), but it is deliberately NOT modelled as a
    // SharedSegment: southeastern-chatham.toml's own `chatham-london`
    // segment bundles this stretch together with its own Victoria-Herne
    // Hill approach (which this file's Sevenoaks branch never touches), so
    // reusing that name verbatim would incorrectly also mark Victoria/BKJ/
    // Herne Hill as SharedSegment - exactly the trap
    // thameslink-cambridge.toml's own OVERLAP (a)/(b) comment warns about.
    // Confirms an incident at Swanley matches both lines independently,
    // each still scoped ExclusiveSegment, never SharedSegment - mirrors
    // bfr_station_overlap_matches_thameslink_core_and_thameslink_southern_
    // as_independent_exclusive_segments above.
    #[test]
    fn say_station_overlap_matches_chatham_and_thameslink_southern_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-6",
            "Signal failure at Swanley",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["SAY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-chatham".to_string(), "thameslink-southern".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.14. REVISED after review: the flagged Task 5.6 coordination
    // (London Bridge/East Croydon) does NOT clear this batch's two-source
    // bar after all - an earlier draft reused `southern-bml-north` here and
    // asserted a genuine SharedSegment pair, but the only non-Wikipedia
    // source found (thetrainline.com) is not one of COMMON.md's four
    // approved second-source categories and only shows service-existence,
    // not physical track sharing. See thameslink-southern.toml's own
    // BRIGHTON BRANCH header comment for the full writeup. This file's own
    // Brighton branch now uses its own segment name (`thameslink-brighton`)
    // at East Croydon instead, so an incident here is a THIRD independent
    // ExclusiveSegment station-overlap match alongside
    // southern-brighton-main-line's own `southern-bml-north` and
    // southern-oxted-uckfield's own `oxted-trunk` - mirrors
    // bfr_station_overlap_matches_thameslink_core_and_thameslink_southern_
    // as_independent_exclusive_segments above.
    #[test]
    fn ecr_station_overlap_matches_brighton_main_line_and_oxted_uckfield_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-7",
            "Signal failure at East Croydon",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["ECR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "thameslink-southern".to_string(),
                "southern-brighton-main-line".to_string(),
                "southern-oxted-uckfield".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.14. Blackfriars is this file's own core-boundary junction for
    // the Sevenoaks/Sutton Loop branches (`thameslink-southern-trunk`)
    // and also thameslink-core.toml's own station (`thameslink-core`) -
    // station overlap only, same judgment call as every other core-boundary
    // overlap in this batch (STP in thameslink-bedford.toml/
    // thameslink-cambridge.toml, LBG above) - mirrors
    // stp_station_overlap_matches_thameslink_core_bedford_and_highspeed_as_independent_exclusive_segments
    // above.
    #[test]
    fn bfr_station_overlap_matches_thameslink_core_and_thameslink_southern_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-8",
            "Signal failure at Blackfriars",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["BFR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["thameslink-core".to_string(), "thameslink-southern".to_string()]));
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 5.14. Wimbledon is this file's own Sutton Loop station
    // (`thameslink-sutton-loop`, reached via Haydons Road) and also all
    // three SWR files' own shared `swr-trunk-waterloo` station (reached via
    // Clapham Junction) - a physically distinct approach, so station
    // overlap only against the SWR trio, not a fourth member of their own
    // shared trunk. Confirms an incident at Wimbledon still propagates
    // across the three SWR lines as SharedSegment (mirrors
    // swr_shared_trunk_incident_propagates above) while this file's own
    // match stays independently ExclusiveSegment.
    #[test]
    fn wim_station_overlap_matches_swr_trunk_and_thameslink_southern_as_independent_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-9",
            "Signal failure at Wimbledon",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["WIM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "swr-south-west-main".to_string(),
                "swr-portsmouth-direct".to_string(),
                "swr-alton".to_string(),
                "thameslink-southern".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "thameslink-southern" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment, "thameslink-southern should be ExclusiveSegment (different segment name)");
            } else {
                assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
            }
        }
    }

    // Task 5.14. Sevenoaks is this file's own branch terminus
    // (`thameslink-sevenoaks-otford`, reached via Bat & Ball) and also
    // southeastern-main-line.toml's own station (`seml-weald`, reached via
    // Dunton Green) - two physically distinct approaches per Sevenoaks
    // station's own Wikipedia article (see thameslink-southern.toml's own
    // SEV comment), so station overlap only, not a shared segment.
    #[test]
    fn sev_station_overlap_matches_seml_and_thameslink_southern_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-10",
            "Signal failure at Sevenoaks",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["SEV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-main-line".to_string(), "thameslink-southern".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Task 10.1 (ScotRail Central Belt): no sibling ScotRail line exists yet
    // in `lines/` to share `scotrail-central-belt`/
    // `scotrail-central-belt-edinburgh-throat` with, so there is no
    // shared-segment propagation to assert today -- see
    // `lines/scotrail-central-belt.toml`'s own comments for the segment-
    // naming groundwork left for Task 10.2. Only the exclusive-segment
    // non-propagation assertion applies for now, mirroring
    // `swr_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_central_belt_exclusive_segment_incident_does_not_propagate() {
        // Falkirk High is also lumo.toml's own station (merged separately,
        // Batch 10), on its exclusive `lumo-glasgow` segment -- station-level
        // overlap, distinct segment names, both stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-1",
            "Signal failure at Falkirk High",
            "Signal failure causing delays to ScotRail services at Falkirk High.",
            &["SR"],
            &["FKK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-central-belt".to_string(), "lumo".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    // Task 10.2 (ScotRail Glasgow Suburban): no other `lines/*.toml` file
    // touches North Clyde/Argyle Line territory yet, and this file
    // deliberately stops short of sharing a segment with
    // `scotrail-central-belt.toml` (see this file's own comments on the
    // Airdrie/Bathgate boundary decision) -- so there is no shared-segment
    // propagation to assert today, mirroring
    // `scotrail_central_belt_exclusive_segment_incident_does_not_propagate`.
    // Only the exclusive-segment non-propagation assertion applies for now.
    #[test]
    fn scotrail_glasgow_suburban_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-2",
            "Signal failure at Milngavie",
            "Signal failure causing delays to ScotRail services at Milngavie.",
            &["SR"],
            &["MLN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-glasgow-suburban".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.3 (ScotRail Ayrshire Coast): no other `lines/*.toml` file
    // touches this line's Glasgow-Ayr trunk or its Girvan/Stranraer branch
    // yet (see `lines/scotrail-ayrshire.toml`'s own comments), so there is
    // no shared-segment propagation to assert today -- only exclusive-
    // segment non-propagation for each of this file's two segments,
    // mirroring `scotrail_central_belt_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_ayrshire_glasgow_ayr_trunk_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-3",
            "Signal failure at Kilwinning",
            "Signal failure causing delays to ScotRail services at Kilwinning.",
            &["SR"],
            &["KWN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-ayrshire".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn scotrail_ayrshire_stranraer_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-4",
            "Level crossing fault at Girvan",
            "Level crossing fault causing delays to ScotRail services at Girvan.",
            &["SR"],
            &["GIR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-ayrshire".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.4 (ScotRail Fife Circle + Borders Railway): this file
    // bundles two genuinely separate routes with distinct segment-name
    // prefixes (`scotrail-fife-circle*` / `scotrail-borders`), neither of
    // which is shared with any other `lines/*.toml` file today (see
    // `lines/scotrail-fife-borders.toml`'s own comments on why the
    // Edinburgh Waverley/Haymarket overlap with `scotrail-central-belt`
    // isn't modelled as a shared segment) -- so, mirroring Task 10.3's
    // two-exclusive-segments treatment, one exclusive-segment
    // non-propagation assertion per route, not a shared-segment one.
    #[test]
    fn scotrail_fife_circle_exclusive_segment_incident_does_not_propagate() {
        // Kirkcaldy is also lner-ecml.toml's own station (its own
        // `ecml-aberdeen` segment, merged separately), on the Edinburgh-
        // Aberdeen main line via the Forth Bridge -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-5",
            "Signal failure at Kirkcaldy",
            "Signal failure causing delays to ScotRail services at Kirkcaldy.",
            &["SR"],
            &["KDY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-fife-borders".to_string(), "lner-ecml".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment", m.line.id);
        }
    }

    #[test]
    fn scotrail_borders_railway_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-6",
            "Points failure at Galashiels",
            "Points failure causing delays to ScotRail services at Galashiels.",
            &["SR"],
            &["GAL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-fife-borders".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.5 (ScotRail Highland Main Line): no sibling `lines/*.toml`
    // file touches this line's `scotrail-highland-main-line` segment yet
    // (see `lines/scotrail-highland-main-line.toml`'s own comments on the
    // Inverness-area segment-naming handoff left open for Tasks
    // 10.6/10.7/10.10), so there is no shared-segment propagation to
    // assert today -- only exclusive-segment non-propagation, mirroring
    // `scotrail_central_belt_exclusive_segment_incident_does_not_propagate`.
    // The incident is placed at Kingussie (on the line's exclusive
    // Perth-Carrbridge segment), not Inverness, since Inverness's own
    // segment fate is deliberately left open for a later task to decide.
    #[test]
    fn scotrail_highland_main_line_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-7",
            "Signal failure at Kingussie",
            "Signal failure causing delays to ScotRail services at Kingussie.",
            &["SR"],
            &["KIN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-highland-main-line".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.6 (ScotRail Far North Line): the Wick and Thurso branches
    // each get their own exclusive segment
    // (`scotrail-far-north-wick`/`scotrail-far-north-thurso`), used only by
    // this one file, so an incident on either branch should stay local to
    // `scotrail-far-north` with `ExclusiveSegment` scope -- mirroring
    // `swr_exclusive_segment_incident_does_not_propagate`.
    //
    // This file also tags Inverness-Dingwall as
    // `scotrail-inverness-dingwall-trunk`, a segment name Task 10.7's
    // Kyle of Lochalsh Line (`lines/scotrail-kyle.toml`) now independently
    // confirmed and reused for its own Inverness-Dingwall stations -- see
    // `scotrail_shared_inverness_dingwall_trunk_incident_propagates` below
    // for the resulting cross-file `SharedSegment` propagation assertion.
    // The same-file trunk-vs-branch structure this file also has
    // (Inverness-Georgemas Junction shared by both the Wick and Thurso
    // branches) is separately asserted directly via `SegmentRegistry` in
    // `crates/aggregator/src/segments.rs`, since that particular sharing
    // is internal to one line/file and `MatchScope::SharedSegment` only
    // applies across distinct line files.
    #[test]
    fn scotrail_far_north_wick_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-8",
            "Points failure at Wick",
            "Points failure causing delays to ScotRail services at Wick.",
            &["SR"],
            &["WCK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-far-north".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn scotrail_far_north_thurso_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-9",
            "Signal failure at Thurso",
            "Signal failure causing delays to ScotRail services at Thurso.",
            &["SR"],
            &["THS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-far-north".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.7 (ScotRail Kyle of Lochalsh Line): this line's own exclusive
    // track west of Dingwall Junction (Garve through Kyle of Lochalsh) is
    // tagged `scotrail-kyle-exclusive`, used only by this one file, so an
    // incident there should stay local to `scotrail-kyle` with
    // `ExclusiveSegment` scope -- mirroring
    // `swr_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_kyle_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-10",
            "Landslip near Strathcarron",
            "Landslip causing delays to ScotRail services at Strathcarron.",
            &["SR"],
            &["STC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-kyle".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.7 (ScotRail Kyle of Lochalsh Line): the load-bearing
    // shared-trunk test this Batch 10 pairing exists for. Task 10.6
    // (`scotrail-far-north.toml`) reserved the segment name
    // `scotrail-inverness-dingwall-trunk` for the Inverness-Dingwall
    // stretch both lines' services physically run over; this file
    // independently re-confirmed that shared-track claim (see
    // `lines/scotrail-kyle.toml`'s own Sources comments) and reused the
    // exact same segment name. Now that both files exist,
    // `SegmentRegistry::is_shared` correctly reports the segment shared
    // between two distinct line IDs, so an incident at Dingwall (a
    // station on that shared segment) should match BOTH
    // `scotrail-far-north` and `scotrail-kyle`, each with
    // `MatchScope::SharedSegment` -- mirroring
    // `swr_shared_trunk_incident_propagates`.
    #[test]
    fn scotrail_shared_inverness_dingwall_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-11",
            "Signal failure at Dingwall",
            "Signal failure causing delays to ScotRail services at Dingwall.",
            &["SR"],
            &["DIN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-far-north".to_string(), "scotrail-kyle".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Task 10.8/10.9 (ScotRail West Highland Line pairing): the
    // load-bearing shared-trunk test this Batch 10 pairing exists for,
    // mirroring `scotrail_shared_inverness_dingwall_trunk_incident_propagates`.
    // Task 10.8 (`scotrail-west-highland-fort-william.toml`) reserved the
    // segment name `scotrail-west-highland-glasgow-crianlarich` for the
    // Glasgow-Crianlarich stretch both West Highland Line branches
    // physically run over (combined trains splitting/joining at
    // Crianlarich); Task 10.9 (`scotrail-west-highland-oban.toml`)
    // independently re-confirmed that shared-track claim against a fresh
    // fetch of Wikipedia's "West Highland Line" and "Crianlarich railway
    // station" articles (see that file's own verification-note comments)
    // and reused the exact same segment name. `SegmentRegistry::is_shared`
    // now correctly reports the segment shared between two distinct line
    // IDs, so an incident at Ardlui (a station on that shared segment)
    // matches BOTH `scotrail-west-highland-fort-william` and
    // `scotrail-west-highland-oban`, each with `MatchScope::SharedSegment`.
    #[test]
    fn scotrail_west_highland_shares_glasgow_crianlarich_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-12",
            "Landslip near Ardlui",
            "Landslip causing delays to ScotRail services at Ardlui.",
            &["SR"],
            &["AUI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-west-highland-fort-william".to_string(),
                "scotrail-west-highland-oban".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Task 10.8: this line's own exclusive track between Crianlarich and
    // Fort William (tagged `scotrail-west-highland-fort-william-exclusive`,
    // used only by this one file) should stay local to
    // `scotrail-west-highland-fort-william` with `ExclusiveSegment` scope --
    // mirroring `scotrail_ayrshire_glasgow_ayr_trunk_incident_does_not_propagate`.
    #[test]
    fn scotrail_west_highland_fort_william_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-13",
            "Signal failure at Tulloch",
            "Signal failure causing delays to ScotRail services at Tulloch.",
            &["SR"],
            &["TUL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-west-highland-fort-william".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.8: the Mallaig extension beyond Fort William (tagged
    // `scotrail-west-highland-mallaig`, used only by this one file) is a
    // second, distinct exclusive segment within the same file -- mirroring
    // `scotrail_ayrshire_stranraer_branch_incident_does_not_propagate`'s
    // identical trunk+branch treatment.
    #[test]
    fn scotrail_west_highland_mallaig_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-14",
            "Landslip near Glenfinnan",
            "Landslip causing delays to ScotRail services at Glenfinnan.",
            &["SR"],
            &["GLF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-west-highland-fort-william".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.8/10.9: unlike the Crianlarich reservation above, this
    // line's Glasgow-area sharing with `scotrail-glasgow-suburban.toml`'s
    // own `scotrail-glasgow-suburban-west-trunk` segment (Dalmuir -
    // Dumbarton Central) is a REAL, already-merged sibling -- see this
    // line's own Sources comments for the independent verification. Task
    // 10.9 (`scotrail-west-highland-oban.toml`) independently confirmed
    // that Oban services also call at Dumbarton Central before diverging
    // near Craigendoran Junction (Dumbarton Central's own Wikipedia
    // article explicitly names "trains ... between Glasgow and Oban and
    // Mallaig") and reused this exact segment name for its own DMR/DBC
    // entries too, making this a genuine three-way shared segment. An
    // incident at Dumbarton Central should therefore match ALL THREE of
    // `scotrail-glasgow-suburban`, `scotrail-west-highland-fort-william`
    // and `scotrail-west-highland-oban` with `MatchScope::SharedSegment`,
    // mirroring `scotrail_shared_inverness_dingwall_trunk_incident_propagates`.
    #[test]
    fn scotrail_west_highland_shares_glasgow_suburban_west_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-15",
            "Signal failure at Dumbarton Central",
            "Signal failure causing delays to ScotRail services at Dumbarton Central.",
            &["SR"],
            &["DBC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-glasgow-suburban".to_string(),
                "scotrail-west-highland-fort-william".to_string(),
                "scotrail-west-highland-oban".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Post-review fix round 2: unlike Dumbarton Central above, Hyndland
    // (and Dalreoch/Partick alongside it) is NOT physically traversed by
    // West Highland Line trains -- they run non-stop between Glasgow
    // Queen Street and Dumbarton Central. Hyndland/Dalreoch/Partick were
    // previously mistakenly tagged onto the same
    // `scotrail-glasgow-suburban-west-trunk` segment name as DMR/DBC,
    // which made `SegmentRegistry::is_shared` (name-keyed, not
    // station-keyed) incorrectly report an incident here as shared with
    // the West Highland lines too. They are now retagged onto
    // `scotrail-glasgow-suburban-west-approach`, exclusive to this file --
    // see `lines/scotrail-glasgow-suburban.toml`'s own HYN comment for the
    // full explanation. An incident at Hyndland should therefore match
    // only `scotrail-glasgow-suburban`, with `ExclusiveSegment` scope.
    #[test]
    fn scotrail_glasgow_suburban_west_approach_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-15B",
            "Signal failure at Hyndland",
            "Signal failure causing delays to ScotRail services at Hyndland.",
            &["SR"],
            &["HYN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-glasgow-suburban".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 3.1 (station-catalogue-completeness plan), FILL-IN piece:
    // Kilpatrick (KPT) is one of the 19 previously-missing minor stations
    // added to `lines/scotrail-glasgow-suburban.toml` by this task. It sits
    // on the Dalmuir-Dumbarton Central stretch itself, so (per that file's
    // own KPT comment) it inherits the genuinely-shared
    // `scotrail-glasgow-suburban-west-trunk` segment rather than the
    // exclusive `west-approach` segment most of the other 18 new stations
    // use. Before this task, `has_station("KPT")` returned false for this
    // line and an incident there was invisible to the matcher entirely.
    // Neither West Highland sibling file lists KPT itself (their own
    // stations skip straight from DMR to DBC, per their own Sources
    // comments: "WHL trains run non-stop" over this stretch), so unlike
    // `scotrail_west_highland_shares_glasgow_suburban_west_trunk_incident_propagates`
    // (which uses the pre-existing, all-three-files DBC station) this
    // incident only station-matches `scotrail-glasgow-suburban` itself --
    // but its scope is still correctly `SharedSegment`, because
    // `SegmentRegistry::is_shared` keys on the `west-trunk` segment NAME,
    // which the West Highland files do reuse, not on which specific
    // stations carry it. This proves the segment-name inheritance is
    // correct for a station that didn't exist in the catalogue at all
    // until this task.
    #[test]
    fn scotrail_glasgow_suburban_new_kilpatrick_station_on_shared_west_trunk_segment() {
        let lines = load_line("scotrail-glasgow-suburban");
        let suburban = lines.get("scotrail-glasgow-suburban").expect("scotrail-glasgow-suburban line should exist");
        assert!(suburban.has_station("KPT"), "Kilpatrick (KPT) should now be a station on scotrail-glasgow-suburban");

        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-17",
            "Signal failure at Kilpatrick",
            "Signal failure causing delays to ScotRail services at Kilpatrick.",
            &["SR"],
            &["KPT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-glasgow-suburban".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::SharedSegment);
    }

    // Task 3.1, BRANCH-RESEARCH piece: the previously-unmodelled Lanarkshire
    // branch group (Whifflet spur, Hamilton Circle, Larkhall branch, Lanark
    // branch) added to `lines/scotrail-glasgow-suburban.toml` by this task.
    // Whifflet (WFF) itself is a real junction (the Coatbridge Central
    // terminus spur diverges there), on the new exclusive
    // `scotrail-glasgow-suburban-whifflet-branch` segment -- not shared with
    // any sibling `lines/*.toml` file (grepped clean before this task, see
    // that file's own Task 3.1 BRANCH-RESEARCH comment). Mirrors
    // `scotrail_glasgow_suburban_exclusive_segment_incident_does_not_propagate`
    // but for a station that didn't exist in the catalogue at all until this
    // task.
    #[test]
    fn scotrail_glasgow_suburban_new_whifflet_branch_incident_does_not_propagate() {
        let lines = load_line("scotrail-glasgow-suburban");
        let suburban = lines.get("scotrail-glasgow-suburban").expect("scotrail-glasgow-suburban line should exist");
        assert!(suburban.has_station("WFF"), "Whifflet (WFF) should now be a station on scotrail-glasgow-suburban");

        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-18",
            "Signal failure at Whifflet",
            "Signal failure causing delays to ScotRail services at Whifflet.",
            &["SR"],
            &["WFF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-glasgow-suburban".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.9 (ScotRail West Highland Line, Oban arm): this line's own
    // exclusive track beyond Crianlarich (tagged
    // `scotrail-west-highland-oban-exclusive`, used only by this one file)
    // should stay local to `scotrail-west-highland-oban` with
    // `ExclusiveSegment` scope -- mirroring
    // `scotrail_west_highland_fort_william_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_west_highland_oban_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-16",
            "Signal failure at Taynuilt",
            "Signal failure causing delays to ScotRail services at Taynuilt.",
            &["SR"],
            &["TAY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-west-highland-oban".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.9 (fix round 1, post-review): GLQ (Glasgow Queen Street) is
    // reused verbatim as `scotrail-west-highland-glasgow-terminus` by both
    // `scotrail-west-highland-fort-william` and
    // `scotrail-west-highland-oban` -- see both files' own GLQ comments
    // for why (the "combined trains ... splitting at Crianlarich" sourcing
    // means the shared corridor genuinely starts at GLQ itself, not just
    // from Dalmuir onward). GLQ is ALSO a real `[[stations]]` entry in
    // `scotrail-central-belt.toml` (its own exclusive
    // `scotrail-central-belt` segment) and `scotrail-glasgow-suburban.toml`
    // (its own `scotrail-glasgow-suburban-core` segment) -- both genuinely
    // different physical platform groups/services at the same named
    // station, unaffected by this fix.
    //
    // UPDATE (added alongside `lines/scotrail-bathgate.toml`): that file's
    // own GLQ entry reuses `scotrail-glasgow-suburban.toml`'s exact
    // `scotrail-glasgow-suburban-core` segment name (a genuine shared
    // fact -- see that file's own sourcing), so Glasgow Suburban's own
    // scope at GLQ changes from `ExclusiveSegment` to `SharedSegment` too,
    // and `scotrail-bathgate` itself now also matches here. So an incident
    // at GLQ correctly matches all five lines, with three different
    // scopes: the two West Highland lines get `SharedSegment` on their own
    // `scotrail-west-highland-glasgow-terminus` segment; Glasgow Suburban
    // and Bathgate get `SharedSegment` on their own, separate
    // `scotrail-glasgow-suburban-core` segment; Central Belt and Lumo stay
    // `ExclusiveSegment` on their own unrelated segments.
    #[test]
    fn scotrail_west_highland_shares_glasgow_terminus_incident_propagates() {
        // Glasgow Queen Street is also lumo.toml's own terminus (merged
        // separately, Batch 10), on its exclusive `lumo-glasgow` segment --
        // an independent match, ExclusiveSegment (station overlap,
        // distinct segment name, not part of either shared trunk below).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-17",
            "Points failure at Glasgow Queen Street",
            "Points failure causing delays to ScotRail services at Glasgow Queen Street.",
            &["SR"],
            &["GLQ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-central-belt".to_string(),
                "scotrail-glasgow-suburban".to_string(),
                "scotrail-bathgate".to_string(),
                "scotrail-west-highland-fort-william".to_string(),
                "scotrail-west-highland-oban".to_string(),
                "lumo".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "scotrail-west-highland-fort-william"
                | "scotrail-west-highland-oban"
                | "scotrail-glasgow-suburban"
                | "scotrail-bathgate" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // Task 10.10 (ScotRail Aberdeen - Inverness Line): this line's own
    // exclusive track (Aberdeen through Inverness Airport, tagged
    // `scotrail-aberdeen-inverness`) is used only by this one file, so an
    // incident there should stay local with `ExclusiveSegment` scope --
    // mirroring `swr_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_aberdeen_inverness_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-18",
            "Signal failure at Elgin",
            "Signal failure causing delays to ScotRail services at Elgin.",
            &["SR"],
            &["ELG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-aberdeen-inverness".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.10 (ScotRail Aberdeen - Inverness Line): the load-bearing
    // shared-trunk test this pairing exists for. Task 10.5
    // (`scotrail-highland-main-line.toml`) reserved the segment name
    // `scotrail-highland-inverness-approach` for its own Inverness station
    // entry, deliberately not assuming a shared segment on
    // platform-sharing evidence alone; this file independently confirmed
    // genuine track-sharing (Wikipedia's Millburn Junction detail,
    // cross-checked against railwaycodes.org.uk's own ELR database -- see
    // `lines/scotrail-aberdeen-inverness.toml`'s own Sources comments) and
    // reused the exact same segment name.
    //
    // Inverness is also a station on `scotrail-far-north` and
    // `scotrail-kyle` (tagged `scotrail-inverness-dingwall-trunk`, that
    // pair's own independently-confirmed shared trunk from Task 10.6/10.7)
    // -- `LineDefinition::has_station` matches on station presence
    // regardless of segment name, so an incident at Inverness hits all
    // four lines, mirroring
    // `scotrail_west_highland_shares_glasgow_terminus_incident_propagates`'s
    // precedent for a station where two independent shared-trunk pairs
    // happen to converge. Since Inverness sits on a genuine cross-file
    // shared segment for *both* pairs (not merely a same-line exclusive
    // segment for either), all four lines get `MatchScope::SharedSegment`
    // here -- unlike the Glasgow Queen Street case, where only two of the
    // four lines had a cross-file shared segment at that station.
    #[test]
    fn scotrail_shared_highland_inverness_approach_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-19",
            "Points failure at Inverness",
            "Points failure causing delays to ScotRail services at Inverness.",
            &["SR"],
            &["INV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-highland-main-line".to_string(),
                "scotrail-aberdeen-inverness".to_string(),
                "scotrail-far-north".to_string(),
                "scotrail-kyle".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Follow-up task closing the Batch 10 gap flagged in
    // `scotrail-central-belt.toml` (Shotts and Bathgate had no dedicated
    // files). `scotrail-shotts.toml` has no internal branching -- one
    // exclusive segment, `scotrail-shotts-exclusive`, covering everything
    // from Glasgow Central to Slateford. An incident at Shotts itself (the
    // line's own namesake mid-corridor station) should therefore match
    // only `scotrail-shotts`, with `ExclusiveSegment` scope, mirroring
    // `scotrail_glasgow_suburban_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_shotts_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-20",
            "Signal failure at Shotts",
            "Signal failure causing delays to ScotRail services at Shotts.",
            &["SR"],
            &["SHS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-shotts".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 3.1 (station-catalogue-completeness plan), fix round 1
    // post-review: `scotrail-glasgow-suburban.toml`'s Task 3.1 FILL-IN
    // piece added Uddingston (UDD) and Bellshill (BLH), both of which were
    // already `[[stations]]` entries in this file (`scotrail-shotts.toml`)
    // -- an undisclosed cross-file collision the review caught, since this
    // file's own pre-existing CBL comment had explicitly pre-flagged this
    // exact scenario. Verified as genuine physical track sharing, not
    // coincidental station-name overlap: Wikipedia's "Shotts line" article
    // states "Until Holytown Junction the line [is] used by Argyle Line
    // services", i.e. Argyle Line services (this file's own sibling)
    // physically run over the same Uddingston-Bellshill stretch this
    // file's Shotts-branded services use. Both files' UDD/BLH entries were
    // retagged onto a new shared segment, `scotrail-uddingston-bellshill-
    // trunk` -- see both files' own UDD/BLH comments for the full sourcing.
    // An incident at Bellshill should therefore match both
    // `scotrail-shotts` and `scotrail-glasgow-suburban` with
    // `MatchScope::SharedSegment`, mirroring
    // `scotrail_shared_inverness_dingwall_trunk_incident_propagates`.
    #[test]
    fn scotrail_uddingston_bellshill_trunk_shares_glasgow_suburban_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-21",
            "Signal failure at Bellshill",
            "Signal failure causing delays to ScotRail services at Bellshill.",
            &["SR"],
            &["BLH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-shotts".to_string(), "scotrail-glasgow-suburban".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // `scotrail-bathgate.toml`'s own middle section (Drumgelloch through
    // Edinburgh Park) is genuinely exclusive to that file today -- no other
    // `lines/*.toml` file has a station entry there. An incident at
    // Bathgate itself (the line's own namesake town) should therefore
    // match only `scotrail-bathgate`, with `ExclusiveSegment` scope,
    // mirroring `scotrail_shotts_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_bathgate_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-21",
            "Points failure at Bathgate",
            "Points failure causing delays to ScotRail services at Bathgate.",
            &["SR"],
            &["BHG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-bathgate".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `scotrail-central-belt.toml`'s own Haymarket comment pre-emptively
    // reserved `scotrail-central-belt-edinburgh-throat` for whoever
    // eventually modelled the Shotts and Bathgate routings, on the sourced
    // basis that both genuinely rejoin the Falkirk High routing's own
    // track before Haymarket and Edinburgh Waverley (RAILSCOT/Wikipedia
    // "Airdrie-Bathgate rail link" and "Edinburgh-Bathgate line" for
    // Bathgate's own Newbridge Junction rejoin; Wikipedia "Haymarket
    // railway station", independently, for Shotts's own 1853 Slateford
    // connection). `scotrail-shotts.toml` and `scotrail-bathgate.toml` both
    // reuse this exact segment name for their own Haymarket/Edinburgh
    // Waverley entries, so an incident at Haymarket should match all three
    // of `scotrail-central-belt`, `scotrail-shotts` and `scotrail-bathgate`
    // with `MatchScope::SharedSegment`, mirroring
    // `scotrail_west_highland_shares_glasgow_suburban_west_trunk_incident_propagates`'s
    // three-way shared-segment shape.
    //
    // Haymarket is also a real, major interchange for several other
    // already-merged lines with no track-sharing claim sourced against this
    // throat (`scotrail-fife-borders`, `tpe-anglo-scottish`, `lner-ecml`,
    // `lumo`) -- each of those stays `ExclusiveSegment` on its own,
    // unrelated segment, mirroring
    // `scotrail_west_highland_shares_glasgow_terminus_incident_propagates`'s
    // mixed-scope shape at a heavily-overlapped hub station.
    #[test]
    fn scotrail_central_belt_edinburgh_throat_shared_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-22",
            "Overhead line damage at Haymarket",
            "Overhead line damage causing delays to ScotRail services at Haymarket.",
            &["SR"],
            &["HYM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-central-belt".to_string(),
                "scotrail-shotts".to_string(),
                "scotrail-bathgate".to_string(),
                "scotrail-fife-borders".to_string(),
                "tpe-anglo-scottish".to_string(),
                "lner-ecml".to_string(),
                "lumo".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "scotrail-central-belt" | "scotrail-shotts" | "scotrail-bathgate" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // `scotrail-glasgow-suburban.toml`'s own Bellgrove comment already
    // named Bathgate as one of the three eastbound splits from its North
    // Clyde core trackage. `scotrail-bathgate.toml` reuses that file's own
    // `scotrail-glasgow-suburban-core` segment name for its own Charing
    // Cross/Glasgow Queen Street/Bellgrove entries, so an incident at
    // Charing Cross should match both `scotrail-glasgow-suburban` and
    // `scotrail-bathgate`, each with `MatchScope::SharedSegment`, mirroring
    // `scotrail_shared_inverness_dingwall_trunk_incident_propagates`'s
    // two-way shared-segment shape. Charing Cross (unlike Glasgow Queen
    // Street itself, which several unrelated lines also touch -- see
    // `scotrail_west_highland_shares_glasgow_terminus_incident_propagates`)
    // is not a `[[stations]]` entry in any other `lines/*.toml` file, so
    // this test's own `matched_ids` stays a clean two-line set.
    #[test]
    fn scotrail_bathgate_shares_glasgow_suburban_core_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-23",
            "Signal failure at Charing Cross",
            "Signal failure causing delays to ScotRail services at Charing Cross.",
            &["SR"],
            &["CHC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-glasgow-suburban".to_string(), "scotrail-bathgate".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // `scotrail-glasgow-suburban.toml`'s own Airdrie comment already noted
    // that real electrified track continues beyond its own North Clyde
    // terminus towards Bathgate. `scotrail-bathgate.toml` reuses that
    // file's own `scotrail-glasgow-suburban-airdrie-branch` segment name
    // for its own Airdrie entry (the junction-in-service-pattern where
    // Bathgate-bound trains continue past the North Clyde terminus
    // pattern), so an incident at Airdrie should match both
    // `scotrail-glasgow-suburban` and `scotrail-bathgate`, each with
    // `MatchScope::SharedSegment`, mirroring
    // `scotrail_bathgate_shares_glasgow_suburban_core_incident_propagates`
    // immediately above.
    #[test]
    fn scotrail_bathgate_shares_glasgow_suburban_airdrie_branch_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-24",
            "Points failure at Airdrie",
            "Points failure causing delays to ScotRail services at Airdrie.",
            &["SR"],
            &["ADR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-glasgow-suburban".to_string(), "scotrail-bathgate".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment, "{} should be SharedSegment", m.line.id);
        }
    }

    // Task 10.2 (chiltern-main-line.toml catalogue-completeness pass).
    // Sudbury & Harrow Road, Princes Risborough and a full run of other
    // Marylebone-approach stations were previously omitted from this file
    // as "minor intermediate calls" per its own stale header comment.
    // Princes Risborough needed particular care: confirmed (see that
    // station's own comment in lines/chiltern-main-line.toml) to be
    // genuinely on this line's Marylebone-Bicester North-Banbury route,
    // not only the separate Oxford or Aylesbury routes.
    #[test]
    fn chiltern_main_line_has_previously_omitted_marylebone_approach_stations() {
        let lines = load_line("chiltern-main-line");
        let line = lines.get("chiltern-main-line").expect("chiltern-main-line should exist");
        assert!(line.has_station("SUD"), "chiltern-main-line should now include Sudbury & Harrow Road (SUD)");
        assert!(line.has_station("PRR"), "chiltern-main-line should now include Princes Risborough (PRR)");
    }

    // Task 10.2, continued: Lapworth, Widney Manor, Olton, Acocks Green,
    // Tyseley, Small Heath and Bordesley were previously omitted on the
    // Hatton -> Birmingham approach (see that stretch's own comments in
    // lines/chiltern-main-line.toml). Widney Manor, Olton and Acocks Green
    // also appear in `wmr-snow-hill.toml`'s own independently-worked
    // "omitted" list (Batch 9, a separate isolated worktree this task
    // couldn't see or coordinate with live), but per the Task 10.2
    // controller ruling this file deliberately keeps them on its own
    // existing `chiltern-birmingham-approach` segment rather than
    // guessing at whatever segment name that concurrent agent picks -- so,
    // as this catalogue is loaded in this worktree today, no sibling line
    // shares that segment name yet. Skip the second (shared-segment)
    // assertion, and say so here, per the Testing convention: "Skip this
    // second assertion... when the new station's segment has no sibling."
    #[test]
    fn chiltern_main_line_has_previously_omitted_birmingham_approach_stations() {
        let lines = load_line("chiltern-main-line");
        let line = lines.get("chiltern-main-line").expect("chiltern-main-line should exist");
        for crs in ["LPW", "WMR", "OLT", "ACG", "TYS", "SMA", "BBS"] {
            assert!(line.has_station(crs), "chiltern-main-line should now include {crs}");
        }
    }

    // Station-catalogue-completeness plan, Task 5.1, FILL-IN piece:
    // southeastern-chatham.toml previously omitted the four suburban/
    // slow-line stations between Herne Hill and Bromley South (West
    // Dulwich, Sydenham Hill, Penge East, Kent House) for unconfirmed
    // fast/slow calling pattern reasons. They're now confirmed and
    // inserted on the existing `chatham-london` segment - this is a
    // regression guard that `has_station` picks them up via
    // `LineDefinition::from_dir` (i.e. the TOML actually parses and the
    // stations aren't silently dropped or misspelled).
    #[test]
    fn chatham_fillin_suburban_stations_are_now_modelled() {
        let lines = load_line("southeastern-chatham");
        let chatham = lines.get("southeastern-chatham").expect("southeastern-chatham line should exist");
        for crs in ["WDU", "SYH", "PNE", "KTH"] {
            assert!(chatham.has_station(crs), "southeastern-chatham should now have station {crs}");
            assert_eq!(chatham.segment_for(crs), Some("chatham-london"), "{crs} should be on chatham-london");
        }
    }

    // Station-catalogue-completeness plan, Task 5.1, BRANCH-RESEARCH
    // piece: the Ramsgate-onward coastal-loop continuation (Minster,
    // Sandwich, Deal, Walmer, Martin Mill) is now confirmed and modelled
    // on a new `chatham-deal` segment (see southeastern-chatham.toml's
    // own header comment for why this is a new segment rather than a
    // continuation of `chatham-coastal`). `chatham-deal` isn't reused by
    // any other file in the catalogue (grepped `lines/*.toml` before
    // picking the name), so per this task's recipe a shared-segment
    // MatchScope assertion is skipped - instead this mirrors
    // `chatham_exclusive_segment_incident_does_not_propagate` above and
    // confirms an incident on the new branch stays ExclusiveSegment and
    // scoped to this file alone, same as the rest of this file's
    // segments.
    #[test]
    fn chatham_deal_branch_stations_are_now_modelled_and_stay_exclusive() {
        let lines = load_all_lines();
        let chatham = lines.get("southeastern-chatham").expect("southeastern-chatham line should exist");
        for crs in ["MSR", "SDW", "DEA", "WAM", "MTM"] {
            assert!(chatham.has_station(crs), "southeastern-chatham should now have station {crs}");
            assert_eq!(chatham.segment_for(crs), Some("chatham-deal"), "{crs} should be on chatham-deal");
        }

        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-8",
            "Signal failure at Deal",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["DEA"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["southeastern-chatham".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Station-catalogue-completeness plan, Task 5.3:
    // southeastern-main-line.toml previously omitted the suburban
    // stopping-pattern stations between London Bridge and Orpington (New
    // Cross, St Johns, Lewisham, Hither Green, Grove Park, Chislehurst,
    // Petts Wood) pending route-diagram confirmation. They're now
    // confirmed and inserted on the existing `seml-london` segment - a
    // regression guard that `has_station`/`segment_for` pick them up via
    // `LineDefinition::from_dir` (i.e. the TOML actually parses and the
    // stations aren't silently dropped or misspelled).
    #[test]
    fn seml_fillin_suburban_stations_are_now_modelled() {
        let lines = load_line("southeastern-main-line");
        let seml = lines.get("southeastern-main-line").expect("southeastern-main-line should exist");
        for crs in ["NWX", "SAJ", "LEW", "HGR", "GRP", "CIT", "PET"] {
            assert!(seml.has_station(crs), "southeastern-main-line should now have station {crs}");
            assert_eq!(seml.segment_for(crs), Some("seml-london"), "{crs} should be on seml-london");
        }
    }

    // New Cross (NWX) is now a three-way station overlap: this file's own
    // `seml-london`, southeastern-metro-north-kent.toml's
    // `southeastern-lewisham-corridor`, and overground-windrush.toml's own
    // `overground-windrush-new-cross` (a different route entirely, the
    // London Overground Windrush line's own New Cross terminus branch).
    // Three different segment names for the same physical station -
    // confirms an incident there matches all three lines independently,
    // each still scoped ExclusiveSegment, never SharedSegment. Mirrors
    // lbg_station_overlap_matches_senk_thameslink_core_seml_and_hayes_as_independent_exclusive_segments
    // above.
    #[test]
    fn nwx_station_overlap_matches_seml_senk_and_windrush_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-14",
            "Signal failure at New Cross",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["NWX"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-main-line".to_string(),
                "southeastern-metro-north-kent".to_string(),
                "overground-windrush".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Hither Green (HGR) is a station overlap between this file's own
    // `seml-london` and southeastern-metro-north-kent.toml's own
    // `senk-dartford-loop` (the Dartford Loop line's own exclusive tracks
    // diverge AT Hither Green, per southeastern-main-line.toml's own header
    // comment - the station itself is shared, the tracks beyond it are
    // not). Confirms an incident there matches both lines independently,
    // each still scoped ExclusiveSegment, never SharedSegment.
    #[test]
    fn hgr_station_overlap_matches_seml_and_senk_dartford_loop_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-15",
            "Signal failure at Hither Green",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["HGR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-main-line".to_string(), "southeastern-metro-north-kent".to_string()])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::ExclusiveSegment, "{} should be ExclusiveSegment, not shared", m.line.id);
        }
    }

    // Grove Park, Chislehurst and Petts Wood are new to the catalogue - no
    // other file models them, so an incident there should stay exclusive
    // to southeastern-main-line alone. Mirrors
    // chatham_deal_branch_stations_are_now_modelled_and_stay_exclusive
    // above.
    #[test]
    fn seml_grove_park_chislehurst_petts_wood_are_exclusive_to_seml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        for crs in ["GRP", "CIT", "PET"] {
            let inc = incident(
                "SE-16",
                "Signal failure",
                "Signal failure causing delays to Southeastern services.",
                &["SE"],
                &[crs],
            );
            let matches = lines_affected_by(&inc, &lines, &registry);
            let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
            assert_eq!(
                matched_ids,
                HashSet::from(["southeastern-main-line".to_string()]),
                "{crs} should match only southeastern-main-line"
            );
            assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
        }
    }

    // Station-catalogue-completeness plan, Task 5.4
    // (southern-brighton-main-line.toml): a fresh route-diagram pass
    // confirmed Clapham Junction (CLJ) and Wivelsfield (WVF) as genuinely
    // missing, currently-Southern-served stations - see each entry's own
    // comment in the TOML file for sourcing. Regression guard: the TOML
    // actually parses and both stations are picked up via
    // `LineDefinition::from_dir` on their expected segment.
    #[test]
    fn southern_brighton_main_line_fillin_stations_are_now_modelled() {
        let lines = load_line("southern-brighton-main-line");
        let bml = lines.get("southern-brighton-main-line").expect("southern-brighton-main-line should exist");
        assert!(bml.has_station("CLJ"), "southern-brighton-main-line should now have station CLJ");
        assert_eq!(bml.segment_for("CLJ"), Some("southern-bml-victoria"), "CLJ should be on southern-bml-victoria");
        assert!(bml.has_station("WVF"), "southern-brighton-main-line should now have station WVF");
        assert_eq!(bml.segment_for("WVF"), Some("southern-bml-south"), "WVF should be on southern-bml-south");
    }

    // Clapham Junction (CLJ) turns out to be a six-way station overlap once
    // southern-brighton-main-line.toml's own entry is added: three SWR
    // files (swr-south-west-main.toml, swr-portsmouth-direct.toml,
    // swr-alton.toml) already share the literal `swr-trunk-waterloo`
    // segment name there (a genuine shared physical trunk out of Waterloo),
    // so those three should resolve as SharedSegment together; the two
    // Overground files each use their own exclusive segment name
    // (`overground-windrush-clapham-branch`, `overground-mildmay-clapham-
    // branch`) and this file's own new `southern-bml-victoria` is likewise
    // unique to it (grepped `lines/*.toml` before picking the name) - all
    // three of those stay ExclusiveSegment, independent of the SWR trio and
    // of each other. Mirrors the mixed shared/exclusive pattern already
    // exercised elsewhere in this file (e.g. the LBG/DVP multi-file
    // overlaps), just with more lines at once.
    #[test]
    fn clj_station_overlap_matches_swr_trunk_shared_and_overground_and_bml_as_mixed_scope() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-1",
            "Signal failure at Clapham Junction",
            "Signal failure causing delays to services.",
            &["SN"],
            &["CLJ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "swr-south-west-main".to_string(),
                "swr-portsmouth-direct".to_string(),
                "swr-alton".to_string(),
                "overground-windrush".to_string(),
                "overground-mildmay".to_string(),
                "southern-brighton-main-line".to_string(),
            ])
        );
        for m in &matches {
            let expected = if ["swr-south-west-main", "swr-portsmouth-direct", "swr-alton"].contains(&m.line.id.as_str())
            {
                MatchScope::SharedSegment
            } else {
                MatchScope::ExclusiveSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // Task 4.1 (station-catalogue completeness): `tfw-cambrian.toml`
    // previously omitted every coast-branch request-stop halt for brevity.
    // A dozen of those (Penhelig, Tonfanau, Llwyngwril, Llanaber, Talybont,
    // Dyffryn Ardudwy, Llanbedr, Pensarn, Llandanwg, Tygwyn, Talsarnau,
    // Llandecwyn, Penychain) are confirmed currently-open stations and are
    // now individually listed. Before this fix, `has_station` returned
    // false for all of them, so `/stations/<crs>` would have silently
    // returned an empty disruption list instead of resolving to this line.
    // Llangelynin, by contrast, is confirmed closed (disused since 1991)
    // and is deliberately still absent -- asserted here too as a guard
    // against it being mistakenly re-added later.
    #[test]
    fn tfw_cambrian_has_station_includes_newly_added_coast_request_stops() {
        let lines = load_line("tfw-cambrian");
        let cambrian = lines.get("tfw-cambrian").expect("tfw-cambrian line should exist");
        for crs in [
            "PHG", "TNF", "LLW", "LLA", "TLB", "DYF", "LBR", "PES", "LDN", "TYG", "TAL", "LLC", "PNC",
        ] {
            assert!(cambrian.has_station(crs), "tfw-cambrian should now list {crs}");
        }
        assert!(
            !cambrian.has_station("LGY"),
            "Llangelynin is closed (disused since 1991) and should not be listed"
        );
    }

    // `tfw-cambrian-coast` is not (yet) a cross-file shared segment -- no
    // other line in the catalogue reaches the Cambrian Coast branch (see
    // `lines/tfw-cambrian.toml`'s own bundled-file comment) -- so unlike
    // `swr_shared_trunk_incident_propagates`, there is no sibling line to
    // assert a `MatchScope::SharedSegment` against for a newly-added
    // station here. This test instead confirms an incident at one of the
    // newly-added stations (Tonfanau) resolves to `tfw-cambrian` alone,
    // with `MatchScope::ExclusiveSegment`, exactly like the rest of the
    // coast branch already did before this fix.
    #[test]
    fn tfw_cambrian_new_coast_station_incident_resolves_exclusive_segment() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TFW-CAM-1",
            "Points failure at Tonfanau",
            "Points failure causing delays to Transport for Wales services at Tonfanau.",
            &["AW"],
            &["TNF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-cambrian".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 4.5 (station-catalogue completeness): `tfw-north-wales-coast.toml`
    // previously omitted 9 minor intermediate calls -- Shotton, Conwy,
    // Penmaenmawr, Llanfairfechan, and (on Anglesey, after Bangor)
    // Llanfairpwll, Bodorgan, Tŷ Croes, Rhosneigr and Valley -- for brevity.
    // All 9 are confirmed currently-open stations and are now individually
    // listed. Before this fix, `has_station` returned false for all of
    // them, so `/stations/<crs>` would have silently returned an empty
    // disruption list instead of resolving to this line. Llandudno (a
    // different line's branch territory) and the file's own closed-station
    // exclusions (e.g. Menai Bridge, Gaerwen) are deliberately still
    // absent -- asserted here too as a guard against them being mistakenly
    // added later.
    #[test]
    fn tfw_north_wales_coast_has_station_includes_newly_added_minor_calls() {
        let lines = load_line("tfw-north-wales-coast");
        let nwc = lines
            .get("tfw-north-wales-coast")
            .expect("tfw-north-wales-coast line should exist");
        for crs in ["SHT", "CNW", "PMW", "LLF", "LPG", "BOR", "TYC", "RHO", "VAL"] {
            assert!(nwc.has_station(crs), "tfw-north-wales-coast should now list {crs}");
        }
        assert!(
            !nwc.has_station("LLD"),
            "Llandudno itself is a different line's branch territory and should not be listed"
        );
    }

    // `tfw-north-wales-coast` is not a cross-file shared segment: this file's
    // own comments confirm both its Chester overlap with
    // `wcml-north-wales.toml` and its Llandudno Junction overlap with
    // `tfw-conwy-valley.toml` are station-overlap-only, not shared trunks.
    // So, like `tfw_cambrian_new_coast_station_incident_resolves_exclusive_segment`,
    // there is no sibling line to assert a `MatchScope::SharedSegment`
    // against for a newly-added station here. This test instead confirms an
    // incident at one of the newly-added stations (Conwy) resolves to
    // `tfw-north-wales-coast` alone, with `MatchScope::ExclusiveSegment`.
    #[test]
    fn tfw_north_wales_coast_new_station_incident_resolves_exclusive_segment() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TFW-NWC-1",
            "Signalling fault at Conwy",
            "Signalling fault causing delays to Transport for Wales services at Conwy.",
            &["AW"],
            &["CNW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-north-wales-coast".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.3: `northern-calder-valley.toml`'s previously-missing
    // intermediate halts. Mytholmroyd (MYT) sits between Sowerby Bridge and
    // Hebden Bridge on this file's sole segment, `northern-calder-valley`,
    // which per that file's own top-of-file comment has no shared-trunk
    // sibling anywhere in this catalogue - so there is no SharedSegment
    // assertion to make here, only that `has_station` picks up the new
    // station and that an incident there stays exclusive, mirroring
    // `calder_valley_exclusive_segment_incident_does_not_propagate` above.
    #[test]
    fn calder_valley_mytholmroyd_has_station_and_stays_exclusive() {
        let lines = load_line("northern-calder-valley");
        let calder_valley = lines.get("northern-calder-valley").expect("northern-calder-valley should load");
        assert!(calder_valley.has_station("MYT"), "northern-calder-valley should now list Mytholmroyd (MYT)");
        assert_eq!(calder_valley.segment_for("MYT"), Some("northern-calder-valley"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-10",
            "Signal failure at Mytholmroyd",
            "Signal failure causing delays on the Calder Valley Line at Mytholmroyd.",
            &["NT"],
            &["MYT"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-calder-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.4: `northern-cumbrian-coast.toml`'s previously-missing
    // intermediate halts. Sellafield (SEL) sits between St Bees/Corkickle
    // and Whitehaven on this file's sole non-junction segment,
    // `northern-cumbrian-coast`, which (per a fresh grep of `lines/*.toml`
    // while writing this test) is not shared by any sibling line in this
    // catalogue - so there is no SharedSegment assertion to make here, only
    // that `has_station` picks up the new station and that an incident
    // there stays exclusive to this line.
    #[test]
    fn cumbrian_coast_sellafield_has_station_and_stays_exclusive() {
        let lines = load_line("northern-cumbrian-coast");
        let cumbrian_coast = lines.get("northern-cumbrian-coast").expect("northern-cumbrian-coast should load");
        assert!(cumbrian_coast.has_station("SEL"), "northern-cumbrian-coast should now list Sellafield (SEL)");
        assert_eq!(cumbrian_coast.segment_for("SEL"), Some("northern-cumbrian-coast"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-11",
            "Trespass incident at Sellafield",
            "Trespass incident causing delays on the Cumbrian Coast Line at Sellafield.",
            &["NT"],
            &["SEL"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-cumbrian-coast".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.6: `northern-furness.toml`'s previously-missing intermediate
    // halts. Grange-over-Sands (GOS) sits between Carnforth and Ulverston on
    // this file's exclusive `northern-furness-branch` segment, which (per a
    // fresh grep of `lines/*.toml` while writing this test) is not shared by
    // any sibling line in this catalogue - so there is no SharedSegment
    // assertion to make here, only that `has_station` picks up the new
    // station and that an incident there stays exclusive to this line.
    #[test]
    fn furness_grange_over_sands_has_station_and_stays_exclusive() {
        let lines = load_line("northern-furness");
        let furness = lines.get("northern-furness").expect("northern-furness should load");
        assert!(furness.has_station("GOS"), "northern-furness should now list Grange-over-Sands (GOS)");
        assert_eq!(furness.segment_for("GOS"), Some("northern-furness-branch"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-12",
            "Signal failure at Grange-over-Sands",
            "Signal failure causing delays on the Furness Line at Grange-over-Sands.",
            &["NT"],
            &["GOS"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-furness".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.7: `northern-hope-valley.toml`'s previously-missing intermediate
    // halts. Bamford (BAM) sits between Hope and Hathersage on this file's
    // sole segment, `northern-hope-valley`, which (per a grep of `lines/
    // *.toml` for the literal `segment = "northern-hope-valley"` value while
    // writing this test) is not shared by any sibling line - so there is no
    // SharedSegment assertion to make here, only that `has_station` picks up
    // the new station and that an incident there stays exclusive. This is
    // deliberately distinct from `emr-regional.toml`, which correctly
    // excludes these same physical stations for EMR's own fast service - see
    // this file's own comment and Task 7.3.
    #[test]
    fn hope_valley_bamford_has_station_and_stays_exclusive() {
        let lines = load_line("northern-hope-valley");
        let hope_valley = lines.get("northern-hope-valley").expect("northern-hope-valley should load");
        assert!(hope_valley.has_station("BAM"), "northern-hope-valley should now list Bamford (BAM)");
        assert_eq!(hope_valley.segment_for("BAM"), Some("northern-hope-valley"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-13",
            "Signal failure at Bamford",
            "Signal failure causing delays on the Hope Valley Line at Bamford.",
            &["NT"],
            &["BAM"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-hope-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.8: `northern-lakes.toml`'s previously-missing intermediate
    // halts. Burneside (BUD) sits between Kendal and Staveley on this file's
    // sole segment, `northern-lakes`, which (per a grep of `lines/*.toml`
    // while writing this test) is not shared by any sibling line - so there
    // is no SharedSegment assertion to make here, only that `has_station`
    // picks up the new station and that an incident there stays exclusive.
    #[test]
    fn lakes_burneside_has_station_and_stays_exclusive() {
        let lines = load_line("northern-lakes");
        let lakes = lines.get("northern-lakes").expect("northern-lakes should load");
        assert!(lakes.has_station("BUD"), "northern-lakes should now list Burneside (BUD)");
        assert_eq!(lakes.segment_for("BUD"), Some("northern-lakes"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-14",
            "Signal failure at Burneside",
            "Signal failure causing delays on the Lakes Line at Burneside.",
            &["NT"],
            &["BUD"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-lakes".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.9: `northern-tyne-valley.toml`'s previously-missing intermediate
    // halts. Prudhoe (PRU) sits between Stocksfield and Wylam on this file's
    // sole segment, `northern-tyne-valley`, which (per a grep of `lines/
    // *.toml` while writing this test) is not shared by any sibling line -
    // so there is no SharedSegment assertion to make here, only that
    // `has_station` picks up the new station and that an incident there
    // stays exclusive.
    #[test]
    fn tyne_valley_prudhoe_has_station_and_stays_exclusive() {
        let lines = load_line("northern-tyne-valley");
        let tyne_valley = lines.get("northern-tyne-valley").expect("northern-tyne-valley should load");
        assert!(tyne_valley.has_station("PRU"), "northern-tyne-valley should now list Prudhoe (PRU)");
        assert_eq!(tyne_valley.segment_for("PRU"), Some("northern-tyne-valley"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-15",
            "Signal failure at Prudhoe",
            "Signal failure causing delays on the Tyne Valley Line at Prudhoe.",
            &["NT"],
            &["PRU"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-tyne-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 9.1 (2026-09-01) fill-in for `lines/wmr-snow-hill.toml`: verify
    // newly-added stations (previously omitted as "minor calls") are now
    // recognised by `has_station`. Bordesley is a good pick since it's the
    // one whose segment placement (shared `wmr-snow-hill-trunk`, not either
    // exclusive branch) needed live-source confirmation, not just an
    // insertion.
    #[test]
    fn wmr_snow_hill_recognises_newly_added_stations() {
        let lines = load_line("wmr-snow-hill");
        let line = lines.get("wmr-snow-hill").expect("wmr-snow-hill should load");
        for crs in ["BBS", "SMA", "ACG", "OLT", "WMR", "SRI", "HLG", "YRD", "WYT", "EWD", "WDE", "DZY", "WWW", "WMC", "STY", "BKD", "HAG", "LYE", "CRA", "OHL", "ROW", "LGG", "THW", "JEQ"] {
            assert!(line.has_station(crs), "{crs} should now be recognised on wmr-snow-hill");
        }
        // Fernhill Heath was named in this task's brief but confirmed closed
        // since 1965 (never reopened) against two independent sources, so it
        // deliberately stays out.
        assert!(!line.has_station("FNH"), "Fernhill Heath is closed and must not be added");
    }

    // None of wmr-snow-hill.toml's three segments (`wmr-snow-hill-trunk`,
    // `wmr-snow-hill-dorridge`, `wmr-snow-hill-stratford`) are shared with
    // any other file's segment tag in the catalogue -- confirmed both by the
    // file's own header comment (no segment-name collision found by grep)
    // and by the task controller's own pre-check. `chiltern-main-line.toml`
    // (Task 10.2, a parallel worktree neither task could see) independently
    // added Bordesley too, on its own exclusive `chiltern-birmingham-approach`
    // segment -- station-level overlap only, same "overlap is fine,
    // segment-sharing is a deliberate choice" precedent as
    // `lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate`
    // above, so still ExclusiveSegment for both.
    #[test]
    fn wmr_snow_hill_bordesley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident("WMR-1", "Signal failure at Bordesley", "Signal failure causing delays to West Midlands Railway services.", &["LM"], &["BBS"]);
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
}
