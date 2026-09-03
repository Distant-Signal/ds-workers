//! Segment registry: cross-line view of which segments are shared and
//! which are exclusive, derived from the full set of loaded line
//! definitions. Ported from `src/segments.py`.

use std::collections::{HashMap, HashSet};

use common::LineDefinition;

/// Indexes segment usage across all known lines.
pub struct SegmentRegistry {
    /// segment -> ordered list of unique line IDs that include it.
    segment_lines: HashMap<String, Vec<String>>,
    /// (line_id, crs) -> segment.
    station_segments: HashMap<(String, String), String>,
}

impl SegmentRegistry {
    pub fn new(lines: &HashMap<String, LineDefinition>) -> Self {
        let mut segment_lines: HashMap<String, Vec<String>> = HashMap::new();
        let mut station_segments: HashMap<(String, String), String> = HashMap::new();

        for line in lines.values() {
            for station in &line.stations {
                if let Some(segment) = &station.segment {
                    let entry = segment_lines.entry(segment.clone()).or_default();
                    if !entry.contains(&line.id) {
                        entry.push(line.id.clone());
                    }
                    station_segments
                        .insert((line.id.clone(), station.crs.clone()), segment.clone());
                }
            }
        }

        Self {
            segment_lines,
            station_segments,
        }
    }

    /// Every line ID that includes this segment, in load order.
    ///
    /// Ported public API surface from the Python `SegmentRegistry`
    /// (`src/segments.py`); currently exercised only by unit tests, not by
    /// the `bin` target that clippy's dead-code lint checks against.
    #[allow(dead_code)]
    pub fn lines_for_segment(&self, segment: &str) -> Vec<String> {
        self.segment_lines.get(segment).cloned().unwrap_or_default()
    }

    /// A segment is shared if more than one line uses it.
    pub fn is_shared(&self, segment: &str) -> bool {
        self.segment_lines
            .get(segment)
            .map(|v| v.len() > 1)
            .unwrap_or(false)
    }

    /// True if `line_id` is the only line using this segment.
    pub fn is_exclusive_to(&self, segment: &str, line_id: &str) -> bool {
        matches!(self.segment_lines.get(segment), Some(users) if users == &[line_id.to_string()])
    }

    /// Ported public API surface from the Python `SegmentRegistry`
    /// (`src/segments.py`); currently exercised only by unit tests, not by
    /// the `bin` target that clippy's dead-code lint checks against.
    #[allow(dead_code)]
    pub fn segment_at(&self, line_id: &str, crs: &str) -> Option<&str> {
        self.station_segments
            .get(&(line_id.to_string(), crs.to_string()))
            .map(|s| s.as_str())
    }

    /// Which of this line's segments are touched by these stations.
    pub fn segments_touched_by(
        &self,
        line: &LineDefinition,
        affected_stations: &[String],
    ) -> HashSet<String> {
        affected_stations
            .iter()
            .filter_map(|crs| {
                self.station_segments
                    .get(&(line.id.clone(), crs.clone()))
                    .cloned()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn load_all_lines() -> HashMap<String, LineDefinition> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        LineDefinition::from_dir(&dir)
            .expect("lines/ directory should parse")
            .into_iter()
            .map(|l| (l.id.clone(), l))
            .collect()
    }

    // Was `..._across_three_swr_lines` until `lines/swr-kingston-loop.toml`
    // and `lines/swr-chessington.toml` were added. Those two model SWR's
    // suburban SLOW-line corridor out of Waterloo and reuse
    // `swr-trunk-waterloo` verbatim for the Waterloo-Raynes Park stretch
    // they genuinely share with the three fast-line files, so the trunk now
    // has five users. Both new files' own headers work through why reusing
    // the name is correct here (and why it is not the situation
    // `great-northern-suburban.toml` warns about).
    #[test]
    fn shared_trunk_segment_is_shared_across_every_swr_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        assert!(registry.is_shared("swr-trunk-waterloo"));
        let mut users = registry.lines_for_segment("swr-trunk-waterloo");
        users.sort();
        assert_eq!(
            users,
            vec![
                "swr-alton",
                "swr-chessington",
                "swr-kingston-loop",
                "swr-portsmouth-direct",
                "swr-south-west-main",
            ]
        );
    }

    // The Kingston Loop and the Chessington branch leave the South West
    // Main Line at *different* junctions -- Chessington at Raynes Park, the
    // loop one station further on at New Malden -- and share no track
    // beyond the trunk. This asserts the whole junction structure the two
    // files were written to model:
    //   * both carry RAY on the shared trunk (the README's "junction
    //     stations belong to the shared trunk" rule),
    //   * NEM is on the trunk for the loop and absent from Chessington
    //     entirely (Chessington services never pass through New Malden),
    //   * each line's own post-junction segments are exclusive to it.
    #[test]
    fn swr_suburban_lines_split_at_their_own_junctions_off_the_shared_trunk() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let loop_line = &lines["swr-kingston-loop"];
        let chessington = &lines["swr-chessington"];

        // Raynes Park: the shared junction, on the trunk for both.
        assert_eq!(
            registry.segment_at("swr-kingston-loop", "RAY"),
            Some("swr-trunk-waterloo")
        );
        assert_eq!(
            registry.segment_at("swr-chessington", "RAY"),
            Some("swr-trunk-waterloo")
        );

        // New Malden: the Kingston Loop's own junction off the SWML, also
        // on the trunk -- and not a Chessington station at all.
        assert_eq!(
            registry.segment_at("swr-kingston-loop", "NEM"),
            Some("swr-trunk-waterloo")
        );
        assert_eq!(registry.segment_at("swr-chessington", "NEM"), None);
        assert!(!chessington.has_station("NEM"));

        // Motspur Park is Chessington's own junction, on the reusable
        // Epsom-line segment -- and not a Kingston Loop station.
        assert_eq!(
            registry.segment_at("swr-chessington", "MOT"),
            Some("swr-epsom-line")
        );
        assert!(!loop_line.has_station("MOT"));

        // Surbiton and Berrylands belong to neither: SUR stays exclusively
        // with the three fast-line files, BRS is uncovered by any file.
        assert!(!loop_line.has_station("SUR"));
        assert!(!chessington.has_station("SUR"));
        assert!(!loop_line.has_station("BRS"));
        assert!(!chessington.has_station("BRS"));

        // Each line's post-junction segments are exclusive to it.
        assert!(registry.is_exclusive_to("swr-kingston-loop", "swr-kingston-loop"));
        assert!(registry.is_exclusive_to("swr-windsor-lines", "swr-kingston-loop"));
        assert!(registry.is_exclusive_to("swr-chessington-branch", "swr-chessington"));
        assert!(registry.is_exclusive_to("swr-epsom-line", "swr-chessington"));
        assert!(!registry.is_shared("swr-kingston-loop"));
        assert!(!registry.is_shared("swr-chessington-branch"));

        // And the trunk is touched together with each line's own exclusive
        // segment -- same shape as `segments_touched_by_finds_shared_and_
        // exclusive_together` asserts for the Alton branch.
        let touched_loop =
            registry.segments_touched_by(loop_line, &["RAY".to_string(), "KNG".to_string()]);
        assert_eq!(touched_loop.len(), 2);
        assert!(touched_loop.contains("swr-trunk-waterloo"));
        assert!(touched_loop.contains("swr-kingston-loop"));

        let touched_chess =
            registry.segments_touched_by(chessington, &["RAY".to_string(), "CSS".to_string()]);
        assert_eq!(touched_chess.len(), 2);
        assert!(touched_chess.contains("swr-trunk-waterloo"));
        assert!(touched_chess.contains("swr-chessington-branch"));
    }

    #[test]
    fn exclusive_branch_segment_is_not_shared() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        assert!(!registry.is_shared("swr-alton-branch"));
        assert!(registry.is_exclusive_to("swr-alton-branch", "swr-alton"));
        assert!(!registry.is_exclusive_to("swr-alton-branch", "swr-south-west-main"));
    }

    #[test]
    fn segment_at_returns_the_right_segment_for_a_station() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        assert_eq!(
            registry.segment_at("swr-alton", "WOK"),
            Some("swr-trunk-waterloo")
        );
        assert_eq!(
            registry.segment_at("swr-alton", "AON"),
            Some("swr-alton-branch")
        );
        assert_eq!(registry.segment_at("swr-alton", "NOTASTATION"), None);
    }

    #[test]
    fn segments_touched_by_finds_shared_and_exclusive_together() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let alton = &lines["swr-alton"];
        let touched = registry.segments_touched_by(alton, &["WOK".to_string(), "AON".to_string()]);
        assert_eq!(touched.len(), 2);
        assert!(touched.contains("swr-trunk-waterloo"));
        assert!(touched.contains("swr-alton-branch"));
    }

    #[test]
    fn overground_canonbury_curve_is_shared_between_mildmay_and_windrush() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        assert!(registry.is_shared("overground-canonbury-curve"));
        let mut users = registry.lines_for_segment("overground-canonbury-curve");
        users.sort();
        assert_eq!(users, vec!["overground-mildmay", "overground-windrush"]);
    }

    #[test]
    fn overground_exclusive_segments_are_not_shared() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        assert!(!registry.is_shared("overground-liberty"));
        assert!(!registry.is_shared("overground-suffragette"));
        assert!(!registry.is_shared("overground-weaver-chingford"));
    }

    // Task 10.6 (ScotRail Far North Line): unlike `swr-alton`'s trunk
    // (`swr-trunk-waterloo`, shared cross-file with two sibling lines),
    // `scotrail-far-north-trunk` (Alness-Georgemas Junction) is shared
    // same-file between this one line's own Wick and Thurso branches --
    // there is no sibling `lines/*.toml` file for either branch, so
    // `is_shared`/`MatchScope::SharedSegment` (a cross-line-file concept)
    // doesn't apply here the way it does for SWR. What *is* true, and
    // worth asserting directly, is the same structural fact
    // `segments_touched_by_finds_shared_and_exclusive_together` asserts
    // for SWR: the trunk segment is touched together with *each* branch's
    // own exclusive segment, proving the Georgemas Junction split and the
    // Alness-Georgemas shared stretch are both tagged correctly within
    // this one file.
    #[test]
    fn scotrail_far_north_georgemas_trunk_is_shared_by_both_branches() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let far_north = &lines["scotrail-far-north"];

        // Alness (trunk) + Wick (exclusive Wick branch).
        let touched_wick =
            registry.segments_touched_by(far_north, &["ASS".to_string(), "WCK".to_string()]);
        assert_eq!(touched_wick.len(), 2);
        assert!(touched_wick.contains("scotrail-far-north-trunk"));
        assert!(touched_wick.contains("scotrail-far-north-wick"));

        // Alness (trunk) + Thurso (exclusive Thurso branch) -- the same
        // trunk segment, proving both branches share it.
        let touched_thurso =
            registry.segments_touched_by(far_north, &["ASS".to_string(), "THS".to_string()]);
        assert_eq!(touched_thurso.len(), 2);
        assert!(touched_thurso.contains("scotrail-far-north-trunk"));
        assert!(touched_thurso.contains("scotrail-far-north-thurso"));

        // Neither branch segment is shared with the other, or with
        // anything outside this file yet.
        assert!(registry.is_exclusive_to("scotrail-far-north-wick", "scotrail-far-north"));
        assert!(registry.is_exclusive_to("scotrail-far-north-thurso", "scotrail-far-north"));
        assert!(registry.is_exclusive_to("scotrail-far-north-trunk", "scotrail-far-north"));
    }
}
