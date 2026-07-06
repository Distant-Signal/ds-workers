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
                    station_segments.insert((line.id.clone(), station.crs.clone()), segment.clone());
                }
            }
        }

        Self { segment_lines, station_segments }
    }

    /// Every line ID that includes this segment, in load order.
    pub fn lines_for_segment(&self, segment: &str) -> Vec<String> {
        self.segment_lines.get(segment).cloned().unwrap_or_default()
    }

    /// A segment is shared if more than one line uses it.
    pub fn is_shared(&self, segment: &str) -> bool {
        self.segment_lines.get(segment).map(|v| v.len() > 1).unwrap_or(false)
    }

    /// True if `line_id` is the only line using this segment.
    pub fn is_exclusive_to(&self, segment: &str, line_id: &str) -> bool {
        matches!(self.segment_lines.get(segment), Some(users) if users == &[line_id.to_string()])
    }

    pub fn segment_at(&self, line_id: &str, crs: &str) -> Option<&str> {
        self.station_segments
            .get(&(line_id.to_string(), crs.to_string()))
            .map(|s| s.as_str())
    }

    /// Which of this line's segments are touched by these stations.
    pub fn segments_touched_by(&self, line: &LineDefinition, affected_stations: &[String]) -> HashSet<String> {
        affected_stations
            .iter()
            .filter_map(|crs| self.station_segments.get(&(line.id.clone(), crs.clone())).cloned())
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

    #[test]
    fn shared_trunk_segment_is_shared_across_three_swr_lines() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        assert!(registry.is_shared("swr-trunk-waterloo"));
        let mut users = registry.lines_for_segment("swr-trunk-waterloo");
        users.sort();
        assert_eq!(
            users,
            vec!["swr-alton", "swr-portsmouth-direct", "swr-south-west-main"]
        );
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
        assert_eq!(registry.segment_at("swr-alton", "WOK"), Some("swr-trunk-waterloo"));
        assert_eq!(registry.segment_at("swr-alton", "AON"), Some("swr-alton-branch"));
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
}
