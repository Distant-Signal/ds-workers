//! The fixed set of incident `category` values the enricher stores.
//!
//! The primary pass's `category` was free text with no prompt guidance, so
//! one cause came back as `signal_failure`, `Signalling Fault`,
//! `signal fault` and so on (the eval dataset's `category_any_of` synonym
//! lists are a sample). [`normalise`] folds whatever the model sends -- case,
//! whitespace, punctuation, synonyms -- onto [`CATEGORIES`], and maps
//! anything it does not recognise to [`FALLBACK`] rather than dropping it.
//! `llm::parse_primary` applies it to every primary response, so the live,
//! batch and eval paths all store the same vocabulary. The primary schema
//! also lists [`CATEGORIES`] as an `enum`, so a backend that enforces the
//! schema already sends one of them; [`normalise`] is the net for one that
//! does not.
//!
//! The category is the incident's *cause*, whole-incident. The *effect* of
//! each period is `impact_type`. Nothing outside the enricher reads
//! `incidents.extracted_category` today (it is advisory, see
//! docs/superpowers/specs/2026-09-01-disruption-impact-type-design.md).

/// Every category, in the order the prompt and schema list them.
/// [`FALLBACK`] is last.
pub(crate) const CATEGORIES: [&str; 22] = [
    "signal_failure",
    "points_failure",
    "track_fault",
    "power_supply_failure",
    "level_crossing_failure",
    "train_fault",
    "engineering_works",
    "bridge_strike",
    "landslip",
    "flooding",
    "severe_weather",
    "obstruction_on_line",
    "animal_on_line",
    "trespass",
    "person_hit_by_train",
    "fire",
    "emergency_services_incident",
    "vandalism",
    "industrial_action",
    "staff_shortage",
    "operational_incident",
    FALLBACK,
];

/// What an unrecognised (or empty) category is stored as.
pub(crate) const FALLBACK: &str = "other";

/// A normalised category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Normalised {
    /// One of [`CATEGORIES`].
    pub category: &'static str,
    /// Whether the raw value was recognised: `false` means it fell back to
    /// [`FALLBACK`] (a raw `other`/`unknown` counts as recognised).
    pub recognised: bool,
}

/// Lowercases, trims and folds every run of non-alphanumerics to one `_`
/// (as the eval's `normalize_label` does): `" Signal-Failure "` is
/// `signal_failure`.
fn fold(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.trim().chars() {
        if ch.is_alphanumeric() {
            out.extend(ch.to_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

/// Ordered rules for a folded value that is not already a category: the
/// first whose every group has a token in the value wins. A group is a set
/// of alternatives. Order matters where a token is shared (`bridge_strike`
/// before `industrial_action`; `signal_failure` and `points_failure` before
/// `train_fault`; `engineering_works` before `emergency_services_incident`,
/// for "emergency engineering works").
const RULES: &[(&str, &[&[&str]])] = &[
    (
        "other",
        &[&[
            "unknown",
            "unspecified",
            "none",
            "na",
            "misc",
            "miscellaneous",
        ]],
    ),
    (
        "bridge_strike",
        &[&["bridge"], &["strike", "struck", "bash", "hit"]],
    ),
    (
        "level_crossing_failure",
        &[&["level"], &["crossing", "crossings"]],
    ),
    ("signal_failure", &[&["track"], &["circuit", "circuits"]]),
    (
        "signal_failure",
        &[&[
            "signal",
            "signals",
            "signalling",
            "signaling",
            "axle",
            "interlocking",
        ]],
    ),
    ("points_failure", &[&["points", "switch", "switches"]]),
    (
        "power_supply_failure",
        &[&[
            "overhead",
            "ohle",
            "ole",
            "wires",
            "wire",
            "conductor",
            "power",
            "electrical",
            "electricity",
            "electrification",
            "substation",
        ]],
    ),
    (
        "engineering_works",
        &[&[
            "engineering",
            "possession",
            "possessions",
            "maintenance",
            "improvement",
            "improvements",
            "upgrade",
            "upgrades",
            "works",
            "work",
            "renewal",
            "renewals",
            "replacement",
            "bus",
            "buses",
        ]],
    ),
    (
        "landslip",
        &[&[
            "landslip",
            "landslide",
            "landslips",
            "landslides",
            "embankment",
        ]],
    ),
    (
        "flooding",
        &[&["flood", "flooding", "flooded", "floods", "water"]],
    ),
    (
        "severe_weather",
        &[&[
            "weather",
            "wind",
            "winds",
            "windy",
            "snow",
            "ice",
            "icy",
            "heat",
            "hot",
            "storm",
            "storms",
            "fog",
            "frost",
            "lightning",
            "temperature",
            "temperatures",
        ]],
    ),
    (
        "animal_on_line",
        &[&[
            "animal",
            "animals",
            "livestock",
            "cattle",
            "cow",
            "cows",
            "sheep",
            "deer",
            "horse",
            "horses",
        ]],
    ),
    (
        "trespass",
        &[&["trespass", "trespasser", "trespassers", "trespassing"]],
    ),
    (
        "person_hit_by_train",
        &[&["fatality", "fatalities", "suicide"]],
    ),
    (
        "person_hit_by_train",
        &[
            &["person", "people", "pedestrian"],
            &["hit", "struck", "strike"],
        ],
    ),
    (
        "obstruction_on_line",
        &[&[
            "tree",
            "trees",
            "obstruction",
            "obstructions",
            "object",
            "objects",
            "debris",
        ]],
    ),
    ("fire", &[&["fire", "fires", "smoke"]]),
    (
        "emergency_services_incident",
        &[&[
            "police",
            "security",
            "emergency",
            "ambulance",
            "medical",
            "unwell",
            "ill",
            "illness",
            "alert",
            "suspicious",
        ]],
    ),
    (
        "vandalism",
        &[&[
            "vandalism",
            "vandals",
            "theft",
            "stolen",
            "criminal",
            "damage",
        ]],
    ),
    (
        "industrial_action",
        &[&["strike", "strikes", "industrial", "walkout"]],
    ),
    (
        "staff_shortage",
        &[&[
            "staff", "staffing", "crew", "driver", "drivers", "guard", "guards", "shortage",
        ]],
    ),
    (
        "train_fault",
        &[
            &[
                "train",
                "trains",
                "unit",
                "units",
                "locomotive",
                "rolling",
                "stock",
                "vehicle",
            ],
            &[
                "fault",
                "faults",
                "failure",
                "failures",
                "failed",
                "broken",
                "defective",
                "defect",
                "breakdown",
            ],
        ],
    ),
    ("train_fault", &[&["breakdown", "brokendown"]]),
    (
        "track_fault",
        &[
            &["track", "rail", "rails", "line"],
            &[
                "fault",
                "failure",
                "defect",
                "defective",
                "broken",
                "buckled",
            ],
        ],
    ),
    (
        "operational_incident",
        &[&[
            "congestion",
            "disruption",
            "operational",
            "delays",
            "delay",
            "earlier",
        ]],
    ),
];

/// Maps a raw model `category` to one of [`CATEGORIES`].
pub(crate) fn normalise(raw: &str) -> Normalised {
    let folded = fold(raw);
    if let Some(&category) = CATEGORIES.iter().find(|&&c| c == folded) {
        return Normalised {
            category,
            recognised: true,
        };
    }
    let tokens: Vec<&str> = folded.split('_').filter(|t| !t.is_empty()).collect();
    let has = |alternatives: &[&str]| alternatives.iter().any(|alt| tokens.contains(alt));
    for &(category, groups) in RULES {
        if groups.iter().all(|group| has(group)) {
            return Normalised {
                category,
                recognised: true,
            };
        }
    }
    Normalised {
        category: FALLBACK,
        recognised: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn assert_maps(raw: &str, want: &str) {
        let got = normalise(raw);
        assert_eq!(got.category, want, "{raw:?}");
        assert!(got.recognised, "{raw:?} should be recognised");
    }

    #[test]
    fn every_category_maps_to_itself_and_is_unique() {
        for category in CATEGORIES {
            assert_maps(category, category);
        }
        let unique: std::collections::BTreeSet<_> = CATEGORIES.iter().collect();
        assert_eq!(unique.len(), CATEGORIES.len());
        assert_eq!(CATEGORIES.last(), Some(&FALLBACK));
    }

    #[test]
    fn every_rule_targets_a_category() {
        for (category, groups) in RULES {
            assert!(CATEGORIES.contains(category), "{category}");
            assert!(!groups.is_empty(), "{category}");
        }
    }

    #[test]
    fn case_whitespace_and_punctuation_fold() {
        assert_maps("  Signal Failure ", "signal_failure");
        assert_maps("SIGNAL-FAILURE", "signal_failure");
        assert_maps("signal__failure\n", "signal_failure");
        assert_maps("Engineering Works.", "engineering_works");
    }

    /// Every synonym the eval dataset's `category_any_of` lists accept maps
    /// to the case's canonical category.
    #[test]
    fn the_eval_datasets_synonyms_map_to_one_category() {
        let cases: [(&str, &[&str]); 7] = [
            (
                "signal_failure",
                &[
                    "signal_failure",
                    "signalling_failure",
                    "signalling_fault",
                    "signal_fault",
                ],
            ),
            ("points_failure", &["points_failure", "points_fault"]),
            (
                "industrial_action",
                &["strike", "industrial_action", "strike_action"],
            ),
            (
                "train_fault",
                &[
                    "broken_down_train",
                    "train_failure",
                    "train_fault",
                    "failed_train",
                ],
            ),
            ("trespass", &["trespass", "trespasser", "trespass_incident"]),
            (
                "engineering_works",
                &[
                    "engineering_works",
                    "emergency_engineering_works",
                    "engineering_work",
                    "emergency_engineering_work",
                    "engineering_overrun",
                    "planned_engineering_works",
                    "rail_replacement",
                    "rail_replacement_bus",
                ],
            ),
            ("landslip", &["landslip", "landslide"]),
        ];
        for (want, synonyms) in cases {
            for raw in synonyms {
                assert_maps(raw, want);
            }
        }
    }

    /// The same against the dataset file itself, so a new case's synonym
    /// list that splits across categories (or isn't recognised) fails here.
    #[test]
    fn every_dataset_cases_accepted_categories_agree() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/eval/dataset.jsonl");
        let raw = std::fs::read_to_string(path).unwrap();
        let mut checked = 0;
        for line in raw.lines().filter(|l| !l.trim().is_empty()) {
            let case: serde_json::Value = serde_json::from_str(line).unwrap();
            let Some(accepted) = case["expected"]["category_any_of"].as_array() else {
                continue;
            };
            let mapped: std::collections::BTreeSet<_> = accepted
                .iter()
                .map(|v| {
                    let got = normalise(v.as_str().unwrap());
                    assert!(got.recognised, "{v} in {}", case["id"]);
                    got.category
                })
                .collect();
            assert_eq!(mapped.len(), 1, "{} maps to {mapped:?}", case["id"]);
            checked += 1;
        }
        assert!(checked > 0);
    }

    #[test]
    fn other_common_phrasings_map() {
        assert_maps("Overhead line equipment failure", "power_supply_failure");
        assert_maps("track circuit failure", "signal_failure");
        assert_maps("broken rail", "track_fault");
        assert_maps("bridge strike", "bridge_strike");
        assert_maps("vehicle struck a bridge", "bridge_strike");
        assert_maps("level crossing fault", "level_crossing_failure");
        assert_maps("person hit by a train", "person_hit_by_train");
        assert_maps("fatality", "person_hit_by_train");
        assert_maps("fallen tree", "obstruction_on_line");
        assert_maps("high winds", "severe_weather");
        assert_maps("Flooding", "flooding");
        assert_maps("cable theft", "vandalism");
        assert_maps("police incident", "emergency_services_incident");
        assert_maps("shortage of train crew", "staff_shortage");
        assert_maps("lineside fire", "fire");
        assert_maps("animals on the line", "animal_on_line");
        assert_maps("unknown", "other");
    }

    fn primary_response(category: &str) -> String {
        serde_json::json!({
            "category": category,
            "periods": [{
                "scope_description": null, "date_range": null, "schedule_window": null,
                "resolution_status": "ongoing", "apparent_severity": "normal",
                "impact_type": null
            }]
        })
        .to_string()
    }

    /// `parse_primary`, the one parse every path (live, batch, eval) uses,
    /// stores the normalised value, and keeps an unknown one as the
    /// fallback rather than failing or dropping it.
    #[test]
    fn parse_primary_normalises_the_category() {
        let parsed = crate::llm::parse_primary(&primary_response(" Signalling Fault ")).unwrap();
        assert_eq!(parsed.category, "signal_failure");
        let parsed = crate::llm::parse_primary(&primary_response("Rail Replacement Bus")).unwrap();
        assert_eq!(parsed.category, "engineering_works");
        let parsed = crate::llm::parse_primary(&primary_response("a goose")).unwrap();
        assert_eq!(parsed.category, FALLBACK);
        assert_eq!(parsed.periods.len(), 1);
    }

    #[test]
    fn an_unrecognised_value_falls_back_instead_of_being_dropped() {
        for raw in ["", "   ", "zzz", "something_else_entirely", "🚂"] {
            let got = normalise(raw);
            assert_eq!(got.category, FALLBACK, "{raw:?}");
            assert!(!got.recognised, "{raw:?}");
        }
    }
}
