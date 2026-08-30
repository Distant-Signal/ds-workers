//! Read/write query functions the aggregator's own poll loop uses. Reads
//! `incidents`/`station_samples` (written by the four existing pollers);
//! writes `line_status`/`line_status_history` (read by the api crate's
//! new endpoints, Task 5).

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{IncidentMessage, LineStatusReport, StationSample};
use sqlx::{PgPool, Row};

/// One incident loaded from the `incidents` table for this aggregation
/// cycle, paired with our own `first_seen_at` clock. Deliberately not part
/// of `common::IncidentMessage` -- the wire type pollers/the API share --
/// since `first_seen_at` is a fact only this crate's staleness check cares
/// about. See docs/superpowers/specs/2026-07-16-stale-incident-handling-design.md.
pub struct LoadedIncident {
    pub message: IncidentMessage,
    pub first_seen_at: DateTime<Utc>,
    /// `Vec<ExtractionPeriod>` JSON (see
    /// docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md
    /// §1/§3), or `None` if no extraction has succeeded yet. Deserialized
    /// into `aggregation`'s private `ExtractionPeriod` mirror lazily, in
    /// `aggregation::apply_extraction`/`has_recurring_schedule` -- not here,
    /// so this crate's DB layer stays agnostic to the JSON shape those
    /// functions consume.
    pub extracted_periods: Option<serde_json::Value>,
}

pub async fn load_incidents(pool: &PgPool) -> Result<Vec<LoadedIncident>> {
    let rows = sqlx::query(
        "SELECT incident_id, summary, description, operators, affected_stations, \
                priority, validity_periods, is_planned, is_cleared, first_seen_at, \
                extracted_periods \
         FROM incidents \
         WHERE NOT is_cleared",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let validity_json: serde_json::Value = row.try_get("validity_periods")?;
            let message = IncidentMessage {
                incident_id: row.try_get("incident_id")?,
                summary: row.try_get("summary")?,
                description: row.try_get("description")?,
                operators: row.try_get("operators")?,
                affected_stations: row.try_get("affected_stations")?,
                priority: row.try_get("priority")?,
                validity: serde_json::from_value(validity_json)?,
                is_planned: row.try_get("is_planned")?,
                is_cleared: row.try_get("is_cleared")?,
            };
            Ok(LoadedIncident {
                message,
                first_seen_at: row.try_get("first_seen_at")?,
                extracted_periods: row.try_get("extracted_periods")?,
            })
        })
        .collect()
}

pub async fn load_station_samples(pool: &PgPool) -> Result<HashMap<String, StationSample>> {
    let rows = sqlx::query("SELECT crs, polled_at, departures FROM station_samples")
        .fetch_all(pool)
        .await?;

    rows.into_iter()
        .map(|row| {
            let crs: String = row.try_get("crs")?;
            let departures_json: serde_json::Value = row.try_get("departures")?;
            let sample = StationSample {
                crs: crs.clone(),
                polled_at: row.try_get("polled_at")?,
                departures: serde_json::from_value(departures_json)?,
            };
            Ok((crs, sample))
        })
        .collect()
}

pub async fn load_custom_lines(pool: &PgPool) -> Result<Vec<common::CustomLine>> {
    let rows = sqlx::query(
        "SELECT id, name, operators, stations, headcode_prefixes, destination_crs_filter \
         FROM custom_lines",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(common::CustomLine {
                id: row.try_get("id")?,
                name: row.try_get("name")?,
                operators: row.try_get("operators")?,
                stations: row.try_get("stations")?,
                headcode_prefixes: row.try_get("headcode_prefixes")?,
                destination_crs_filter: row.try_get("destination_crs_filter")?,
            })
        })
        .collect()
}

/// Deletes `line_status` rows for any `line_id` not in `current_line_ids`.
/// Called every cycle with the freshly-merged static+custom line set, so a
/// deleted custom line's last-known status is removed on the next cycle
/// rather than lingering forever (custom lines are the only way a line can
/// disappear between cycles — the static catalogue is fixed for the
/// process's lifetime).
///
/// Scoped to `source = 'aggregator'`: this crate is no longer the only
/// writer of `line_status`. TfL lines are written by the api crate's
/// `/private/tfl-line-status` ingest and are pruned by that endpoint
/// against its own batch — they are invisible to this crate's line set, so
/// an unscoped DELETE here would wipe them on the very next cycle.
pub async fn prune_removed_lines(pool: &PgPool, current_line_ids: &[String]) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM line_status WHERE source = 'aggregator' AND NOT (line_id = ANY($1))",
    )
    .bind(current_line_ids)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Fetches the currently-stored `statuses` JSON for one line, if any row
/// exists yet.
async fn existing_statuses(pool: &PgPool, line_id: &str) -> Result<Option<serde_json::Value>> {
    let row = sqlx::query("SELECT statuses FROM line_status WHERE line_id = $1")
        .bind(line_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.try_get("statuses")).transpose()?)
}

/// Strips volatile fields that `aggregation::aggregate` recomputes fresh on
/// every cycle even when nothing about the line's status has actually
/// changed, so that a byte-for-byte comparison of the resulting `statuses`
/// JSON reflects only meaningful changes:
///
/// - `validity.from_date`: the no-incident/no-inference fallback paths
///   (`good_service()`, the LDBWS-inferred branch of `infer_from_samples`,
///   and `validity_for_output`'s empty-periods case) stamp this with a
///   fresh `Utc::now()` on every call. Incident-driven statuses are
///   unaffected: their `from_date` comes from the incident's own stored
///   `validity_periods` and stays stable across cycles as long as the
///   incident data doesn't change.
/// - `sample_stats`: recomputed from live LDBWS samples every poll cycle,
///   so its counts and `avg_delay_minutes` roll over every cycle even when
///   the line's actual status is unchanged.
/// - the `(live samples show: ...)` suffix `escalate_from_sample_stats`
///   (aggregation.rs) appends to `reason` on escalation: it carries the
///   same live counts as `sample_stats`, just formatted into text instead
///   of left structured, so it churns every cycle for the same reason and
///   needs the same stripping.
///
/// Without stripping all three, a "change" would be seen on every single
/// poll cycle for most lines, defeating the point of only recording history
/// on real changes.
fn normalize_for_diff(statuses: &serde_json::Value) -> serde_json::Value {
    match statuses.as_array() {
        Some(entries) => serde_json::Value::Array(entries.iter().map(normalize_entry_for_diff).collect()),
        None => statuses.clone(),
    }
}

/// Per-entry half of `normalize_for_diff`, split out so
/// `carry_forward_ldbws_from_date` (below) can reuse the exact same
/// "does this entry mean the same thing as last cycle" definition when
/// deciding whether to carry forward `from_date`, rather than
/// re-implementing a second, potentially-diverging notion of equality.
fn normalize_entry_for_diff(entry: &serde_json::Value) -> serde_json::Value {
    let mut entry = entry.clone();
    if let Some(validity) = entry.get_mut("validity").and_then(|v| v.as_object_mut()) {
        validity.remove("from_date");
    }
    if let Some(obj) = entry.as_object_mut() {
        obj.remove("sample_stats");
    }
    if let Some(reason) = entry.get_mut("reason")
        && let Some(stripped) = reason.as_str().map(strip_live_sample_annotation)
    {
        *reason = serde_json::Value::String(stripped.to_string());
    }
    entry
}

/// The one `DataQuality` value affected by the `Utc::now()`-every-cycle bug
/// this exists to work around. See
/// docs/superpowers/specs/2026-08-30-inferred-time-ranges-design.md.
const LDBWS_INFERRED: &str = "ldbws-inferred";

/// Given the previous cycle's stored `statuses` JSON array (`existing`) and
/// this cycle's freshly-computed one (`fresh`), returns a copy of `fresh`
/// with each `"ldbws-inferred"` entry's `validity.from_date` overwritten by
/// the positionally-corresponding entry in `existing`, provided that entry
/// is also `"ldbws-inferred"` and the two entries are equal once run
/// through `normalize_entry_for_diff` (i.e. "same underlying disruption,
/// just a fresh poll of it" -- `sample_stats` and the live-sample-count
/// reason suffix are allowed to churn without defeating the carry-forward,
/// since `normalize_entry_for_diff` already strips both).
///
/// Positional matching (`existing[i]` vs. `fresh[i]`) is safe today because
/// `infer_from_samples`/`good_service()` (`aggregation.rs`) only ever
/// produce a single-entry `statuses` array per line -- see the design doc's
/// Open Question 2 for what would need to change if that ever stops being
/// true.
///
/// A new or genuinely-changed status (no prior entry at this position,
/// prior entry has a different `data_quality`, or the stripped content
/// differs) is left with its fresh `Utc::now()` stamp untouched -- this is
/// the correct behavior for those cases, not a gap in the fix.
fn carry_forward_ldbws_from_date(existing: &serde_json::Value, fresh: &serde_json::Value) -> serde_json::Value {
    let mut fresh = fresh.clone();
    let existing_entries = existing.as_array();
    let Some(fresh_entries) = fresh.as_array_mut() else { return fresh };

    for (i, entry) in fresh_entries.iter_mut().enumerate() {
        if entry.get("data_quality").and_then(|v| v.as_str()) != Some(LDBWS_INFERRED) {
            continue;
        }
        let Some(existing_entry) = existing_entries.and_then(|arr| arr.get(i)) else { continue };
        if existing_entry.get("data_quality").and_then(|v| v.as_str()) != Some(LDBWS_INFERRED) {
            continue;
        }
        if normalize_entry_for_diff(entry) != normalize_entry_for_diff(existing_entry) {
            continue;
        }
        let Some(old_from_date) = existing_entry.get("validity").and_then(|v| v.get("from_date")).cloned() else {
            continue;
        };
        if let Some(validity) = entry.get_mut("validity").and_then(|v| v.as_object_mut()) {
            validity.insert("from_date".to_string(), old_from_date);
        }
    }

    fresh
}

/// Strips a trailing `" (live samples show: ...)"` annotation from a
/// status `reason`, if present. See `normalize_for_diff`: the annotation's
/// live counts roll over almost every poll cycle even when nothing about
/// the underlying disruption has changed, so it must not participate in
/// change detection.
fn strip_live_sample_annotation(reason: &str) -> &str {
    const MARKER: &str = " (live samples show: ";
    match reason.rfind(MARKER) {
        Some(idx) if reason.ends_with(')') => &reason[..idx],
        _ => reason,
    }
}

/// Upserts one line's computed report into `line_status` (always), and
/// inserts a `line_status_history` snapshot only if the statuses actually
/// changed since the last cycle.
pub async fn write_line_status(pool: &PgPool, report: &LineStatusReport) -> Result<()> {
    let fresh_statuses_json = serde_json::to_value(&report.statuses)?;
    let existing = existing_statuses(pool, &report.id).await?;

    // Carry forward `from_date` for any `ldbws-inferred` entry whose content
    // is unchanged from last cycle, before comparing/persisting -- see
    // docs/superpowers/specs/2026-08-30-inferred-time-ranges-design.md. This
    // does not change `changed`'s outcome (normalize_for_diff already strips
    // `from_date` from both sides), only what gets stored.
    let statuses_json = match &existing {
        Some(existing) => carry_forward_ldbws_from_date(existing, &fresh_statuses_json),
        None => fresh_statuses_json,
    };

    let changed = match &existing {
        None => true,
        Some(existing) => normalize_for_diff(existing) != normalize_for_diff(&statuses_json),
    };

    sqlx::query(
        r#"
        INSERT INTO line_status (line_id, name, mode_name, operators, statuses, computed_at, source)
        VALUES ($1, $2, $3, $4, $5, NOW(), 'aggregator')
        ON CONFLICT (line_id) DO UPDATE SET
            name        = EXCLUDED.name,
            mode_name   = EXCLUDED.mode_name,
            operators   = EXCLUDED.operators,
            statuses    = EXCLUDED.statuses,
            computed_at = NOW(),
            source      = 'aggregator'
        "#,
    )
    .bind(&report.id)
    .bind(&report.name)
    .bind(&report.mode_name)
    .bind(&report.operators)
    .bind(&statuses_json)
    .execute(pool)
    .await?;

    if changed {
        sqlx::query(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) VALUES ($1, $2, NOW())",
        )
        .bind(&report.id)
        .bind(&statuses_json)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Deletes `line_status_history` rows older than `retention_days`.
pub async fn prune_history(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM line_status_history WHERE computed_at < NOW() - ($1 || ' days')::interval",
    )
    .bind(retention_days.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                load_incidents_excludes_cleared_rows -- --ignored` against docker compose's postgres"]
    async fn load_incidents_excludes_cleared_rows() {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.expect("connect to postgres");

        sqlx::query(
            "INSERT INTO incidents \
                (incident_id, summary, description, operators, affected_stations, priority, validity_periods, is_planned, is_cleared) \
             VALUES \
                ('TEST-ACTIVE', 'active', 'active incident', '{}', '{}', 0, '[]', false, false), \
                ('TEST-CLEARED', 'cleared', 'cleared incident', '{}', '{}', 0, '[]', false, true) \
             ON CONFLICT (incident_id) DO UPDATE SET is_cleared = EXCLUDED.is_cleared",
        )
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        let loaded = load_incidents(&pool).await.expect("load_incidents");
        let ids: Vec<&str> = loaded.iter().map(|i| i.message.incident_id.as_str()).collect();

        sqlx::query("DELETE FROM incidents WHERE incident_id IN ('TEST-ACTIVE', 'TEST-CLEARED')")
            .execute(&pool)
            .await
            .expect("cleanup fixture rows");

        assert!(ids.contains(&"TEST-ACTIVE"), "non-cleared incident should be loaded");
        assert!(!ids.contains(&"TEST-CLEARED"), "cleared incident should be excluded");
    }

    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p aggregator \
                prune_removed_lines_leaves_other_sources_alone -- --ignored`"]
    async fn prune_removed_lines_leaves_other_sources_alone() {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.expect("connect to postgres");

        sqlx::query(
            "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
             VALUES \
                ('TEST-AGG', 'test aggregator line', 'national-rail', '{}', '[]', 'aggregator'), \
                ('TEST-TFL', 'test tfl line', 'tube', '{TfL}', '[]', 'tfl') \
             ON CONFLICT (line_id) DO UPDATE SET source = EXCLUDED.source",
        )
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        // An empty current-line set is the worst case: the aggregator has
        // nothing of its own left, so anything it does not own must still
        // survive.
        prune_removed_lines(&pool, &[]).await.expect("prune_removed_lines");

        let survivors: Vec<String> =
            sqlx::query_scalar("SELECT line_id FROM line_status WHERE line_id IN ('TEST-AGG', 'TEST-TFL')")
                .fetch_all(&pool)
                .await
                .expect("read survivors");

        sqlx::query("DELETE FROM line_status WHERE line_id IN ('TEST-AGG', 'TEST-TFL')")
            .execute(&pool)
            .await
            .expect("cleanup fixture rows");

        assert!(!survivors.contains(&"TEST-AGG".to_string()), "the aggregator's own stale row should go");
        assert!(survivors.contains(&"TEST-TFL".to_string()), "a TfL-owned row must not be collateral damage");
    }

    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p aggregator \
                a_stable_ldbws_inferred_status_carries_its_from_date_across_two_cycles -- --ignored`"]
    async fn a_stable_ldbws_inferred_status_carries_its_from_date_across_two_cycles() {
        // Real two-cycle aggregate() -> write_line_status() sequence, per
        // docs/superpowers/specs/2026-08-30-inferred-time-ranges-design.md's
        // "Testing" section: a stable LdbwsInferred status across two cycles
        // should still produce exactly one line_status_history row
        // (unchanged from today) AND a stable `from_date` in the second
        // cycle's stored row (the new behavior this design fixes).
        use crate::aggregation::aggregate;
        use crate::queries::LoadedIncident;
        use crate::segments::SegmentRegistry;
        use common::{Defaults, LineDefinition, StationDeparture, StationSample};
        use std::collections::HashMap;

        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.expect("connect to postgres");

        const LINE_ID: &str = "TEST-CARRY-FORWARD-LINE";

        let line = LineDefinition {
            id: LINE_ID.to_string(),
            name: "Test Carry-Forward Line".to_string(),
            mode: "national-rail".to_string(),
            category: "regional".to_string(),
            operators: vec!["SW".to_string()],
            stations: vec![],
            sample_stations: vec!["AHT".to_string()],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: HashMap::new(),
            exclusive_segments: vec![],
            destination_crs_filter: vec!["AON".to_string()],
            headcode_prefixes: vec![],
        };
        let mut lines = HashMap::new();
        lines.insert(LINE_ID.to_string(), line);

        fn departure(delay_minutes: i32) -> StationDeparture {
            StationDeparture {
                service_id: "svc".to_string(),
                operator: "SW".to_string(),
                destination_crs: "AON".to_string(),
                scheduled: "10:00".to_string(),
                estimated: "10:00".to_string(),
                is_cancelled: false,
                delay_minutes,
                cancel_reason: None,
                delay_reason: if delay_minutes > 0 { Some("signal failure".to_string()) } else { None },
                headcode: None,
                skipped_stations: vec![],
            }
        }

        // 1 of 4 delayed (>= the 5-minute default threshold) -> exactly at
        // the 25% minor-delays default threshold -> a stable, non-good-
        // service `LdbwsInferred` status, the design doc's primary/
        // high-volume case (not the lower-stakes good_service() fallback).
        let samples: HashMap<String, StationSample> = HashMap::from([(
            "AHT".to_string(),
            StationSample {
                crs: "AHT".to_string(),
                polled_at: Utc::now(),
                departures: vec![departure(10), departure(0), departure(0), departure(0)],
            },
        )]);

        let registry = SegmentRegistry::new(&lines);
        let defaults = Defaults::default();
        let no_incidents: Vec<LoadedIncident> = vec![];

        // Cleanup any leftovers from a prior failed run before starting.
        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1")
            .bind(LINE_ID)
            .execute(&pool)
            .await
            .expect("pre-test cleanup of line_status_history");
        sqlx::query("DELETE FROM line_status WHERE line_id = $1")
            .bind(LINE_ID)
            .execute(&pool)
            .await
            .expect("pre-test cleanup of line_status");

        // Cycle 1.
        let reports1 = aggregate(&lines, &no_incidents, &samples, &registry, &defaults);
        let report1 = reports1.get(LINE_ID).expect("line should have a report");
        assert_eq!(
            report1.statuses[0].data_quality,
            common::DataQuality::LdbwsInferred,
            "sanity check: this scenario should hit the LdbwsInferred path, not an incident-derived one"
        );
        write_line_status(&pool, report1).await.expect("write_line_status cycle 1");

        let stored_after_cycle_1: serde_json::Value = sqlx::query_scalar("SELECT statuses FROM line_status WHERE line_id = $1")
            .bind(LINE_ID)
            .fetch_one(&pool)
            .await
            .expect("read stored statuses after cycle 1");
        let from_date_after_cycle_1 = stored_after_cycle_1[0]["validity"]["from_date"].clone();

        // Cycle 2: identical samples (a real re-poll of the same ongoing,
        // unchanged disruption), but a later, distinct Utc::now() internally.
        let reports2 = aggregate(&lines, &no_incidents, &samples, &registry, &defaults);
        let report2 = reports2.get(LINE_ID).expect("line should have a report");
        let fresh_from_date_cycle_2 =
            serde_json::to_value(report2.statuses[0].validity.from_date).expect("serialize fresh from_date");
        write_line_status(&pool, report2).await.expect("write_line_status cycle 2");

        let stored_after_cycle_2: serde_json::Value = sqlx::query_scalar("SELECT statuses FROM line_status WHERE line_id = $1")
            .bind(LINE_ID)
            .fetch_one(&pool)
            .await
            .expect("read stored statuses after cycle 2");
        let from_date_after_cycle_2 = stored_after_cycle_2[0]["validity"]["from_date"].clone();

        let history_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM line_status_history WHERE line_id = $1")
            .bind(LINE_ID)
            .fetch_one(&pool)
            .await
            .expect("count history rows");

        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1")
            .bind(LINE_ID)
            .execute(&pool)
            .await
            .expect("cleanup line_status_history");
        sqlx::query("DELETE FROM line_status WHERE line_id = $1")
            .bind(LINE_ID)
            .execute(&pool)
            .await
            .expect("cleanup line_status");

        assert_eq!(
            history_count, 1,
            "a stable status across two cycles should still write exactly one history row, unchanged from today"
        );
        assert_eq!(
            from_date_after_cycle_2, from_date_after_cycle_1,
            "the stored from_date must stay stable across two cycles of an unchanged disruption"
        );
        assert_ne!(
            fresh_from_date_cycle_2, from_date_after_cycle_2,
            "sanity check: cycle 2's own freshly-computed from_date must differ from what actually got \
             stored, proving the carry-forward -- not coincidence -- is what kept the stored value stable"
        );
    }

    #[test]
    fn normalize_for_diff_ignores_sample_stats_changes() {
        let a = serde_json::json!([
            {
                "severity": "good-service",
                "reason": "Good service",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "live",
                "sample_stats": {
                    "total": 10,
                    "delayed": 2,
                    "cancelled": 0,
                    "avg_delay_minutes": 1.5
                }
            }
        ]);
        let b = serde_json::json!([
            {
                "severity": "good-service",
                "reason": "Good service",
                "validity": {"from_date": "2026-07-09T10:01:00Z"},
                "data_quality": "live",
                "sample_stats": {
                    "total": 11,
                    "delayed": 5,
                    "cancelled": 1,
                    "avg_delay_minutes": 4.2
                }
            }
        ]);

        assert_eq!(normalize_for_diff(&a), normalize_for_diff(&b));
    }

    #[test]
    fn normalize_for_diff_still_detects_real_changes() {
        let a = serde_json::json!([
            {
                "severity": "good-service",
                "reason": "Good service",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "live",
                "sample_stats": {
                    "total": 10,
                    "delayed": 2,
                    "cancelled": 0,
                    "avg_delay_minutes": 1.5
                }
            }
        ]);
        let b = serde_json::json!([
            {
                "severity": "minor-delays",
                "reason": "Minor delays",
                "validity": {"from_date": "2026-07-09T10:01:00Z"},
                "data_quality": "live",
                "sample_stats": {
                    "total": 10,
                    "delayed": 2,
                    "cancelled": 0,
                    "avg_delay_minutes": 1.5
                }
            }
        ]);

        assert_ne!(normalize_for_diff(&a), normalize_for_diff(&b));
    }

    #[test]
    fn normalize_for_diff_ignores_live_sample_annotation_churn() {
        let a = serde_json::json!([
            {
                "severity": "severe-delays",
                "reason": "Major improvement works in the Wrexham General area (live samples show: 5 of 9 sampled services delayed.)",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "live"
            }
        ]);
        let b = serde_json::json!([
            {
                "severity": "severe-delays",
                "reason": "Major improvement works in the Wrexham General area (live samples show: 7 of 14 sampled services delayed.)",
                "validity": {"from_date": "2026-07-09T10:01:00Z"},
                "data_quality": "live"
            }
        ]);

        assert_eq!(normalize_for_diff(&a), normalize_for_diff(&b));
    }

    #[test]
    fn normalize_for_diff_still_detects_a_reason_change_under_a_live_sample_annotation() {
        let a = serde_json::json!([
            {
                "severity": "severe-delays",
                "reason": "Major improvement works in the Wrexham General area (live samples show: 5 of 9 sampled services delayed.)",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "live"
            }
        ]);
        let b = serde_json::json!([
            {
                "severity": "severe-delays",
                "reason": "Signal failure near Chester (live samples show: 5 of 9 sampled services delayed.)",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "live"
            }
        ]);

        assert_ne!(normalize_for_diff(&a), normalize_for_diff(&b));
    }

    #[test]
    fn strip_live_sample_annotation_only_strips_a_trailing_annotation() {
        assert_eq!(
            strip_live_sample_annotation("Works in the area (live samples show: 5 of 9 sampled services delayed.)"),
            "Works in the area",
        );
        assert_eq!(strip_live_sample_annotation("Works in the area"), "Works in the area");
        // A reason mentioning "live samples show" mid-sentence rather than as
        // the appended trailing annotation must not be touched.
        assert_eq!(
            strip_live_sample_annotation("Works in the area (live samples show: something) more text"),
            "Works in the area (live samples show: something) more text",
        );
    }

    // --- carry_forward_ldbws_from_date ---
    //
    // See docs/superpowers/specs/2026-08-30-inferred-time-ranges-design.md's
    // "Testing" section for the full list this covers.

    fn ldbws_status(from_date: &str, severity: &str, reason: &str) -> serde_json::Value {
        serde_json::json!([
            {
                "severity": severity,
                "reason": reason,
                "validity": {"from_date": from_date, "to_date": null, "is_now": true},
                "data_quality": "ldbws-inferred",
                "disruption": {
                    "category": "RealTime",
                    "description": reason,
                    "affected_stops": ["PAD"],
                    "affected_routes": [],
                    "source": "ldbws-sampling"
                },
                "sample_stats": {"total": 10, "delayed": 3, "cancelled": 0, "skipped": 0, "avg_delay_minutes": 4.0}
            }
        ])
    }

    fn knowledgebase_status(from_date: &str) -> serde_json::Value {
        serde_json::json!([
            {
                "severity": "minor-delays",
                "reason": "Signal failure near Reading",
                "validity": {"from_date": from_date, "to_date": null, "is_now": true},
                "data_quality": "knowledgebase",
                "disruption": {
                    "category": "RealTime",
                    "description": "Signal failure near Reading",
                    "affected_stops": [],
                    "affected_routes": [],
                    "source": "knowledgebase-incident-1"
                }
            }
        ])
    }

    #[test]
    fn carry_forward_keeps_the_old_from_date_when_content_is_unchanged() {
        let existing = ldbws_status("2026-08-30T06:00:00Z", "minor-delays", "3 of 10 sampled services delayed.");
        let fresh = ldbws_status("2026-08-30T09:30:00Z", "minor-delays", "3 of 10 sampled services delayed.");

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], existing[0]["validity"]["from_date"],
            "unchanged content should carry forward the OLD from_date, not the fresh stamp"
        );
    }

    #[test]
    fn carry_forward_uses_the_fresh_stamp_when_severity_changes() {
        let existing = ldbws_status("2026-08-30T06:00:00Z", "minor-delays", "3 of 10 sampled services delayed.");
        let fresh = ldbws_status("2026-08-30T09:30:00Z", "severe-delays", "7 of 10 sampled services delayed.");

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], fresh[0]["validity"]["from_date"],
            "a genuine severity change must not carry forward the old from_date"
        );
    }

    #[test]
    fn carry_forward_uses_the_fresh_stamp_when_reason_changes() {
        let existing = ldbws_status("2026-08-30T06:00:00Z", "minor-delays", "3 of 10 sampled services delayed.");
        let fresh = ldbws_status(
            "2026-08-30T09:30:00Z",
            "minor-delays",
            "3 of 10 sampled services skipping a scheduled stop.",
        );

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], fresh[0]["validity"]["from_date"],
            "a genuine reason change must not carry forward the old from_date"
        );
    }

    #[test]
    fn carry_forward_uses_the_fresh_stamp_when_no_prior_entry_exists() {
        let existing = serde_json::json!([]);
        let fresh = ldbws_status("2026-08-30T09:30:00Z", "minor-delays", "3 of 10 sampled services delayed.");

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], fresh[0]["validity"]["from_date"],
            "a line with no prior stored status has nothing to carry forward from"
        );
    }

    #[test]
    fn carry_forward_uses_the_fresh_stamp_when_the_prior_entry_has_a_different_data_quality() {
        let existing = knowledgebase_status("2026-08-30T06:00:00Z");
        let fresh = ldbws_status("2026-08-30T09:30:00Z", "minor-delays", "3 of 10 sampled services delayed.");

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], fresh[0]["validity"]["from_date"],
            "an incident replaced by LDBWS inference is a new status, not a continuation"
        );
    }

    #[test]
    fn carry_forward_never_touches_a_non_ldbws_fresh_entry() {
        // A Knowledgebase/Planned entry's from_date already comes from real
        // incident data and must be left alone regardless of what the
        // previous cycle stored.
        let existing = knowledgebase_status("2026-08-30T06:00:00Z");
        let fresh = knowledgebase_status("2026-08-30T09:30:00Z");

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(result, fresh, "non-ldbws-inferred entries must pass through untouched");
    }

    #[test]
    fn carry_forward_applies_to_good_service_entries_too() {
        let existing = ldbws_status("2026-08-30T06:00:00Z", "good-service", "Good Service");
        let fresh = ldbws_status("2026-08-30T09:30:00Z", "good-service", "Good Service");

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], existing[0]["validity"]["from_date"],
            "good_service() entries should carry forward exactly like any other ldbws-inferred entry"
        );
    }

    #[test]
    fn carry_forward_is_not_defeated_by_sample_stats_or_live_annotation_churn() {
        // Deliberately built without the shared `ldbws_status`/`disruption`
        // helper: `disruption.description` is not one of the fields
        // `normalize_entry_for_diff` strips (nor should it be -- a real
        // `infer_from_samples` entry never puts the live-sample-count
        // suffix into `description`, only into top-level `reason`), so
        // exercising that specific stripping needs a fixture that isolates
        // it, matching this file's existing
        // `normalize_for_diff_ignores_live_sample_annotation_churn` style.
        let existing = serde_json::json!([
            {
                "severity": "severe-delays",
                "reason": "Points failure at Crewe (live samples show: 4 of 8 sampled services delayed.)",
                "validity": {"from_date": "2026-08-30T06:00:00Z", "to_date": null, "is_now": true},
                "data_quality": "ldbws-inferred",
                "sample_stats": {"total": 8, "delayed": 4, "cancelled": 0, "skipped": 0, "avg_delay_minutes": 12.0}
            }
        ]);
        let fresh = serde_json::json!([
            {
                "severity": "severe-delays",
                "reason": "Points failure at Crewe (live samples show: 9 of 17 sampled services delayed.)",
                "validity": {"from_date": "2026-08-30T09:30:00Z", "to_date": null, "is_now": true},
                "data_quality": "ldbws-inferred",
                "sample_stats": {"total": 17, "delayed": 9, "cancelled": 1, "skipped": 2, "avg_delay_minutes": 15.5}
            }
        ]);

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], existing[0]["validity"]["from_date"],
            "sample_stats/live-count-suffix churn alone must not defeat the carry-forward"
        );
        // And the churned fields themselves must still be the fresh values,
        // not accidentally overwritten along with from_date.
        assert_eq!(result[0]["sample_stats"], fresh[0]["sample_stats"]);
        assert_eq!(result[0]["reason"], fresh[0]["reason"]);
    }
}
