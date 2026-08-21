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
pub async fn prune_removed_lines(pool: &PgPool, current_line_ids: &[String]) -> Result<u64> {
    let result = sqlx::query("DELETE FROM line_status WHERE NOT (line_id = ANY($1))")
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
///
/// Without stripping both, a "change" would be seen on every single poll
/// cycle for most lines, defeating the point of only recording history on
/// real changes.
fn normalize_for_diff(statuses: &serde_json::Value) -> serde_json::Value {
    let mut statuses = statuses.clone();
    if let Some(entries) = statuses.as_array_mut() {
        for entry in entries {
            if let Some(validity) = entry.get_mut("validity").and_then(|v| v.as_object_mut()) {
                validity.remove("from_date");
            }
            if let Some(obj) = entry.as_object_mut() {
                obj.remove("sample_stats");
            }
        }
    }
    statuses
}

/// Upserts one line's computed report into `line_status` (always), and
/// inserts a `line_status_history` snapshot only if the statuses actually
/// changed since the last cycle.
pub async fn write_line_status(pool: &PgPool, report: &LineStatusReport) -> Result<()> {
    let statuses_json = serde_json::to_value(&report.statuses)?;

    let changed = match existing_statuses(pool, &report.id).await? {
        None => true,
        Some(existing) => normalize_for_diff(&existing) != normalize_for_diff(&statuses_json),
    };

    sqlx::query(
        r#"
        INSERT INTO line_status (line_id, name, mode_name, operators, statuses, computed_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (line_id) DO UPDATE SET
            name        = EXCLUDED.name,
            mode_name   = EXCLUDED.mode_name,
            operators   = EXCLUDED.operators,
            statuses    = EXCLUDED.statuses,
            computed_at = NOW()
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
}
