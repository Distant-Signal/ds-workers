//! Persistence for extraction results. Deliberately uses runtime-checked
//! `sqlx::query`/`query_as` rather than the `query!`/`query_as!` macro
//! family -- see `crates/api/src/data/queries.rs` module docs for why this
//! project avoids that family project-wide (no `DATABASE_URL`/`.sqlx` cache
//! guaranteed at compile time).

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::llm::ExtractionPeriod;

pub struct IncidentState {
    pub summary: String,
    pub description: String,
    pub source_text_hash: Option<String>,
    pub extraction_model_version: Option<String>,
    /// Reference date threaded into `LlmClient::extract_primary` for
    /// year-less-date resolution (design §1) -- the incident's own
    /// `first_seen_at`, always populated (`NOT NULL DEFAULT NOW()` since
    /// `20260716180000_incident_first_seen.sql`).
    pub first_seen_at: DateTime<Utc>,
}

/// Fetches the extractable prose for one incident, plus what it was last
/// (successfully) extracted against -- lets the caller skip re-running the
/// LLM entirely when nothing has changed since. Returns `Ok(None)` if the
/// incident no longer exists (e.g. it was cleared/purged between the
/// stream event or sweep row being read and processing running).
type IncidentStateRow = (String, String, Option<String>, Option<String>, DateTime<Utc>);

pub async fn fetch_incident_state(pool: &PgPool, incident_id: &str) -> anyhow::Result<Option<IncidentState>> {
    let row: Option<IncidentStateRow> = sqlx::query_as(
        "SELECT summary, description, source_text_hash, extraction_model_version, first_seen_at \
         FROM incidents WHERE incident_id = $1",
    )
    .bind(incident_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(summary, description, source_text_hash, extraction_model_version, first_seen_at)| IncidentState {
        summary,
        description,
        source_text_hash,
        extraction_model_version,
        first_seen_at,
    }))
}

/// Persists a completed extraction. `category`/`periods` are the fields
/// this crate writes going forward -- `extracted_periods` replaces the six
/// deprecated flat columns (`extracted_resolution_status`,
/// `extracted_schedule_window`, `extracted_eta`, `extraction_confidence`,
/// `extracted_severity`, `extracted_severity_confidence`), which are left
/// untouched at the SQL/table level (design §3/§5's two-step migration --
/// this code path simply stops writing them). `periods` is the output of
/// `combine::combine_periods`, so its `resolution_status_confidence`/
/// `severity_confidence` fields are already populated.
pub async fn write_extraction(
    pool: &PgPool,
    incident_id: &str,
    category: &str,
    periods: &[ExtractionPeriod],
    model_version: &str,
    text_hash: &str,
) -> anyhow::Result<()> {
    let periods_json = serde_json::to_value(periods)?;

    sqlx::query(
        "UPDATE incidents SET \
            source_text_hash = $2, \
            extracted_category = $3, \
            extracted_periods = $4, \
            extraction_model_version = $5, \
            extracted_at = NOW() \
         WHERE incident_id = $1",
    )
    .bind(incident_id)
    .bind(text_hash)
    .bind(category)
    .bind(&periods_json)
    .bind(model_version)
    .execute(pool)
    .await?;

    Ok(())
}
