//! Persistence for extraction results. Deliberately uses runtime-checked
//! `sqlx::query`/`query_as` rather than the `query!`/`query_as!` macro
//! family -- see `crates/api/src/data/queries.rs` module docs for why this
//! project avoids that family project-wide (no `DATABASE_URL`/`.sqlx` cache
//! guaranteed at compile time).

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::llm::PrimaryExtraction;

pub struct IncidentState {
    pub summary: String,
    pub description: String,
    pub source_text_hash: Option<String>,
    pub extraction_model_version: Option<String>,
}

/// Fetches the extractable prose for one incident, plus what it was last
/// (successfully) extracted against -- lets the caller skip re-running the
/// LLM entirely when nothing has changed since. Returns `Ok(None)` if the
/// incident no longer exists (e.g. it was cleared/purged between the
/// stream event or sweep row being read and processing running).
pub async fn fetch_incident_state(pool: &PgPool, incident_id: &str) -> anyhow::Result<Option<IncidentState>> {
    let row: Option<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT summary, description, source_text_hash, extraction_model_version \
         FROM incidents WHERE incident_id = $1",
    )
    .bind(incident_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(summary, description, source_text_hash, extraction_model_version)| IncidentState {
        summary,
        description,
        source_text_hash,
        extraction_model_version,
    }))
}

/// Persists a completed extraction. `resolution_status`/`confidence` and
/// `severity`/`severity_confidence` are passed separately from `extraction`
/// because they're the output of `combine::combine`/`combine::combine_severity`,
/// not the raw primary-pass verdicts.
#[allow(clippy::too_many_arguments)]
pub async fn write_extraction(
    pool: &PgPool,
    incident_id: &str,
    extraction: &PrimaryExtraction,
    resolution_status: &str,
    confidence: &str,
    severity: &str,
    severity_confidence: &str,
    model_version: &str,
    text_hash: &str,
) -> anyhow::Result<()> {
    let schedule_window_json = extraction
        .schedule_window
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let eta: Option<DateTime<Utc>> = extraction.eta;

    sqlx::query(
        "UPDATE incidents SET \
            source_text_hash = $2, \
            extracted_category = $3, \
            extracted_resolution_status = $4, \
            extracted_schedule_window = $5, \
            extracted_eta = $6, \
            extraction_confidence = $7, \
            extracted_severity = $8, \
            extracted_severity_confidence = $9, \
            extraction_model_version = $10, \
            extracted_at = NOW() \
         WHERE incident_id = $1",
    )
    .bind(incident_id)
    .bind(text_hash)
    .bind(&extraction.category)
    .bind(resolution_status)
    .bind(&schedule_window_json)
    .bind(eta)
    .bind(confidence)
    .bind(severity)
    .bind(severity_confidence)
    .bind(model_version)
    .execute(pool)
    .await?;

    Ok(())
}
