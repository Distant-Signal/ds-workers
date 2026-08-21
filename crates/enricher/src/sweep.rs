//! Hourly reconciliation sweep: the backstop that catches any incident
//! whose text changed but whose `incident-text-changed` Redis Stream event
//! was missed (publish failure, consumer downtime, etc). Deliberately uses
//! runtime-checked `sqlx::query_as` rather than the `query_as!` macro --
//! see `crates/api/src/data/queries.rs` module docs for why this project
//! avoids the `query!`/`query_as!` macro family project-wide (no
//! `DATABASE_URL`/`.sqlx` cache guaranteed at compile time).

use sqlx::PgPool;

use crate::hash::text_hash;

pub struct SweepRow {
    pub incident_id: String,
    pub summary: String,
    pub description: String,
    pub source_text_hash: Option<String>,
    pub extraction_model_version: Option<String>,
}

/// Incidents whose current text hash doesn't match what's stored, or whose
/// last extraction ran under a different model/prompt version -- either
/// case means the stored extraction (if any) is stale and needs redoing.
/// Pure so it's testable without a database; `fetch_sweep_rows` below is
/// the thin, untested DB-fetching wrapper, following this codebase's
/// existing pattern of keeping query functions thin and testing the pure
/// logic they feed.
pub fn incidents_needing_extraction(rows: &[SweepRow], current_model_version: &str) -> Vec<String> {
    rows.iter()
        .filter(|row| {
            let current_hash = text_hash(&row.summary, &row.description);
            row.source_text_hash.as_deref() != Some(current_hash.as_str())
                || row.extraction_model_version.as_deref() != Some(current_model_version)
        })
        .map(|row| row.incident_id.clone())
        .collect()
}

pub async fn fetch_sweep_rows(pool: &PgPool) -> anyhow::Result<Vec<SweepRow>> {
    let rows = sqlx::query_as::<_, SweepRowRecord>(
        "SELECT incident_id, summary, description, source_text_hash, extraction_model_version \
         FROM incidents WHERE NOT is_cleared",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| SweepRow {
            incident_id: r.incident_id,
            summary: r.summary,
            description: r.description,
            source_text_hash: r.source_text_hash,
            extraction_model_version: r.extraction_model_version,
        })
        .collect())
}

#[derive(sqlx::FromRow)]
struct SweepRowRecord {
    incident_id: String,
    summary: String,
    description: String,
    source_text_hash: Option<String>,
    extraction_model_version: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, summary: &str, hash: Option<&str>, model: Option<&str>) -> SweepRow {
        SweepRow {
            incident_id: id.to_string(),
            summary: summary.to_string(),
            description: "desc".to_string(),
            source_text_hash: hash.map(str::to_string),
            extraction_model_version: model.map(str::to_string),
        }
    }

    #[test]
    fn never_extracted_incident_needs_extraction() {
        let rows = vec![row("A", "text", None, None)];
        assert_eq!(incidents_needing_extraction(&rows, "gpt-oss-20b"), vec!["A"]);
    }

    #[test]
    fn matching_hash_and_model_does_not_need_extraction() {
        let hash = text_hash("text", "desc");
        let rows = vec![row("A", "text", Some(&hash), Some("gpt-oss-20b"))];
        assert!(incidents_needing_extraction(&rows, "gpt-oss-20b").is_empty());
    }

    #[test]
    fn changed_text_needs_re_extraction_even_with_matching_model() {
        let stale_hash = text_hash("old text", "desc");
        let rows = vec![row("A", "new text", Some(&stale_hash), Some("gpt-oss-20b"))];
        assert_eq!(incidents_needing_extraction(&rows, "gpt-oss-20b"), vec!["A"]);
    }

    #[test]
    fn model_version_bump_forces_re_extraction_even_with_matching_hash() {
        let hash = text_hash("text", "desc");
        let rows = vec![row("A", "text", Some(&hash), Some("gpt-oss-20b@prompt-v1"))];
        assert_eq!(incidents_needing_extraction(&rows, "gpt-oss-20b@prompt-v2"), vec!["A"]);
    }
}
