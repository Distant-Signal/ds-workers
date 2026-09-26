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
type IncidentStateRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    DateTime<Utc>,
);

pub async fn fetch_incident_state(
    pool: &PgPool,
    incident_id: &str,
) -> anyhow::Result<Option<IncidentState>> {
    let row: Option<IncidentStateRow> = sqlx::query_as(
        "SELECT summary, description, source_text_hash, extraction_model_version, first_seen_at \
         FROM incidents WHERE incident_id = $1",
    )
    .bind(incident_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(summary, description, source_text_hash, extraction_model_version, first_seen_at)| {
            IncidentState {
                summary,
                description,
                source_text_hash,
                extraction_model_version,
                first_seen_at,
            }
        },
    ))
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
///
/// `expected_summary`/`expected_description` are the exact `summary`/
/// `description` text `process_incident` read (via
/// [`fetch_incident_state`]) *before* running the three LLM calls this
/// extraction is the result of -- not necessarily the row's *current* text.
/// The stream loop, the hourly sweep, and the reclaim loop can all call
/// `process_incident` for the same `incident_id` concurrently with no
/// per-incident lock, so a slow extraction (e.g. a sweep re-running after a
/// model-version bump) can still be in flight when the incident's text
/// changes again and a second, faster extraction (typically the
/// stream-triggered one) finishes and writes first. Without a guard here,
/// the slow extraction would then land second and silently overwrite the
/// fresher result with output computed from stale text.
///
/// The `AND summary = $6 AND description = $7` clause closes that race:
/// the UPDATE only applies if the row's live text still matches what was
/// read at extraction start. Comparing the raw text (rather than, say,
/// adding a new "current live text hash" column and comparing hashes) needs
/// no schema change and no risk of a hash-algorithm drift between this
/// query and `common::text_hash::text_hash` -- it's a strictly stronger
/// check than a hash comparison would be, since it can't false-positive on a collision.
/// Returns `Ok(false)` (nothing written) when the guard rejects the write,
/// distinguishing "this extraction is stale, discard it" from "the DB call
/// itself failed" -- the caller uses this to decide how to log/ack rather
/// than treating a stale write as an error.
///
/// `#[allow(clippy::too_many_arguments)]`: `expected_summary`/
/// `expected_description` push this from seven to eight arguments; bundling
/// them (or the whole race-guard pair) into a struct just to satisfy the
/// lint would separate the guard's two halves from the five extraction
/// fields they're guarding, for no readability win at this call's one
/// call site (`main.rs`'s `process_incident`).
#[allow(clippy::too_many_arguments)]
pub async fn write_extraction(
    pool: &PgPool,
    incident_id: &str,
    category: &str,
    periods: &[ExtractionPeriod],
    model_version: &str,
    text_hash: &str,
    expected_summary: &str,
    expected_description: &str,
) -> anyhow::Result<bool> {
    let periods_json = serde_json::to_value(periods)?;

    let result = sqlx::query(
        "UPDATE incidents SET \
            source_text_hash = $2, \
            extracted_category = $3, \
            extracted_periods = $4, \
            extraction_model_version = $5, \
            extracted_at = NOW() \
         WHERE incident_id = $1 AND summary = $6 AND description = $7",
    )
    .bind(incident_id)
    .bind(text_hash)
    .bind(category)
    .bind(&periods_json)
    .bind(model_version)
    .bind(expected_summary)
    .bind(expected_description)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    async fn test_pool() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    fn one_period() -> ExtractionPeriod {
        ExtractionPeriod {
            scope_description: None,
            date_range: None,
            schedule_window: None,
            resolution_status: "ongoing".to_string(),
            apparent_severity: "moderate_disruption".to_string(),
            impact_type: None,
            resolution_status_confidence: "high".to_string(),
            severity_confidence: "high".to_string(),
        }
    }

    /// Reproduces finding #1's exact race: a slow extraction (e.g. a sweep
    /// re-run after a model-version bump) is still holding the summary/
    /// description it read at extraction start when the incident's text
    /// changes underneath it (simulating a faster, concurrent
    /// stream-triggered extraction winning the race to update the row
    /// first). The slow extraction's write must be rejected -- it must not
    /// clobber the fresher text/state now sitting in the row -- and
    /// `write_extraction` must report that via `Ok(false)`, not an error
    /// and not a silent, indistinguishable success.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher write_extraction -- --ignored`"]
    async fn write_extraction_discards_a_stale_write_when_the_text_moved_underneath_it() {
        let pool = test_pool().await;
        let incident_id = "TEST-ENRICHER-RACE-GUARD-1";
        let original_summary = "Signal failure at Crewe";
        let original_description = "Trains between Crewe and Chester delayed by up to 20 minutes.";
        let updated_summary = "Signal failure at Crewe (update)";
        let updated_description = "Trains between Crewe and Chester delayed by up to 20 minutes. Normal service has now resumed.";

        sqlx::query(
            "INSERT INTO incidents (incident_id, summary, description, operators, affected_stations, priority) \
             VALUES ($1, $2, $3, '{}', '{}', 3) \
             ON CONFLICT (incident_id) DO UPDATE SET summary = EXCLUDED.summary, description = EXCLUDED.description, \
                 source_text_hash = NULL, extraction_model_version = NULL, extracted_periods = NULL, \
                 extracted_category = NULL",
        )
        .bind(incident_id)
        .bind(original_summary)
        .bind(original_description)
        .execute(&pool)
        .await
        .expect("seed fixture incident row");

        // The "slow" extraction read this text at its extraction start.
        let stale_hash = common::text_hash::text_hash(original_summary, original_description);

        // A faster, concurrent extraction (or a direct edit) changes the
        // row's text before the slow extraction above gets to write --
        // exactly the race finding #1 describes.
        sqlx::query("UPDATE incidents SET summary = $2, description = $3 WHERE incident_id = $1")
            .bind(incident_id)
            .bind(updated_summary)
            .bind(updated_description)
            .execute(&pool)
            .await
            .expect("simulate a concurrent text change");

        // The slow extraction now tries to write its (stale) result.
        let applied = write_extraction(
            &pool,
            incident_id,
            "signal_failure",
            &[one_period()],
            "test-model@periods-v2",
            &stale_hash,
            original_summary,
            original_description,
        )
        .await
        .expect("write_extraction must not itself error on a stale write, just reject it");

        assert!(
            !applied,
            "a write whose expected summary/description no longer match the row must be rejected"
        );

        let row: (Option<String>, Option<String>, String, String) = sqlx::query_as(
            "SELECT extracted_category, extraction_model_version, summary, description \
             FROM incidents WHERE incident_id = $1",
        )
        .bind(incident_id)
        .fetch_one(&pool)
        .await
        .expect("fetch row after the rejected write");

        assert_eq!(
            row.0, None,
            "the stale extraction's category must not have been written"
        );
        assert_eq!(
            row.1, None,
            "the stale extraction's model version must not have been written"
        );
        assert_eq!(
            row.2, updated_summary,
            "the fresher text must survive untouched"
        );
        assert_eq!(
            row.3, updated_description,
            "the fresher text must survive untouched"
        );

        // Sanity check the positive case in the same test: a write whose
        // expected text DOES match the row's current text must still apply
        // normally.
        let current_hash = common::text_hash::text_hash(updated_summary, updated_description);
        let applied = write_extraction(
            &pool,
            incident_id,
            "signal_failure",
            &[one_period()],
            "test-model@periods-v2",
            &current_hash,
            updated_summary,
            updated_description,
        )
        .await
        .expect("write_extraction against matching text must not error");
        assert!(
            applied,
            "a write whose expected summary/description match the row's current text must apply"
        );

        sqlx::query("DELETE FROM incidents WHERE incident_id = $1")
            .bind(incident_id)
            .execute(&pool)
            .await
            .expect("cleanup");
    }
}
