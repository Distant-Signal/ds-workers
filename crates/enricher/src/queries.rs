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
    /// The previous extraction's output, read only so `churn` can compare
    /// it with its replacement before `write_extraction` overwrites it.
    /// Kept as raw JSON here (parsed leniently in `churn`) so a stored value
    /// that no longer deserializes can never fail this fetch.
    pub extracted_category: Option<String>,
    pub extracted_periods: Option<serde_json::Value>,
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
    Option<String>,
    Option<serde_json::Value>,
);

pub async fn fetch_incident_state(
    pool: &PgPool,
    incident_id: &str,
) -> anyhow::Result<Option<IncidentState>> {
    let row: Option<IncidentStateRow> = sqlx::query_as(
        "SELECT summary, description, source_text_hash, extraction_model_version, first_seen_at, \
                extracted_category, extracted_periods \
         FROM incidents WHERE incident_id = $1",
    )
    .bind(incident_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(
            summary,
            description,
            source_text_hash,
            extraction_model_version,
            first_seen_at,
            extracted_category,
            extracted_periods,
        )| IncidentState {
            summary,
            description,
            source_text_hash,
            extraction_model_version,
            first_seen_at,
            extracted_category,
            extracted_periods,
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

/// The summary/description a stored extraction was
/// computed from, recovered from `incident_history` by recomputing
/// `common::text_hash::text_hash` in SQL (`sha256(summary || 0x00 ||
/// description)`, hex) -- every text version is snapshotted there by
/// `upsert_incidents`, so no schema change is needed. Verified read-only in
/// production on 2026-09-27: all 1,801 stored `source_text_hash` values
/// match a history row. `Ok(None)` if nothing matches (history purged, or a
/// hash written by some other path) -- the caller then does a full
/// extraction, exactly as today.
pub async fn fetch_extracted_source_text(
    pool: &PgPool,
    incident_id: &str,
    source_text_hash: &str,
) -> anyhow::Result<Option<(String, String)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT summary, description FROM incident_history \
         WHERE incident_id = $1 \
           AND encode(sha256(convert_to(summary, 'UTF8') || '\\x00'::bytea \
                             || convert_to(description, 'UTF8')), 'hex') = $2 \
         ORDER BY recorded_at DESC, id DESC LIMIT 1",
    )
    .bind(incident_id)
    .bind(source_text_hash)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Re-stamps an incident's *existing* extraction as
/// describing its current text, for a change `text_delta::classify` judged a
/// semantic no-op -- no LLM call, extraction columns untouched,
/// `extracted_at` untouched (it still says when the LLM last ran).
///
/// Guards, all in the WHERE clause so the check and the write are atomic:
/// - `summary = $4 AND description = $5`: the same stale-text guard as
///   [`write_extraction`] -- the text must still be what was classified.
/// - `source_text_hash = $3`: the extraction being carried forward must
///   still be the one classified against (a concurrent full extraction may
///   have replaced it; then this is a no-op and that one wins).
/// - `extraction_model_version = $6`: never carry an old model's reading
///   past a model bump -- the sweep must re-extract those.
///
/// `Ok(false)` = a guard rejected it; the caller falls back to a full
/// extraction (or acks, if the text moved -- same as a stale write).
pub async fn carry_forward_extraction(
    pool: &PgPool,
    incident_id: &str,
    new_text_hash: &str,
    old_text_hash: &str,
    expected_summary: &str,
    expected_description: &str,
    model_version: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE incidents SET source_text_hash = $2 \
         WHERE incident_id = $1 AND source_text_hash = $3 \
           AND summary = $4 AND description = $5 AND extraction_model_version = $6",
    )
    .bind(incident_id)
    .bind(new_text_hash)
    .bind(old_text_hash)
    .bind(expected_summary)
    .bind(expected_description)
    .bind(model_version)
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
    #[ignore = "requires a live database; run with `cargo test -p enricher write_extraction -- --ignored --test-threads=1`"]
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

    async fn seed(pool: &PgPool, incident_id: &str, summary: &str, description: &str) {
        sqlx::query("DELETE FROM incident_history WHERE incident_id = $1")
            .bind(incident_id)
            .execute(pool)
            .await
            .expect("clear history");
        sqlx::query(
            "INSERT INTO incidents (incident_id, summary, description, operators, affected_stations, priority) \
             VALUES ($1, $2, $3, '{}', '{}', 3) \
             ON CONFLICT (incident_id) DO UPDATE SET summary = EXCLUDED.summary, description = EXCLUDED.description, \
                 source_text_hash = NULL, extraction_model_version = NULL, extracted_periods = NULL, \
                 extracted_category = NULL",
        )
        .bind(incident_id)
        .bind(summary)
        .bind(description)
        .execute(pool)
        .await
        .expect("seed incident");
    }

    async fn add_history(pool: &PgPool, incident_id: &str, summary: &str, description: &str) {
        sqlx::query(
            "INSERT INTO incident_history (incident_id, summary, description, operators, affected_stations, is_planned, priority) \
             VALUES ($1, $2, $3, '{}', '{}', false, 3)",
        )
        .bind(incident_id)
        .bind(summary)
        .bind(description)
        .execute(pool)
        .await
        .expect("seed history");
    }

    async fn cleanup(pool: &PgPool, incident_id: &str) {
        for table in ["incident_history", "incidents"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE incident_id = $1"))
                .bind(incident_id)
                .execute(pool)
                .await
                .expect("cleanup");
        }
    }

    /// The SQL re-computation of `text_hash` must match the Rust one, byte
    /// for byte, including non-ASCII text -- otherwise no old text is ever
    /// recovered and every edit is labelled `unknown`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher fetch_extracted_source_text -- --ignored --test-threads=1`"]
    async fn fetch_extracted_source_text_recovers_the_hashed_version_from_history() {
        let pool = test_pool().await;
        let incident_id = "TEST-ENRICHER-HISTORY-TEXT-1";
        seed(&pool, incident_id, "s", "d").await;
        add_history(
            &pool,
            incident_id,
            "Crewe \u{2013} Chester",
            "Caf\u{e9} <p>closed</p>",
        )
        .await;
        add_history(&pool, incident_id, "Crewe - Chester", "reopened").await;

        let hash =
            common::text_hash::text_hash("Crewe \u{2013} Chester", "Caf\u{e9} <p>closed</p>");
        let found = fetch_extracted_source_text(&pool, incident_id, &hash)
            .await
            .unwrap();
        assert_eq!(
            found,
            Some((
                "Crewe \u{2013} Chester".to_string(),
                "Caf\u{e9} <p>closed</p>".to_string()
            ))
        );
        let missing = fetch_extracted_source_text(&pool, incident_id, "not-a-hash")
            .await
            .unwrap();
        assert_eq!(missing, None);

        cleanup(&pool, incident_id).await;
    }

    /// Every guard of `carry_forward_extraction`: it applies only while the
    /// text, the extraction being carried and the model version are all
    /// still what the caller classified against.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher carry_forward_extraction -- --ignored --test-threads=1`"]
    async fn carry_forward_extraction_applies_only_when_every_guard_holds() {
        let pool = test_pool().await;
        let incident_id = "TEST-ENRICHER-CARRY-FORWARD-GUARDS-1";
        let (summary, old_description, new_description) = ("s", "<p>closed</p>", "closed");
        let model = "test-model@periods-v2";
        let old_hash = common::text_hash::text_hash(summary, old_description);
        let new_hash = common::text_hash::text_hash(summary, new_description);

        // Extraction stored against the old text; the row now holds the
        // new (semantically identical) text.
        let reset = |pool: PgPool| {
            let old_hash = old_hash.clone();
            async move {
                seed(&pool, incident_id, summary, old_description).await;
                assert!(
                    write_extraction(
                        &pool,
                        incident_id,
                        "signal_failure",
                        &[one_period()],
                        model,
                        &old_hash,
                        summary,
                        old_description,
                    )
                    .await
                    .unwrap()
                );
                sqlx::query("UPDATE incidents SET description = $2 WHERE incident_id = $1")
                    .bind(incident_id)
                    .bind(new_description)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        };
        let hash_now = |pool: PgPool| async move {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT source_text_hash FROM incidents WHERE incident_id = $1",
            )
            .bind(incident_id)
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        // Text moved again since classification.
        reset(pool.clone()).await;
        assert!(
            !carry_forward_extraction(
                &pool,
                incident_id,
                &new_hash,
                &old_hash,
                summary,
                "stale",
                model
            )
            .await
            .unwrap()
        );
        // The stored extraction was replaced since classification.
        assert!(
            !carry_forward_extraction(
                &pool,
                incident_id,
                &new_hash,
                "other-hash",
                summary,
                new_description,
                model
            )
            .await
            .unwrap()
        );
        // A model bump: never carry an old model's reading forward.
        assert!(
            !carry_forward_extraction(
                &pool,
                incident_id,
                &new_hash,
                &old_hash,
                summary,
                new_description,
                "other@v"
            )
            .await
            .unwrap()
        );
        assert_eq!(
            hash_now(pool.clone()).await.as_deref(),
            Some(old_hash.as_str())
        );

        // All guards hold.
        assert!(
            carry_forward_extraction(
                &pool,
                incident_id,
                &new_hash,
                &old_hash,
                summary,
                new_description,
                model
            )
            .await
            .unwrap()
        );
        assert_eq!(
            hash_now(pool.clone()).await.as_deref(),
            Some(new_hash.as_str())
        );

        cleanup(&pool, incident_id).await;
    }
}
