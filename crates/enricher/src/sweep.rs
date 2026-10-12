//! Hourly reconciliation sweep: the backstop that catches any incident
//! whose text changed but whose `incident-text-changed` Redis Stream event
//! was missed (publish failure, consumer downtime, etc). Deliberately uses
//! runtime-checked `sqlx::query_as` rather than the `query_as!` macro --
//! see `crates/api/src/data/queries.rs` module docs for why this project
//! avoids the `query!`/`query_as!` macro family project-wide (no
//! `DATABASE_URL`/`.sqlx` cache guaranteed at compile time).

use sqlx::PgPool;

use common::text_hash::text_hash;

pub(crate) struct SweepRow {
    pub incident_id: String,
    pub summary: String,
    pub description: String,
    pub source_text_hash: Option<String>,
    pub extraction_model_version: Option<String>,
}

/// Incidents whose current text hash doesn't match what's stored, or whose
/// last extraction ran under a different model/prompt version -- either
/// case means the stored extraction (if any) is stale and needs redoing.
/// Pure so it's testable without a database. `fetch_sweep_rows` already
/// applies the same test in SQL; this stays the authority over what it
/// returns (see [`SQL_TEXT_HASH`]).
pub(crate) fn incidents_needing_extraction(
    rows: &[SweepRow],
    current_model_version: &str,
) -> Vec<String> {
    rows.iter()
        .filter(|row| {
            let current_hash = text_hash(&row.summary, &row.description);
            row.source_text_hash.as_deref() != Some(current_hash.as_str())
                || row.extraction_model_version.as_deref() != Some(current_model_version)
        })
        .map(|row| row.incident_id.clone())
        .collect()
}

/// [`common::text_hash::text_hash`] in SQL: the lowercase hex SHA-256 of
/// the summary's UTF-8 bytes, a NUL byte, then the description's. Built in
/// since Postgres 11 (`sha256(bytea)`). `convert_to(.., 'UTF8')` gives the
/// same bytes Rust hashes whatever the database encoding.
///
/// It can only err towards returning too much: the stored hash was computed
/// by `text_hash`, so if this expression ever hashed different bytes it
/// would mismatch every row, the sweep would fetch them all (as before
/// this filter), and [`incidents_needing_extraction`] would still pick out
/// exactly the stale ones. `the_sql_text_hash_matches_text_hash` pins the
/// two together.
pub(crate) const SQL_TEXT_HASH: &str = "encode(sha256(convert_to(summary, 'UTF8') \
     || decode('00', 'hex') || convert_to(description, 'UTF8')), 'hex')";

/// The live incidents whose extraction is stale, with their text: not
/// cleared, still listed by the feed, and either a stored
/// `source_text_hash` that isn't the current text's (or none) or an
/// `extraction_model_version` other than `model_version` (or none) -- the
/// same test as [`incidents_needing_extraction`], in SQL, so a sweep only
/// transfers the text of the few rows that need work instead of every live
/// incident's (~918 rows, ~1.4 MB in prod, 2026-10).
///
/// An incident the feed stopped listing ("ended", `source_removed_at`, see
/// docs/superpowers/specs/2026-10-06-incident-source-removal-design.md)
/// can never be shown as live again without being re-listed, and a
/// re-listed one clears `source_removed_at` and is swept again. The
/// per-text backoff is applied later, by `preflight`, as before.
pub(crate) async fn fetch_sweep_rows(
    pool: &PgPool,
    model_version: &str,
) -> anyhow::Result<Vec<SweepRow>> {
    let sql = format!(
        "SELECT incident_id, summary, description, source_text_hash, extraction_model_version \
         FROM incidents WHERE NOT is_cleared AND source_removed_at IS NULL \
         AND (source_text_hash IS DISTINCT FROM {SQL_TEXT_HASH} \
              OR extraction_model_version IS DISTINCT FROM $1)"
    );
    let rows = sqlx::query_as::<_, SweepRowRecord>(&sql)
        .bind(model_version)
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
        assert_eq!(
            incidents_needing_extraction(&rows, "gpt-oss-20b"),
            vec!["A"]
        );
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
        assert_eq!(
            incidents_needing_extraction(&rows, "gpt-oss-20b"),
            vec!["A"]
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p enricher \
                sweep -- --ignored --test-threads=1`"]
    async fn the_sweep_loads_only_live_incidents() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        let cleanup = "DELETE FROM incidents WHERE incident_id LIKE 'TEST-SWEEP-%'";
        sqlx::query(cleanup).execute(&pool).await.expect("cleanup");
        sqlx::query(
            "INSERT INTO incidents \
                (incident_id, summary, description, operators, affected_stations, priority, \
                 is_planned, is_cleared, source_removed_at) \
             VALUES \
                ('TEST-SWEEP-ACTIVE', 's', 'd', '{}', '{}', 1, false, false, NULL), \
                ('TEST-SWEEP-CLEARED', 's', 'd', '{}', '{}', 1, false, true, NULL), \
                ('TEST-SWEEP-ENDED', 's', 'd', '{}', '{}', 1, true, false, now())",
        )
        .execute(&pool)
        .await
        .expect("seed");

        let rows = fetch_sweep_rows(&pool, "m@v").await.expect("sweep rows");
        sqlx::query(cleanup).execute(&pool).await.expect("cleanup");

        let ids: Vec<&str> = rows
            .iter()
            .map(|r| r.incident_id.as_str())
            .filter(|id| id.starts_with("TEST-SWEEP-"))
            .collect();
        assert_eq!(ids, vec!["TEST-SWEEP-ACTIVE"]);
    }

    async fn test_pool() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        sqlx::postgres::PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    /// Texts whose bytes are easy to get wrong: non-ASCII, a 4-byte
    /// character, combining marks, line endings, quotes, backslashes, an
    /// empty field, and the boundary case the separator exists for.
    const AWKWARD_TEXTS: [(&str, &str); 8] = [
        ("Signal failure at Crewe", "Lines are closed."),
        ("", ""),
        ("ab", ""),
        ("a", "b"),
        (
            "Fares \u{a3}5 \u{2013} caf\u{e9}",
            "Cafe\u{301} \u{1f686} rail\r\nreplacement",
        ),
        ("<p>It's \"closed\"</p>", "back\\slash\tand\ttabs\n"),
        (
            "  leading and trailing  ",
            "\u{a0}non-breaking\u{2028}separator",
        ),
        ("Z\u{fc}rich", "\u{5d0}\u{5d1} \u{4e2d}\u{6587} \u{1f600}"),
    ];

    /// The SQL hash is byte-for-byte `text_hash`, so a fresh row isn't
    /// re-fetched every sweep.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p enricher sweep -- --ignored --test-threads=1`"]
    async fn the_sql_text_hash_matches_text_hash() {
        let pool = test_pool().await;
        for (summary, description) in AWKWARD_TEXTS {
            let sql: String = sqlx::query_scalar(&format!(
                "SELECT {SQL_TEXT_HASH} FROM (SELECT $1::text AS summary, $2::text AS description) t"
            ))
            .bind(summary)
            .bind(description)
            .fetch_one(&pool)
            .await
            .expect("hash in SQL");
            assert_eq!(
                sql,
                text_hash(summary, description),
                "{summary:?} / {description:?}"
            );
        }
    }

    /// Only stale live rows come back: a missing or old hash, or a missing
    /// or old model version. Fresh rows (awkward text included), cleared and
    /// ended ones don't. The result is what the old fetch-everything query
    /// plus `incidents_needing_extraction` selected.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p enricher sweep -- --ignored --test-threads=1`"]
    #[expect(
        clippy::too_many_lines,
        reason = "test code: one scenario, seed to assertions"
    )]
    async fn the_sweep_query_returns_only_stale_live_incidents() {
        let pool = test_pool().await;
        let cleanup = "DELETE FROM incidents WHERE incident_id LIKE 'TEST-SWEEPQ-%'";
        sqlx::query(cleanup).execute(&pool).await.expect("cleanup");
        let model = "m@v2";
        let insert = |id: String,
                      summary: &'static str,
                      description: &'static str,
                      hash: Option<String>,
                      version: Option<&'static str>,
                      cleared: bool,
                      ended: bool| {
            let pool = pool.clone();
            async move {
                sqlx::query(
                    "INSERT INTO incidents \
                        (incident_id, summary, description, operators, affected_stations, \
                         priority, is_planned, is_cleared, source_removed_at, \
                         source_text_hash, extraction_model_version) \
                     VALUES ($1, $2, $3, '{}', '{}', 1, false, $4, \
                             CASE WHEN $5 THEN now() END, $6, $7)",
                )
                .bind(id)
                .bind(summary)
                .bind(description)
                .bind(cleared)
                .bind(ended)
                .bind(hash)
                .bind(version)
                .execute(&pool)
                .await
                .expect("seed");
            }
        };
        let mut want = Vec::new();
        for (i, (summary, description)) in AWKWARD_TEXTS.into_iter().enumerate() {
            let fresh = Some(text_hash(summary, description));
            insert(
                format!("TEST-SWEEPQ-FRESH-{i}"),
                summary,
                description,
                fresh.clone(),
                Some(model),
                false,
                false,
            )
            .await;
            for (kind, hash, version) in [
                ("NO-HASH", None, Some(model)),
                ("OLD-TEXT", Some(text_hash("old", description)), Some(model)),
                ("NO-MODEL", fresh.clone(), None),
                ("OLD-MODEL", fresh.clone(), Some("m@v1")),
            ] {
                let id = format!("TEST-SWEEPQ-{kind}-{i}");
                insert(
                    id.clone(),
                    summary,
                    description,
                    hash,
                    version,
                    false,
                    false,
                )
                .await;
                want.push(id);
            }
            insert(
                format!("TEST-SWEEPQ-CLEARED-{i}"),
                summary,
                description,
                None,
                None,
                true,
                false,
            )
            .await;
            insert(
                format!("TEST-SWEEPQ-ENDED-{i}"),
                summary,
                description,
                None,
                None,
                false,
                true,
            )
            .await;
        }

        let rows = fetch_sweep_rows(&pool, model).await.expect("sweep rows");
        sqlx::query(cleanup).execute(&pool).await.expect("cleanup");

        let rows: Vec<SweepRow> = rows
            .into_iter()
            .filter(|r| r.incident_id.starts_with("TEST-SWEEPQ-"))
            .collect();
        let mut got: Vec<String> = rows.iter().map(|r| r.incident_id.clone()).collect();
        got.sort();
        want.sort();
        assert_eq!(got, want);
        let mut needing = incidents_needing_extraction(&rows, model);
        needing.sort();
        assert_eq!(
            needing, want,
            "the Rust check agrees with every row SQL returned"
        );
        let awkward = rows
            .iter()
            .find(|r| r.incident_id == "TEST-SWEEPQ-NO-HASH-4")
            .expect("row");
        assert_eq!(
            (awkward.summary.as_str(), awkward.description.as_str()),
            AWKWARD_TEXTS[4],
            "the text comes back intact"
        );
    }

    #[test]
    fn model_version_bump_forces_re_extraction_even_with_matching_hash() {
        let hash = text_hash("text", "desc");
        let rows = vec![row("A", "text", Some(&hash), Some("gpt-oss-20b@prompt-v1"))];
        assert_eq!(
            incidents_needing_extraction(&rows, "gpt-oss-20b@prompt-v2"),
            vec!["A"]
        );
    }
}
