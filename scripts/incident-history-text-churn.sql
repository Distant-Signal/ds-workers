-- =============================================================================
-- Offline analysis: are incident text updates small edits or full rewrites?
-- =============================================================================
--
-- Companion to the enricher's churn metrics
-- (`distant_signal_enricher_extraction_rerun_total` /
-- `distant_signal_enricher_extraction_churn_total`, see
-- `crates/enricher/src/churn.rs`). Those say how much the LLM's extraction
-- moves when an incident's text changes; this says how much the TEXT itself
-- moved. Together they answer whether a diff-aware prompt is worth building:
-- churn is only a problem worth solving if updates are mostly small edits
-- whose untouched parts nonetheless get re-extracted differently.
--
-- READ-ONLY. Wrapped in a READ ONLY transaction that is rolled back; it
-- creates nothing, needs no extensions (no pg_trgm), and can be run against
-- production:
--
--     psql "$DATABASE_URL" -f scripts/incident-history-text-churn.sql
--
-- HOW VERSIONS ARE PAIRED
-- -----------------------
-- `incident_history` gets a full snapshot row every time
-- `upsert_incidents` sees an incident change (including its first insert),
-- so consecutive rows per `incident_id`, ordered by (recorded_at, id), are
-- consecutive versions. A snapshot can be triggered by a non-text change
-- (operators, priority, validity periods, clearance), so pairs whose
-- summary+description are byte-identical are counted separately as
-- `metadata_only` -- those never trigger an enricher re-extraction.
--
-- "Text" is `summary || E'\n' || description`, the same two fields
-- `common::text_hash::text_hash` hashes to decide whether to re-extract.
--
-- HOW A TEXT CHANGE IS CLASSIFIED (first matching rule wins)
-- ---------------------------------------------------------
--   append           new text starts with the old text (pure addition at the end)
--   truncation       old text starts with the new text (pure removal at the end)
--   small_edit       word-set Jaccard similarity >= 0.8
--   partial_rewrite  word-set Jaccard similarity >= 0.5
--   rewrite          word-set Jaccard similarity <  0.5
--
-- Word-set Jaccard = |distinct words in both| / |distinct words in either|,
-- words being lowercase runs of [a-z0-9]. It ignores order and repetition,
-- so it is a cheap "how much vocabulary survived" measure, not an edit
-- distance. Description HTML tags contribute tokens like `p`/`br`, which
-- nudges similarity up slightly on both sides equally.
--
-- To restrict to a time window, add e.g. `WHERE recorded_at >= NOW() - INTERVAL '30 days'`
-- to the `versions` CTE in both queries (it then only pairs versions
-- recorded inside the window).
-- =============================================================================

BEGIN READ ONLY;

-- -----------------------------------------------------------------------------
-- 1. Summary: one row per change class.
-- -----------------------------------------------------------------------------
WITH versions AS (
    SELECT
        id,
        incident_id,
        recorded_at,
        summary || E'\n' || description AS new_text,
        LAG(summary || E'\n' || description)
            OVER (PARTITION BY incident_id ORDER BY recorded_at, id) AS old_text
    FROM incident_history
),
pairs AS (
    SELECT * FROM versions WHERE old_text IS NOT NULL
),
scored AS (
    SELECT
        p.incident_id,
        length(p.new_text) - length(p.old_text) AS len_delta,
        length(p.old_text) AS old_len,
        CASE
            WHEN p.old_text = p.new_text THEN 'metadata_only'
            WHEN starts_with(p.new_text, p.old_text) THEN 'append'
            WHEN starts_with(p.old_text, p.new_text) THEN 'truncation'
            WHEN j.jaccard >= 0.8 THEN 'small_edit'
            WHEN j.jaccard >= 0.5 THEN 'partial_rewrite'
            ELSE 'rewrite'
        END AS change_class,
        j.jaccard
    FROM pairs p
    CROSS JOIN LATERAL (
        SELECT CASE WHEN count(*) = 0 THEN 1.0
                    ELSE (count(*) FILTER (WHERE in_old AND in_new))::numeric / count(*)
               END AS jaccard
        FROM (
            SELECT word, bool_or(side = 'old') AS in_old, bool_or(side = 'new') AS in_new
            FROM (
                SELECT 'old' AS side, w AS word
                FROM regexp_split_to_table(lower(p.old_text), '[^a-z0-9]+') AS w
                UNION ALL
                SELECT 'new', w
                FROM regexp_split_to_table(lower(p.new_text), '[^a-z0-9]+') AS w
            ) tokens
            WHERE word <> ''
            GROUP BY word
        ) words
    ) j
)
SELECT
    change_class,
    count(*) AS version_pairs,
    count(DISTINCT incident_id) AS incidents,
    CASE WHEN change_class <> 'metadata_only' THEN
        round(100.0 * count(*)
              / NULLIF(sum(count(*) FILTER (WHERE change_class <> 'metadata_only')) OVER (), 0), 1)
    END AS pct_of_text_changes,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY abs(len_delta)) AS median_abs_len_delta_chars,
    percentile_disc(0.9) WITHIN GROUP (ORDER BY abs(len_delta)) AS p90_abs_len_delta_chars,
    round(percentile_cont(0.5) WITHIN GROUP (
        ORDER BY abs(len_delta)::numeric / NULLIF(old_len, 0)) :: numeric, 3)
        AS median_abs_len_delta_ratio,
    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY jaccard)::numeric, 3) AS median_word_jaccard,
    round(percentile_cont(0.1) WITHIN GROUP (ORDER BY jaccard)::numeric, 3) AS p10_word_jaccard
FROM scored
GROUP BY change_class
ORDER BY array_position(
    ARRAY['metadata_only', 'append', 'truncation', 'small_edit', 'partial_rewrite', 'rewrite'],
    change_class
);

-- -----------------------------------------------------------------------------
-- 2. How many TEXT versions each incident goes through (i.e. how many
--    text-change re-extractions the enricher would run per incident).
-- -----------------------------------------------------------------------------
WITH versions AS (
    SELECT
        incident_id,
        summary || E'\n' || description AS new_text,
        LAG(summary || E'\n' || description)
            OVER (PARTITION BY incident_id ORDER BY recorded_at, id) AS old_text
    FROM incident_history
),
per_incident AS (
    SELECT incident_id,
           count(*) FILTER (WHERE old_text IS NOT NULL AND old_text <> new_text) AS text_changes
    FROM versions
    GROUP BY incident_id
)
SELECT
    CASE WHEN text_changes >= 10 THEN '10+' ELSE text_changes::text END AS text_changes_per_incident,
    count(*) AS incidents
FROM per_incident
GROUP BY 1
ORDER BY min(text_changes);

ROLLBACK;
