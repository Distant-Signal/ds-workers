-- =============================================================================
-- RESEARCH PROTOTYPE: export incident_history as JSONL for the enricher's
-- offline replay harness (crates/enricher/src/replay_eval.rs).
-- =============================================================================
--
-- READ-ONLY (a READ ONLY transaction, rolled back). One JSON object per line.
-- Run with unaligned, tuples-only output so each row is exactly one raw JSON
-- line (do NOT use COPY ... TO STDOUT: its text format escapes backslashes,
-- which corrupts the JSON):
--
--     psql "$DATABASE_URL" -qAt -f scripts/export-incident-history-for-replay.sql \
--         > incident-history.jsonl
--
-- or, against the cluster:
--
--     kubectl -n distant-signal exec -i distant-signal-postgres-0 -- \
--         psql -U distant_signal -d distant_signal -qAt \
--         < scripts/export-incident-history-for-replay.sql > incident-history.jsonl
--
-- `first_seen_at` comes from `incidents` (NULL if the incident was purged);
-- the harness falls back to the first history row's `recorded_at`, matching
-- the reference date production passes to the primary pass.
-- =============================================================================

BEGIN READ ONLY;

SELECT row_to_json(t)
FROM (
    SELECT h.incident_id, h.recorded_at, h.summary, h.description, h.is_planned,
           i.first_seen_at
    FROM incident_history h
    LEFT JOIN incidents i USING (incident_id)
    ORDER BY h.incident_id, h.recorded_at, h.id
) t;

ROLLBACK;
