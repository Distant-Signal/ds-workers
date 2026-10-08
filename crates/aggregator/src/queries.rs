//! Read/write query functions the aggregator's own poll loop uses. Reads
//! `incidents`/`station_samples` (written by the four existing pollers);
//! writes `line_status`/`line_status_history` (read by the api crate's
//! new endpoints, Task 5).

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use common::{IncidentMessage, LineStatusReport, StationSample};
use sqlx::{PgConnection, PgExecutor, PgPool, Row};

/// One incident loaded from the `incidents` table for this aggregation
/// cycle, paired with our own `active_since` clock. Deliberately not part
/// of `common::IncidentMessage` -- the wire type pollers/the API share --
/// since `active_since` is a fact only this crate's staleness check cares
/// about. See docs/superpowers/specs/2026-07-16-stale-incident-handling-design.md.
pub(crate) struct LoadedIncident {
    pub message: IncidentMessage,
    /// When the incident's current episode began: `incidents.active_since`
    /// (stamped on insert, on a reopen and on a text change while uncleared;
    /// 2026-10-06), or `first_seen_at` for a row written before that column
    /// existed. The rail-day cutoff runs from here.
    pub active_since: DateTime<Utc>,
    /// `Vec<ExtractionPeriod>` JSON (see
    /// docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md
    /// §1/§3), or `None` if no extraction has succeeded yet. Deserialized
    /// into `aggregation`'s private `ExtractionPeriod` mirror lazily, in
    /// `aggregation::apply_extraction`/`has_recurring_schedule` -- not here,
    /// so this crate's DB layer stays agnostic to the JSON shape those
    /// functions consume.
    ///
    /// Only ever `Some` when the extraction describes the incident's
    /// CURRENT text -- see [`LoadedIncident::new`]. A stored extraction
    /// whose `source_text_hash` no longer matches `message`'s
    /// summary/description is dropped to `None` at load time.
    pub extracted_periods: Option<serde_json::Value>,
}

impl LoadedIncident {
    /// Builds a `LoadedIncident`, keeping `extracted_periods` only if the
    /// extraction was computed from the incident's current text.
    ///
    /// # Why stale extraction is dropped rather than applied
    ///
    /// `poller-incidents` overwrites `summary`/`description` as soon as the
    /// feed's text changes, but the enricher only rewrites
    /// `extracted_periods` (and `source_text_hash`) once its LLM calls over
    /// the NEW text finish -- minutes, with a slow local model. Until then
    /// the stored periods describe prose that no longer exists: an old
    /// "resolved"/"residual delays only" reading would demote a disruption
    /// the new text says is live, an old "severe" escalation would outlast
    /// an update that downgraded it, and an old `impact_type`/annotations
    /// would keep being shown. Comparing `source_text_hash` against
    /// `common::text_hash::text_hash` of the current text (the exact digest
    /// the enricher stamps) and treating a mismatch -- or a missing hash,
    /// which proves nothing -- as `None` makes such an incident behave
    /// exactly like one that has never been enriched, and makes the new
    /// extraction apply the instant the enricher writes it.
    ///
    /// `extraction_model_version` is deliberately NOT part of this check:
    /// extraction from an older model over the *current* text is still a
    /// valid reading of that text, and keeps being used until the enricher's
    /// sweep replaces it -- same as before a model bump.
    ///
    /// Gated here, at the one place a DB row becomes a `LoadedIncident`,
    /// rather than inside each consumer, so every reader of
    /// `extracted_periods` (`apply_extraction`, `governing_impact_type`,
    /// `has_recurring_schedule`) is covered by construction.
    pub(crate) fn new(
        message: IncidentMessage,
        active_since: DateTime<Utc>,
        source_text_hash: Option<&str>,
        extracted_periods: Option<serde_json::Value>,
    ) -> Self {
        let extracted_periods = extracted_periods.filter(|_| {
            source_text_hash.is_some_and(|stored| {
                stored == common::text_hash::text_hash(&message.summary, &message.description)
            })
        });
        LoadedIncident {
            message,
            active_since,
            extracted_periods,
        }
    }
}

/// Per-row resilience for every loader below.
///
/// # Why a bad row is skipped, not propagated
///
/// These loaders used to build their result with `.map(...).collect()` over
/// a closure returning `Result`, so ONE malformed JSONB row -- a
/// `validity_periods` or `departures` value some other service wrote in a
/// shape `serde_json::from_value` can't accept -- failed the deserialization
/// of the WHOLE batch. `run_cycle` (main.rs) is a straight `?`-chain, so
/// that single row took down every line's status write for the cycle AND
/// (before this change) every retention prune queued behind it, including
/// `trust_event_backlog`'s RDM-licensing-mandated 1-day window. Skipping the
/// one row loses exactly one incident/station/custom line -- visible, loud,
/// and per-row in the logs -- instead of the entire cycle.
///
/// Returns `None` after logging, so callers can `filter_map` over it.
#[expect(
    clippy::needless_pass_by_value,
    reason = "callers hand over values they no longer need"
)]
fn skip_bad_row<T>(table: &'static str, key: Option<String>, result: Result<T>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::warn!(
                table,
                key = key.as_deref().unwrap_or("<unreadable>"),
                error = ?err,
                "skipping a malformed row rather than failing the whole aggregation cycle"
            );
            metrics::counter!(common::metrics::metric_name(
                "aggregator_malformed_rows_skipped_total"
            ))
            .increment(1);
            None
        }
    }
}

/// Deserializes one `incidents` row. Split out of `load_incidents` so the
/// `?` shorthand stays usable per row while the caller decides, per row,
/// whether a failure is fatal -- see `skip_bad_row`.
fn incident_from_row(row: &sqlx::postgres::PgRow) -> Result<LoadedIncident> {
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
    let source_text_hash: Option<String> = row.try_get("source_text_hash")?;
    Ok(LoadedIncident::new(
        message,
        row.try_get("active_since")?,
        source_text_hash.as_deref(),
        row.try_get("extracted_periods")?,
    ))
}

/// Live incidents only: not cleared by RDM, and still listed by the feed
/// (`source_removed_at IS NULL`). An incident the feed stopped listing
/// without clearing it is "ended", not live (2026-10-06,
/// docs/superpowers/specs/2026-10-06-incident-source-removal-design.md).
pub(crate) async fn load_incidents(pool: &PgPool) -> Result<Vec<LoadedIncident>> {
    let rows = sqlx::query(
        "SELECT incident_id, summary, description, operators, affected_stations, \
                priority, validity_periods, is_planned, is_cleared, \
                COALESCE(active_since, first_seen_at) AS active_since, \
                source_text_hash, extracted_periods \
         FROM incidents \
         WHERE NOT is_cleared AND source_removed_at IS NULL",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            skip_bad_row(
                "incidents",
                row.try_get("incident_id").ok(),
                incident_from_row(&row),
            )
        })
        .collect())
}

/// Deserializes one `station_samples` row -- see `incident_from_row`.
fn station_sample_from_row(row: &sqlx::postgres::PgRow) -> Result<(String, StationSample)> {
    let crs: String = row.try_get("crs")?;
    let departures_json: serde_json::Value = row.try_get("departures")?;
    let sample = StationSample {
        crs: crs.clone(),
        polled_at: row.try_get("polled_at")?,
        departures: serde_json::from_value(departures_json)?,
    };
    Ok((crs, sample))
}

pub(crate) async fn load_station_samples(pool: &PgPool) -> Result<HashMap<String, StationSample>> {
    let rows = sqlx::query("SELECT crs, polled_at, departures FROM station_samples")
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            skip_bad_row(
                "station_samples",
                row.try_get("crs").ok(),
                station_sample_from_row(&row),
            )
        })
        .collect())
}

/// Deletes `station_samples` rows for stations no line samples any more
/// (`sampled` is every line's `sample_stations`, static and custom),
/// returning their CRS codes, sorted.
///
/// `station_samples` is keyed by CRS and only ever upserted, so when a
/// station leaves the catalogue its last row stays forever. In production
/// DDG and WNE, dropped from `lines/*.toml` on 2026-09-21 (DDG replaced by
/// LMS on the WMR Snow Hill line; WNE a mistagged Windermere, now WDM),
/// sat there from 2026-09-22 on, and every aggregator cycle logged them as
/// stale samples. Nothing reads them for inference (lines only look up
/// their own `sample_stations`), but `api`'s departure board and the
/// notifier's skip check read rows by CRS and would serve the frozen board.
///
/// Only rows older than `min_age_minutes` go, so a station a newer `api`
/// has just started sampling (a catalogue change rolling out, or a custom
/// line created since this cycle loaded the lines) is not deleted while
/// still live. An empty `sampled` deletes nothing rather than everything.
pub(crate) async fn prune_orphaned_station_samples(
    pool: &PgPool,
    sampled: &[String],
    min_age_minutes: i64,
) -> Result<Vec<String>> {
    if sampled.is_empty() {
        return Ok(Vec::new());
    }
    let mut pruned: Vec<String> = sqlx::query_scalar(
        "DELETE FROM station_samples \
         WHERE crs::text <> ALL($1::text[]) \
           AND polled_at < NOW() - make_interval(mins => $2::int) \
         RETURNING crs::text",
    )
    .bind(sampled)
    .bind(i32::try_from(min_age_minutes)?)
    .fetch_all(pool)
    .await?;
    pruned.sort();
    Ok(pruned)
}

/// Deserializes one `custom_lines` row -- see `incident_from_row`.
fn custom_line_from_row(row: &sqlx::postgres::PgRow) -> Result<common::CustomLine> {
    Ok(common::CustomLine {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        operators: row.try_get("operators")?,
        stations: row.try_get("stations")?,
        headcode_prefixes: row.try_get("headcode_prefixes")?,
        destination_crs_filter: row.try_get("destination_crs_filter")?,
    })
}

pub(crate) async fn load_custom_lines(pool: &PgPool) -> Result<Vec<common::CustomLine>> {
    let rows = sqlx::query(
        "SELECT id, name, operators, stations, headcode_prefixes, destination_crs_filter \
         FROM custom_lines",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            skip_bad_row(
                "custom_lines",
                row.try_get("id").ok(),
                custom_line_from_row(&row),
            )
        })
        .collect())
}

/// Every station's name (`stations`, the station reference feed), for
/// resolving the places an incident names (`common::station_resolver`, the
/// matcher's station evidence since 2026-10-06) and "No trains between X
/// and Y" against a line (`no_trains::closed_section`). About 2,600 short
/// rows. The api's `load_station_gazetteer` runs the same query at ingest,
/// so `incidents.affected_lines` and the live statuses agree.
pub(crate) async fn load_station_names(
    pool: &PgPool,
) -> Result<crate::no_trains::StationGazetteer> {
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT crs::text, name FROM stations")
        .fetch_all(pool)
        .await?;
    Ok(crate::no_trains::StationGazetteer::new(rows))
}

/// Every `full_coverage_line_stats` row with `availability = 'available'`
/// AND `service_date = today`. A stale (yesterday's) or still-`pending`
/// row is simply absent from the returned map --
/// `aggregation::merge_full_coverage` already treats a missing key
/// identically to "no signal yet" (Pending), so no new branch is needed
/// in `aggregation.rs` for this. See
/// docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md
/// Decision 3 and
/// docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md's
/// Correction 1 (a direct SQL query, not the design doc's own HTTP
/// sketch, since this crate already holds its own `PgPool`).
///
/// `today` here is deliberately the plain UTC calendar date
/// (`chrono::Utc::now().date_naive()`), NOT this file's own
/// `london_calendar_day` -- a real, considered choice, not an oversight
/// of that existing convention: `full_coverage_line_stats.service_date`
/// is written by `full-coverage-consumer` (Task 13) and
/// `schedule-reference` (Task 7) against plain UTC dates (neither has a
/// rail-day or Europe/London-calendar-day concept of its own), so this
/// read must match that same key convention or it would silently miss
/// every row during the roughly one BST hour a day (23:00-23:59 UTC, when
/// London's calendar day has already rolled over but UTC's hasn't) where
/// the two conventions disagree. `london_calendar_day` stays the right
/// choice for this file's OTHER queries (daily-stats bucketing, an
/// aggregator-internal concern with no cross-service writer to match).
pub(crate) async fn load_full_coverage_line_stats(
    pool: &PgPool,
    today: NaiveDate,
) -> Result<HashMap<String, common::SampleStats>> {
    let rows = sqlx::query(
        "SELECT line_id, total, delayed, cancelled, skipped, avg_delay_minutes \
         FROM full_coverage_line_stats WHERE availability = 'available' AND service_date = $1",
    )
    .bind(today)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            skip_bad_row(
                "full_coverage_line_stats",
                row.try_get("line_id").ok(),
                full_coverage_stats_from_row(&row),
            )
        })
        .collect())
}

/// Deserializes one `full_coverage_line_stats` row -- see
/// `incident_from_row`. Per-row skipping matters here for the same reason
/// even though this loader's caller already fails open on an `Err`: failing
/// open discards EVERY line's full-coverage signal for the cycle, where a
/// single unreadable row (a NULL `avg_delay_minutes` a future writer
/// permits, say) should only cost that one line's.
#[expect(
    clippy::cast_sign_loss,
    reason = "database counts are non-negative and far below u32::MAX"
)]
fn full_coverage_stats_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<(String, common::SampleStats)> {
    let line_id: String = row.try_get("line_id")?;
    let stats = common::SampleStats {
        total: row.try_get::<i32, _>("total")? as usize,
        delayed: row.try_get::<i32, _>("delayed")? as usize,
        cancelled: row.try_get::<i32, _>("cancelled")? as usize,
        skipped: row.try_get::<i32, _>("skipped")? as usize,
        avg_delay_minutes: row.try_get("avg_delay_minutes")?,
    };
    Ok((line_id, stats))
}

/// Deletes `line_status` rows for any `line_id` not in `current_line_ids`.
/// Called every cycle with the freshly-merged static+custom line set, so a
/// deleted custom line's last-known status is removed on the next cycle
/// rather than lingering forever (custom lines are the only way a line can
/// disappear between cycles — the static catalogue is fixed for the
/// process's lifetime).
///
/// Scoped to `source = 'aggregator'`: this crate is no longer the only
/// writer of `line_status`. `TfL` lines are written by the api crate's
/// `/private/tfl-line-status` ingest and are pruned by that endpoint
/// against its own batch — they are invisible to this crate's line set, so
/// an unscoped DELETE here would wipe them on the very next cycle.
///
/// **Guards against an EMPTY `current_line_ids`.** `NOT (line_id =
/// ANY($1))` is true for every row when `$1` is an empty array — Postgres's
/// `ANY` over an empty array never matches, so the `NOT` flips that to
/// "match everything." An empty list here almost certainly means the
/// static+custom line catalogue failed to load or came back empty (a typo'd
/// `--lines-dir`, a custom-lines fetch that errored and was swallowed
/// upstream, etc.) rather than "every line was deliberately removed" — the
/// static catalogue alone is never legitimately empty in a real deployment.
/// Without this guard, that misconfiguration would silently DELETE every
/// aggregator-sourced `line_status` row on the very next cycle, since
/// nothing here previously distinguished "the catalogue is genuinely down
/// to zero lines" from "the catalogue failed to load." Rather than proceed
/// into a delete that can wipe the whole table, this no-ops and logs a
/// warning: the next cycle retries with (hopefully) a populated catalogue,
/// and existing rows survive the gap either way.
pub(crate) async fn prune_removed_lines(pool: &PgPool, current_line_ids: &[String]) -> Result<u64> {
    if current_line_ids.is_empty() {
        tracing::warn!(
            "prune_removed_lines called with an empty line-id list; skipping the prune rather \
             than deleting every aggregator-sourced line_status row (this usually means the \
             line catalogue failed to load or came back empty)"
        );
        return Ok(0);
    }

    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM line_status WHERE source = 'aggregator' AND NOT (line_id = ANY($1))",
        )
        .bind(current_line_ids),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Fetches the currently-stored `statuses` JSON for one line, if any row
/// exists yet.
async fn existing_statuses(
    conn: &mut PgConnection,
    line_id: &str,
) -> Result<Option<serde_json::Value>> {
    let row = sqlx::query("SELECT statuses FROM line_status WHERE line_id = $1")
        .bind(line_id)
        .fetch_optional(&mut *conn)
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
/// - `sample_stats`/`sample_availability`: recomputed from live LDBWS
///   samples every poll cycle, so their counts (and `sample_availability`'s
///   `BelowThreshold.observed`) roll over every cycle even when the line's
///   actual status is unchanged.
/// - the `(live samples show: ...)` suffix `escalate_from_sample_stats`
///   (aggregation.rs) appends to `reason` on escalation: it carries the
///   same live counts as `sample_stats`, just formatted into text instead
///   of left structured, so it churns every cycle for the same reason and
///   needs the same stripping.
/// - the live counts `classify()` (aggregation.rs) bakes directly into a
///   sample-inferred `reason`, e.g. `"5 of 9 sampled services delayed."` --
///   these fluctuate on essentially every poll cycle just like
///   `sample_stats` does, for the same underlying reason (they're computed
///   from the same live departure counts), so they need the same
///   normalization. See `normalize_sample_counts`'s own doc comment for why
///   only the counts -- not the `"(most cited: ...)"` suffix
///   `infer_from_samples` also appends -- are stripped here.
///
/// Without stripping all of these, a "change" would be seen on every single
/// poll cycle for most lines, defeating the point of only recording history
/// on real changes.
fn normalize_for_diff(statuses: &serde_json::Value) -> serde_json::Value {
    match statuses.as_array() {
        Some(entries) => {
            serde_json::Value::Array(entries.iter().map(normalize_entry_for_diff).collect())
        }
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
        obj.remove("sample_availability");
        // Same reasoning as the pair above, extended to the Decision-1
        // full-coverage fields: nothing produces these yet, but once
        // something does they will fluctuate every cycle independent of
        // real disruption state, exactly like sample_stats/sample_availability
        // already do -- see
        // docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md
        // Decision 1.
        obj.remove("full_coverage_stats");
        obj.remove("full_coverage_availability");
    }
    if let Some(reason) = entry.get_mut("reason")
        && let Some(text) = reason.as_str()
    {
        let normalized = normalize_sample_counts(strip_live_sample_annotation(text));
        *reason = serde_json::Value::String(normalized);
    }
    entry
}

/// The `DataQuality` values affected by the `Utc::now()`-every-cycle bug
/// this exists to work around: an inferred status has no incident of its own
/// to take a stable `validity.from_date` from, so `aggregation.rs` stamps it
/// with a fresh `Utc::now()` on every single cycle. See
/// docs/superpowers/specs/2026-08-30-inferred-time-ranges-design.md.
///
/// `"trust-inferred"` belongs here alongside `"ldbws-inferred"` for exactly
/// the same reason, and is not speculative: `aggregation::merge_full_coverage_stats`
/// really does stamp `DataQuality::TrustInferred` on a live line's status
/// (`lines/tfw-conwy-valley.toml` has `full_coverage_enabled = true`), whose
/// `validity.from_date` came from the same `Utc::now()`-per-cycle
/// `good_service()`/`infer_from_samples` construction. Recognizing only
/// `"ldbws-inferred"` meant such a status re-stamped its "since" timestamp
/// every cycle -- the passenger-facing "disrupted since" clock permanently
/// reading "just now" -- which is the precise bug this function exists to
/// prevent.
const INFERRED_DATA_QUALITIES: [&str; 2] = ["ldbws-inferred", "trust-inferred"];

/// Whether a stored/fresh status entry's `data_quality` is one of the
/// inferred kinds whose `from_date` needs carrying forward.
fn inferred_data_quality(entry: &serde_json::Value) -> Option<&str> {
    let quality = entry.get("data_quality").and_then(|v| v.as_str())?;
    INFERRED_DATA_QUALITIES
        .contains(&quality)
        .then_some(quality)
}

/// Given the previous cycle's stored `statuses` JSON array (`existing`) and
/// this cycle's freshly-computed one (`fresh`), returns a copy of `fresh`
/// with each inferred entry's (`INFERRED_DATA_QUALITIES`)
/// `validity.from_date` overwritten by the positionally-corresponding entry
/// in `existing`, provided that entry carries the SAME `data_quality` and
/// the two entries are equal once run through `normalize_entry_for_diff`
/// (i.e. "same underlying disruption, just a fresh poll of it" --
/// `sample_stats`, the live-sample-count reason suffix, and the live counts
/// baked directly into a sample-inferred `reason` are all allowed to churn
/// without defeating the carry-forward, since `normalize_entry_for_diff`
/// already strips all three).
///
/// The two `data_quality` values must MATCH, not merely both be inferred: a
/// line crossing from `ldbws-inferred` to `trust-inferred` (or back) has had
/// the provenance of its published severity change, which is a genuine
/// status change deserving its own fresh timestamp and history row.
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
fn carry_forward_ldbws_from_date(
    existing: &serde_json::Value,
    fresh: &serde_json::Value,
) -> serde_json::Value {
    let mut fresh = fresh.clone();
    let existing_entries = existing.as_array();
    let Some(fresh_entries) = fresh.as_array_mut() else {
        return fresh;
    };

    for (i, entry) in fresh_entries.iter_mut().enumerate() {
        let Some(quality) = inferred_data_quality(entry) else {
            continue;
        };
        let Some(existing_entry) = existing_entries.and_then(|arr| arr.get(i)) else {
            continue;
        };
        if inferred_data_quality(existing_entry) != Some(quality) {
            continue;
        }
        if normalize_entry_for_diff(entry) != normalize_entry_for_diff(existing_entry) {
            continue;
        }
        let Some(old_from_date) = existing_entry
            .get("validity")
            .and_then(|v| v.get("from_date"))
            .cloned()
        else {
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

/// Replaces the fluctuating live counts `classify()` (aggregation.rs) bakes
/// directly into a sample-inferred `reason` -- e.g. `"5 of 9 sampled
/// services delayed."` -- with a stable placeholder (`"N of M sampled
/// services delayed."`), so two cycles' worth of pure count wobble (5-of-9
/// vs. 7-of-14, same underlying situation) normalize to the same identity.
/// Every `classify()` template has this exact `"<count> of <count> sampled
/// services <cause>"` shape (`aggregation.rs`'s `cancelled`/`delayed`/
/// `skipping a scheduled stop` clauses, singly or joined with `", "` in the
/// delay+skip-tie case), so a marker-based scan for `" of "` immediately
/// followed by a digit run and `" sampled services"` catches all of them
/// without a regex dependency or a full parse -- mirrors
/// `strip_live_sample_annotation`'s marker-based approach.
///
/// Deliberately does NOT touch the `" (most cited: ...)"` suffix
/// `infer_from_samples` separately appends after `classify()` runs: unlike
/// the raw counts, `most_common`'s pick of the most-cited free-text delay/
/// cancel reason is real information about *why* services are disrupted,
/// not per-cycle sampling noise. If it genuinely changes (e.g. "Signal
/// failure" to "Engineering works"), that's a real change in the reported
/// cause worth its own history entry -- collapsing it away would hide a
/// real change behind this fix meant only to suppress noise. See
/// `normalize_entry_for_diff`'s regression tests for both directions of
/// this.
///
/// # UTF-8 safety
///
/// Every index this function computes must land on a `char` boundary, and
/// `reason` is raw free-text: the Knowledgebase incident summary, or
/// LDBWS-inferred Darwin text, both of which routinely carry en dashes,
/// `£`, curly quotes and accented place names. An earlier version derived
/// the start of the leading digit run with
/// `before.rfind(|c| !c.is_ascii_digit()).map_or(0, |p| p + 1)`, which
/// assumes the character before the digits is exactly one byte wide --
/// `rfind` returns the *start* byte of that character, so `p + 1` lands
/// mid-character for any multi-byte one and the following slice panicked
/// the whole process ("byte index is not a char boundary"). Real National
/// Rail prose hits that on strings as ordinary as `"Platforms 1–3 of 5
/// closed"`. Since `normalize_sample_counts` runs inside
/// `normalize_entry_for_diff` -> `write_line_status`, awaited straight from
/// `main`, the panic unwound `main` itself and the pod crash-looped on the
/// same live incident every cycle, freezing line-status updates *and* all
/// retention pruning for as long as the incident stayed uncleared. The
/// backwards scan below therefore walks `char_indices()` and advances by
/// the found character's own `len_utf8()`, so a multi-byte character can
/// never produce a non-boundary index. See
/// `normalize_sample_counts_is_utf8_safe_for_real_national_rail_prose`.
fn normalize_sample_counts(reason: &str) -> String {
    const OF_MARKER: &str = " of ";
    const SERVICES_MARKER: &str = " sampled services";

    let mut result = String::with_capacity(reason.len());
    let mut copied_to = 0usize;
    let mut search_from = 0usize;

    while let Some(of_rel) = reason[search_from..].find(OF_MARKER) {
        let of_idx = search_from + of_rel;

        // Digit run immediately preceding " of ", scanning back only as far
        // as `copied_to` (text already emitted for an earlier match).
        // Walked with `char_indices().rev()` -- NOT `rfind` plus a `+ 1`
        // byte bump -- so the boundary always lands after the *whole*
        // preceding character however many bytes it occupies; see this
        // function's "UTF-8 safety" doc section for the panic that shape
        // used to cause.
        let before = &reason[copied_to..of_idx];
        let first_digits_start = copied_to
            + before
                .char_indices()
                .rev()
                .find(|(_, c)| !c.is_ascii_digit())
                .map_or(0, |(p, c)| p + c.len_utf8());
        let first_digits = &reason[first_digits_start..of_idx];

        // Digit run immediately following " of ".
        let after_of_start = of_idx + OF_MARKER.len();
        let after_of = &reason[after_of_start..];
        let second_digits_len = after_of
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after_of.len());
        let second_digits_end = after_of_start + second_digits_len;
        let second_digits = &reason[after_of_start..second_digits_end];
        let after_digits = &reason[second_digits_end..];

        if !first_digits.is_empty()
            && !second_digits.is_empty()
            && after_digits.starts_with(SERVICES_MARKER)
        {
            result.push_str(&reason[copied_to..first_digits_start]);
            result.push('N');
            result.push_str(OF_MARKER);
            result.push('M');
            copied_to = second_digits_end;
        }

        // Always advance past this " of " occurrence, matched or not, so a
        // non-count " of " (e.g. "delayed because of engineering works")
        // can't stall the scan.
        search_from = of_idx + OF_MARKER.len();
    }

    result.push_str(&reason[copied_to..]);
    result
}

/// Upserts one line's computed report into `line_status` (always), and
/// inserts a `line_status_history` snapshot only if the statuses actually
/// changed since the last cycle.
///
/// Takes a `&mut PgConnection` rather than `&PgPool` (as it did before this
/// doc comment was added) so `run_cycle` (main.rs) can batch many lines'
/// worth of these writes into a handful of `sqlx::Transaction`s per cycle
/// instead of every call being its own autocommitted, independently
/// WAL-fsync-ing statement -- see
/// docs/superpowers/specs/2026-09-02-slow-query-warnings-research.md,
/// Recommendation #3, and the design decision recorded on
/// `crate::main::WRITE_CHUNK_SIZE`. Both `sqlx::Transaction<'_, Postgres>`
/// and a pool-acquired `PoolConnection<Postgres>` deref to `PgConnection`,
/// so this function still works standalone for callers/tests that don't
/// need batching -- just `pool.acquire().await?` first and pass
/// `&mut *conn`.
///
/// `upcoming` (2026-10-06) is written to `line_status.upcoming` every time.
/// It is a note beside the statuses, not part of them, so a change to it
/// alone never adds a `line_status_history` row.
pub(crate) async fn write_line_status(
    conn: &mut PgConnection,
    report: &LineStatusReport,
    upcoming: &[common::UpcomingDisruption],
) -> Result<()> {
    let fresh_statuses_json = serde_json::to_value(&report.statuses)?;
    let upcoming_json = serde_json::to_value(upcoming)?;
    let existing = existing_statuses(&mut *conn, &report.id).await?;

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
        r"
        INSERT INTO line_status (line_id, name, mode_name, operators, statuses, computed_at, source,
                                 upcoming)
        VALUES ($1, $2, $3, $4, $5, NOW(), 'aggregator', $6)
        ON CONFLICT (line_id) DO UPDATE SET
            name        = EXCLUDED.name,
            mode_name   = EXCLUDED.mode_name,
            operators   = EXCLUDED.operators,
            statuses    = EXCLUDED.statuses,
            computed_at = NOW(),
            source      = 'aggregator',
            upcoming    = EXCLUDED.upcoming
        ",
    )
    .bind(&report.id)
    .bind(&report.name)
    .bind(&report.mode_name)
    .bind(&report.operators)
    .bind(&statuses_json)
    .bind(&upcoming_json)
    .execute(&mut *conn)
    .await?;

    if changed {
        sqlx::query(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) VALUES ($1, $2, NOW())",
        )
        .bind(&report.id)
        .bind(&statuses_json)
        .execute(&mut *conn)
        .await?;
    }

    Ok(())
}

/// `statement_timeout` for each retention prune statement, raised with
/// `SET LOCAL` above the pool's 60s default (`common::pg`). The unbatched
/// `schedule_*` prunes delete a whole service date at a time
/// (`schedule_calling_points_full`: ~1M rows), which can legitimately take
/// minutes; 10 minutes still bounds a runaway well inside the cycle.
pub(crate) const RETENTION_STATEMENT_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(600);

/// Runs one retention `DELETE` in its own transaction under
/// [`RETENTION_STATEMENT_TIMEOUT`]. Same autocommit-per-statement semantics
/// the prunes always had, just with the longer budget.
pub(crate) async fn execute_retention_delete(
    pool: &PgPool,
    query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
) -> Result<sqlx::postgres::PgQueryResult> {
    let mut tx = pool.begin().await?;
    common::pg::set_local_statement_timeout(&mut tx, RETENTION_STATEMENT_TIMEOUT).await?;
    let result = query.execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(result)
}

/// Deletes `line_status_history` rows older than `retention_days`.
///
/// Probes `MIN(computed_at)` first (one descent of
/// `line_status_history_computed_at`) and skips the `DELETE` when nothing is
/// old enough, which is every cycle but the few after a row ages out.
pub(crate) async fn prune_history(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let due: Option<bool> = sqlx::query_scalar(
        "SELECT (SELECT MIN(computed_at) FROM line_status_history) \
                < NOW() - ($1 || ' days')::interval",
    )
    .bind(retention_days.to_string())
    .fetch_one(pool)
    .await?;
    if due != Some(true) {
        return Ok(0);
    }
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM line_status_history WHERE computed_at < NOW() - ($1 || ' days')::interval",
        )
        .bind(retention_days.to_string()),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Batch size for `prune_trust_event_backlog`'s delete loop. Same order as
/// `PRUNE_TRAINS_BATCH`; the backlog's steady state deletes about 1/24 of a
/// ~550k-row table per hour of backlog, so a cycle after downtime can have
/// tens of thousands of rows to remove.
const PRUNE_TRUST_EVENT_BACKLOG_BATCH: i64 = 5000;

/// Prunes `trust_event_backlog` rows older than `retention_days`. See
/// `Config::trust_event_backlog_retention_days`'s own doc comment for
/// the licensing safeguard this default (1) exists to enforce -- this
/// function itself has no opinion on the value passed in; it prunes
/// whatever it's told to.
///
/// Probes `MIN(received_at)` first (one descent of
/// `trust_event_backlog_received_at`) and returns without deleting when
/// nothing is past the cutoff. Otherwise deletes in
/// `PRUNE_TRUST_EVENT_BACKLOG_BATCH`-row statements, oldest first, like
/// `prune_trains`, and stops as soon as a batch comes back short. The
/// cutoff is fixed once per call so a long prune never chases rows that
/// only aged out while it ran.
pub(crate) async fn prune_trust_event_backlog(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let (oldest, cutoff): (Option<DateTime<Utc>>, DateTime<Utc>) = sqlx::query_as(
        "SELECT (SELECT MIN(received_at) FROM trust_event_backlog), \
                NOW() - ($1 || ' days')::interval",
    )
    .bind(retention_days.to_string())
    .fetch_one(pool)
    .await?;
    if oldest.is_none_or(|oldest| oldest >= cutoff) {
        return Ok(0);
    }
    let mut pruned = 0u64;
    loop {
        let result = execute_retention_delete(
            pool,
            sqlx::query(
                "DELETE FROM trust_event_backlog WHERE id IN ( \
                    SELECT id FROM trust_event_backlog \
                    WHERE received_at < $1 \
                    ORDER BY received_at \
                    LIMIT $2 \
                 )",
            )
            .bind(cutoff)
            .bind(PRUNE_TRUST_EVENT_BACKLOG_BATCH),
        )
        .await?;
        let rows_affected = result.rows_affected();
        pruned += rows_affected;
        if rows_affected < PRUNE_TRUST_EVENT_BACKLOG_BATCH as u64 {
            break;
        }
    }
    Ok(pruned)
}

/// Prunes `schedule_destination_departures` rows for service dates older
/// than `retention_days`.
///
/// **This table is the one CIF-derived published product that genuinely
/// needs pruning**, unlike its sibling `schedule_network_departures`, which
/// has no pruning job anywhere in this repo. The sibling's wholesale
/// replace is scoped per `(crs, service_date)` over ~2,500 CRS codes, so
/// its steady-state size is trivial. This one holds ONE ROW PER
/// DEPARTURE -- roughly 377,000 rows per service date -- and its wholesale
/// replace is scoped to a single day, so without this job every day the
/// service has ever seen accumulates forever.
///
/// `GET /public/trains/search` now reads past service dates too (up to 7
/// days back, `crates/api/src/routes/trains.rs::SEARCH_WINDOW_BACKWARD_DAYS`)
/// -- this window used to exist only to protect the PRODUCER's edges, and
/// now also has to keep a real consumer's supported range intact. See
/// `Config::schedule_destination_departures_retention_days` for why the
/// default is 8, one day more than that 7-day window strictly needs.
///
/// Modelled on `prune_history` and `prune_trust_event_backlog` directly
/// above, with the one difference that this table's age column is a `DATE`
/// (`service_date`, a rail day) rather than a `TIMESTAMPTZ`, so the
/// comparison is against `CURRENT_DATE`. The comparison is strictly `<`,
/// never `<=`: today's rows must survive any retention value, including 0.
///
/// A single unbatched `DELETE`, unlike `prune_trains` a little further down
/// -- which loops in `PRUNE_TRAINS_BATCH`-sized chunks. That is the right
/// call here and worth stating: this deletes at most one service date's
/// worth of rows per run once the window is steady, it runs against a table
/// nothing reads for past dates, and the aggregator's cycle is a forgiving
/// place to spend the time. If lock duration or WAL volume ever does bite,
/// `service_date` partitioning with a partition swap is the standard
/// mitigation -- reach for that rather than for a batching loop.
pub(crate) async fn prune_schedule_destination_departures(
    pool: &PgPool,
    retention_days: i64,
) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM schedule_destination_departures \
         WHERE service_date < CURRENT_DATE - ($1 || ' days')::interval",
        )
        .bind(retention_days.to_string()),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Prunes `schedule_calling_points_full` rows for service dates older than
/// `retention_days`.
///
/// **The biggest of the three products that had no pruning job at all until
/// 2026-09-25.** One row per calling point of every non-cancelled schedule,
/// for every date of the forward window per publish cycle (today to
/// today+28 by default) -- realistically 2-3x
/// `schedule_destination_departures`' ~377,000 rows per date, because unlike
/// that product this one keeps the passing points and junction TIPLOCs too.
/// Its wholesale replace is scoped to one `service_date`, and
/// `schedule-reference` publishes a new `service_date` every day, so without
/// this job every day the service has ever seen accumulates forever.
///
/// Same shape as `prune_schedule_destination_departures` directly above, for
/// the same reasons, including the strictly-`<` comparison against
/// `CURRENT_DATE` so today's rows survive any retention value including 0.
/// See `Config::schedule_derived_products_retention_days` for the window and
/// for the one reader whose reach this deliberately bounds.
pub(crate) async fn prune_schedule_calling_points_full(
    pool: &PgPool,
    retention_days: i64,
) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM schedule_calling_points_full \
         WHERE service_date < CURRENT_DATE - ($1 || ' days')::interval",
        )
        .bind(retention_days.to_string()),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Prunes `schedule_services` rows (one per schedule per service date, the
/// service-mode product) for service dates older than `retention_days`.
/// The caller passes the tracked-`trains` window (30 days by default, never
/// less than the other schedule products'), so a tracked bus's page keeps
/// saying it is a bus for as long as the page exists. Same strictly-`<`
/// comparison as the other schedule prunes.
pub(crate) async fn prune_schedule_services(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM schedule_services \
         WHERE service_date < CURRENT_DATE - ($1 || ' days')::interval",
        )
        .bind(retention_days.to_string()),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Prunes `schedule_network_departures` rows for service dates older than
/// `retention_days`.
///
/// Small per day -- one JSONB row per `(crs, service_date)` over ~2,500 CRS
/// codes, which is exactly the reasoning that was used to argue this table
/// needed no pruning at all. That reasoning bounded the wrong dimension: the
/// CRS key space is bounded, `service_date` is not, so this grows by ~2,500
/// rows (each holding up to `MAX_DEPARTURES_PER_STATION` departures) per day,
/// forever. Cheap to keep bounded, and nothing reads a `service_date` past
/// the window (`queries::get_schedule_network_departures` is a single-date
/// lookup for a board being rendered now).
pub(crate) async fn prune_schedule_network_departures(
    pool: &PgPool,
    retention_days: i64,
) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM schedule_network_departures \
         WHERE service_date < CURRENT_DATE - ($1 || ' days')::interval",
        )
        .bind(retention_days.to_string()),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Prunes `schedule_line_population` rows for service dates older than
/// `retention_days`.
///
/// One JSONB row per `(line_id, service_date)` over ~109 catalogued lines --
/// the smallest of the three, but each `population` blob holds every resolved
/// schedule touching that line for that day, so the rows are individually
/// large. Same unbounded-`service_date` growth as its two siblings above, and
/// the same single-date reader
/// (`full-coverage-consumer`'s reload via `GET /private/schedule-line-population`,
/// which asks for one `(line_id, service_date)` it is gating right now).
pub(crate) async fn prune_schedule_line_population(
    pool: &PgPool,
    retention_days: i64,
) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM schedule_line_population \
         WHERE service_date < CURRENT_DATE - ($1 || ' days')::interval",
        )
        .bind(retention_days.to_string()),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Prunes `line_train_summaries` rows for service dates older than
/// `retention_days` -- called with `schedule_line_population`'s own
/// retention right after it, since these rows are derived from that table
/// (one row per train per line per date, rewritten with each population
/// publish; see the table's migration). A reader that finds no rows falls
/// back to the population, so the two never need pruning atomically.
pub(crate) async fn prune_line_train_summaries(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM line_train_summaries \
         WHERE service_date < CURRENT_DATE - ($1 || ' days')::interval",
        )
        .bind(retention_days.to_string()),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Batch size for `prune_trains`'s delete loop -- mirrors
/// `crates/api/src/data/legacy_backfill.rs`'s own `BATCH` precedent for
/// bounding one statement's row count, just sized for a DELETE instead of
/// that module's SELECT-then-UPDATE passes.
const PRUNE_TRAINS_BATCH: i64 = 1000;

/// Prunes `trains` rows on two retention tiers, per
/// docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §5
/// and the later untracked-trains-retention follow-up: a train with at
/// least one `train_subscriptions` row referencing it (`trains_id`) is
/// kept for `retention_days` (a real user tracked that journey); a train
/// with NO such row -- nobody is following it -- is kept for the shorter
/// `untracked_retention_days` instead. A backward-only predicate on the
/// parent row in both cases -- `ON DELETE CASCADE` on
/// `train_movement_events`/`train_current_state` (Task 9) does the rest
/// inside each batch's statement; `train_subscriptions.trains_id`'s `ON
/// DELETE SET NULL` (Task 1) is what makes a pruned train's per-user
/// subscription survive this delete as a no-live-data historical record,
/// identically under either tier.
///
/// Two separate batched loops run in sequence rather than one combined
/// query: the first only ever touches rows with no `train_subscriptions`
/// row (`NOT EXISTS`), the second only ever touches rows with at least
/// one (`EXISTS`), so neither loop can delete a row the other tier owns
/// regardless of which runs first or how the two retention windows
/// compare. Both loops delete in bounded batches (`PRUNE_TRAINS_BATCH`
/// rows per statement, looped until a batch comes back short) rather
/// than one unbounded `DELETE`, so a national-scale prune never holds one
/// long lock / one large WAL burst across potentially millions of rows --
/// see this fix's own review note. Each batch is still its own standalone
/// statement/transaction (same as the loop this mirrors in
/// `legacy_backfill.rs`), so a crash mid-prune loses at most one batch's
/// worth of progress, never the whole run. A tier whose cutoff is older
/// than every row's `service_date` is skipped without issuing its `DELETE`
/// at all (see `trains_prune_due`). Each batch takes the OLDEST eligible
/// rows (`ORDER BY service_date`), which also pins the plan to a range scan
/// of `trains_service_date`: without it, a stale row estimate plus `LIMIT`
/// made the planner pick a whole-table seq scan it expected to stop early
/// (DB review part 2, DB2-6).
pub(crate) async fn prune_trains(
    pool: &PgPool,
    retention_days: i64,
    untracked_retention_days: i64,
) -> Result<u64> {
    let (untracked_due, tracked_due) =
        trains_prune_due(pool, retention_days, untracked_retention_days).await?;
    let mut pruned = 0u64;
    if untracked_due {
        pruned += prune_trains_tier(pool, untracked_retention_days, "NOT EXISTS").await?;
    }
    if tracked_due {
        pruned += prune_trains_tier(pool, retention_days, "EXISTS").await?;
    }
    Ok(pruned)
}

/// One `prune_trains` tier: batches of the oldest eligible rows until a
/// batch comes back short. `subscription_test` is `EXISTS` (tracked) or
/// `NOT EXISTS` (untracked).
async fn prune_trains_tier(
    pool: &PgPool,
    retention_days: i64,
    subscription_test: &'static str,
) -> Result<u64> {
    let sql = format!(
        "DELETE FROM trains WHERE id IN ( \
            SELECT id FROM trains \
            WHERE service_date < CURRENT_DATE - ($1 || ' days')::interval \
              AND {subscription_test} ( \
                  SELECT 1 FROM train_subscriptions \
                  WHERE train_subscriptions.trains_id = trains.id \
              ) \
            ORDER BY service_date \
            LIMIT $2 \
         )"
    );
    let mut pruned = 0u64;
    loop {
        let rows_affected = execute_retention_delete(
            pool,
            sqlx::query(&sql)
                .bind(retention_days.to_string())
                .bind(PRUNE_TRAINS_BATCH),
        )
        .await?
        .rows_affected();
        pruned += rows_affected;
        if rows_affected < PRUNE_TRAINS_BATCH as u64 {
            return Ok(pruned);
        }
    }
}

/// Whether either `trains` retention tier can have anything to delete,
/// as `(untracked_due, tracked_due)`: `MIN(service_date)` (one descent of
/// `trains_service_date`) against each tier's cutoff. Lets
/// `prune_trains` and `archive::archive_and_prune_trains` skip their
/// delete loops entirely on the ~every cycle where no train is old enough,
/// instead of paying one `DELETE ... LIMIT` per tier whose plan depends on
/// how stale `trains`' statistics are (DB review part 2, DB2-6).
pub(crate) async fn trains_prune_due(
    pool: &PgPool,
    retention_days: i64,
    untracked_retention_days: i64,
) -> Result<(bool, bool)> {
    let (untracked_due, tracked_due): (Option<bool>, Option<bool>) = sqlx::query_as(
        "WITH oldest AS (SELECT MIN(service_date) AS d FROM trains) \
         SELECT d < CURRENT_DATE - ($1 || ' days')::interval, \
                d < CURRENT_DATE - ($2 || ' days')::interval \
         FROM oldest",
    )
    .bind(untracked_retention_days.to_string())
    .bind(retention_days.to_string())
    .fetch_one(pool)
    .await?;
    Ok((untracked_due == Some(true), tracked_due == Some(true)))
}

/// The plain Europe/London CALENDAR day (midnight-to-midnight) `instant`
/// falls on -- matching `frontend/lib/dateFormat.ts`'s `londonDayKey`, the
/// convention the Timeline tab already groups by. Deliberately NOT
/// `next_rail_day_boundary`'s rail-day 02:00 cutoff, a different boundary
/// used elsewhere in this crate for incident staleness -- see
/// docs/superpowers/specs/2026-08-31-line-history-graphics-design.md, Open
/// question 5, for why these two conventions coexist.
pub(crate) fn london_calendar_day(instant: DateTime<Utc>) -> NaiveDate {
    instant
        .with_timezone(&chrono_tz::Europe::London)
        .date_naive()
}

/// The plain UTC 30-minute bucket `instant` falls in, truncated to the
/// bucket's start (e.g. 14:07:12Z -> 14:00:00Z, 14:37:12Z -> 14:30:00Z).
/// Originally `utc_hour_start` (1-hour buckets); renamed and narrowed to
/// 30-minute buckets when the trend chart's granularity was doubled --
/// see this crate's git history for the hourly-era version. Deliberately
/// NOT routed through `chrono_tz::Europe::London` the way
/// `london_calendar_day` is -- Decision 4 of
/// docs/superpowers/specs/2026-09-02-trend-chart-granularity-design.md
/// explains why (still applies unchanged at the new granularity):
/// `line_status_half_hourly_stats` only ever backs a rolling 24-hour
/// window, which has no calendar-day identity worth preserving through a
/// DST transition the way the daily table's London-local `day` does. A
/// plain UTC truncation has no 23/25-hour-day edge case to get wrong at
/// all. Never displayed directly to a viewer -- always rendered through
/// `frontend/lib/dateFormat.ts`'s `formatTime` (London wall-clock) first.
///
/// Implemented via explicit `NaiveDate`/`Timelike` arithmetic rather than
/// `chrono::DurationRound::duration_trunc`, since that trait's `round`
/// Cargo feature was not confirmed enabled for this crate's `chrono`
/// dependency -- this avoids depending on an unverified feature flag for
/// what is otherwise a few-line truncation.
#[expect(
    clippy::expect_used,
    reason = "a constant or range-checked time is always valid"
)]
pub(crate) fn utc_half_hour_start(instant: DateTime<Utc>) -> DateTime<Utc> {
    use chrono::Timelike;
    let bucket_minute = if instant.minute() < 30 { 0 } else { 30 };
    instant
        .date_naive()
        .and_hms_opt(instant.hour(), bucket_minute, 0)
        .expect("hour() is always 0-23 and bucket_minute is always 0 or 30, so this can never fail")
        .and_utc()
}

/// Upserts one line's contribution to its `(line_id, day)` daily rollup row
/// for this cycle. Called from `run_cycle` (`main.rs`, a later task) AT MOST
/// ONCE per line per cycle, gated on the line having had ANY raw sample
/// coverage this cycle (`report.statuses.first().and_then(|s| s.sample_stats)`
/// being `Some`) -- that gate is what `sample_cycles` counts, so it always
/// increments by 1 whenever this function is called, regardless of whether
/// `stats` is `Some` or `None`.
///
/// `stats`, when `Some`, must be the DEDUPED per-cycle contribution from
/// `dedup::dedup_new_sample_stats` (this cycle's genuinely NEW distinct
/// trains only, by Darwin `service_id`) -- deliberately NOT the raw,
/// undeduped `SampleStats` attached to the line's report. Summing this
/// across a day therefore yields true per-distinct-train totals rather than
/// poll-cycle-weighted counts, closing the "rate is per sampled poll cycle,
/// not per train" v1 limitation flagged in
/// docs/superpowers/specs/2026-08-31-line-history-graphics-design.md
/// Decision 2 -- see `crates/aggregator/src/dedup.rs`'s module doc, which
/// names this function as its intended consumer.
///
/// `stats: None` is the common, expected case (most cycles see zero new
/// trains once a line's currently-dwelling services have already been
/// counted this period) -- it still counts as a covered cycle
/// (`sample_cycles += 1`) but contributes zero to every other sum.
///
/// Generic over `E: PgExecutor` (rather than `&PgPool`) so `run_cycle`
/// (main.rs) can call this with `&mut *tx` from inside a batched
/// `sqlx::Transaction` -- see `write_line_status`'s doc comment for the
/// full rationale. A single query, so (unlike `write_line_status`) no
/// `&mut PgConnection` is needed here: any executor works, including a
/// bare `&PgPool` for standalone callers/tests.
#[expect(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "per-day train counts stay far below 2^52 and i64::MAX"
)]
pub(crate) async fn record_daily_stats<'c, E>(
    executor: E,
    line_id: &str,
    day: NaiveDate,
    stats: Option<&common::SampleStats>,
) -> Result<()>
where
    E: PgExecutor<'c>,
{
    let (total, delayed, cancelled, skipped, running, delay_minutes_sum) = match stats {
        Some(s) => {
            let running = s.total.saturating_sub(s.cancelled) as i64;
            (
                s.total as i64,
                s.delayed as i64,
                s.cancelled as i64,
                s.skipped as i64,
                running,
                s.avg_delay_minutes * running as f64,
            )
        }
        None => (0, 0, 0, 0, 0, 0.0),
    };
    sqlx::query(
        "INSERT INTO line_status_daily_stats
            (line_id, day, sample_cycles, total, delayed, cancelled, skipped,
             running_count, delay_minutes_sum)
         VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (line_id, day) DO UPDATE SET
            sample_cycles     = line_status_daily_stats.sample_cycles + 1,
            total             = line_status_daily_stats.total + EXCLUDED.total,
            delayed           = line_status_daily_stats.delayed + EXCLUDED.delayed,
            cancelled         = line_status_daily_stats.cancelled + EXCLUDED.cancelled,
            skipped           = line_status_daily_stats.skipped + EXCLUDED.skipped,
            running_count     = line_status_daily_stats.running_count + EXCLUDED.running_count,
            delay_minutes_sum = line_status_daily_stats.delay_minutes_sum + EXCLUDED.delay_minutes_sum",
    )
    .bind(line_id)
    .bind(day)
    .bind(total)
    .bind(delayed)
    .bind(cancelled)
    .bind(skipped)
    .bind(running)
    .bind(delay_minutes_sum)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mirrors `prune_history`'s shape exactly -- called unconditionally every
/// cycle from `run_cycle`, same as `prune_history`, now that
/// `daily_stats_retention_days` always carries a real value (see
/// `config.rs` and docs/superpowers/plans/2026-09-01-ldbws-data-retention.md).
#[expect(
    clippy::cast_possible_truncation,
    reason = "a retention period in days never approaches i32::MAX"
)]
pub(crate) async fn prune_daily_stats(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query("DELETE FROM line_status_daily_stats WHERE day < (CURRENT_DATE - $1::int)")
            .bind(retention_days as i32),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Half-hourly-granularity sibling of `record_daily_stats` -- same
/// accumulate-upsert shape, same "fed the DEDUPED per-cycle contribution,
/// not raw `SampleStats`" contract (see that function's own doc comment,
/// which applies here unchanged), keyed on `half_hour_start` (a plain UTC
/// 30-minute boundary from `utc_half_hour_start`, Decision 4 of the
/// original hourly design, still applicable at the new granularity)
/// instead of a London calendar `day`. Originally `record_hourly_stats`
/// writing hour-keyed rows -- renamed alongside `utc_half_hour_start` when
/// the bucket size was halved; see git history for the hourly-era version.
///
/// Called at the exact same call site as `record_daily_stats`, fed the
/// SAME `deduped: Option<&SampleStats>` value for a given line/cycle --
/// see `main.rs`'s `run_cycle` and
/// docs/superpowers/specs/2026-09-02-trend-chart-granularity-design.md
/// Decision 2. This invariant (both calls see the identical value) is
/// what makes a day's 48 half-hourly rows sum back to that day's
/// `line_status_daily_stats` row -- see this file's
/// `half_hourly_and_daily_stats_reconcile_for_a_single_line_and_period`
/// test.
///
/// Generic over `E: PgExecutor` for the same reason as `record_daily_stats`
/// -- see that function's doc comment.
#[expect(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "per-slot train counts stay far below 2^52 and i64::MAX"
)]
pub(crate) async fn record_half_hourly_stats<'c, E>(
    executor: E,
    line_id: &str,
    half_hour_start: DateTime<Utc>,
    stats: Option<&common::SampleStats>,
) -> Result<()>
where
    E: PgExecutor<'c>,
{
    let (total, delayed, cancelled, skipped, running, delay_minutes_sum) = match stats {
        Some(s) => {
            let running = s.total.saturating_sub(s.cancelled) as i64;
            (
                s.total as i64,
                s.delayed as i64,
                s.cancelled as i64,
                s.skipped as i64,
                running,
                s.avg_delay_minutes * running as f64,
            )
        }
        None => (0, 0, 0, 0, 0, 0.0),
    };
    sqlx::query(
        "INSERT INTO line_status_half_hourly_stats
            (line_id, half_hour_start, sample_cycles, total, delayed, cancelled, skipped,
             running_count, delay_minutes_sum)
         VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (line_id, half_hour_start) DO UPDATE SET
            sample_cycles     = line_status_half_hourly_stats.sample_cycles + 1,
            total             = line_status_half_hourly_stats.total + EXCLUDED.total,
            delayed           = line_status_half_hourly_stats.delayed + EXCLUDED.delayed,
            cancelled         = line_status_half_hourly_stats.cancelled + EXCLUDED.cancelled,
            skipped           = line_status_half_hourly_stats.skipped + EXCLUDED.skipped,
            running_count     = line_status_half_hourly_stats.running_count + EXCLUDED.running_count,
            delay_minutes_sum = line_status_half_hourly_stats.delay_minutes_sum + EXCLUDED.delay_minutes_sum",
    )
    .bind(line_id)
    .bind(half_hour_start)
    .bind(total)
    .bind(delayed)
    .bind(cancelled)
    .bind(skipped)
    .bind(running)
    .bind(delay_minutes_sum)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mirrors `prune_daily_stats`'s shape exactly, called unconditionally
/// every cycle from `run_cycle`, keyed on the `half_hourly_stats_retention_hours`
/// config knob (default 48, unchanged from the hourly-era `Decision 5` default
/// -- NOT a reuse of either `history_retention_days` or
/// `daily_stats_retention_days`, both of which govern unrelated tables).
/// Retention here is measured in wall-clock hours, not bucket count, so
/// halving the bucket size (1h -> 30min) does not change this default: 48
/// hours of real time is still 48 hours of real time, it just now holds
/// roughly twice as many rows per line (~96 instead of ~48) to cover the
/// same window -- a trivial row-count increase for Postgres, not a reason
/// to touch this default or its unit. See `config.rs`'s doc comment on
/// this field for the full reasoning.
pub(crate) async fn prune_half_hourly_stats(pool: &PgPool, retention_hours: i64) -> Result<u64> {
    let result = execute_retention_delete(pool, sqlx::query(
        "DELETE FROM line_status_half_hourly_stats WHERE half_hour_start < NOW() - ($1 || ' hours')::interval",
    )
    .bind(retention_hours.to_string())).await?;
    Ok(result.rows_affected())
}

// --- Decision 4 scaffolding: line_status_{daily,half_hourly}_coverage_stats ---
//
// Sibling pair of record_daily_stats/record_half_hourly_stats above, same
// accumulate-upsert shape, `resolved_windows` in place of `sample_cycles`.
// See crates/ds-store/migrations/20260903200000_line_status_daily_coverage_stats.sql's
// own doc comment for why this is a wholly separate table rather than a
// `source` column on the existing one.
//
// **Judgment call**, since neither the design doc's sketch nor any
// existing code defines this: these functions are fed directly from
// whatever `LineStatus.full_coverage_stats` a line's Layer-3 merge
// produced THIS CYCLE -- NOT a deduped "new distinct trains this cycle"
// value the way `record_daily_stats`/`record_half_hourly_stats` are fed
// `dedup::dedup_new_sample_stats`'s output. `dedup`'s per-service ledger is
// specifically keyed on Darwin `service_id`, an LDBWS-schema concept with
// no defined analog for a full-coverage producer's own materialized
// signal -- whether/how such a producer should express "this cycle's NEW
// contribution" (rather than, say, a running total-so-far for the day) is
// Option B's own future design question, not resolved here. Accumulating
// whatever raw value is passed each cycle is the same posture this
// scaffolding takes everywhere else: correct today (nothing is ever
// passed, since full_coverage_stats stays `None`), and a real design
// decision a future consumer's own integration work will need to make
// explicit.

/// Full-coverage sibling of `record_daily_stats` -- identical
/// accumulate-upsert shape, `resolved_windows` incrementing by 1 every
/// call exactly like `sample_cycles` does. See this section's own module
/// doc comment for the "what counts as a cycle's contribution" judgment
/// call.
#[expect(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "per-day train counts stay far below 2^52 and i64::MAX"
)]
pub(crate) async fn record_daily_coverage_stats<'c, E>(
    executor: E,
    line_id: &str,
    day: NaiveDate,
    stats: Option<&common::SampleStats>,
) -> Result<()>
where
    E: PgExecutor<'c>,
{
    let (total, delayed, cancelled, skipped, running, delay_minutes_sum) = match stats {
        Some(s) => {
            let running = s.total.saturating_sub(s.cancelled) as i64;
            (
                s.total as i64,
                s.delayed as i64,
                s.cancelled as i64,
                s.skipped as i64,
                running,
                s.avg_delay_minutes * running as f64,
            )
        }
        None => (0, 0, 0, 0, 0, 0.0),
    };
    sqlx::query(
        "INSERT INTO line_status_daily_coverage_stats
            (line_id, day, resolved_windows, total, delayed, cancelled, skipped,
             running_count, delay_minutes_sum)
         VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (line_id, day) DO UPDATE SET
            resolved_windows  = line_status_daily_coverage_stats.resolved_windows + 1,
            total             = line_status_daily_coverage_stats.total + EXCLUDED.total,
            delayed           = line_status_daily_coverage_stats.delayed + EXCLUDED.delayed,
            cancelled         = line_status_daily_coverage_stats.cancelled + EXCLUDED.cancelled,
            skipped           = line_status_daily_coverage_stats.skipped + EXCLUDED.skipped,
            running_count     = line_status_daily_coverage_stats.running_count + EXCLUDED.running_count,
            delay_minutes_sum = line_status_daily_coverage_stats.delay_minutes_sum + EXCLUDED.delay_minutes_sum",
    )
    .bind(line_id)
    .bind(day)
    .bind(total)
    .bind(delayed)
    .bind(cancelled)
    .bind(skipped)
    .bind(running)
    .bind(delay_minutes_sum)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mirrors `prune_daily_stats`'s shape exactly. **Judgment call**: reuses
/// the same `daily_stats_retention_days` config knob rather than adding a
/// new one -- a reasonable default for a sibling table with the same shape
/// and no real data yet to suggest it needs a different window; revisit
/// once a real producer exists.
#[expect(
    clippy::cast_possible_truncation,
    reason = "a retention period in days never approaches i32::MAX"
)]
pub(crate) async fn prune_daily_coverage_stats(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM line_status_daily_coverage_stats WHERE day < (CURRENT_DATE - $1::int)",
        )
        .bind(retention_days as i32),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Deletes `full_coverage_line_stats` rows whose `service_date` is more
/// than `retention_days` days old -- the per-day history that table keeps
/// since 2026-09-27 (see `Config::full_coverage_line_stats_retention_days`).
/// Same shape as `prune_daily_coverage_stats`. A `service_date`-only
/// predicate on a `(line_id, service_date)` key is a sequential scan, which
/// is fine at ~250 rows per day.
#[expect(
    clippy::cast_possible_truncation,
    reason = "a retention period in days never approaches i32::MAX"
)]
pub(crate) async fn prune_full_coverage_line_stats(
    pool: &PgPool,
    retention_days: i64,
) -> Result<u64> {
    let result = execute_retention_delete(
        pool,
        sqlx::query(
            "DELETE FROM full_coverage_line_stats WHERE service_date < (CURRENT_DATE - $1::int)",
        )
        .bind(retention_days as i32),
    )
    .await?;
    Ok(result.rows_affected())
}

/// Half-hourly-granularity sibling of `record_daily_coverage_stats` --
/// same relationship `record_half_hourly_stats` already has to
/// `record_daily_stats`. See this section's own module doc comment for
/// the "what counts as a cycle's contribution" judgment call.
#[expect(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "per-slot train counts stay far below 2^52 and i64::MAX"
)]
pub(crate) async fn record_half_hourly_coverage_stats<'c, E>(
    executor: E,
    line_id: &str,
    half_hour_start: DateTime<Utc>,
    stats: Option<&common::SampleStats>,
) -> Result<()>
where
    E: PgExecutor<'c>,
{
    let (total, delayed, cancelled, skipped, running, delay_minutes_sum) = match stats {
        Some(s) => {
            let running = s.total.saturating_sub(s.cancelled) as i64;
            (
                s.total as i64,
                s.delayed as i64,
                s.cancelled as i64,
                s.skipped as i64,
                running,
                s.avg_delay_minutes * running as f64,
            )
        }
        None => (0, 0, 0, 0, 0, 0.0),
    };
    sqlx::query(
        "INSERT INTO line_status_half_hourly_coverage_stats
            (line_id, half_hour_start, resolved_windows, total, delayed, cancelled, skipped,
             running_count, delay_minutes_sum)
         VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (line_id, half_hour_start) DO UPDATE SET
            resolved_windows  = line_status_half_hourly_coverage_stats.resolved_windows + 1,
            total             = line_status_half_hourly_coverage_stats.total + EXCLUDED.total,
            delayed           = line_status_half_hourly_coverage_stats.delayed + EXCLUDED.delayed,
            cancelled         = line_status_half_hourly_coverage_stats.cancelled + EXCLUDED.cancelled,
            skipped           = line_status_half_hourly_coverage_stats.skipped + EXCLUDED.skipped,
            running_count     = line_status_half_hourly_coverage_stats.running_count + EXCLUDED.running_count,
            delay_minutes_sum = line_status_half_hourly_coverage_stats.delay_minutes_sum + EXCLUDED.delay_minutes_sum",
    )
    .bind(line_id)
    .bind(half_hour_start)
    .bind(total)
    .bind(delayed)
    .bind(cancelled)
    .bind(skipped)
    .bind(running)
    .bind(delay_minutes_sum)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mirrors `prune_half_hourly_stats`'s shape exactly. **Judgment call**:
/// reuses the same `half_hourly_stats_retention_hours` config knob -- same
/// reasoning as `prune_daily_coverage_stats`'s own note.
pub(crate) async fn prune_half_hourly_coverage_stats(
    pool: &PgPool,
    retention_hours: i64,
) -> Result<u64> {
    let result = execute_retention_delete(pool, sqlx::query(
        "DELETE FROM line_status_half_hourly_coverage_stats WHERE half_hour_start < NOW() - ($1 || ' hours')::interval",
    )
    .bind(retention_hours.to_string())).await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::too_many_lines,
    reason = "test code: casts of small known test values; fixtures sit next to their use; paired test values share names; scenario tests read top to bottom"
)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                load_incidents_excludes_cleared_rows -- --ignored --test-threads=1` against docker compose's postgres"]
    async fn load_incidents_excludes_cleared_rows() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        sqlx::query(
            "INSERT INTO incidents \
                (incident_id, summary, description, operators, affected_stations, priority, validity_periods, is_planned, is_cleared, source_removed_at) \
             VALUES \
                ('TEST-ACTIVE', 'active', 'active incident', '{}', '{}', 0, '[]', false, false, NULL), \
                ('TEST-CLEARED', 'cleared', 'cleared incident', '{}', '{}', 0, '[]', false, true, NULL), \
                ('TEST-ENDED', 'ended', 'unlisted incident', '{}', '{}', 0, '[]', false, false, now()), \
                ('TEST-ENDED-PLANNED', 'ended', 'unlisted planned work', '{}', '{}', 0, '[]', true, false, now()) \
             ON CONFLICT (incident_id) DO UPDATE SET is_cleared = EXCLUDED.is_cleared, \
                 is_planned = EXCLUDED.is_planned, source_removed_at = EXCLUDED.source_removed_at",
        )
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        let loaded = load_incidents(&pool).await.expect("load_incidents");
        let ids: Vec<&str> = loaded
            .iter()
            .map(|i| i.message.incident_id.as_str())
            .collect();

        sqlx::query(
            "DELETE FROM incidents WHERE incident_id IN \
                ('TEST-ACTIVE', 'TEST-CLEARED', 'TEST-ENDED', 'TEST-ENDED-PLANNED')",
        )
        .execute(&pool)
        .await
        .expect("cleanup fixture rows");

        assert!(
            ids.contains(&"TEST-ACTIVE"),
            "non-cleared incident should be loaded"
        );
        assert!(
            !ids.contains(&"TEST-CLEARED"),
            "cleared incident should be excluded"
        );
        assert!(
            !ids.contains(&"TEST-ENDED") && !ids.contains(&"TEST-ENDED-PLANNED"),
            "an incident the feed no longer lists is not live, planned or not"
        );
    }

    /// 2026-10-06: the cutoff's anchor is `active_since`, falling back to
    /// `first_seen_at` for a row written before the column existed; and an
    /// incident the feed no longer lists stays out even when its extraction
    /// would exempt it from the cutoff (a strike day, today) -- "Ended"
    /// beats every rule.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                load_incidents_anchors_on_active_since -- --ignored --test-threads=1`"]
    async fn load_incidents_anchors_on_active_since_and_ended_beats_exemptions() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        let summary = "Industrial action to affect services today";
        let hash = common::text_hash::text_hash(summary, "");
        let today = Utc::now().to_rfc3339();
        let periods = serde_json::json!([{
            "scope_description": null,
            "date_range": {"from_date": today, "to_date": today},
            "schedule_window": null,
            "resolution_status": "ongoing",
            "apparent_severity": "severe_disruption",
            "resolution_status_confidence": "high",
            "severity_confidence": "low",
            "impact_type": null
        }]);
        sqlx::query(
            "INSERT INTO incidents \
                (incident_id, summary, description, operators, affected_stations, priority, \
                 validity_periods, is_planned, is_cleared, first_seen_at, active_since, \
                 source_removed_at, source_text_hash, extracted_periods) \
             VALUES \
                ('TEST-AS-ARMED', $1, '', '{}', '{}', 0, '[]', false, false, \
                 now() - interval '9 days', now() - interval '1 hour', NULL, $2, $3), \
                ('TEST-AS-LEGACY', $1, '', '{}', '{}', 0, '[]', false, false, \
                 now() - interval '9 days', NULL, NULL, $2, $3), \
                ('TEST-AS-ENDED', $1, '', '{}', '{}', 0, '[]', false, false, \
                 now() - interval '9 days', now() - interval '1 hour', now(), $2, $3) \
             ON CONFLICT (incident_id) DO UPDATE SET active_since = EXCLUDED.active_since, \
                 first_seen_at = EXCLUDED.first_seen_at, \
                 source_removed_at = EXCLUDED.source_removed_at",
        )
        .bind(summary)
        .bind(&hash)
        .bind(&periods)
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        let loaded = load_incidents(&pool).await.expect("load_incidents");
        sqlx::query(
            "DELETE FROM incidents WHERE incident_id IN \
                ('TEST-AS-ARMED', 'TEST-AS-LEGACY', 'TEST-AS-ENDED')",
        )
        .execute(&pool)
        .await
        .expect("cleanup fixture rows");

        let find = |id: &str| loaded.iter().find(|i| i.message.incident_id == id);
        let now = Utc::now();
        let armed = find("TEST-AS-ARMED").expect("live row is loaded");
        assert!(now - armed.active_since < chrono::Duration::hours(2));
        assert!(
            armed.extracted_periods.is_some(),
            "the extraction is current"
        );
        let legacy = find("TEST-AS-LEGACY").expect("live row is loaded");
        assert!(
            now - legacy.active_since > chrono::Duration::days(8),
            "a NULL active_since reads as first_seen_at"
        );
        assert!(
            find("TEST-AS-ENDED").is_none(),
            "source_removed_at beats a strike-day exemption"
        );
    }

    /// `line_status.upcoming` (2026-10-06) is written every cycle without a
    /// history row of its own.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                write_line_status_stores_upcoming -- --ignored --test-threads=1`"]
    async fn write_line_status_stores_upcoming_without_history() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE: &str = "test-upcoming-line";
        let report = LineStatusReport {
            id: LINE.to_string(),
            name: "Upcoming".to_string(),
            mode_name: "national-rail".to_string(),
            operators: vec!["TP".to_string()],
            statuses: vec![],
        };
        let note = common::UpcomingDisruption {
            from: "2026-10-10T23:00:00Z".parse().unwrap(),
            to: Some("2026-10-11T23:00:00Z".parse().unwrap()),
            summary: "Industrial action on Sunday 11 October".to_string(),
            incident_id: "1D3D4694".to_string(),
        };
        let mut conn = pool.acquire().await.unwrap();
        write_line_status(&mut conn, &report, std::slice::from_ref(&note))
            .await
            .unwrap();
        write_line_status(&mut conn, &report, &[]).await.unwrap();
        write_line_status(&mut conn, &report, std::slice::from_ref(&note))
            .await
            .unwrap();
        let (upcoming, history): (serde_json::Value, i64) = sqlx::query_as(
            "SELECT upcoming, (SELECT COUNT(*) FROM line_status_history WHERE line_id = $1) \
             FROM line_status WHERE line_id = $1",
        )
        .bind(LINE)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        sqlx::query("DELETE FROM line_status_history WHERE line_id = $1")
            .bind(LINE)
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("DELETE FROM line_status WHERE line_id = $1")
            .bind(LINE)
            .execute(&mut *conn)
            .await
            .unwrap();
        let stored: Vec<common::UpcomingDisruption> = serde_json::from_value(upcoming).unwrap();
        assert_eq!(stored, vec![note]);
        assert_eq!(history, 1, "only the first write adds history");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                load_incidents_skips_one_malformed_row_instead_of_failing_the_batch -- --ignored --test-threads=1`"]
    async fn load_incidents_skips_one_malformed_row_instead_of_failing_the_batch() {
        // The real failure shape: `validity_periods` is JSONB, so Postgres
        // accepts any valid JSON in it, but `serde_json::from_value` into
        // `Vec<ValidityPeriod>` rejects anything that isn't an array of
        // periods with the right fields. One such row used to fail the WHOLE
        // batch -- every line's status write for the cycle, plus (before
        // retention was split out) every prune behind it, including
        // `trust_event_backlog`'s licensing-mandated window.
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        sqlx::query(
            "INSERT INTO incidents \
                (incident_id, summary, description, operators, affected_stations, priority, validity_periods, is_planned, is_cleared) \
             VALUES \
                ('TEST-GOOD-1', 'good', 'fine', '{}', '{}', 0, '[]', false, false), \
                ('TEST-BAD-JSONB', 'bad', 'malformed', '{}', '{}', 0, '{\"not\": \"an array of periods\"}', false, false), \
                ('TEST-GOOD-2', 'good', 'fine', '{}', '{}', 0, '[]', false, false) \
             ON CONFLICT (incident_id) DO UPDATE SET validity_periods = EXCLUDED.validity_periods",
        )
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        let loaded = load_incidents(&pool).await;

        sqlx::query(
            "DELETE FROM incidents WHERE incident_id IN \
             ('TEST-GOOD-1', 'TEST-BAD-JSONB', 'TEST-GOOD-2')",
        )
        .execute(&pool)
        .await
        .expect("cleanup fixture rows");

        let loaded = loaded.expect("one malformed row must not fail the whole load");
        let ids: Vec<&str> = loaded
            .iter()
            .map(|i| i.message.incident_id.as_str())
            .collect();
        assert!(
            ids.contains(&"TEST-GOOD-1") && ids.contains(&"TEST-GOOD-2"),
            "both well-formed rows must survive alongside the bad one, got {ids:?}"
        );
        assert!(
            !ids.contains(&"TEST-BAD-JSONB"),
            "the malformed row must be skipped, not silently coerced"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                load_incidents_ignores_extraction_stamped_for_superseded_text -- --ignored --test-threads=1`"]
    async fn load_incidents_ignores_extraction_stamped_for_superseded_text() {
        // End-to-end over the real columns: extraction written for text A
        // (by an OLDER model version -- which must not matter) is loaded;
        // once `summary`/`description` move to text B without the enricher
        // having re-run, `extracted_periods` must load as `None` (the
        // never-enriched state); once periods stamped for B are written,
        // they load again.
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        let id = "TEST-STALE-EXTRACTION";
        let periods_a = serde_json::json!([{ "resolution_status": "resolved", "marker": "A" }]);
        let periods_b = serde_json::json!([{ "resolution_status": "ongoing", "marker": "B" }]);
        let hash_a = common::text_hash::text_hash("summary", "text A");
        let hash_b = common::text_hash::text_hash("summary", "text B");

        async fn loaded_periods(pool: &PgPool, id: &str) -> Option<serde_json::Value> {
            load_incidents(pool)
                .await
                .expect("load_incidents")
                .into_iter()
                .find(|i| i.message.incident_id == id)
                .expect("fixture incident must load")
                .extracted_periods
        }

        sqlx::query(
            "INSERT INTO incidents \
                (incident_id, summary, description, operators, affected_stations, priority, \
                 validity_periods, is_planned, is_cleared, source_text_hash, extracted_periods, \
                 extraction_model_version) \
             VALUES ($1, 'summary', 'text A', '{}', '{}', 0, '[]', false, false, $2, $3, \
                     'an-older-model-version') \
             ON CONFLICT (incident_id) DO UPDATE SET \
                summary = EXCLUDED.summary, description = EXCLUDED.description, \
                is_cleared = false, source_text_hash = EXCLUDED.source_text_hash, \
                extracted_periods = EXCLUDED.extracted_periods, \
                extraction_model_version = EXCLUDED.extraction_model_version",
        )
        .bind(id)
        .bind(&hash_a)
        .bind(&periods_a)
        .execute(&pool)
        .await
        .expect("seed fixture row");

        let with_matching_text = loaded_periods(&pool, id).await;

        // The feed's text changes; the enricher hasn't caught up yet.
        sqlx::query("UPDATE incidents SET description = 'text B' WHERE incident_id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .expect("change text");
        let after_text_change = loaded_periods(&pool, id).await;

        // The enricher lands re-extraction for the new text.
        sqlx::query(
            "UPDATE incidents SET source_text_hash = $2, extracted_periods = $3 \
             WHERE incident_id = $1",
        )
        .bind(id)
        .bind(&hash_b)
        .bind(&periods_b)
        .execute(&pool)
        .await
        .expect("write re-extraction");
        let after_re_extraction = loaded_periods(&pool, id).await;

        sqlx::query("DELETE FROM incidents WHERE incident_id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .expect("cleanup fixture row");

        assert_eq!(
            with_matching_text,
            Some(periods_a),
            "extraction matching the current text applies, even from an older model version"
        );
        assert_eq!(
            after_text_change, None,
            "extraction stamped for superseded text must load as absent"
        );
        assert_eq!(
            after_re_extraction,
            Some(periods_b),
            "re-extraction for the new text must apply as soon as it is written"
        );
    }

    /// The production case (2026-10-01): DDG and WNE left the catalogue on
    /// 2026-09-21 but their rows stayed, reported stale every cycle. Rows
    /// for unsampled stations older than the limit go; sampled stations
    /// (however old) and recent unsampled ones (a station another `api`
    /// version has just started sampling) stay; an empty set prunes nothing.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_orphaned_station_samples_deletes_only_old_unsampled_rows \
                -- --ignored --test-threads=1`"]
    async fn prune_orphaned_station_samples_deletes_only_old_unsampled_rows() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        let fixtures = ["ZQA", "ZQB", "ZQC", "ZQD"];
        let cleanup = || async {
            sqlx::query("DELETE FROM station_samples WHERE crs::text = ANY($1::text[])")
                .bind(&fixtures[..])
                .execute(&pool)
                .await
                .expect("cleanup fixture rows");
        };
        cleanup().await;
        sqlx::query(
            "INSERT INTO station_samples (crs, polled_at, departures) VALUES \
                ('ZQA', NOW() - interval '9 days', '[]'), \
                ('ZQB', NOW() - interval '9 days', '[]'), \
                ('ZQC', NOW() - interval '1 minute', '[]'), \
                ('ZQD', NOW() - interval '9 days', '[]')",
        )
        .execute(&pool)
        .await
        .expect("seed fixture rows");
        let remaining = || async {
            let mut crs: Vec<String> = sqlx::query_scalar(
                "SELECT crs::text FROM station_samples WHERE crs::text = ANY($1::text[])",
            )
            .bind(&fixtures[..])
            .fetch_all(&pool)
            .await
            .expect("read fixture rows");
            crs.sort();
            crs
        };

        let nothing = prune_orphaned_station_samples(&pool, &[], 15).await;
        let after_nothing = remaining().await;
        // ZQD stands in for every real sampled station in a shared test DB.
        let sampled = ["ZQD".to_string()];
        let pruned = prune_orphaned_station_samples(&pool, &sampled, 15).await;
        let after = remaining().await;
        cleanup().await;

        assert_eq!(nothing.expect("prune"), Vec::<String>::new());
        assert_eq!(after_nothing, vec!["ZQA", "ZQB", "ZQC", "ZQD"]);
        let pruned = pruned.expect("prune");
        assert!(
            pruned.contains(&"ZQA".to_string()) && pruned.contains(&"ZQB".to_string()),
            "{pruned:?}"
        );
        assert_eq!(
            after,
            vec!["ZQC", "ZQD"],
            "a recent unsampled row and an old sampled one both stay"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                load_station_samples_skips_one_malformed_row_instead_of_failing_the_batch \
                -- --ignored --test-threads=1`"]
    async fn load_station_samples_skips_one_malformed_row_instead_of_failing_the_batch() {
        // Same shape as the incidents case, for the other JSONB loader: a
        // `departures` value that isn't a `Vec<StationDeparture>` must cost
        // that one station's coverage for the cycle, not every station's.
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        sqlx::query(
            "INSERT INTO station_samples (crs, polled_at, departures) VALUES \
                ('ZZG', NOW(), '[]'), \
                ('ZZB', NOW(), '[{\"service_id\": 42}]') \
             ON CONFLICT (crs) DO UPDATE SET departures = EXCLUDED.departures, \
                polled_at = EXCLUDED.polled_at",
        )
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        let loaded = load_station_samples(&pool).await;

        sqlx::query("DELETE FROM station_samples WHERE crs IN ('ZZG', 'ZZB')")
            .execute(&pool)
            .await
            .expect("cleanup fixture rows");

        let loaded = loaded.expect("one malformed row must not fail the whole load");
        assert!(
            loaded.contains_key("ZZG"),
            "the well-formed station sample must survive"
        );
        assert!(
            !loaded.contains_key("ZZB"),
            "the malformed station sample must be skipped"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p aggregator \
                prune_removed_lines_leaves_other_sources_alone -- --ignored --test-threads=1`"]
    async fn prune_removed_lines_leaves_other_sources_alone() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

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

        // A non-empty current-line set that no longer includes 'TEST-AGG':
        // that row is genuinely stale and should go, while the TfL-owned
        // row (a different `source`, never in this crate's line set at
        // all) must survive regardless of what this list contains. Note
        // this deliberately does NOT use an empty `current_line_ids` --
        // see `prune_removed_lines_no_ops_on_an_empty_line_id_list` below
        // for that guard's own regression test.
        prune_removed_lines(&pool, &["some-other-line".to_string()])
            .await
            .expect("prune_removed_lines");

        let survivors: Vec<String> = sqlx::query_scalar(
            "SELECT line_id FROM line_status WHERE line_id IN ('TEST-AGG', 'TEST-TFL')",
        )
        .fetch_all(&pool)
        .await
        .expect("read survivors");

        sqlx::query("DELETE FROM line_status WHERE line_id IN ('TEST-AGG', 'TEST-TFL')")
            .execute(&pool)
            .await
            .expect("cleanup fixture rows");

        assert!(
            !survivors.contains(&"TEST-AGG".to_string()),
            "the aggregator's own stale row should go"
        );
        assert!(
            survivors.contains(&"TEST-TFL".to_string()),
            "a TfL-owned row must not be collateral damage"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p aggregator \
                prune_removed_lines_no_ops_on_an_empty_line_id_list -- --ignored --test-threads=1`"]
    async fn prune_removed_lines_no_ops_on_an_empty_line_id_list() {
        // Signal Box Audit Low finding: an EMPTY `current_line_ids` (e.g.
        // from a misconfigured or failed-to-load line catalogue) must not
        // wipe every aggregator-sourced `line_status` row. `NOT (line_id =
        // ANY($1))` over an empty `$1` is true for every row, so without a
        // guard this would delete everything the aggregator owns.
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        sqlx::query(
            "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
             VALUES \
                ('TEST-AGG-EMPTY', 'test aggregator line', 'national-rail', '{}', '[]', \
                 'aggregator') \
             ON CONFLICT (line_id) DO UPDATE SET source = EXCLUDED.source",
        )
        .execute(&pool)
        .await
        .expect("seed fixture row");

        let removed = prune_removed_lines(&pool, &[])
            .await
            .expect("prune_removed_lines must no-op, not error, on an empty list");

        let survivors: Vec<String> =
            sqlx::query_scalar("SELECT line_id FROM line_status WHERE line_id = 'TEST-AGG-EMPTY'")
                .fetch_all(&pool)
                .await
                .expect("read survivors");

        sqlx::query("DELETE FROM line_status WHERE line_id = 'TEST-AGG-EMPTY'")
            .execute(&pool)
            .await
            .expect("cleanup fixture row");

        assert_eq!(removed, 0, "an empty line-id list must delete nothing");
        assert!(
            survivors.contains(&"TEST-AGG-EMPTY".to_string()),
            "an empty line-id list must not wipe existing aggregator-sourced rows"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p aggregator \
                a_stable_ldbws_inferred_status_carries_its_from_date_across_two_cycles -- --ignored --test-threads=1`"]
    async fn a_stable_ldbws_inferred_status_carries_its_from_date_across_two_cycles() {
        // Real two-cycle aggregate() -> write_line_status() sequence, per
        // docs/superpowers/specs/2026-08-30-inferred-time-ranges-design.md's
        // "Testing" section: a stable LdbwsInferred status across two cycles
        // should still produce exactly one line_status_history row
        // (unchanged from today) AND a stable `from_date` in the second
        // cycle's stored row (the new behavior this design fixes).
        use crate::aggregation::aggregate;
        use crate::queries::LoadedIncident;
        use common::segments::SegmentRegistry;
        use common::{Defaults, LineDefinition, StationDeparture, StationSample};
        use std::collections::HashMap;

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

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
            destination_crs_filter: vec!["AON".to_string()],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
            pass_through: Vec::new(),
            crs_aliases: std::collections::BTreeMap::new(),
            trunk_for: Vec::new(),
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
                delay_reason: if delay_minutes > 0 {
                    Some("signal failure".to_string())
                } else {
                    None
                },
                headcode: None,
                skipped_stations: vec![],
                platform: None,
                planned_platform: None,
                rsid: None,
                calling_points: Vec::new(),
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
        let reports1 = aggregate(
            &lines,
            &no_incidents,
            &samples,
            &registry,
            &defaults,
            &crate::no_trains::StationGazetteer::default(),
        );
        let report1 = reports1.get(LINE_ID).expect("line should have a report");
        assert_eq!(
            report1.statuses[0].data_quality,
            common::DataQuality::LdbwsInferred,
            "sanity check: this scenario should hit the LdbwsInferred path, not an incident-derived one"
        );
        // write_line_status now takes a `&mut PgConnection` (see its doc
        // comment) rather than `&PgPool`, so a standalone caller acquires
        // one explicitly rather than passing the pool directly.
        let mut conn = pool.acquire().await.expect("acquire connection");
        write_line_status(&mut conn, report1, &[])
            .await
            .expect("write_line_status cycle 1");

        let stored_after_cycle_1: serde_json::Value =
            sqlx::query_scalar("SELECT statuses FROM line_status WHERE line_id = $1")
                .bind(LINE_ID)
                .fetch_one(&pool)
                .await
                .expect("read stored statuses after cycle 1");
        let from_date_after_cycle_1 = stored_after_cycle_1[0]["validity"]["from_date"].clone();

        // Cycle 2: identical samples (a real re-poll of the same ongoing,
        // unchanged disruption), but a later, distinct Utc::now() internally.
        let reports2 = aggregate(
            &lines,
            &no_incidents,
            &samples,
            &registry,
            &defaults,
            &crate::no_trains::StationGazetteer::default(),
        );
        let report2 = reports2.get(LINE_ID).expect("line should have a report");
        let fresh_from_date_cycle_2 = serde_json::to_value(report2.statuses[0].validity.from_date)
            .expect("serialize fresh from_date");
        write_line_status(&mut conn, report2, &[])
            .await
            .expect("write_line_status cycle 2");

        let stored_after_cycle_2: serde_json::Value =
            sqlx::query_scalar("SELECT statuses FROM line_status WHERE line_id = $1")
                .bind(LINE_ID)
                .fetch_one(&pool)
                .await
                .expect("read stored statuses after cycle 2");
        let from_date_after_cycle_2 = stored_after_cycle_2[0]["validity"]["from_date"].clone();

        let history_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM line_status_history WHERE line_id = $1")
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

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                a_mid_chunk_failure_rolls_back_every_write_in_that_chunk_but_not_earlier_committed_chunks \
                -- --ignored --test-threads=1` against docker compose's postgres"]
    async fn a_mid_chunk_failure_rolls_back_every_write_in_that_chunk_but_not_earlier_committed_chunks()
     {
        // Pins down the exact batching semantics `run_cycle`'s
        // `WRITE_CHUNK_SIZE` chunking relies on (see main.rs's doc comment
        // on that constant, "Why chunked transactions, not one
        // whole-cycle transaction"): within ONE chunk's transaction, an
        // earlier line's already-queued write is rolled back if a LATER
        // line's write in the SAME chunk fails -- but a chunk that already
        // committed before the failing one is left untouched. This is
        // deliberately neither "one giant whole-cycle transaction" (an
        // earlier chunk's good writes would never be exposed to a later
        // chunk's failure in the real code either way, since each chunk is
        // its own transaction) nor "every write is its own autocommit"
        // (the pre-mitigation behavior this change replaces, where even a
        // single already-succeeded write earlier in the SAME loop
        // iteration group would have survived a later one's failure) --
        // it's the chosen middle ground, checked here against a real
        // Postgres rather than only asserted in prose.
        use common::{DataQuality, LineStatus, SampleAvailability, Severity, ValidityPeriod};

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        const COMMITTED_CHUNK_LINE: &str = "TEST-TX-COMMITTED-CHUNK";
        const ROLLED_BACK_LINE: &str = "TEST-TX-ROLLED-BACK";

        fn report(id: &str) -> LineStatusReport {
            LineStatusReport {
                id: id.to_string(),
                name: "Test Line".to_string(),
                mode_name: "national-rail".to_string(),
                operators: vec![],
                statuses: vec![LineStatus {
                    severity: Severity::GoodService,
                    reason: "Good Service".to_string(),
                    validity: ValidityPeriod {
                        from_date: Utc::now(),
                        to_date: None,
                        is_now: true,
                    },
                    disruption: None,
                    data_quality: DataQuality::default(),
                    sample_stats: None,
                    sample_availability: SampleAvailability::NoCoverage,
                    full_coverage_stats: None,
                    full_coverage_availability: common::FullCoverageAvailability::NotEnabled,
                }],
            }
        }

        // Cleanup any leftovers from a prior failed run before starting.
        sqlx::query("DELETE FROM line_status WHERE line_id IN ($1, $2)")
            .bind(COMMITTED_CHUNK_LINE)
            .bind(ROLLED_BACK_LINE)
            .execute(&pool)
            .await
            .expect("pre-test cleanup");

        // Chunk 1: stands in for an earlier chunk in the same cycle that
        // already committed -- must survive a LATER chunk's failure
        // untouched.
        let mut tx1 = pool.begin().await.expect("begin chunk 1");
        write_line_status(&mut tx1, &report(COMMITTED_CHUNK_LINE), &[])
            .await
            .expect("write committed-chunk line");
        tx1.commit().await.expect("commit chunk 1");

        // Chunk 2: one line's write succeeds first (queued, not yet
        // committed), then a second statement in the SAME transaction
        // deliberately violates line_status's PRIMARY KEY by re-inserting
        // that same line_id without ON CONFLICT handling -- a stand-in for
        // "some later write in this chunk fails" that doesn't require
        // hacking write_line_status itself to fail on demand.
        let mut tx2 = pool.begin().await.expect("begin chunk 2");
        write_line_status(&mut tx2, &report(ROLLED_BACK_LINE), &[])
            .await
            .expect("write rolled-back line (should succeed within the still-open transaction)");
        let conflict_result = sqlx::query(
            "INSERT INTO line_status (line_id, name, mode_name, operators, statuses) \
             VALUES ($1, 'x', 'x', '{}', '[]')",
        )
        .bind(ROLLED_BACK_LINE)
        .execute(&mut *tx2)
        .await;
        assert!(
            conflict_result.is_err(),
            "sanity check: the forced PRIMARY KEY conflict must actually fail, or this test isn't \
             exercising the failure path it claims to"
        );
        // `run_cycle` never calls `.rollback()` explicitly -- a failing
        // write's `?` returns early out of the chunk loop, dropping the
        // transaction un-committed, and sqlx rolls back on drop. Rolling
        // back explicitly here is equivalent in effect but deterministic
        // to await in a test (no reliance on drop-time async cleanup
        // racing the assertions below).
        tx2.rollback().await.expect("rollback chunk 2");

        let committed_survives: Option<String> =
            sqlx::query_scalar("SELECT line_id FROM line_status WHERE line_id = $1")
                .bind(COMMITTED_CHUNK_LINE)
                .fetch_optional(&pool)
                .await
                .expect("check committed-chunk line");
        let rolled_back_gone: Option<String> =
            sqlx::query_scalar("SELECT line_id FROM line_status WHERE line_id = $1")
                .bind(ROLLED_BACK_LINE)
                .fetch_optional(&pool)
                .await
                .expect("check rolled-back line");

        sqlx::query("DELETE FROM line_status WHERE line_id IN ($1, $2)")
            .bind(COMMITTED_CHUNK_LINE)
            .bind(ROLLED_BACK_LINE)
            .execute(&pool)
            .await
            .expect("cleanup");

        assert_eq!(
            committed_survives.as_deref(),
            Some(COMMITTED_CHUNK_LINE),
            "an earlier chunk that already committed must survive a later chunk's failure"
        );
        assert!(
            rolled_back_gone.is_none(),
            "the failing chunk's earlier-in-that-chunk write must be rolled back too, not left as a \
             partial commit"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                writing_more_lines_than_one_chunk_holds_writes_every_line_exactly_once_with_none_lost_or_duplicated \
                -- --ignored --test-threads=1` against docker compose's postgres"]
    async fn writing_more_lines_than_one_chunk_holds_writes_every_line_exactly_once_with_none_lost_or_duplicated()
     {
        // Complements `a_mid_chunk_failure_rolls_back_every_write_in_that_chunk_but_not_earlier_committed_chunks`,
        // above: that test pins the FAILURE-path semantics (a later chunk's
        // rollback must not touch an earlier, already-committed chunk).
        // This one pins the HAPPY-path semantics of the exact same
        // `report_list.chunks(WRITE_CHUNK_SIZE)` pattern `run_cycle` (main.rs)
        // actually uses: writing strictly more lines than fit in a single
        // chunk must produce exactly one `line_status` row per line -- no
        // line dropped at the chunk boundary (e.g. an off-by-one in a future
        // edit to the chunking/loop logic silently skipping the first or
        // last line of a chunk), and no line double-written (e.g. an
        // accidental re-iteration of an already-committed chunk). Chosen
        // count is `WRITE_CHUNK_SIZE + 5`, guaranteeing at least one full
        // chunk plus a short final partial chunk, so both the
        // full-chunk-to-full-chunk boundary and the last-partial-chunk case
        // are covered by the one test.
        use common::{DataQuality, LineStatus, SampleAvailability, Severity, ValidityPeriod};

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        const LINE_COUNT: usize = crate::WRITE_CHUNK_SIZE + 5;
        const PREFIX: &str = "TEST-TX-BOUNDARY-";

        fn line_id(i: usize) -> String {
            format!("{PREFIX}{i:03}")
        }

        fn report(id: &str) -> LineStatusReport {
            LineStatusReport {
                id: id.to_string(),
                name: "Test Line".to_string(),
                mode_name: "national-rail".to_string(),
                operators: vec![],
                statuses: vec![LineStatus {
                    severity: Severity::GoodService,
                    reason: "Good Service".to_string(),
                    validity: ValidityPeriod {
                        from_date: Utc::now(),
                        to_date: None,
                        is_now: true,
                    },
                    disruption: None,
                    data_quality: DataQuality::default(),
                    sample_stats: None,
                    sample_availability: SampleAvailability::NoCoverage,
                    full_coverage_stats: None,
                    full_coverage_availability: common::FullCoverageAvailability::NotEnabled,
                }],
            }
        }

        // Cleanup any leftovers from a prior failed run before starting.
        sqlx::query("DELETE FROM line_status WHERE line_id LIKE $1")
            .bind(format!("{PREFIX}%"))
            .execute(&pool)
            .await
            .expect("pre-test cleanup");

        let ids: Vec<String> = (0..LINE_COUNT).map(line_id).collect();
        let reports: Vec<LineStatusReport> = ids.iter().map(|id| report(id)).collect();

        // Exactly mirrors run_cycle's own loop shape (main.rs): chunk, begin
        // a transaction per chunk, write every report in the chunk, commit.
        for chunk in reports.chunks(crate::WRITE_CHUNK_SIZE) {
            let mut tx = pool.begin().await.expect("begin chunk");
            for report in chunk {
                write_line_status(&mut tx, report, &[])
                    .await
                    .expect("write line in chunk");
            }
            tx.commit().await.expect("commit chunk");
        }

        let written: Vec<String> =
            sqlx::query_scalar("SELECT line_id FROM line_status WHERE line_id LIKE $1")
                .bind(format!("{PREFIX}%"))
                .fetch_all(&pool)
                .await
                .expect("read back written lines");

        sqlx::query("DELETE FROM line_status WHERE line_id LIKE $1")
            .bind(format!("{PREFIX}%"))
            .execute(&pool)
            .await
            .expect("cleanup");

        assert_eq!(
            written.len(),
            LINE_COUNT,
            "every line across both a full chunk and the trailing partial chunk must be written \
             exactly once -- a mismatch here would mean a line was dropped or duplicated at the \
             chunk boundary"
        );
        let mut written_sorted = written.clone();
        written_sorted.sort();
        let mut expected_sorted = ids.clone();
        expected_sorted.sort();
        assert_eq!(
            written_sorted, expected_sorted,
            "the exact set of written line_ids must match what was submitted -- not just the count"
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
    fn normalize_for_diff_ignores_full_coverage_field_changes() {
        // Decision 1's two new fields must be stripped the same way
        // sample_stats/sample_availability already are -- see
        // normalize_entry_for_diff's own doc comment for why.
        let a = serde_json::json!([
            {
                "severity": "good-service",
                "reason": "Good service",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "live",
                "full_coverage_stats": {
                    "total": 10,
                    "delayed": 2,
                    "cancelled": 0,
                    "skipped": 0,
                    "avg_delay_minutes": 1.5
                },
                "full_coverage_availability": {"state": "available"}
            }
        ]);
        let b = serde_json::json!([
            {
                "severity": "good-service",
                "reason": "Good service",
                "validity": {"from_date": "2026-07-09T10:01:00Z"},
                "data_quality": "live",
                "full_coverage_stats": {
                    "total": 12,
                    "delayed": 3,
                    "cancelled": 1,
                    "skipped": 0,
                    "avg_delay_minutes": 2.1
                },
                "full_coverage_availability": {"state": "pending"}
            }
        ]);

        assert_eq!(normalize_for_diff(&a), normalize_for_diff(&b));
    }

    #[test]
    fn normalize_for_diff_ignores_sample_availability_only_changes() {
        let a = serde_json::json!([
            {
                "severity": "good-service",
                "reason": "Good service",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "live",
                "sample_availability": {"state": "below-threshold", "observed": 2, "required": 3}
            }
        ]);
        let b = serde_json::json!([
            {
                "severity": "good-service",
                "reason": "Good service",
                "validity": {"from_date": "2026-07-09T10:01:00Z"},
                "data_quality": "live",
                "sample_availability": {"state": "below-threshold", "observed": 3, "required": 3}
            }
        ]);
        assert_eq!(normalize_for_diff(&a), normalize_for_diff(&b));

        let c = serde_json::json!([
            {
                "severity": "good-service",
                "reason": "Good service",
                "validity": {"from_date": "2026-07-09T10:02:00Z"},
                "data_quality": "live",
                "sample_availability": {"state": "no-coverage"}
            }
        ]);
        assert_eq!(
            normalize_for_diff(&a),
            normalize_for_diff(&c),
            "no-coverage <-> below-threshold churn must not register as changed either"
        );
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
            strip_live_sample_annotation(
                "Works in the area (live samples show: 5 of 9 sampled services delayed.)"
            ),
            "Works in the area",
        );
        assert_eq!(
            strip_live_sample_annotation("Works in the area"),
            "Works in the area"
        );
        // A reason mentioning "live samples show" mid-sentence rather than as
        // the appended trailing annotation must not be touched.
        assert_eq!(
            strip_live_sample_annotation(
                "Works in the area (live samples show: something) more text"
            ),
            "Works in the area (live samples show: something) more text",
        );
    }

    // --- normalize_sample_counts ---
    //
    // See that function's own doc comment for the reasoning behind
    // stripping only the counts and deliberately leaving "(most cited:
    // ...)" text untouched.

    #[test]
    fn normalize_sample_counts_replaces_a_single_count_clause() {
        assert_eq!(
            normalize_sample_counts("5 of 9 sampled services delayed."),
            "N of M sampled services delayed.",
        );
        assert_eq!(
            normalize_sample_counts("12 of 340 sampled services cancelled."),
            "N of M sampled services cancelled.",
        );
    }

    #[test]
    fn normalize_sample_counts_replaces_both_clauses_of_a_combined_delay_skip_reason() {
        assert_eq!(
            normalize_sample_counts(
                "5 of 9 sampled services delayed, 3 of 9 sampled services skipping a scheduled stop."
            ),
            "N of M sampled services delayed, N of M sampled services skipping a scheduled stop.",
        );
    }

    #[test]
    fn normalize_sample_counts_leaves_the_most_cited_suffix_untouched() {
        assert_eq!(
            normalize_sample_counts(
                "5 of 9 sampled services delayed. (most cited: Signal failure)"
            ),
            "N of M sampled services delayed. (most cited: Signal failure)",
        );
    }

    #[test]
    fn normalize_sample_counts_leaves_unrelated_text_alone() {
        assert_eq!(normalize_sample_counts("Good Service"), "Good Service",);
        // "of" not attached to a "<digits> of <digits> sampled services"
        // shape must not be touched, and must not stall the scan.
        assert_eq!(
            normalize_sample_counts(
                "Delayed because of engineering works, 5 of 9 sampled services delayed."
            ),
            "Delayed because of engineering works, N of M sampled services delayed.",
        );
    }

    #[test]
    fn normalize_sample_counts_is_utf8_safe_for_real_national_rail_prose() {
        // Real reproductions, not synthetic edge cases: every string below
        // is the shape of live free text this function is fed (a
        // Knowledgebase incident summary, or LDBWS-inferred Darwin text),
        // and each one panicked the WHOLE aggregator process -- "byte index
        // is not a char boundary" -- before the `char_indices()` rewrite.
        // A panic here unwinds `main` (normalize_sample_counts <-
        // normalize_entry_for_diff <- write_line_status <- run_cycle <-
        // main), so the pod crash-looped on the same uncleared incident
        // every cycle, freezing line-status writes AND retention pruning.
        // These assert the *pass-through* behavior each string should have
        // had all along: none of them is a "<n> of <m> sampled services"
        // count clause, so none should be rewritten.
        for text in [
            // en dash in a platform range, immediately before " of "
            "Platforms 1–3 of 5 closed",
            // accented place name immediately before " of "
            "café of 9 sampled services",
            // curly apostrophe/quotes
            "Queen’s Park of 4 platforms closed",
            "“Platform 2” of 6 out of use",
            // currency symbol
            "Compensation of £10 available",
            // non-breaking space and a degree sign
            "Speed restriction of 20 mph (rails at 50°C)",
            // multi-byte character directly abutting a real digit run that
            // is NOT followed by " sampled services"
            "Lines blocked –3 of 5 platforms affected",
        ] {
            assert_eq!(
                normalize_sample_counts(text),
                text,
                "non-ASCII text must pass through untouched, never panic: {text:?}"
            );
        }

        // The same non-ASCII prose combined with a genuine count clause:
        // the clause is still normalized, the multi-byte text around it is
        // preserved byte-for-byte.
        assert_eq!(
            normalize_sample_counts(
                "Platforms 1–3 closed at Queen’s Park — 5 of 9 sampled services delayed. \
                 (most cited: Signal failure)"
            ),
            "Platforms 1–3 closed at Queen’s Park — N of M sampled services delayed. \
             (most cited: Signal failure)",
        );
        // A count clause whose leading digit run is immediately preceded by
        // a multi-byte character (an em dash with no space, worst case for
        // the old `p + 1` byte bump) still normalizes.
        assert_eq!(
            normalize_sample_counts("Severe delays—5 of 9 sampled services delayed."),
            "Severe delays—N of M sampled services delayed.",
        );
    }

    #[test]
    fn normalize_entry_for_diff_survives_non_ascii_incident_text() {
        // The end-to-end shape of the production crash-loop: a real
        // incident summary with an en dash flowing through the actual
        // diff-normalization entry point `write_line_status` calls.
        let entry = serde_json::json!({
            "severity": "part-closed",
            "reason": "Platforms 1–3 of 5 closed at Llandudno Junction",
            "validity": {"from_date": "2026-09-24T10:00:00Z"},
            "data_quality": "knowledgebase"
        });
        assert_eq!(
            normalize_entry_for_diff(&entry)["reason"],
            serde_json::Value::String(
                "Platforms 1–3 of 5 closed at Llandudno Junction".to_string()
            )
        );
    }

    #[test]
    fn normalize_entry_for_diff_ignores_sample_derived_count_churn() {
        // The write-side regression this fix exists for: two cycles of the
        // exact same underlying situation, live counts wobbling, must
        // normalize to the same identity so `write_line_status` does not
        // insert a fresh `line_status_history` row for pure count noise.
        let a = serde_json::json!([
            {
                "severity": "minor-delays",
                "reason": "5 of 9 sampled services delayed. (most cited: Signal failure)",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "ldbws-inferred"
            }
        ]);
        let b = serde_json::json!([
            {
                "severity": "minor-delays",
                "reason": "7 of 14 sampled services delayed. (most cited: Signal failure)",
                "validity": {"from_date": "2026-07-09T10:05:00Z"},
                "data_quality": "ldbws-inferred"
            }
        ]);

        assert_eq!(normalize_for_diff(&a), normalize_for_diff(&b));
    }

    #[test]
    fn normalize_entry_for_diff_still_detects_a_genuine_most_cited_cause_change() {
        // Design decision: the count fluctuating minute-to-minute is noise,
        // but a genuine change in the most-cited reported cause (e.g.
        // Signal failure -> Engineering works) is real information a human
        // cares about, so it must still register as a change even though
        // the counts themselves also happen to differ.
        let a = serde_json::json!([
            {
                "severity": "minor-delays",
                "reason": "5 of 9 sampled services delayed. (most cited: Signal failure)",
                "validity": {"from_date": "2026-07-09T10:00:00Z"},
                "data_quality": "ldbws-inferred"
            }
        ]);
        let b = serde_json::json!([
            {
                "severity": "minor-delays",
                "reason": "7 of 14 sampled services delayed. (most cited: Engineering works)",
                "validity": {"from_date": "2026-07-09T10:05:00Z"},
                "data_quality": "ldbws-inferred"
            }
        ]);

        assert_ne!(normalize_for_diff(&a), normalize_for_diff(&b));
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
        let existing = ldbws_status(
            "2026-08-30T06:00:00Z",
            "minor-delays",
            "3 of 10 sampled services delayed.",
        );
        let fresh = ldbws_status(
            "2026-08-30T09:30:00Z",
            "minor-delays",
            "3 of 10 sampled services delayed.",
        );

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], existing[0]["validity"]["from_date"],
            "unchanged content should carry forward the OLD from_date, not the fresh stamp"
        );
    }

    #[test]
    fn carry_forward_uses_the_fresh_stamp_when_severity_changes() {
        let existing = ldbws_status(
            "2026-08-30T06:00:00Z",
            "minor-delays",
            "3 of 10 sampled services delayed.",
        );
        let fresh = ldbws_status(
            "2026-08-30T09:30:00Z",
            "severe-delays",
            "7 of 10 sampled services delayed.",
        );

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], fresh[0]["validity"]["from_date"],
            "a genuine severity change must not carry forward the old from_date"
        );
    }

    #[test]
    fn carry_forward_uses_the_fresh_stamp_when_reason_changes() {
        let existing = ldbws_status(
            "2026-08-30T06:00:00Z",
            "minor-delays",
            "3 of 10 sampled services delayed.",
        );
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
        let fresh = ldbws_status(
            "2026-08-30T09:30:00Z",
            "minor-delays",
            "3 of 10 sampled services delayed.",
        );

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], fresh[0]["validity"]["from_date"],
            "a line with no prior stored status has nothing to carry forward from"
        );
    }

    #[test]
    fn carry_forward_uses_the_fresh_stamp_when_the_prior_entry_has_a_different_data_quality() {
        let existing = knowledgebase_status("2026-08-30T06:00:00Z");
        let fresh = ldbws_status(
            "2026-08-30T09:30:00Z",
            "minor-delays",
            "3 of 10 sampled services delayed.",
        );

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

        assert_eq!(
            result, fresh,
            "non-ldbws-inferred entries must pass through untouched"
        );
    }

    /// `ldbws_status`'s sibling for the other inferred provenance -- the one
    /// `aggregation::merge_full_coverage_stats` stamps when full-coverage
    /// data determines a line's severity with no incident present.
    fn trust_status(from_date: &str, severity: &str, reason: &str) -> serde_json::Value {
        let mut statuses = ldbws_status(from_date, severity, reason);
        statuses[0]["data_quality"] = serde_json::Value::String("trust-inferred".to_string());
        statuses
    }

    #[test]
    fn carry_forward_also_covers_trust_inferred_entries() {
        // `trust-inferred` has exactly the same problem `ldbws-inferred` does:
        // no incident of its own to take a stable `from_date` from, so
        // `aggregation.rs` re-stamps it with `Utc::now()` every cycle.
        // Recognizing only `ldbws-inferred` meant a `TrustInferred` status
        // (live today -- `lines/tfw-conwy-valley.toml` sets
        // `full_coverage_enabled = true`) reported "disrupted since just now"
        // forever, no matter how long the disruption had actually been
        // running.
        let existing = trust_status(
            "2026-09-24T06:00:00Z",
            "part-suspended",
            "6 of 10 sampled services cancelled.",
        );
        let fresh = trust_status(
            "2026-09-24T09:30:00Z",
            "part-suspended",
            "6 of 10 sampled services cancelled.",
        );

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], existing[0]["validity"]["from_date"],
            "an unchanged trust-inferred status must keep its original since-timestamp"
        );
    }

    #[test]
    fn carry_forward_treats_a_provenance_change_between_inferred_kinds_as_a_new_status() {
        // Both sides are inferred, but the signal that determined the
        // published severity changed (LDBWS sampling -> full-coverage TRUST
        // data, or back). That is a real change in what the status means, so
        // it gets a fresh stamp rather than silently inheriting the other
        // provenance's clock.
        let existing = ldbws_status(
            "2026-09-24T06:00:00Z",
            "part-suspended",
            "6 of 10 sampled services cancelled.",
        );
        let fresh = trust_status(
            "2026-09-24T09:30:00Z",
            "part-suspended",
            "6 of 10 sampled services cancelled.",
        );

        let result = carry_forward_ldbws_from_date(&existing, &fresh);

        assert_eq!(
            result[0]["validity"]["from_date"], fresh[0]["validity"]["from_date"],
            "a provenance change must not carry the old from_date forward"
        );
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

    // --- london_calendar_day ---
    //
    // Mirrors the DST-transition rigor of `aggregation.rs`'s
    // `next_rail_day_boundary_*` tests, but for the plain calendar-day
    // boundary instead of the rail-day 02:00 one.

    #[test]
    fn london_calendar_day_just_before_london_midnight_in_bst_stays_on_the_earlier_day() {
        // 2026-07-15 22:59 UTC is 2026-07-15 23:59 BST (July is daylight
        // saving, UTC+1) -- still the same London calendar day.
        let instant: DateTime<Utc> = "2026-07-15T22:59:00Z".parse().unwrap();
        assert_eq!(
            london_calendar_day(instant),
            NaiveDate::from_ymd_opt(2026, 7, 15).unwrap()
        );
    }

    #[test]
    fn london_calendar_day_just_after_london_midnight_in_bst_rolls_to_the_next_day() {
        // 2026-07-15 23:00 UTC is 2026-07-16 00:00 BST -- just crossed into
        // the next London calendar day.
        let instant: DateTime<Utc> = "2026-07-15T23:00:00Z".parse().unwrap();
        assert_eq!(
            london_calendar_day(instant),
            NaiveDate::from_ymd_opt(2026, 7, 16).unwrap()
        );
    }

    #[test]
    fn london_calendar_day_around_london_midnight_in_gmt() {
        // January is GMT (UTC+0), so the London calendar day boundary lines
        // up exactly with the UTC one -- no offset to account for.
        let just_before: DateTime<Utc> = "2026-01-15T23:59:00Z".parse().unwrap();
        let just_after: DateTime<Utc> = "2026-01-16T00:00:00Z".parse().unwrap();
        assert_eq!(
            london_calendar_day(just_before),
            NaiveDate::from_ymd_opt(2026, 1, 15).unwrap()
        );
        assert_eq!(
            london_calendar_day(just_after),
            NaiveDate::from_ymd_opt(2026, 1, 16).unwrap()
        );
    }

    #[test]
    fn london_calendar_day_across_the_spring_forward_transition() {
        // UK clocks spring forward at 01:00 UTC on 2026-03-29, jumping local
        // time from 01:00 GMT straight to 02:00 BST. Neither side of that
        // jump is anywhere near local midnight, so the calendar day must
        // stay 2026-03-29 on both sides -- this exercises that converting a
        // UTC instant (never ambiguous/missing, unlike the reverse
        // direction `next_rail_day_boundary` deals with) through the jump
        // doesn't perturb the resulting date.
        let just_before: DateTime<Utc> = "2026-03-29T00:59:00Z".parse().unwrap();
        let just_after: DateTime<Utc> = "2026-03-29T01:30:00Z".parse().unwrap();
        assert_eq!(
            london_calendar_day(just_before),
            NaiveDate::from_ymd_opt(2026, 3, 29).unwrap()
        );
        assert_eq!(
            london_calendar_day(just_after),
            NaiveDate::from_ymd_opt(2026, 3, 29).unwrap()
        );
    }

    #[test]
    fn london_calendar_day_across_the_fall_back_transition() {
        // UK clocks fall back at 01:00 UTC on 2026-10-25, jumping local time
        // from 02:00 BST back to 01:00 GMT. Again nowhere near local
        // midnight, so the calendar day is unaffected by the repeated local
        // hour.
        let just_before: DateTime<Utc> = "2026-10-25T00:30:00Z".parse().unwrap();
        let just_after: DateTime<Utc> = "2026-10-25T01:30:00Z".parse().unwrap();
        assert_eq!(
            london_calendar_day(just_before),
            NaiveDate::from_ymd_opt(2026, 10, 25).unwrap()
        );
        assert_eq!(
            london_calendar_day(just_after),
            NaiveDate::from_ymd_opt(2026, 10, 25).unwrap()
        );
    }

    // --- utc_half_hour_start ---
    //
    // Deliberately no DST-transition cases here, unlike london_calendar_day's
    // suite above -- see Decision 4 / this function's own doc comment for why
    // a plain UTC truncation has nothing DST-related to get wrong.

    #[test]
    fn utc_half_hour_start_truncates_down_to_the_bucket_start() {
        // First half of the hour truncates to :00...
        let instant: DateTime<Utc> = "2026-08-15T14:07:12Z".parse().unwrap();
        assert_eq!(
            utc_half_hour_start(instant),
            "2026-08-15T14:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        // ...second half truncates to :30.
        let instant: DateTime<Utc> = "2026-08-15T14:37:12Z".parse().unwrap();
        assert_eq!(
            utc_half_hour_start(instant),
            "2026-08-15T14:30:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn utc_half_hour_start_on_an_exact_bucket_boundary_is_a_no_op() {
        let on_the_hour: DateTime<Utc> = "2026-08-15T14:00:00Z".parse().unwrap();
        assert_eq!(utc_half_hour_start(on_the_hour), on_the_hour);
        let on_the_half_hour: DateTime<Utc> = "2026-08-15T14:30:00Z".parse().unwrap();
        assert_eq!(utc_half_hour_start(on_the_half_hour), on_the_half_hour);
    }

    #[test]
    fn utc_half_hour_start_just_before_midnight_stays_on_the_same_utc_day() {
        let instant: DateTime<Utc> = "2026-08-15T23:59:59Z".parse().unwrap();
        assert_eq!(
            utc_half_hour_start(instant),
            "2026-08-15T23:30:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn utc_half_hour_start_across_the_uk_spring_forward_transition_is_unaffected() {
        // Unlike london_calendar_day_across_the_spring_forward_transition, this
        // is purely a sanity check that UTC arithmetic doesn't care that the
        // UK clock changed at all -- there is no "skipped" or "repeated" UTC
        // half-hour on this date, only on the London-local wall clock.
        let instant: DateTime<Utc> = "2026-03-29T01:45:00Z".parse().unwrap();
        assert_eq!(
            utc_half_hour_start(instant),
            "2026-03-29T01:30:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    // --- record_daily_stats / prune_daily_stats ---

    async fn cleanup_daily_stats(pool: &PgPool, line_id: &str) {
        sqlx::query("DELETE FROM line_status_daily_stats WHERE line_id = $1")
            .bind(line_id)
            .execute(pool)
            .await
            .expect("cleanup line_status_daily_stats");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_daily_stats_accumulates_deduped_contributions_across_a_day -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn record_daily_stats_accumulates_deduped_contributions_across_a_day() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-DAILY-STATS-ACCUMULATE";
        let day = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap();

        cleanup_daily_stats(&pool, LINE_ID).await;

        let cycle1 = common::SampleStats {
            total: 4,
            delayed: 1,
            cancelled: 1,
            skipped: 0,
            avg_delay_minutes: 6.0,
        };
        let cycle2 = common::SampleStats {
            total: 2,
            delayed: 2,
            cancelled: 0,
            skipped: 1,
            avg_delay_minutes: 12.0,
        };

        record_daily_stats(&pool, LINE_ID, day, Some(&cycle1))
            .await
            .expect("record cycle 1");
        record_daily_stats(&pool, LINE_ID, day, Some(&cycle2))
            .await
            .expect("record cycle 2");

        let row = sqlx::query(
            "SELECT sample_cycles, total, delayed, cancelled, skipped, running_count, delay_minutes_sum \
             FROM line_status_daily_stats WHERE line_id = $1 AND day = $2",
        )
        .bind(LINE_ID)
        .bind(day)
        .fetch_one(&pool)
        .await
        .expect("read accumulated row");

        cleanup_daily_stats(&pool, LINE_ID).await;

        let sample_cycles: i64 = row.try_get("sample_cycles").unwrap();
        let total: i64 = row.try_get("total").unwrap();
        let delayed: i64 = row.try_get("delayed").unwrap();
        let cancelled: i64 = row.try_get("cancelled").unwrap();
        let skipped: i64 = row.try_get("skipped").unwrap();
        let running_count: i64 = row.try_get("running_count").unwrap();
        let delay_minutes_sum: f64 = row.try_get("delay_minutes_sum").unwrap();

        assert_eq!(
            sample_cycles, 2,
            "two Some(stats) calls should each count as one covered cycle"
        );
        assert_eq!(total, 6);
        assert_eq!(delayed, 3);
        assert_eq!(cancelled, 1);
        assert_eq!(skipped, 1);
        // running_count = (4 - 1) + (2 - 0) = 5.
        assert_eq!(running_count, 5);
        // delay_minutes_sum = 6.0 * 3 + 12.0 * 2 = 42.0.
        assert!((delay_minutes_sum - 42.0).abs() < 1e-9);

        // Recovering each cycle's avg_delay_minutes via division must match
        // within floating-point tolerance -- checked per-cycle here since
        // the accumulated row only proves the *sum* recovers correctly when
        // divided by the accumulated running_count as a whole.
        let recovered_overall_avg = delay_minutes_sum / running_count as f64;
        assert!((recovered_overall_avg - (42.0 / 5.0)).abs() < 1e-9);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_daily_stats_none_still_counts_the_cycle_but_adds_nothing -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    #[expect(clippy::float_cmp, reason = "a sum of no samples is exactly 0.0")]
    async fn record_daily_stats_none_still_counts_the_cycle_but_adds_nothing() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-DAILY-STATS-NONE";
        let day = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap();

        cleanup_daily_stats(&pool, LINE_ID).await;

        record_daily_stats(&pool, LINE_ID, day, None)
            .await
            .expect("record a None cycle");
        record_daily_stats(&pool, LINE_ID, day, None)
            .await
            .expect("record a second None cycle");

        let row = sqlx::query(
            "SELECT sample_cycles, total, delayed, cancelled, skipped, running_count, delay_minutes_sum \
             FROM line_status_daily_stats WHERE line_id = $1 AND day = $2",
        )
        .bind(LINE_ID)
        .bind(day)
        .fetch_one(&pool)
        .await
        .expect("read row");

        cleanup_daily_stats(&pool, LINE_ID).await;

        let sample_cycles: i64 = row.try_get("sample_cycles").unwrap();
        let total: i64 = row.try_get("total").unwrap();
        let delay_minutes_sum: f64 = row.try_get("delay_minutes_sum").unwrap();

        assert_eq!(sample_cycles, 2, "None still counts as a covered cycle");
        assert_eq!(total, 0, "None must contribute zero to every sum column");
        assert_eq!(delay_minutes_sum, 0.0);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_daily_stats_a_new_day_starts_a_fresh_row -- --ignored --test-threads=1` against docker \
                compose's postgres"]
    async fn record_daily_stats_a_new_day_starts_a_fresh_row() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-DAILY-STATS-NEW-DAY";
        let day1 = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();

        cleanup_daily_stats(&pool, LINE_ID).await;

        let stats = common::SampleStats {
            total: 5,
            delayed: 1,
            cancelled: 0,
            skipped: 0,
            avg_delay_minutes: 3.0,
        };
        record_daily_stats(&pool, LINE_ID, day1, Some(&stats))
            .await
            .expect("record day 1");
        record_daily_stats(&pool, LINE_ID, day2, Some(&stats))
            .await
            .expect("record day 2");

        let day2_cycles: i64 = sqlx::query_scalar(
            "SELECT sample_cycles FROM line_status_daily_stats WHERE line_id = $1 AND day = $2",
        )
        .bind(LINE_ID)
        .bind(day2)
        .fetch_one(&pool)
        .await
        .expect("read day 2 row");

        cleanup_daily_stats(&pool, LINE_ID).await;

        assert_eq!(
            day2_cycles, 1,
            "a new day must start its own fresh row, not accumulate into day 1's"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_daily_stats_deletes_only_rows_older_than_the_retention_window -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn prune_daily_stats_deletes_only_rows_older_than_the_retention_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const OLD_LINE_ID: &str = "TEST-DAILY-STATS-PRUNE-OLD";
        const RECENT_LINE_ID: &str = "TEST-DAILY-STATS-PRUNE-RECENT";
        const RETENTION_DAYS: i64 = 30;

        cleanup_daily_stats(&pool, OLD_LINE_ID).await;
        cleanup_daily_stats(&pool, RECENT_LINE_ID).await;

        sqlx::query(
            "INSERT INTO line_status_daily_stats (line_id, day, sample_cycles, total) VALUES \
                ($1, CURRENT_DATE - ($3::int + 1), 1, 1), \
                ($2, CURRENT_DATE - ($3::int - 1), 1, 1)",
        )
        .bind(OLD_LINE_ID)
        .bind(RECENT_LINE_ID)
        .bind(RETENTION_DAYS as i32)
        .execute(&pool)
        .await
        .expect("seed old and recent rows");

        prune_daily_stats(&pool, RETENTION_DAYS)
            .await
            .expect("prune_daily_stats");

        let old_survives: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM line_status_daily_stats WHERE line_id = $1")
                .bind(OLD_LINE_ID)
                .fetch_one(&pool)
                .await
                .expect("count old survivors");
        let recent_survives: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM line_status_daily_stats WHERE line_id = $1")
                .bind(RECENT_LINE_ID)
                .fetch_one(&pool)
                .await
                .expect("count recent survivors");

        cleanup_daily_stats(&pool, OLD_LINE_ID).await;
        cleanup_daily_stats(&pool, RECENT_LINE_ID).await;

        assert_eq!(
            old_survives, 0,
            "a row older than the retention window should be pruned"
        );
        assert_eq!(
            recent_survives, 1,
            "a row within the retention window should be kept"
        );
    }

    // --- record_half_hourly_stats / prune_half_hourly_stats ---

    async fn cleanup_half_hourly_stats(pool: &PgPool, line_id: &str) {
        sqlx::query("DELETE FROM line_status_half_hourly_stats WHERE line_id = $1")
            .bind(line_id)
            .execute(pool)
            .await
            .expect("cleanup line_status_half_hourly_stats");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_half_hourly_stats_accumulates_deduped_contributions_within_a_bucket -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn record_half_hourly_stats_accumulates_deduped_contributions_within_a_bucket() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-HALF-HOURLY-STATS-ACCUMULATE";
        let bucket: DateTime<Utc> = "2026-08-31T14:00:00Z".parse().unwrap();

        cleanup_half_hourly_stats(&pool, LINE_ID).await;

        let cycle1 = common::SampleStats {
            total: 4,
            delayed: 1,
            cancelled: 1,
            skipped: 0,
            avg_delay_minutes: 6.0,
        };
        let cycle2 = common::SampleStats {
            total: 2,
            delayed: 2,
            cancelled: 0,
            skipped: 1,
            avg_delay_minutes: 12.0,
        };

        record_half_hourly_stats(&pool, LINE_ID, bucket, Some(&cycle1))
            .await
            .expect("record cycle 1");
        record_half_hourly_stats(&pool, LINE_ID, bucket, Some(&cycle2))
            .await
            .expect("record cycle 2");

        let row = sqlx::query(
            "SELECT sample_cycles, total, delayed, cancelled, skipped, running_count, delay_minutes_sum \
             FROM line_status_half_hourly_stats WHERE line_id = $1 AND half_hour_start = $2",
        )
        .bind(LINE_ID)
        .bind(bucket)
        .fetch_one(&pool)
        .await
        .expect("read accumulated row");

        cleanup_half_hourly_stats(&pool, LINE_ID).await;

        let sample_cycles: i64 = row.try_get("sample_cycles").unwrap();
        let total: i64 = row.try_get("total").unwrap();
        let running_count: i64 = row.try_get("running_count").unwrap();
        let delay_minutes_sum: f64 = row.try_get("delay_minutes_sum").unwrap();

        assert_eq!(sample_cycles, 2);
        assert_eq!(total, 6);
        assert_eq!(running_count, 5);
        assert!((delay_minutes_sum - 42.0).abs() < 1e-9);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_half_hourly_stats_a_new_bucket_starts_a_fresh_row -- --ignored --test-threads=1` against docker \
                compose's postgres"]
    async fn record_half_hourly_stats_a_new_bucket_starts_a_fresh_row() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-HALF-HOURLY-STATS-NEW-BUCKET";
        let bucket1: DateTime<Utc> = "2026-08-31T14:00:00Z".parse().unwrap();
        let bucket2: DateTime<Utc> = "2026-08-31T14:30:00Z".parse().unwrap();

        cleanup_half_hourly_stats(&pool, LINE_ID).await;

        let stats = common::SampleStats {
            total: 5,
            delayed: 1,
            cancelled: 0,
            skipped: 0,
            avg_delay_minutes: 3.0,
        };
        record_half_hourly_stats(&pool, LINE_ID, bucket1, Some(&stats))
            .await
            .expect("record bucket 1");
        record_half_hourly_stats(&pool, LINE_ID, bucket2, Some(&stats))
            .await
            .expect("record bucket 2");

        let bucket2_cycles: i64 = sqlx::query_scalar(
            "SELECT sample_cycles FROM line_status_half_hourly_stats WHERE line_id = $1 AND half_hour_start = $2",
        )
        .bind(LINE_ID)
        .bind(bucket2)
        .fetch_one(&pool)
        .await
        .expect("read bucket 2 row");

        cleanup_half_hourly_stats(&pool, LINE_ID).await;

        assert_eq!(
            bucket2_cycles, 1,
            "a new bucket must start its own fresh row, not accumulate into bucket 1's"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_half_hourly_stats_a_bucket_boundary_crossing_a_day_boundary_is_unaffected -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn record_half_hourly_stats_a_bucket_boundary_crossing_a_day_boundary_is_unaffected() {
        // 23:30Z and the next day's 00:00Z are adjacent buckets that also cross
        // a UTC calendar day -- confirms record_half_hourly_stats treats this
        // exactly like any other bucket boundary, with no special-casing or
        // corruption.
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-HALF-HOURLY-STATS-DAY-BOUNDARY";
        let bucket1: DateTime<Utc> = "2026-08-31T23:30:00Z".parse().unwrap();
        let bucket2: DateTime<Utc> = "2026-09-01T00:00:00Z".parse().unwrap();

        cleanup_half_hourly_stats(&pool, LINE_ID).await;

        let stats = common::SampleStats {
            total: 3,
            delayed: 0,
            cancelled: 0,
            skipped: 0,
            avg_delay_minutes: 1.0,
        };
        record_half_hourly_stats(&pool, LINE_ID, bucket1, Some(&stats))
            .await
            .expect("record bucket 1");
        record_half_hourly_stats(&pool, LINE_ID, bucket2, Some(&stats))
            .await
            .expect("record bucket 2");

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM line_status_half_hourly_stats WHERE line_id = $1",
        )
        .bind(LINE_ID)
        .fetch_one(&pool)
        .await
        .expect("count rows");

        cleanup_half_hourly_stats(&pool, LINE_ID).await;

        assert_eq!(
            count, 2,
            "two adjacent buckets either side of a day boundary must stay two separate rows"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_half_hourly_stats_deletes_only_rows_older_than_the_retention_window -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn prune_half_hourly_stats_deletes_only_rows_older_than_the_retention_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const OLD_LINE_ID: &str = "TEST-HALF-HOURLY-STATS-PRUNE-OLD";
        const RECENT_LINE_ID: &str = "TEST-HALF-HOURLY-STATS-PRUNE-RECENT";
        const RETENTION_HOURS: i64 = 48;

        cleanup_half_hourly_stats(&pool, OLD_LINE_ID).await;
        cleanup_half_hourly_stats(&pool, RECENT_LINE_ID).await;

        sqlx::query(
            "INSERT INTO line_status_half_hourly_stats (line_id, half_hour_start, sample_cycles, total) VALUES \
                ($1, NOW() - (($3 + 1) || ' hours')::interval, 1, 1), \
                ($2, NOW() - (($3 - 1) || ' hours')::interval, 1, 1)",
        )
        .bind(OLD_LINE_ID)
        .bind(RECENT_LINE_ID)
        .bind(RETENTION_HOURS)
        .execute(&pool)
        .await
        .expect("seed old and recent rows");

        prune_half_hourly_stats(&pool, RETENTION_HOURS)
            .await
            .expect("prune_half_hourly_stats");

        let old_survives: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM line_status_half_hourly_stats WHERE line_id = $1",
        )
        .bind(OLD_LINE_ID)
        .fetch_one(&pool)
        .await
        .expect("count old survivors");
        let recent_survives: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM line_status_half_hourly_stats WHERE line_id = $1",
        )
        .bind(RECENT_LINE_ID)
        .fetch_one(&pool)
        .await
        .expect("count recent survivors");

        cleanup_half_hourly_stats(&pool, OLD_LINE_ID).await;
        cleanup_half_hourly_stats(&pool, RECENT_LINE_ID).await;

        assert_eq!(old_survives, 0);
        assert_eq!(recent_survives, 1);
    }

    /// The single most important new test in this plan (Decision 2's
    /// reconciliation invariant, made concrete): feeding the SAME
    /// `Some(&SampleStats)` value to both `record_daily_stats` and
    /// `record_half_hourly_stats` -- exactly as `main.rs`'s `run_cycle` now
    /// does at its one call site -- must produce a daily row and a
    /// half-hourly row whose sums agree. This doesn't call `run_cycle`
    /// itself (that would need a full `aggregate()` pipeline); it directly
    /// exercises the two write functions with an identical input, which is
    /// the actual invariant that matters and is what would regress if a
    /// future edit ever computed two separate `deduped` values instead of
    /// sharing one.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                half_hourly_and_daily_stats_reconcile_for_a_single_line_and_period -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn half_hourly_and_daily_stats_reconcile_for_a_single_line_and_period() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-RECONCILE-DAILY-HALF-HOURLY";
        let day = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap();
        let bucket: DateTime<Utc> = "2026-08-31T14:00:00Z".parse().unwrap();

        cleanup_daily_stats(&pool, LINE_ID).await;
        cleanup_half_hourly_stats(&pool, LINE_ID).await;

        let stats = common::SampleStats {
            total: 7,
            delayed: 2,
            cancelled: 1,
            skipped: 0,
            avg_delay_minutes: 5.0,
        };

        // Same `deduped` value, same call pattern as run_cycle's one call site.
        record_daily_stats(&pool, LINE_ID, day, Some(&stats))
            .await
            .expect("record daily");
        record_half_hourly_stats(&pool, LINE_ID, bucket, Some(&stats))
            .await
            .expect("record half-hourly");

        let daily = sqlx::query(
            "SELECT total, delayed, cancelled, running_count, delay_minutes_sum \
                                  FROM line_status_daily_stats WHERE line_id = $1 AND day = $2",
        )
        .bind(LINE_ID)
        .bind(day)
        .fetch_one(&pool)
        .await
        .expect("read daily row");
        let half_hourly = sqlx::query("SELECT total, delayed, cancelled, running_count, delay_minutes_sum \
                                   FROM line_status_half_hourly_stats WHERE line_id = $1 AND half_hour_start = $2")
            .bind(LINE_ID).bind(bucket).fetch_one(&pool).await.expect("read half-hourly row");

        cleanup_daily_stats(&pool, LINE_ID).await;
        cleanup_half_hourly_stats(&pool, LINE_ID).await;

        let total_d: i64 = daily.try_get("total").unwrap();
        let total_h: i64 = half_hourly.try_get("total").unwrap();
        let delayed_d: i64 = daily.try_get("delayed").unwrap();
        let delayed_h: i64 = half_hourly.try_get("delayed").unwrap();
        let dms_d: f64 = daily.try_get("delay_minutes_sum").unwrap();
        let dms_h: f64 = half_hourly.try_get("delay_minutes_sum").unwrap();

        assert_eq!(
            total_d, total_h,
            "a single half-hour's stats must reconcile with that bucket's own contribution to the day"
        );
        assert_eq!(delayed_d, delayed_h);
        assert!((dms_d - dms_h).abs() < 1e-9);
    }

    // --- record_daily_coverage_stats / record_half_hourly_coverage_stats /
    // prune_{daily,half_hourly}_coverage_stats (Decision 4 scaffolding) ---
    //
    // Mirrors the sample-stats test set above one-for-one, against the new
    // sibling tables. No real writer ever calls these functions in
    // production today (see queries.rs's own module doc comment on this
    // section) -- these tests exist so the write/prune SQL itself is
    // proven correct in advance of a real producer existing.

    async fn cleanup_daily_coverage_stats(pool: &PgPool, line_id: &str) {
        sqlx::query("DELETE FROM line_status_daily_coverage_stats WHERE line_id = $1")
            .bind(line_id)
            .execute(pool)
            .await
            .expect("cleanup line_status_daily_coverage_stats");
    }

    async fn cleanup_half_hourly_coverage_stats(pool: &PgPool, line_id: &str) {
        sqlx::query("DELETE FROM line_status_half_hourly_coverage_stats WHERE line_id = $1")
            .bind(line_id)
            .execute(pool)
            .await
            .expect("cleanup line_status_half_hourly_coverage_stats");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_daily_coverage_stats_accumulates_across_a_day -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn record_daily_coverage_stats_accumulates_across_a_day() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-DAILY-COVERAGE-STATS-ACCUMULATE";
        let day = NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();

        cleanup_daily_coverage_stats(&pool, LINE_ID).await;

        let cycle1 = common::SampleStats {
            total: 4,
            delayed: 1,
            cancelled: 1,
            skipped: 0,
            avg_delay_minutes: 6.0,
        };
        let cycle2 = common::SampleStats {
            total: 2,
            delayed: 2,
            cancelled: 0,
            skipped: 1,
            avg_delay_minutes: 12.0,
        };

        record_daily_coverage_stats(&pool, LINE_ID, day, Some(&cycle1))
            .await
            .expect("record cycle 1");
        record_daily_coverage_stats(&pool, LINE_ID, day, Some(&cycle2))
            .await
            .expect("record cycle 2");

        let row = sqlx::query(
            "SELECT resolved_windows, total, delayed, cancelled, skipped, running_count, delay_minutes_sum \
             FROM line_status_daily_coverage_stats WHERE line_id = $1 AND day = $2",
        )
        .bind(LINE_ID)
        .bind(day)
        .fetch_one(&pool)
        .await
        .expect("read accumulated row");

        cleanup_daily_coverage_stats(&pool, LINE_ID).await;

        let resolved_windows: i64 = row.try_get("resolved_windows").unwrap();
        let total: i64 = row.try_get("total").unwrap();
        let running_count: i64 = row.try_get("running_count").unwrap();
        let delay_minutes_sum: f64 = row.try_get("delay_minutes_sum").unwrap();

        assert_eq!(resolved_windows, 2);
        assert_eq!(total, 6);
        assert_eq!(running_count, 5);
        assert!((delay_minutes_sum - 42.0).abs() < 1e-9);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_daily_coverage_stats_none_still_counts_the_cycle_but_adds_nothing -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    #[expect(clippy::float_cmp, reason = "a sum of no samples is exactly 0.0")]
    async fn record_daily_coverage_stats_none_still_counts_the_cycle_but_adds_nothing() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-DAILY-COVERAGE-STATS-NONE";
        let day = NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();

        cleanup_daily_coverage_stats(&pool, LINE_ID).await;

        record_daily_coverage_stats(&pool, LINE_ID, day, None)
            .await
            .expect("record a None cycle");
        record_daily_coverage_stats(&pool, LINE_ID, day, None)
            .await
            .expect("record a second None cycle");

        let row = sqlx::query(
            "SELECT resolved_windows, total, delay_minutes_sum \
             FROM line_status_daily_coverage_stats WHERE line_id = $1 AND day = $2",
        )
        .bind(LINE_ID)
        .bind(day)
        .fetch_one(&pool)
        .await
        .expect("read row");

        cleanup_daily_coverage_stats(&pool, LINE_ID).await;

        let resolved_windows: i64 = row.try_get("resolved_windows").unwrap();
        let total: i64 = row.try_get("total").unwrap();
        let delay_minutes_sum: f64 = row.try_get("delay_minutes_sum").unwrap();

        assert_eq!(resolved_windows, 2, "None still counts as a covered cycle");
        assert_eq!(total, 0, "None must contribute zero to every sum column");
        assert_eq!(delay_minutes_sum, 0.0);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_daily_coverage_stats_a_new_day_starts_a_fresh_row -- --ignored --test-threads=1` against \
                docker compose's postgres"]
    async fn record_daily_coverage_stats_a_new_day_starts_a_fresh_row() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-DAILY-COVERAGE-STATS-NEW-DAY";
        let day1 = NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2026, 9, 4).unwrap();

        cleanup_daily_coverage_stats(&pool, LINE_ID).await;

        let stats = common::SampleStats {
            total: 5,
            delayed: 1,
            cancelled: 0,
            skipped: 0,
            avg_delay_minutes: 3.0,
        };
        record_daily_coverage_stats(&pool, LINE_ID, day1, Some(&stats))
            .await
            .expect("record day 1");
        record_daily_coverage_stats(&pool, LINE_ID, day2, Some(&stats))
            .await
            .expect("record day 2");

        let day2_windows: i64 = sqlx::query_scalar(
            "SELECT resolved_windows FROM line_status_daily_coverage_stats WHERE line_id = $1 AND day = $2",
        )
        .bind(LINE_ID)
        .bind(day2)
        .fetch_one(&pool)
        .await
        .expect("read day 2 row");

        cleanup_daily_coverage_stats(&pool, LINE_ID).await;

        assert_eq!(
            day2_windows, 1,
            "a new day must start its own fresh row, not accumulate into day 1's"
        );
    }

    /// The per-day `full_coverage_line_stats` history is pruned by
    /// `service_date`, keeping every day inside the window -- including
    /// OTHER days of the same line.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_full_coverage_line_stats -- --ignored --test-threads=1`"]
    async fn prune_full_coverage_line_stats_deletes_only_days_older_than_the_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-FC-LINE-STATS-PRUNE";
        const RETENTION_DAYS: i64 = 30;
        let cleanup = || async {
            sqlx::query("DELETE FROM full_coverage_line_stats WHERE line_id = $1")
                .bind(LINE_ID)
                .execute(&pool)
                .await
                .expect("cleanup");
        };
        cleanup().await;

        sqlx::query(
            "INSERT INTO full_coverage_line_stats (line_id, service_date, availability) VALUES \
                ($1, CURRENT_DATE - ($2::int + 1), 'available'), \
                ($1, CURRENT_DATE - ($2::int - 1), 'available'), \
                ($1, CURRENT_DATE, 'pending')",
        )
        .bind(LINE_ID)
        .bind(RETENTION_DAYS as i32)
        .execute(&pool)
        .await
        .expect("seed three days of one line");

        prune_full_coverage_line_stats(&pool, RETENTION_DAYS)
            .await
            .expect("prune_full_coverage_line_stats");

        let survivors: Vec<NaiveDate> = sqlx::query_scalar(
            "SELECT service_date FROM full_coverage_line_stats WHERE line_id = $1 ORDER BY service_date",
        )
        .bind(LINE_ID)
        .fetch_all(&pool)
        .await
        .expect("survivors");
        cleanup().await;

        let today: NaiveDate = sqlx::query_scalar("SELECT CURRENT_DATE")
            .fetch_one(&pool)
            .await
            .expect("the database's own date, which the prune compares against");
        assert_eq!(
            survivors.len(),
            2,
            "only the day past the window is pruned: {survivors:?}"
        );
        assert_eq!(*survivors.last().unwrap(), today);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_daily_coverage_stats_deletes_only_rows_older_than_the_retention_window -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn prune_daily_coverage_stats_deletes_only_rows_older_than_the_retention_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const OLD_LINE_ID: &str = "TEST-DAILY-COVERAGE-STATS-PRUNE-OLD";
        const RECENT_LINE_ID: &str = "TEST-DAILY-COVERAGE-STATS-PRUNE-RECENT";
        const RETENTION_DAYS: i64 = 30;

        cleanup_daily_coverage_stats(&pool, OLD_LINE_ID).await;
        cleanup_daily_coverage_stats(&pool, RECENT_LINE_ID).await;

        sqlx::query(
            "INSERT INTO line_status_daily_coverage_stats (line_id, day, resolved_windows, total) VALUES \
                ($1, CURRENT_DATE - ($3::int + 1), 1, 1), \
                ($2, CURRENT_DATE - ($3::int - 1), 1, 1)",
        )
        .bind(OLD_LINE_ID)
        .bind(RECENT_LINE_ID)
        .bind(RETENTION_DAYS as i32)
        .execute(&pool)
        .await
        .expect("seed old and recent rows");

        prune_daily_coverage_stats(&pool, RETENTION_DAYS)
            .await
            .expect("prune_daily_coverage_stats");

        let old_survives: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM line_status_daily_coverage_stats WHERE line_id = $1",
        )
        .bind(OLD_LINE_ID)
        .fetch_one(&pool)
        .await
        .expect("count old survivors");
        let recent_survives: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM line_status_daily_coverage_stats WHERE line_id = $1",
        )
        .bind(RECENT_LINE_ID)
        .fetch_one(&pool)
        .await
        .expect("count recent survivors");

        cleanup_daily_coverage_stats(&pool, OLD_LINE_ID).await;
        cleanup_daily_coverage_stats(&pool, RECENT_LINE_ID).await;

        assert_eq!(old_survives, 0);
        assert_eq!(recent_survives, 1);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                record_half_hourly_coverage_stats_accumulates_within_a_bucket -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn record_half_hourly_coverage_stats_accumulates_within_a_bucket() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const LINE_ID: &str = "TEST-HALF-HOURLY-COVERAGE-STATS-ACCUMULATE";
        let bucket: DateTime<Utc> = "2026-09-03T14:00:00Z".parse().unwrap();

        cleanup_half_hourly_coverage_stats(&pool, LINE_ID).await;

        let cycle1 = common::SampleStats {
            total: 4,
            delayed: 1,
            cancelled: 1,
            skipped: 0,
            avg_delay_minutes: 6.0,
        };
        let cycle2 = common::SampleStats {
            total: 2,
            delayed: 2,
            cancelled: 0,
            skipped: 1,
            avg_delay_minutes: 12.0,
        };

        record_half_hourly_coverage_stats(&pool, LINE_ID, bucket, Some(&cycle1))
            .await
            .expect("record cycle 1");
        record_half_hourly_coverage_stats(&pool, LINE_ID, bucket, Some(&cycle2))
            .await
            .expect("record cycle 2");

        let row = sqlx::query(
            "SELECT resolved_windows, total, running_count, delay_minutes_sum \
             FROM line_status_half_hourly_coverage_stats WHERE line_id = $1 AND half_hour_start = $2",
        )
        .bind(LINE_ID)
        .bind(bucket)
        .fetch_one(&pool)
        .await
        .expect("read accumulated row");

        cleanup_half_hourly_coverage_stats(&pool, LINE_ID).await;

        let resolved_windows: i64 = row.try_get("resolved_windows").unwrap();
        let total: i64 = row.try_get("total").unwrap();
        let running_count: i64 = row.try_get("running_count").unwrap();
        let delay_minutes_sum: f64 = row.try_get("delay_minutes_sum").unwrap();

        assert_eq!(resolved_windows, 2);
        assert_eq!(total, 6);
        assert_eq!(running_count, 5);
        assert!((delay_minutes_sum - 42.0).abs() < 1e-9);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_half_hourly_coverage_stats_deletes_only_rows_older_than_the_retention_window -- --ignored --test-threads=1` \
                against docker compose's postgres"]
    async fn prune_half_hourly_coverage_stats_deletes_only_rows_older_than_the_retention_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");
        const OLD_LINE_ID: &str = "TEST-HALF-HOURLY-COVERAGE-STATS-PRUNE-OLD";
        const RECENT_LINE_ID: &str = "TEST-HALF-HOURLY-COVERAGE-STATS-PRUNE-RECENT";
        const RETENTION_HOURS: i64 = 48;

        cleanup_half_hourly_coverage_stats(&pool, OLD_LINE_ID).await;
        cleanup_half_hourly_coverage_stats(&pool, RECENT_LINE_ID).await;

        sqlx::query(
            "INSERT INTO line_status_half_hourly_coverage_stats (line_id, half_hour_start, resolved_windows, total) VALUES \
                ($1, NOW() - (($3 + 1) || ' hours')::interval, 1, 1), \
                ($2, NOW() - (($3 - 1) || ' hours')::interval, 1, 1)",
        )
        .bind(OLD_LINE_ID)
        .bind(RECENT_LINE_ID)
        .bind(RETENTION_HOURS)
        .execute(&pool)
        .await
        .expect("seed old and recent rows");

        prune_half_hourly_coverage_stats(&pool, RETENTION_HOURS)
            .await
            .expect("prune_half_hourly_coverage_stats");

        let old_survives: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM line_status_half_hourly_coverage_stats WHERE line_id = $1",
        )
        .bind(OLD_LINE_ID)
        .fetch_one(&pool)
        .await
        .expect("count old survivors");
        let recent_survives: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM line_status_half_hourly_coverage_stats WHERE line_id = $1",
        )
        .bind(RECENT_LINE_ID)
        .fetch_one(&pool)
        .await
        .expect("count recent survivors");

        cleanup_half_hourly_coverage_stats(&pool, OLD_LINE_ID).await;
        cleanup_half_hourly_coverage_stats(&pool, RECENT_LINE_ID).await;

        assert_eq!(old_survives, 0);
        assert_eq!(recent_survives, 1);
    }

    /// Task 14 of docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md
    /// -- proves `load_full_coverage_line_stats`'s staleness/availability
    /// filtering end to end against a real Postgres. Mirrors this file's
    /// own `load_incidents_excludes_cleared_rows`'s seed/assert/delete
    /// shape (a real, same-crate precedent for this class of test -- the
    /// plan's own note that `station_stats.rs::db_tests` was "the only
    /// real precedent in this repo" for this was itself checked here and
    /// found not to hold; this crate already had several).
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                full_coverage -- --ignored --test-threads=1` against docker compose's postgres"]
    async fn load_full_coverage_line_stats_filters_by_availability_and_service_date() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        const AVAILABLE_TODAY: &str = "TEST-FC-AVAILABLE-TODAY";
        const PENDING_TODAY: &str = "TEST-FC-PENDING-TODAY";
        const AVAILABLE_YESTERDAY: &str = "TEST-FC-AVAILABLE-YESTERDAY";
        let today = Utc::now().date_naive();
        let yesterday = today - chrono::Duration::days(1);

        for id in [AVAILABLE_TODAY, PENDING_TODAY, AVAILABLE_YESTERDAY] {
            sqlx::query("DELETE FROM full_coverage_line_stats WHERE line_id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .expect("cleanup any leftover fixture row");
        }

        sqlx::query(
            "INSERT INTO full_coverage_line_stats \
                (line_id, service_date, availability, total, delayed, cancelled, skipped, avg_delay_minutes) \
             VALUES \
                ($1, $4, 'available', 10, 2, 1, 0, 3.5), \
                ($2, $4, 'pending', 10, 2, 1, 0, 3.5), \
                ($3, $5, 'available', 10, 2, 1, 0, 3.5)",
        )
        .bind(AVAILABLE_TODAY)
        .bind(PENDING_TODAY)
        .bind(AVAILABLE_YESTERDAY)
        .bind(today)
        .bind(yesterday)
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        let loaded = load_full_coverage_line_stats(&pool, today)
            .await
            .expect("load_full_coverage_line_stats");

        for id in [AVAILABLE_TODAY, PENDING_TODAY, AVAILABLE_YESTERDAY] {
            sqlx::query("DELETE FROM full_coverage_line_stats WHERE line_id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .expect("cleanup fixture rows");
        }

        assert!(
            loaded.contains_key(AVAILABLE_TODAY),
            "an available, current-day row must come back"
        );
        assert!(
            !loaded.contains_key(PENDING_TODAY),
            "a pending row must be excluded, even for today"
        );
        assert!(
            !loaded.contains_key(AVAILABLE_YESTERDAY),
            "a stale (yesterday's) row must be excluded, even if available -- the staleness guard"
        );
        assert_eq!(loaded[AVAILABLE_TODAY].total, 10);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trust_event_backlog -- --ignored --test-threads=1`"]
    async fn prune_trust_event_backlog_deletes_only_rows_older_than_the_retention_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        sqlx::query(
            "INSERT INTO trust_event_backlog (train_id, service_date, msg_type, received_at, dedup_key) \
             VALUES ('TEST-PRUNE-OLD', '2026-09-01', '0001', NOW() - interval '2 days', 'test-prune-old'), \
                    ('TEST-PRUNE-NEW', '2026-09-05', '0001', NOW(), 'test-prune-new')",
        )
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        let pruned = prune_trust_event_backlog(&pool, 1).await.expect("prune");
        assert_eq!(
            pruned, 1,
            "only the 2-day-old row should be pruned at a 1-day retention"
        );

        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM trust_event_backlog WHERE train_id = 'TEST-PRUNE-NEW'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(remaining.0, 1);

        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id IN ('TEST-PRUNE-OLD', 'TEST-PRUNE-NEW')")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_schedule_destination_departures -- --ignored --test-threads=1`"]
    async fn prune_schedule_destination_departures_deletes_only_rows_older_than_the_retention_window()
     {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        // Relative to CURRENT_DATE, not hardcoded: the predicate is
        // `service_date < CURRENT_DATE - $1`, so a fixed date would flip
        // this test's meaning as the calendar moved.
        let today = Utc::now().date_naive();
        let stale = today - chrono::Duration::days(5);
        let fresh = today - chrono::Duration::days(1);

        for (service_date, train_uid) in [(stale, "TEST-PRUNE-OLD"), (fresh, "TEST-PRUNE-NEW")] {
            sqlx::query(
                "INSERT INTO schedule_destination_departures \
                    (service_date, destination_crs, scheduled, train_uid, origin_crs) \
                 VALUES ($1, 'ZRB', '08:00:00', $2, 'EUS')",
            )
            .bind(service_date)
            .bind(train_uid)
            .execute(&pool)
            .await
            .expect("seed fixture rows");
        }

        let pruned = prune_schedule_destination_departures(&pool, 2)
            .await
            .expect("prune");
        assert_eq!(
            pruned, 1,
            "only the 5-day-old row should be pruned at a 2-day retention"
        );

        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM schedule_destination_departures \
             WHERE train_uid = 'TEST-PRUNE-NEW'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining.0, 1,
            "yesterday's rows are inside a 2-day window and must survive"
        );

        sqlx::query(
            "DELETE FROM schedule_destination_departures \
             WHERE train_uid IN ('TEST-PRUNE-OLD', 'TEST-PRUNE-NEW')",
        )
        .execute(&pool)
        .await
        .ok();
    }

    /// The three products that had NO pruning job at all until 2026-09-25 --
    /// `schedule_calling_points_full`, `schedule_network_departures`,
    /// `schedule_line_population`. One parameterised test per table rather than
    /// one shared loop, so a failure names the table.
    ///
    /// Both properties the existing `schedule_destination_departures` tests
    /// check are checked here for each table, because both are the ones that
    /// would actually hurt: a stale `service_date` really is deleted (otherwise
    /// the table grows forever, which is the bug), and today's rows survive
    /// even at retention 0 (otherwise a prune blanks a live product between one
    /// CIF delivery and the next).
    ///
    /// Dates are relative to `CURRENT_DATE`, never hardcoded -- the predicate
    /// is `service_date < CURRENT_DATE - $1`, so a fixed date would flip these
    /// tests' meaning as the calendar moved.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_schedule_derived -- --ignored --test-threads=1`"]
    async fn prune_schedule_derived_services_prunes_stale_dates_and_keeps_today() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        let today = Utc::now().date_naive();
        for (service_date, uid) in [
            (today - chrono::Duration::days(5), "TPSS-OLD"),
            (today - chrono::Duration::days(1), "TPSS-NEW"),
            (today, "TPSS-TODAY"),
        ] {
            sqlx::query(
                "INSERT INTO schedule_services (service_date, uid, mode, stp) \
                 VALUES ($1, $2, 'bus', 'P')",
            )
            .bind(service_date)
            .bind(uid)
            .execute(&pool)
            .await
            .expect("seed fixture rows");
        }

        let pruned = prune_schedule_services(&pool, 2).await.expect("prune");
        assert_eq!(pruned, 1, "only the 5-day-old row is pruned at 2 days");
        prune_schedule_services(&pool, 0).await.expect("prune at 0");
        let remaining: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM schedule_services WHERE uid = 'TPSS-TODAY'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(remaining.0, 1, "today's rows survive any retention value");

        sqlx::query("DELETE FROM schedule_services WHERE uid LIKE 'TPSS-%'")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_schedule_derived -- --ignored --test-threads=1`"]
    async fn prune_schedule_derived_calling_points_full_prunes_stale_dates_and_keeps_today() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        let today = Utc::now().date_naive();
        let stale = today - chrono::Duration::days(5);
        let fresh = today - chrono::Duration::days(1);

        for (service_date, uid) in [
            (stale, "TEST-PRUNE-CPF-OLD"),
            (fresh, "TEST-PRUNE-CPF-NEW"),
            (today, "TEST-PRUNE-CPF-TODAY"),
        ] {
            sqlx::query(
                "INSERT INTO schedule_calling_points_full \
                    (service_date, uid, seq, tiploc, kind, booked_departure, day_offset) \
                 VALUES ($1, $2, 0, 'EUSTON', 'origin', '08:00:00', 0)",
            )
            .bind(service_date)
            .bind(uid)
            .execute(&pool)
            .await
            .expect("seed fixture rows");
        }

        let pruned = prune_schedule_calling_points_full(&pool, 2)
            .await
            .expect("prune");
        assert_eq!(
            pruned, 1,
            "only the 5-day-old row should be pruned at a 2-day retention"
        );

        // Even at the most aggressive value, today survives.
        prune_schedule_calling_points_full(&pool, 0)
            .await
            .expect("prune at 0");
        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM schedule_calling_points_full WHERE uid = 'TEST-PRUNE-CPF-TODAY'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining.0, 1,
            "today's rows must survive any retention value -- the predicate is strictly `<`"
        );

        sqlx::query("DELETE FROM schedule_calling_points_full WHERE uid LIKE 'TEST-PRUNE-CPF-%'")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_schedule_derived -- --ignored --test-threads=1`"]
    async fn prune_schedule_derived_network_departures_prunes_stale_dates_and_keeps_today() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        let today = Utc::now().date_naive();
        let stale = today - chrono::Duration::days(5);
        let fresh = today - chrono::Duration::days(1);

        for (service_date, crs) in [(stale, "ZQ1"), (fresh, "ZQ2"), (today, "ZQ3")] {
            sqlx::query(
                "INSERT INTO schedule_network_departures (crs, service_date, departures) \
                 VALUES ($1, $2, '[]'::jsonb)",
            )
            .bind(crs)
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("seed fixture rows");
        }

        let pruned = prune_schedule_network_departures(&pool, 2)
            .await
            .expect("prune");
        assert_eq!(pruned, 1);

        prune_schedule_network_departures(&pool, 0)
            .await
            .expect("prune at 0");
        let remaining: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM schedule_network_departures WHERE crs = 'ZQ3'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            remaining.0, 1,
            "today's rows must survive any retention value -- the predicate is strictly `<`"
        );

        sqlx::query("DELETE FROM schedule_network_departures WHERE crs IN ('ZQ1','ZQ2','ZQ3')")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_schedule_derived -- --ignored --test-threads=1`"]
    async fn prune_schedule_derived_line_population_prunes_stale_dates_and_keeps_today() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        let today = Utc::now().date_naive();
        let stale = today - chrono::Duration::days(5);
        let fresh = today - chrono::Duration::days(1);

        for (service_date, line_id) in [
            (stale, "test-prune-pop-old"),
            (fresh, "test-prune-pop-new"),
            (today, "test-prune-pop-today"),
        ] {
            sqlx::query(
                "INSERT INTO schedule_line_population (line_id, service_date, population) \
                 VALUES ($1, $2, '[]'::jsonb)",
            )
            .bind(line_id)
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("seed fixture rows");
        }

        let pruned = prune_schedule_line_population(&pool, 2)
            .await
            .expect("prune");
        assert_eq!(pruned, 1);

        prune_schedule_line_population(&pool, 0)
            .await
            .expect("prune at 0");
        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM schedule_line_population WHERE line_id = 'test-prune-pop-today'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining.0, 1,
            "today's rows must survive any retention value -- the predicate is strictly `<`"
        );

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id LIKE 'test-prune-pop-%'")
            .execute(&pool)
            .await
            .ok();
    }

    /// `line_train_summaries` follows `schedule_line_population`'s
    /// retention: stale dates go, today's rows stay at any retention.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_line_train_summaries -- --ignored --test-threads=1`"]
    async fn prune_line_train_summaries_prunes_stale_dates_and_keeps_today() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        sqlx::query("DELETE FROM line_train_summaries WHERE line_id = 'test-prune-lts'")
            .execute(&pool)
            .await
            .expect("clean fixture rows");

        let today = Utc::now().date_naive();
        for (service_date, uid) in [
            (today - chrono::Duration::days(5), "TLTS-OLD1"),
            (today - chrono::Duration::days(5), "TLTS-OLD2"),
            (today - chrono::Duration::days(1), "TLTS-NEW"),
            (today, "TLTS-TODAY"),
        ] {
            sqlx::query(
                "INSERT INTO line_train_summaries \
                     (line_id, service_date, uid, has_scope, derivation) \
                 VALUES ('test-prune-lts', $1, $2, true, 'x')",
            )
            .bind(service_date)
            .bind(uid)
            .execute(&pool)
            .await
            .expect("seed fixture rows");
        }

        // At least this fixture's two stale rows (the shared database may
        // hold other tests' old rows too).
        let pruned = prune_line_train_summaries(&pool, 2).await.expect("prune");
        assert!(pruned >= 2, "{pruned}");
        let after_two: Vec<(String,)> = sqlx::query_as(
            "SELECT uid FROM line_train_summaries WHERE line_id = 'test-prune-lts' ORDER BY uid",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            after_two,
            [("TLTS-NEW".to_string(),), ("TLTS-TODAY".to_string(),)]
        );
        prune_line_train_summaries(&pool, 0)
            .await
            .expect("prune at 0");
        let remaining: Vec<(String,)> = sqlx::query_as(
            "SELECT uid FROM line_train_summaries WHERE line_id = 'test-prune-lts' ORDER BY uid",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, [("TLTS-TODAY".to_string(),)]);

        sqlx::query("DELETE FROM line_train_summaries WHERE line_id = 'test-prune-lts'")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_schedule_destination_departures -- --ignored --test-threads=1`"]
    async fn prune_schedule_destination_departures_never_deletes_todays_rows() {
        // The discriminating case, and the one that would actually hurt: a
        // retention window is only ever allowed to reach into the PAST.
        // Deleting today's rows would blank the live search between one CIF
        // delivery and the next, which no retention value should ever be
        // able to do.
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        let today = Utc::now().date_naive();
        sqlx::query(
            "INSERT INTO schedule_destination_departures \
                (service_date, destination_crs, scheduled, train_uid, origin_crs) \
             VALUES ($1, 'ZRB', '08:00:00', 'TEST-PRUNE-TODAY', 'EUS')",
        )
        .bind(today)
        .execute(&pool)
        .await
        .expect("seed today's fixture row");

        // Even at the most aggressive value this config field allows.
        prune_schedule_destination_departures(&pool, 0)
            .await
            .expect("prune");

        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM schedule_destination_departures \
             WHERE train_uid = 'TEST-PRUNE-TODAY'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining.0, 1,
            "today's rows must survive any retention value -- the predicate is strictly \
             `service_date < CURRENT_DATE - $1`, never `<=`"
        );

        sqlx::query(
            "DELETE FROM schedule_destination_departures WHERE train_uid = 'TEST-PRUNE-TODAY'",
        )
        .execute(&pool)
        .await
        .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trains_deletes_only_rows_older_than_the_retention_window -- --ignored --test-threads=1`"]
    async fn prune_trains_deletes_only_rows_older_than_the_retention_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        let old_date: NaiveDate = Utc::now().date_naive() - chrono::Duration::days(40);
        let recent_date: NaiveDate = Utc::now().date_naive() - chrono::Duration::days(5);

        let (old_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-PRUNE-TRAINS-OLD', $1) RETURNING id",
        )
        .bind(old_date)
        .fetch_one(&pool)
        .await
        .expect("seed an old trains row");
        let (recent_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-PRUNE-TRAINS-RECENT', $1) RETURNING id",
        )
        .bind(recent_date)
        .fetch_one(&pool)
        .await
        .expect("seed a recent trains row");

        let pruned = prune_trains(&pool, 30, 30).await.expect("prune_trains");
        assert!(pruned >= 1);

        let old_still_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM trains WHERE id = $1)")
                .bind(old_id)
                .fetch_one(&pool)
                .await
                .expect("check old row");
        assert!(
            !old_still_exists,
            "a 40-day-old trains row must be pruned under a 30-day retention window"
        );

        let recent_still_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM trains WHERE id = $1)")
                .bind(recent_id)
                .fetch_one(&pool)
                .await
                .expect("check recent row");
        assert!(
            recent_still_exists,
            "a 5-day-old trains row must survive a 30-day retention window"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(recent_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trains_cascade_delete_to_child_tables -- --ignored --test-threads=1`"]
    async fn prune_trains_cascade_delete_to_child_tables() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        let old_date: NaiveDate = Utc::now().date_naive() - chrono::Duration::days(40);

        let (old_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-CASCADE-DELETE-OLD', $1) RETURNING id",
        )
        .bind(old_date)
        .fetch_one(&pool)
        .await
        .expect("seed an old trains row");

        sqlx::query(
            "INSERT INTO train_movement_events (trains_id, event_type, msg_type, dedup_key, raw_body) \
             VALUES ($1, 'departure', '0001', 'test-cascade-' || $1, '{}'::jsonb)",
        )
        .bind(old_train_id)
        .execute(&pool)
        .await
        .expect("insert train_movement_events row");

        sqlx::query(
            "INSERT INTO train_current_state (trains_id, status) \
             VALUES ($1, 'en_route')",
        )
        .bind(old_train_id)
        .execute(&pool)
        .await
        .expect("insert train_current_state row");

        let events_before: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(old_train_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            events_before.0, 1,
            "should have 1 movement event before prune"
        );

        let current_state_before: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM train_current_state WHERE trains_id = $1")
                .bind(old_train_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            current_state_before.0, 1,
            "should have 1 current state row before prune"
        );

        let pruned = prune_trains(&pool, 30, 30).await.expect("prune_trains");
        assert!(pruned >= 1, "should have pruned at least one train");

        let train_still_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM trains WHERE id = $1)")
                .bind(old_train_id)
                .fetch_one(&pool)
                .await
                .expect("check train row");
        assert!(!train_still_exists, "old train should be deleted");

        let events_after: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(old_train_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            events_after.0, 0,
            "movement events should be cascade-deleted when train is pruned"
        );

        let current_state_after: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM train_current_state WHERE trains_id = $1")
                .bind(old_train_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            current_state_after.0, 0,
            "current state rows should be cascade-deleted when train is pruned"
        );
    }

    /// A `tracing_subscriber::Layer` counting `sqlx`'s own per-query
    /// tracing events (target `"sqlx::query"`, emitted once per executed
    /// statement -- see `sqlx-core`'s `logger.rs`). Used below to prove
    /// `prune_trains` actually issues MULTIPLE DELETE round trips for a
    /// row count exceeding one batch, rather than the one unbounded
    /// statement it used to be -- a plain "did every row get deleted"
    /// assertion can't distinguish the two, since an unbounded DELETE
    /// deletes everything in a single statement too.
    struct SqlxQueryCounter(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for SqlxQueryCounter {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() == "sqlx::query" {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trains_deletes_all_rows_across_multiple_batches -- --ignored --test-threads=1`"]
    async fn prune_trains_deletes_all_rows_across_multiple_batches() {
        use tracing_subscriber::layer::SubscriberExt;

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        let old_date: NaiveDate = Utc::now().date_naive() - chrono::Duration::days(40);
        const SEEDED_ROWS: i64 = 1500; // > the batch size any sane batched impl would use

        sqlx::query("DELETE FROM trains WHERE train_uid LIKE 'TEST-PRUNE-BATCH-%'")
            .execute(&pool)
            .await
            .ok(); // defensive: clear any leftovers from a previously-aborted run

        let seeded_ids: Vec<i64> = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date) \
             SELECT 'TEST-PRUNE-BATCH-' || gs, $1 FROM generate_series(1, $2) AS gs \
             RETURNING id",
        )
        .bind(old_date)
        .bind(SEEDED_ROWS)
        .fetch_all(&pool)
        .await
        .expect("bulk-seed trains rows older than the retention window");
        assert_eq!(seeded_ids.len() as i64, SEEDED_ROWS);

        // A couple of children on rows scattered across the batch boundary,
        // to confirm cascade delete still works when the parent is removed
        // by a LATER batch iteration, not just the first.
        for &trains_id in &[
            seeded_ids[0],
            seeded_ids[seeded_ids.len() / 2],
            *seeded_ids.last().unwrap(),
        ] {
            sqlx::query(
                "INSERT INTO train_movement_events (trains_id, event_type, msg_type, dedup_key, raw_body) \
                 VALUES ($1, 'departure', '0001', 'test-prune-batch-' || $1, '{}'::jsonb)",
            )
            .bind(trains_id)
            .execute(&pool)
            .await
            .expect("seed a movement event on a batch row");
            sqlx::query(
                "INSERT INTO train_current_state (trains_id, status) VALUES ($1, 'en_route')",
            )
            .bind(trains_id)
            .execute(&pool)
            .await
            .expect("seed a current-state row on a batch row");
        }

        let query_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(SqlxQueryCounter(query_count.clone()));
        let guard = tracing::subscriber::set_default(subscriber);
        let pruned = prune_trains(&pool, 30, 30).await.expect("prune_trains");
        drop(guard);

        assert!(
            pruned >= SEEDED_ROWS as u64,
            "must delete every seeded row despite batching, got {pruned}"
        );
        assert!(
            query_count.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "1500 rows must not fit in a single DELETE round trip once batched -- \
             only {} query event(s) were observed, meaning this is still one unbounded statement",
            query_count.load(std::sync::atomic::Ordering::SeqCst)
        );

        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM trains WHERE train_uid LIKE 'TEST-PRUNE-BATCH-%'",
        )
        .fetch_one(&pool)
        .await
        .expect("count remaining batch rows");
        assert_eq!(remaining, 0, "every batch-seeded row must be gone");

        let remaining_events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM train_movement_events WHERE dedup_key LIKE 'test-prune-batch-%'",
        )
        .fetch_one(&pool)
        .await
        .expect("count remaining movement events");
        assert_eq!(
            remaining_events, 0,
            "cascade delete must still remove child movement events across batches"
        );

        let remaining_state: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM train_current_state WHERE trains_id = ANY($1)",
        )
        .bind(&seeded_ids)
        .fetch_one(&pool)
        .await
        .expect("count remaining current-state rows");
        assert_eq!(
            remaining_state, 0,
            "cascade delete must still remove child current-state rows across batches"
        );
    }

    /// The new two-tier behavior's headline case: an UNTRACKED train (no
    /// `train_subscriptions` row references it) older than the 14-day
    /// untracked tier but younger than the 30-day tracked tier IS pruned.
    /// Under the old single-tier logic (`retention_days` alone, always
    /// 30) this row would have survived -- proving this test actually
    /// exercises the new shorter window, not just a renamed old one.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trains_prunes_an_untracked_train_past_only_the_untracked_window \
                -- --ignored --test-threads=1`"]
    async fn prune_trains_prunes_an_untracked_train_past_only_the_untracked_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        // 20 days: older than the 14-day untracked tier, younger than the
        // 30-day tracked tier.
        let mid_date: NaiveDate = Utc::now().date_naive() - chrono::Duration::days(20);

        let (untracked_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-PRUNE-TIER-UNTRACKED', $1) \
             RETURNING id",
        )
        .bind(mid_date)
        .fetch_one(&pool)
        .await
        .expect("seed an untracked trains row");

        let pruned = prune_trains(&pool, 30, 14).await.expect("prune_trains");
        assert!(pruned >= 1);

        let still_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM trains WHERE id = $1)")
                .bind(untracked_id)
                .fetch_one(&pool)
                .await
                .expect("check untracked row");
        assert!(
            !still_exists,
            "a 20-day-old UNTRACKED trains row must be pruned under a 14-day untracked \
             retention window, even though it's within the 30-day tracked window"
        );
    }

    /// The tiering's protective case: a TRACKED train (has a
    /// `train_subscriptions` row pointing at it) older than the 14-day
    /// untracked tier but younger than the 30-day tracked tier is NOT
    /// pruned. This is the test that would fail if `prune_trains` were
    /// implemented as a naive single blanket 14-day cutover instead of
    /// true two-tier logic -- it proves a real user's tracked journey
    /// survives on the longer, existing `trains_retention_days` window.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trains_keeps_a_tracked_train_past_the_untracked_window \
                -- --ignored --test-threads=1`"]
    async fn prune_trains_keeps_a_tracked_train_past_the_untracked_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        // 20 days: older than the 14-day untracked tier, younger than the
        // 30-day tracked tier.
        let mid_date: NaiveDate = Utc::now().date_naive() - chrono::Duration::days(20);
        let user_id = "TEST-PRUNE-TIER-TRACKED-USER";

        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("prune-tier-tracked@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let (tracked_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-PRUNE-TIER-TRACKED', $1) \
             RETURNING id",
        )
        .bind(mid_date)
        .fetch_one(&pool)
        .await
        .expect("seed a trains row");

        sqlx::query(
            "INSERT INTO train_subscriptions \
                 (user_id, trains_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, 'EUS', NOW())",
        )
        .bind(user_id)
        .bind(tracked_id)
        .bind(mid_date)
        .execute(&pool)
        .await
        .expect("seed a subscription referencing the trains row");

        prune_trains(&pool, 30, 14).await.expect("prune_trains");

        let still_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM trains WHERE id = $1)")
                .bind(tracked_id)
                .fetch_one(&pool)
                .await
                .expect("check tracked row");
        assert!(
            still_exists,
            "a 20-day-old TRACKED trains row must survive the 14-day untracked window -- \
             it has a train_subscriptions row, so trains_retention_days (30) applies instead"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(tracked_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// The existing tier's survival case: a TRACKED train older than the
    /// 30-day tracked tier IS still pruned -- proves `trains_retention_days`'s
    /// existing behavior for subscribed trains is unchanged by adding the
    /// new untracked tier alongside it.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trains_still_prunes_a_tracked_train_past_the_tracked_window \
                -- --ignored --test-threads=1`"]
    async fn prune_trains_still_prunes_a_tracked_train_past_the_tracked_window() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        let old_date: NaiveDate = Utc::now().date_naive() - chrono::Duration::days(40);
        let user_id = "TEST-PRUNE-TIER-TRACKED-OLD-USER";

        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("prune-tier-tracked-old@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let (tracked_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TEST-PRUNE-TIER-TRACKED-OLD', $1) \
             RETURNING id",
        )
        .bind(old_date)
        .fetch_one(&pool)
        .await
        .expect("seed a trains row");

        sqlx::query(
            "INSERT INTO train_subscriptions \
                 (user_id, trains_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, 'EUS', NOW())",
        )
        .bind(user_id)
        .bind(tracked_id)
        .bind(old_date)
        .execute(&pool)
        .await
        .expect("seed a subscription referencing the trains row");

        let pruned = prune_trains(&pool, 30, 14).await.expect("prune_trains");
        assert!(pruned >= 1);

        let still_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM trains WHERE id = $1)")
                .bind(tracked_id)
                .fetch_one(&pool)
                .await
                .expect("check tracked row");
        assert!(
            !still_exists,
            "a 40-day-old TRACKED trains row must still be pruned under the existing \
             30-day trains_retention_days window"
        );

        // The train row (and its cascade-deleted children, if any) are
        // already gone; the subscription row itself survives with
        // trains_id set NULL (ON DELETE SET NULL) rather than being
        // deleted, so only the fixture user needs explicit cleanup here.
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    // ---- Retention prunes: skip-when-nothing-due, short-batch stop, and
    // index use (DB review F10 / part 2 DB2-6). ----

    /// Statements `count_queries` sees per [`execute_retention_delete`]
    /// call: its `BEGIN`, the `SET LOCAL statement_timeout` and the `DELETE`
    /// itself (the one transaction per batch that raises the timeout).
    const QUERIES_PER_RETENTION_DELETE: usize = 3;

    async fn count_queries<F, T>(fut: F) -> (T, usize)
    where
        F: Future<Output = T>,
    {
        use tracing_subscriber::layer::SubscriberExt;
        let query_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(SqlxQueryCounter(query_count.clone()));
        let guard = tracing::subscriber::set_default(subscriber);
        let out = fut.await;
        drop(guard);
        (out, query_count.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// `EXPLAIN` text for `sql` with sequential scans disabled, so the plan
    /// shows whether the predicate can use an index at all (tiny test
    /// tables would otherwise always seq-scan).
    async fn explain_without_seqscan(pool: &PgPool, sql: &str) -> String {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *tx)
            .await
            .unwrap();
        let lines: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {sql}"))
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        lines.join("\n")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                retention_prunes_skip_their_delete_when_nothing_is_due -- --ignored --test-threads=1`"]
    async fn retention_prunes_skip_their_delete_when_nothing_is_due() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        // Windows no fixture row could be older than: each prune must issue
        // exactly its one MIN probe and no DELETE.
        let (pruned, queries) = count_queries(prune_history(&pool, 100_000)).await;
        assert_eq!(pruned.expect("prune_history"), 0);
        assert_eq!(queries, 1, "prune_history must stop after its MIN probe");

        let (pruned, queries) = count_queries(prune_trust_event_backlog(&pool, 100_000)).await;
        assert_eq!(pruned.expect("prune_trust_event_backlog"), 0);
        assert_eq!(
            queries, 1,
            "prune_trust_event_backlog must stop after its MIN probe"
        );

        let (pruned, queries) = count_queries(prune_trains(&pool, 100_000, 100_000)).await;
        assert_eq!(pruned.expect("prune_trains"), 0);
        assert_eq!(queries, 1, "prune_trains must stop after its MIN probe");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trains_stops_on_a_short_batch_and_skips_a_tier_with_nothing_due -- --ignored --test-threads=1`"]
    async fn prune_trains_stops_on_a_short_batch_and_skips_a_tier_with_nothing_due() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        let old_date = Utc::now().date_naive() - chrono::Duration::days(40);
        sqlx::query("DELETE FROM trains WHERE train_uid LIKE 'TEST-PRUNE-SHORT-%'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "INSERT INTO trains (train_uid, service_date) \
             SELECT 'TEST-PRUNE-SHORT-' || gs, $1 FROM generate_series(1, 1500) AS gs",
        )
        .bind(old_date)
        .execute(&pool)
        .await
        .expect("seed trains");

        // Untracked tier 30 days: 1500 rows = one full batch + one short
        // one, then stop. Tracked tier 100000 days: nothing due, no DELETE.
        // Probe + 2 DELETEs (the old loop needed 3 DELETEs), each DELETE
        // in its own `execute_retention_delete` transaction.
        let (pruned, queries) = count_queries(prune_trains(&pool, 100_000, 30)).await;
        let pruned = pruned.expect("prune_trains");
        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM trains WHERE train_uid LIKE 'TEST-PRUNE-SHORT-%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, 0);
        assert!(pruned >= 1500, "pruned {pruned}");
        if pruned == 1500 {
            assert_eq!(
                queries,
                1 + 2 * QUERIES_PER_RETENTION_DELETE,
                "probe + one full batch + one short batch"
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_trust_event_backlog_deletes_in_batches_oldest_first -- --ignored --test-threads=1`"]
    async fn prune_trust_event_backlog_deletes_in_batches_oldest_first() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id LIKE 'TEST-PRUNE-BATCH-%'")
            .execute(&pool)
            .await
            .ok();
        let rows = 2 * PRUNE_TRUST_EVENT_BACKLOG_BATCH + 7;
        sqlx::query(
            "INSERT INTO trust_event_backlog (train_id, service_date, msg_type, received_at, dedup_key) \
             SELECT 'TEST-PRUNE-BATCH-' || gs, CURRENT_DATE - 3, '0001', \
                    NOW() - interval '2 days' - gs * interval '1 second', 'test-prune-batch-' || gs \
             FROM generate_series(1, $1) AS gs",
        )
        .bind(rows)
        .execute(&pool)
        .await
        .expect("seed backlog rows");
        sqlx::query(
            "INSERT INTO trust_event_backlog (train_id, service_date, msg_type, received_at, dedup_key) \
             VALUES ('TEST-PRUNE-BATCH-KEEP', CURRENT_DATE, '0001', NOW(), 'test-prune-batch-keep')",
        )
        .execute(&pool)
        .await
        .expect("seed a fresh row");

        let (pruned, queries) = count_queries(prune_trust_event_backlog(&pool, 1)).await;
        let pruned = pruned.expect("prune_trust_event_backlog");
        let remaining: Vec<String> = sqlx::query_scalar(
            "SELECT train_id FROM trust_event_backlog WHERE train_id LIKE 'TEST-PRUNE-BATCH-%'",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id LIKE 'TEST-PRUNE-BATCH-%'")
            .execute(&pool)
            .await
            .ok();

        assert_eq!(remaining, vec!["TEST-PRUNE-BATCH-KEEP".to_string()]);
        assert!(pruned >= rows as u64, "pruned {pruned}");
        if pruned == rows as u64 {
            assert_eq!(
                queries,
                1 + 3 * QUERIES_PER_RETENTION_DELETE,
                "probe + two full batches + one short batch"
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                retention_prune_predicates_can_use_their_indexes -- --ignored --test-threads=1`"]
    async fn retention_prune_predicates_can_use_their_indexes() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

        for (sql, index) in [
            (
                "SELECT (SELECT MIN(received_at) FROM trust_event_backlog)",
                "trust_event_backlog_received_at",
            ),
            (
                "DELETE FROM trust_event_backlog WHERE id IN (SELECT id FROM trust_event_backlog \
                 WHERE received_at < NOW() - interval '1 day' ORDER BY received_at LIMIT 5000)",
                "trust_event_backlog_received_at",
            ),
            (
                "WITH oldest AS (SELECT MIN(service_date) AS d FROM trains) \
                 SELECT d < CURRENT_DATE - interval '14 days' FROM oldest",
                "trains_service_date",
            ),
            (
                "DELETE FROM trains WHERE id IN (SELECT id FROM trains \
                 WHERE service_date < CURRENT_DATE - interval '14 days' \
                   AND NOT EXISTS (SELECT 1 FROM train_subscriptions \
                                   WHERE train_subscriptions.trains_id = trains.id) \
                 ORDER BY service_date LIMIT 1000)",
                "trains_service_date",
            ),
            (
                "SELECT (SELECT MIN(computed_at) FROM line_status_history)",
                "line_status_history_computed_at",
            ),
            (
                "DELETE FROM line_status_history WHERE computed_at < NOW() - interval '30 days'",
                "line_status_history_computed_at",
            ),
        ] {
            let plan = explain_without_seqscan(&pool, sql).await;
            assert!(
                plan.contains(index),
                "expected {index} in the plan for {sql}:\n{plan}"
            );
        }
    }
}
