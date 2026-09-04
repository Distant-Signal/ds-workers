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
pub async fn load_full_coverage_line_stats(
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

    rows.into_iter()
        .map(|row| {
            let line_id: String = row.try_get("line_id")?;
            let stats = common::SampleStats {
                total: row.try_get::<i32, _>("total")? as usize,
                delayed: row.try_get::<i32, _>("delayed")? as usize,
                cancelled: row.try_get::<i32, _>("cancelled")? as usize,
                skipped: row.try_get::<i32, _>("skipped")? as usize,
                avg_delay_minutes: row.try_get("avg_delay_minutes")?,
            };
            Ok((line_id, stats))
        })
        .collect()
}

/// Deletes `line_status` rows for any `line_id` not in `current_line_ids`.
/// Called every cycle with the freshly-merged static+custom line set, so a
/// deleted custom line's last-known status is removed on the next cycle
/// rather than lingering forever (custom lines are the only way a line can
/// disappear between cycles — the static catalogue is fixed for the
/// process's lifetime).
///
/// Scoped to `source = 'aggregator'`: this crate is no longer the only
/// writer of `line_status`. TfL lines are written by the api crate's
/// `/private/tfl-line-status` ingest and are pruned by that endpoint
/// against its own batch — they are invisible to this crate's line set, so
/// an unscoped DELETE here would wipe them on the very next cycle.
pub async fn prune_removed_lines(pool: &PgPool, current_line_ids: &[String]) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM line_status WHERE source = 'aggregator' AND NOT (line_id = ANY($1))",
    )
    .bind(current_line_ids)
    .execute(pool)
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

/// The one `DataQuality` value affected by the `Utc::now()`-every-cycle bug
/// this exists to work around. See
/// docs/superpowers/specs/2026-08-30-inferred-time-ranges-design.md.
const LDBWS_INFERRED: &str = "ldbws-inferred";

/// Given the previous cycle's stored `statuses` JSON array (`existing`) and
/// this cycle's freshly-computed one (`fresh`), returns a copy of `fresh`
/// with each `"ldbws-inferred"` entry's `validity.from_date` overwritten by
/// the positionally-corresponding entry in `existing`, provided that entry
/// is also `"ldbws-inferred"` and the two entries are equal once run
/// through `normalize_entry_for_diff` (i.e. "same underlying disruption,
/// just a fresh poll of it" -- `sample_stats`, the live-sample-count reason
/// suffix, and the live counts baked directly into a sample-inferred
/// `reason` are all allowed to churn without defeating the carry-forward,
/// since `normalize_entry_for_diff` already strips all three).
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
        if entry.get("data_quality").and_then(|v| v.as_str()) != Some(LDBWS_INFERRED) {
            continue;
        }
        let Some(existing_entry) = existing_entries.and_then(|arr| arr.get(i)) else {
            continue;
        };
        if existing_entry.get("data_quality").and_then(|v| v.as_str()) != Some(LDBWS_INFERRED) {
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
        let before = &reason[copied_to..of_idx];
        let first_digits_start = copied_to
            + before
                .rfind(|c: char| !c.is_ascii_digit())
                .map_or(0, |p| p + 1);
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
pub async fn write_line_status(conn: &mut PgConnection, report: &LineStatusReport) -> Result<()> {
    let fresh_statuses_json = serde_json::to_value(&report.statuses)?;
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
        r#"
        INSERT INTO line_status (line_id, name, mode_name, operators, statuses, computed_at, source)
        VALUES ($1, $2, $3, $4, $5, NOW(), 'aggregator')
        ON CONFLICT (line_id) DO UPDATE SET
            name        = EXCLUDED.name,
            mode_name   = EXCLUDED.mode_name,
            operators   = EXCLUDED.operators,
            statuses    = EXCLUDED.statuses,
            computed_at = NOW(),
            source      = 'aggregator'
        "#,
    )
    .bind(&report.id)
    .bind(&report.name)
    .bind(&report.mode_name)
    .bind(&report.operators)
    .bind(&statuses_json)
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

/// The plain Europe/London CALENDAR day (midnight-to-midnight) `instant`
/// falls on -- matching `frontend/lib/dateFormat.ts`'s `londonDayKey`, the
/// convention the Timeline tab already groups by. Deliberately NOT
/// `next_rail_day_boundary`'s rail-day 02:00 cutoff, a different boundary
/// used elsewhere in this crate for incident staleness -- see
/// docs/superpowers/specs/2026-08-31-line-history-graphics-design.md, Open
/// question 5, for why these two conventions coexist.
pub fn london_calendar_day(instant: DateTime<Utc>) -> NaiveDate {
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
pub fn utc_half_hour_start(instant: DateTime<Utc>) -> DateTime<Utc> {
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
pub async fn record_daily_stats<'c, E>(
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
pub async fn prune_daily_stats(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let result =
        sqlx::query("DELETE FROM line_status_daily_stats WHERE day < (CURRENT_DATE - $1::int)")
            .bind(retention_days as i32)
            .execute(pool)
            .await?;
    Ok(result.rows_affected())
}

/// Half-hourly-granularity sibling of `record_daily_stats` -- same
/// accumulate-upsert shape, same "fed the DEDUPED per-cycle contribution,
/// not raw SampleStats" contract (see that function's own doc comment,
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
pub async fn record_half_hourly_stats<'c, E>(
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
pub async fn prune_half_hourly_stats(pool: &PgPool, retention_hours: i64) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM line_status_half_hourly_stats WHERE half_hour_start < NOW() - ($1 || ' hours')::interval",
    )
    .bind(retention_hours.to_string())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

// --- Decision 4 scaffolding: line_status_{daily,half_hourly}_coverage_stats ---
//
// Sibling pair of record_daily_stats/record_half_hourly_stats above, same
// accumulate-upsert shape, `resolved_windows` in place of `sample_cycles`.
// See crates/api/migrations/20260903200000_line_status_daily_coverage_stats.sql's
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
pub async fn record_daily_coverage_stats<'c, E>(
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
pub async fn prune_daily_coverage_stats(pool: &PgPool, retention_days: i64) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM line_status_daily_coverage_stats WHERE day < (CURRENT_DATE - $1::int)",
    )
    .bind(retention_days as i32)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Half-hourly-granularity sibling of `record_daily_coverage_stats` --
/// same relationship `record_half_hourly_stats` already has to
/// `record_daily_stats`. See this section's own module doc comment for
/// the "what counts as a cycle's contribution" judgment call.
pub async fn record_half_hourly_coverage_stats<'c, E>(
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
pub async fn prune_half_hourly_coverage_stats(pool: &PgPool, retention_hours: i64) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM line_status_half_hourly_coverage_stats WHERE half_hour_start < NOW() - ($1 || ' hours')::interval",
    )
    .bind(retention_hours.to_string())
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
                ('TEST-ACTIVE', 'active', 'active incident', '{}', '{}', 0, '[]', false, false), \
                ('TEST-CLEARED', 'cleared', 'cleared incident', '{}', '{}', 0, '[]', false, true) \
             ON CONFLICT (incident_id) DO UPDATE SET is_cleared = EXCLUDED.is_cleared",
        )
        .execute(&pool)
        .await
        .expect("seed fixture rows");

        let loaded = load_incidents(&pool).await.expect("load_incidents");
        let ids: Vec<&str> = loaded
            .iter()
            .map(|i| i.message.incident_id.as_str())
            .collect();

        sqlx::query("DELETE FROM incidents WHERE incident_id IN ('TEST-ACTIVE', 'TEST-CLEARED')")
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
    }

    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p aggregator \
                prune_removed_lines_leaves_other_sources_alone -- --ignored`"]
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

        // An empty current-line set is the worst case: the aggregator has
        // nothing of its own left, so anything it does not own must still
        // survive.
        prune_removed_lines(&pool, &[])
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
                a_stable_ldbws_inferred_status_carries_its_from_date_across_two_cycles -- --ignored`"]
    async fn a_stable_ldbws_inferred_status_carries_its_from_date_across_two_cycles() {
        // Real two-cycle aggregate() -> write_line_status() sequence, per
        // docs/superpowers/specs/2026-08-30-inferred-time-ranges-design.md's
        // "Testing" section: a stable LdbwsInferred status across two cycles
        // should still produce exactly one line_status_history row
        // (unchanged from today) AND a stable `from_date` in the second
        // cycle's stored row (the new behavior this design fixes).
        use crate::aggregation::aggregate;
        use crate::queries::LoadedIncident;
        use crate::segments::SegmentRegistry;
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
            exclusive_segments: vec![],
            destination_crs_filter: vec!["AON".to_string()],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
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
        let reports1 = aggregate(&lines, &no_incidents, &samples, &registry, &defaults);
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
        write_line_status(&mut conn, report1)
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
        let reports2 = aggregate(&lines, &no_incidents, &samples, &registry, &defaults);
        let report2 = reports2.get(LINE_ID).expect("line should have a report");
        let fresh_from_date_cycle_2 = serde_json::to_value(report2.statuses[0].validity.from_date)
            .expect("serialize fresh from_date");
        write_line_status(&mut conn, report2)
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
                -- --ignored` against docker compose's postgres"]
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
        write_line_status(&mut tx1, &report(COMMITTED_CHUNK_LINE))
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
        write_line_status(&mut tx2, &report(ROLLED_BACK_LINE))
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
                -- --ignored` against docker compose's postgres"]
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
                write_line_status(&mut tx, report)
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
                record_daily_stats_accumulates_deduped_contributions_across_a_day -- --ignored` \
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
                record_daily_stats_none_still_counts_the_cycle_but_adds_nothing -- --ignored` \
                against docker compose's postgres"]
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
                record_daily_stats_a_new_day_starts_a_fresh_row -- --ignored` against docker \
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
                prune_daily_stats_deletes_only_rows_older_than_the_retention_window -- --ignored` \
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
                record_half_hourly_stats_accumulates_deduped_contributions_within_a_bucket -- --ignored` \
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
                record_half_hourly_stats_a_new_bucket_starts_a_fresh_row -- --ignored` against docker \
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
                record_half_hourly_stats_a_bucket_boundary_crossing_a_day_boundary_is_unaffected -- --ignored` \
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
                prune_half_hourly_stats_deletes_only_rows_older_than_the_retention_window -- --ignored` \
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
    /// itself (that would need a full aggregate() pipeline); it directly
    /// exercises the two write functions with an identical input, which is
    /// the actual invariant that matters and is what would regress if a
    /// future edit ever computed two separate `deduped` values instead of
    /// sharing one.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                half_hourly_and_daily_stats_reconcile_for_a_single_line_and_period -- --ignored` \
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
                record_daily_coverage_stats_accumulates_across_a_day -- --ignored` \
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
                record_daily_coverage_stats_none_still_counts_the_cycle_but_adds_nothing -- --ignored` \
                against docker compose's postgres"]
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
                record_daily_coverage_stats_a_new_day_starts_a_fresh_row -- --ignored` against \
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

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                prune_daily_coverage_stats_deletes_only_rows_older_than_the_retention_window -- --ignored` \
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
                record_half_hourly_coverage_stats_accumulates_within_a_bucket -- --ignored` \
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
                prune_half_hourly_coverage_stats_deletes_only_rows_older_than_the_retention_window -- --ignored` \
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
        let today = chrono::Utc::now().date_naive();
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
}
