//! Windowed full-coverage severity: reading `full-coverage-consumer`'s
//! `recent` windows, judging them with the shared
//! `common::full_coverage_window::classify_full_coverage_window`, and --
//! depending on `FULL_COVERAGE_WINDOW_MODE` -- only recording that verdict
//! (`shadow`) or also escalating allow-listed lines with it (`enforce`).
//!
//! See docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md
//! section 6, and its "Decisions (2026-09-27)" section:
//!
//! - **`off` (the default) is today's code path exactly**: nothing here
//!   runs except the retention prune.
//! - **`shadow`** computes a verdict for every line with a fresh window and
//!   stores it in `full_coverage_window_verdicts`, including what it WOULD
//!   have escalated to (`would_escalate_to`) and whether that tier is below
//!   the enforced gate (`below_min_rank`). No `LineStatus` changes.
//! - **`enforce`** additionally applies the verdict, escalate-only, to the
//!   lines named in `FULL_COVERAGE_WINDOW_ENFORCE_LINES` (empty by default:
//!   nothing is enforced until lines are named; `*` means every enabled
//!   line), and only for tiers at or above
//!   `FULL_COVERAGE_WINDOW_MIN_ESCALATION_RANK` (default: the Severe tier --
//!   Severe Delays and Part Suspended). Lower tiers (Minor Delays, Reduced
//!   Service) are still recorded, with `below_min_rank`, so widening the
//!   enforced tiers later is an evidence-based config change.
//! - **Only statuses in effect now** (`aggregation::in_effect_now`, user
//!   decision 2026-10-02) are raised, and the line's "current severity"
//!   (what a verdict must beat) is the worst of those alone. A planned-works
//!   notice that is not in effect stays as published; when nothing on the
//!   line is in effect, an enforced verdict is shown as its own
//!   `TrustInferred` status instead.
//! - **Sparse all-cancelled verdicts** (rule A, user decision 2026-10-02:
//!   `EscalationBasis::SparseAllCancelled`, a window below the sample size
//!   in which every train was cancelled) have their OWN allowlist,
//!   `FULL_COVERAGE_WINDOW_SPARSE_ENFORCE_LINES` (empty by default), in
//!   place of `FULL_COVERAGE_WINDOW_ENFORCE_LINES`: they are always
//!   recorded, but enforced only on the lines named there, so the rule can
//!   stay shadow-only while the pilot enforces the rate tiers.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use common::full_coverage_window::{
    EscalationBasis, FULL_COVERAGE_SPARSE_MIN_CANCELLED,
    FULL_COVERAGE_WINDOW_DEFAULT_MIN_ESCALATION_RANK, WindowVerdict, classify_full_coverage_window,
    escalation_decision,
};
use common::{
    DataQuality, Defaults, Disruption, FullCoverageAvailability, FullCoverageWindowCounts,
    FullCoverageWindowKind, FullCoverageWindowStatsRow, LineDefinition, LineStatus,
    LineStatusReport, Severity, ValidityPeriod, severity_rank, thresholds_for,
};
use sqlx::{PgPool, Row};

use crate::aggregation::in_effect_now;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum WindowMode {
    /// Today's behaviour, exactly.
    #[default]
    Off,
    /// Compute and record verdicts; change nothing.
    Shadow,
    /// Record verdicts, and escalate allow-listed lines.
    Enforce,
}

impl WindowMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            WindowMode::Off => "off",
            WindowMode::Shadow => "shadow",
            WindowMode::Enforce => "enforce",
        }
    }
}

fn non_negative_days(s: &str) -> anyhow::Result<i64> {
    let value: i64 = s
        .trim()
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid retention value {s:?}: {e}"))?;
    anyhow::ensure!(
        value >= 0,
        "retention value must not be negative, got {value}"
    );
    Ok(value)
}

/// The aggregator's windowed full-coverage settings. **All off by
/// default.**
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct WindowArgs {
    /// `off` (default), `shadow` or `enforce` -- see this module's doc.
    #[arg(
        long = "full-coverage-window-mode",
        env = "FULL_COVERAGE_WINDOW_MODE",
        value_enum,
        default_value_t = WindowMode::Off
    )]
    pub mode: WindowMode,
    /// Lines `enforce` may change: a comma list, or `*` for every
    /// full-coverage-enabled line. EMPTY by default, so switching to
    /// `enforce` enforces nothing until the pilot lines are named.
    #[arg(long, env, default_value = "")]
    pub full_coverage_window_enforce_lines: String,
    /// Only verdicts whose `severity_rank` is at least this are enforced.
    /// Default 4: Severe Delays and Part Suspended. 3 would add Minor
    /// Delays and Reduced Service.
    #[arg(long, env, default_value_t = FULL_COVERAGE_WINDOW_DEFAULT_MIN_ESCALATION_RANK)]
    pub full_coverage_window_min_escalation_rank: u8,
    /// Lines `enforce` may change with a SPARSE all-cancelled verdict (a
    /// window below the sample size in which every train was cancelled):
    /// a comma list, or `*` for every full-coverage-enabled line. Separate
    /// from `FULL_COVERAGE_WINDOW_ENFORCE_LINES`, which no longer covers
    /// those verdicts. EMPTY by default: sparse verdicts are recorded
    /// (shadow) but never shown.
    #[arg(long, env, default_value = "")]
    pub full_coverage_window_sparse_enforce_lines: String,
    /// The default of `Defaults::full_coverage_sparse_min_cancelled`: a
    /// sparse window needs at least this many cancelled trains, all of
    /// them (a line's `severity_overrides` may still set its own). 0 turns
    /// the sparse rule off.
    #[arg(long, env, default_value_t = FULL_COVERAGE_SPARSE_MIN_CANCELLED)]
    pub full_coverage_sparse_min_cancelled: i64,
    /// Retention of `full_coverage_line_window_stats` and
    /// `full_coverage_window_verdicts`. Pruned in every mode.
    #[arg(long, env, default_value_t = 14, value_parser = non_negative_days)]
    pub full_coverage_window_stats_retention_days: i64,
}

/// `FULL_COVERAGE_WINDOW_ENFORCE_LINES`, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Allowlist {
    All,
    Lines(HashSet<String>),
}

impl Allowlist {
    pub(crate) fn parse(value: &str) -> Self {
        if value.trim() == "*" {
            return Allowlist::All;
        }
        Allowlist::Lines(
            value
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
        )
    }

    pub(crate) fn contains(&self, line_id: &str) -> bool {
        match self {
            Allowlist::All => true,
            Allowlist::Lines(lines) => lines.contains(line_id),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WindowSettings {
    pub mode: WindowMode,
    pub allowlist: Allowlist,
    /// `FULL_COVERAGE_WINDOW_SPARSE_ENFORCE_LINES`: where a sparse
    /// all-cancelled verdict may change a line.
    pub sparse_allowlist: Allowlist,
    pub min_rank: u8,
}

impl WindowSettings {
    pub(crate) fn from_args(args: &WindowArgs) -> Self {
        Self {
            mode: args.mode,
            allowlist: Allowlist::parse(&args.full_coverage_window_enforce_lines),
            sparse_allowlist: Allowlist::parse(&args.full_coverage_window_sparse_enforce_lines),
            min_rank: args.full_coverage_window_min_escalation_rank,
        }
    }

    /// Whether `enforce` may change `line` (rate-based verdicts, and the
    /// counts shown on every status).
    pub(crate) fn enforces(
        &self,
        line: &LineDefinition,
        full_coverage_enabled_default: bool,
    ) -> bool {
        self.mode == WindowMode::Enforce
            && (line.full_coverage_enabled || full_coverage_enabled_default)
            && self.allowlist.contains(&line.id)
    }

    /// Whether `enforce` may raise `line` with a sparse all-cancelled
    /// verdict: the same conditions, under the sparse allowlist.
    pub(crate) fn enforces_sparse(
        &self,
        line: &LineDefinition,
        full_coverage_enabled_default: bool,
    ) -> bool {
        self.mode == WindowMode::Enforce
            && (line.full_coverage_enabled || full_coverage_enabled_default)
            && self.sparse_allowlist.contains(&line.id)
    }
}

/// A metric label for an optional basis: `none` for a verdict that is not
/// an escalation.
fn basis_label(basis: Option<EscalationBasis>) -> &'static str {
    basis.map_or("none", EscalationBasis::as_str)
}

/// The lines full coverage covers: every line except users' custom lines.
/// `full-coverage-consumer` builds populations and windows for the
/// `lines/*.toml` catalogue only, and `CustomLine`'s `From` impl already
/// marks a custom line as never a full-coverage candidate -- but
/// `FULL_COVERAGE_ENABLED_DEFAULT=true` would otherwise enable it, so each
/// one read as a "missing" window every cycle (and as Pending under
/// `enforce` with `*`). They keep `NotEnabled`.
pub(crate) fn full_coverage_lines(
    lines: &HashMap<String, LineDefinition>,
    custom_ids: &HashSet<String>,
) -> HashMap<String, LineDefinition> {
    lines
        .iter()
        .filter(|(id, _)| !custom_ids.contains(*id))
        .map(|(id, line)| (id.clone(), line.clone()))
        .collect()
}

/// One stored window bucket.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StoredWindow {
    pub bucket_start: DateTime<Utc>,
    pub row: FullCoverageWindowStatsRow,
}

/// A line's newest windows.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct LineWindows {
    pub recent: Option<StoredWindow>,
    pub day_to_date: Option<StoredWindow>,
}

/// What was decided about one line's `recent` window this cycle -- one
/// `full_coverage_window_verdicts` row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VerdictRecord {
    pub line_id: String,
    pub bucket_start: DateTime<Utc>,
    pub evaluated_at: DateTime<Utc>,
    pub window_computed_at: DateTime<Utc>,
    pub mode: WindowMode,
    pub verdict: WindowVerdict,
    /// The rule behind an `Escalate` verdict (`verdict.basis()`); stored in
    /// the `basis` column.
    pub basis: Option<EscalationBasis>,
    pub current_severity: Severity,
    pub would_escalate_to: Option<Severity>,
    pub below_min_rank: bool,
    /// The line is in the allowlist that governs this verdict: the sparse
    /// one for a sparse all-cancelled verdict, the main one otherwise.
    pub in_allowlist: bool,
    pub enforced: bool,
}

/// The worst severity among the statuses in effect at `now`; Good Service
/// when none is.
fn worst_severity(report: &LineStatusReport, now: DateTime<Utc>) -> Severity {
    report
        .statuses
        .iter()
        .filter(|s| in_effect_now(s, now))
        .map(|s| s.severity)
        .max_by_key(|s| severity_rank(*s))
        .unwrap_or(Severity::GoodService)
}

/// Judges every line's `recent` window (every line in `shadow`; the same in
/// `enforce`, where allow-listed lines are also changed) and returns what
/// was decided. A no-op in `off`.
///
/// In `enforce`, for each status of an allow-listed line: the window's
/// counts become `full_coverage_stats`, availability is `Available` when the
/// window is eligible and `Pending` otherwise, and an `Escalate` verdict at
/// or above the rank gate that is strictly worse than the status raises it
/// -- replacing an LDBWS-inferred status (as `TrustInferred`), or annotating
/// an incident's reason. Never lowers anything.
///
/// A sparse all-cancelled verdict raises a line only when the line is in
/// the SPARSE allowlist; on a line only in the main allowlist it is
/// recorded and its counts are shown, but nothing is raised.
pub(crate) fn apply_windows(
    reports: &mut HashMap<String, LineStatusReport>,
    lines: &HashMap<String, LineDefinition>,
    windows: &HashMap<String, LineWindows>,
    defaults: &Defaults,
    settings: &WindowSettings,
    full_coverage_enabled_default: bool,
    now: DateTime<Utc>,
) -> Vec<VerdictRecord> {
    if settings.mode == WindowMode::Off {
        return Vec::new();
    }
    let mut line_ids: Vec<&String> = lines.keys().collect();
    line_ids.sort();
    let mut records = Vec::new();
    for line_id in line_ids {
        let line = &lines[line_id];
        let Some(report) = reports.get_mut(line_id) else {
            continue;
        };
        let enforce = settings.enforces(line, full_coverage_enabled_default);
        let enforce_sparse = settings.enforces_sparse(line, full_coverage_enabled_default);
        let Some(recent) = windows.get(line_id).and_then(|w| w.recent.as_ref()) else {
            metrics::counter!(
                common::metrics::metric_name("aggregator_full_coverage_window_verdicts_total"),
                "verdict" => "missing",
                "basis" => basis_label(None)
            )
            .increment(1);
            if enforce {
                for status in &mut report.statuses {
                    status.full_coverage_availability = FullCoverageAvailability::Pending;
                }
            }
            continue;
        };
        let thresholds = thresholds_for(defaults, &line.severity_overrides);
        let verdict = classify_full_coverage_window(&recent.row, &thresholds, now);
        let current = worst_severity(report, now);
        let decision = escalation_decision(&verdict, current, settings.min_rank);
        let basis = verdict.basis();
        // Which allowlist may raise the line with this verdict.
        let may_raise = if basis == Some(EscalationBasis::SparseAllCancelled) {
            enforce_sparse
        } else {
            enforce
        };
        let mut enforced = false;
        if enforce || may_raise {
            enforced = enforce_on(
                report,
                &recent.row.counts,
                &verdict,
                Enforcement {
                    show_counts: enforce,
                    may_raise,
                },
                settings.min_rank,
                now,
            );
        }
        let label = match &verdict {
            WindowVerdict::Ineligible(reason) => reason.as_str(),
            other => other.label(),
        };
        metrics::counter!(
            common::metrics::metric_name("aggregator_full_coverage_window_verdicts_total"),
            "verdict" => label,
            "basis" => basis_label(basis)
        )
        .increment(1);
        if let Some(severity) = decision.would_escalate_to {
            metrics::counter!(
                common::metrics::metric_name("aggregator_full_coverage_window_escalations_total"),
                "severity" => severity.description(),
                "mode" => if enforced { "enforce" } else { "shadow" },
                "below_min_rank" => if decision.below_min_rank { "true" } else { "false" },
                "basis" => basis_label(basis)
            )
            .increment(1);
        }
        records.push(VerdictRecord {
            line_id: line_id.clone(),
            bucket_start: recent.bucket_start,
            evaluated_at: now,
            window_computed_at: recent.row.computed_at,
            mode: settings.mode,
            verdict,
            basis,
            current_severity: current,
            would_escalate_to: decision.would_escalate_to,
            below_min_rank: decision.below_min_rank,
            in_allowlist: may_raise,
            enforced,
        });
    }
    records
}

/// What `enforce` may do to one line with one verdict.
#[derive(Debug, Clone, Copy)]
struct Enforcement {
    /// The line is in the main allowlist: its statuses show the window's
    /// counts and availability (in place of the legacy whole-day merge).
    show_counts: bool,
    /// The allowlist governing this verdict's basis names the line: the
    /// verdict may raise a status.
    may_raise: bool,
}

/// Applies one eligible-or-not verdict to every status of an allow-listed
/// line, raising only the statuses in effect at `now`. When none is, an
/// enforced verdict is added as its own `TrustInferred` status. Returns
/// whether any severity was raised (or added).
///
/// A line only in the sparse allowlist keeps its legacy counts
/// (`show_counts` false) and is only raised; a line only in the main
/// allowlist with a sparse verdict shows the counts and is not raised.
#[expect(
    clippy::format_push_string,
    reason = "short strings off the hot path; format! reads clearer"
)]
fn enforce_on(
    report: &mut LineStatusReport,
    counts: &FullCoverageWindowCounts,
    verdict: &WindowVerdict,
    enforcement: Enforcement,
    min_rank: u8,
    now: DateTime<Utc>,
) -> bool {
    let Enforcement {
        show_counts,
        may_raise,
    } = enforcement;
    let stats = counts.to_sample_stats();
    let eligible = !matches!(verdict, WindowVerdict::Ineligible(_));
    let availability = if eligible {
        FullCoverageAvailability::Available(stats.clone())
    } else {
        FullCoverageAvailability::Pending
    };
    let any_in_effect = report.statuses.iter().any(|s| in_effect_now(s, now));
    let mut raised = false;
    for status in &mut report.statuses {
        if show_counts {
            status.full_coverage_stats = Some(stats.clone());
            status.full_coverage_availability = availability.clone();
        }
        if !may_raise || !in_effect_now(status, now) {
            continue;
        }
        let WindowVerdict::Escalate {
            severity, reason, ..
        } = verdict
        else {
            continue;
        };
        let rank = severity_rank(*severity);
        if rank < min_rank || rank <= severity_rank(status.severity) {
            continue;
        }
        status.severity = *severity;
        if status.data_quality == DataQuality::LdbwsInferred {
            status.reason.clone_from(reason);
            status.data_quality = DataQuality::TrustInferred;
        } else {
            status
                .reason
                .push_str(&format!(" (train-running data shows: {reason})"));
        }
        raised = true;
    }
    if let WindowVerdict::Escalate {
        severity, reason, ..
    } = verdict
        && may_raise
        && !any_in_effect
        && severity_rank(*severity) >= min_rank
        && severity_rank(*severity) > severity_rank(Severity::GoodService)
    {
        let template = report.statuses.first();
        report.statuses.push(LineStatus {
            severity: *severity,
            reason: reason.clone(),
            validity: ValidityPeriod {
                from_date: now,
                to_date: None,
                is_now: true,
            },
            disruption: Some(Disruption {
                category: "RealTime".to_string(),
                description: reason.clone(),
                affected_stops: vec![],
                affected_routes: vec![],
                source: Some("full-coverage-window".to_string()),
                impact_type: None,
            }),
            data_quality: DataQuality::TrustInferred,
            sample_stats: template.and_then(|s| s.sample_stats.clone()),
            sample_availability: template.map_or(common::SampleAvailability::NoCoverage, |s| {
                s.sample_availability.clone()
            }),
            full_coverage_stats: Some(stats),
            full_coverage_availability: availability,
        });
        raised = true;
    }
    raised
}

/// Each line's newest `recent` and `day_to_date` buckets from the last 30
/// minutes (the staleness check proper is `computed_at` vs now, in
/// `classify_full_coverage_window`). A row that fails to decode is skipped
/// and logged, not fatal.
pub(crate) async fn load_full_coverage_windows(
    pool: &PgPool,
    now: DateTime<Utc>,
) -> anyhow::Result<HashMap<String, LineWindows>> {
    let rows = sqlx::query(
        "SELECT DISTINCT ON (line_id, window_kind)
                line_id, window_kind, bucket_start, service_date, window_start, window_end,
                computed_at, total, on_time, delayed, cancelled_explicit, cancelled_presumed,
                skipped, pending, unobserved, avg_delay_minutes, relevance, presumed_enabled,
                partial, feed_stale, stats_version, cancelled_in_advance
           FROM full_coverage_line_window_stats
          WHERE bucket_start >= $1 - interval '30 minutes'
          ORDER BY line_id, window_kind, bucket_start DESC",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;
    let mut windows: HashMap<String, LineWindows> = HashMap::new();
    for row in &rows {
        match stored_window(row) {
            Ok(stored) => {
                let entry = windows.entry(stored.row.line_id.clone()).or_default();
                match stored.row.window_kind {
                    FullCoverageWindowKind::Recent => entry.recent = Some(stored),
                    FullCoverageWindowKind::DayToDate => entry.day_to_date = Some(stored),
                }
            }
            Err(err) => {
                tracing::warn!(error = ?err, "skipping an undecodable full_coverage_line_window_stats row");
            }
        }
    }
    Ok(windows)
}

#[expect(
    clippy::cast_sign_loss,
    reason = "clamped to >= 0 first; the columns hold small counts and versions"
)]
fn stored_window(row: &sqlx::postgres::PgRow) -> anyhow::Result<StoredWindow> {
    let uint =
        |name: &str| -> anyhow::Result<u32> { Ok(row.try_get::<i32, _>(name)?.max(0) as u32) };
    let kind: String = row.try_get("window_kind")?;
    Ok(StoredWindow {
        bucket_start: row.try_get("bucket_start")?,
        row: FullCoverageWindowStatsRow {
            line_id: row.try_get("line_id")?,
            window_kind: FullCoverageWindowKind::parse(&kind)
                .ok_or_else(|| anyhow::anyhow!("unknown window_kind {kind:?}"))?,
            service_date: row.try_get("service_date")?,
            window_start: row.try_get("window_start")?,
            window_end: row.try_get("window_end")?,
            computed_at: row.try_get("computed_at")?,
            counts: FullCoverageWindowCounts {
                total: uint("total")?,
                on_time: uint("on_time")?,
                delayed: uint("delayed")?,
                cancelled_explicit: uint("cancelled_explicit")?,
                cancelled_presumed: uint("cancelled_presumed")?,
                skipped: uint("skipped")?,
                pending: uint("pending")?,
                unobserved: uint("unobserved")?,
                avg_delay_minutes: row.try_get("avg_delay_minutes")?,
                cancelled_in_advance: uint("cancelled_in_advance")?,
            },
            relevance: row.try_get("relevance")?,
            presumed_enabled: row.try_get("presumed_enabled")?,
            partial: row.try_get("partial")?,
            feed_stale: row.try_get("feed_stale")?,
            stats_version: row.try_get::<i16, _>("stats_version")?.max(0) as u16,
        },
    })
}

fn severity_db(severity: Severity) -> i16 {
    i16::from(severity as u8)
}

/// Upserts this cycle's verdicts, one row per `(line_id, bucket_start)`;
/// the latest evaluation of a bucket wins.
pub(crate) async fn write_verdicts(
    pool: &PgPool,
    records: &[VerdictRecord],
) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    let mut written = 0u64;
    for record in records {
        let (verdict, ineligible_reason, verdict_severity, reason) = match &record.verdict {
            WindowVerdict::Ineligible(r) => ("ineligible", Some(r.as_str()), None, None),
            WindowVerdict::Good => ("good", None, None, None),
            WindowVerdict::Escalate {
                severity, reason, ..
            } => (
                "escalate",
                None,
                Some(severity_db(*severity)),
                Some(reason.as_str()),
            ),
        };
        let result = sqlx::query(
            "INSERT INTO full_coverage_window_verdicts
                (line_id, bucket_start, evaluated_at, window_computed_at, mode, verdict,
                 ineligible_reason, verdict_severity, current_severity, would_escalate_to,
                 below_min_rank, in_allowlist, enforced, reason, basis)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
             ON CONFLICT (line_id, bucket_start) DO UPDATE SET
                evaluated_at       = EXCLUDED.evaluated_at,
                window_computed_at = EXCLUDED.window_computed_at,
                mode               = EXCLUDED.mode,
                verdict            = EXCLUDED.verdict,
                ineligible_reason  = EXCLUDED.ineligible_reason,
                verdict_severity   = EXCLUDED.verdict_severity,
                current_severity   = EXCLUDED.current_severity,
                would_escalate_to  = EXCLUDED.would_escalate_to,
                below_min_rank     = EXCLUDED.below_min_rank,
                in_allowlist       = EXCLUDED.in_allowlist,
                enforced           = EXCLUDED.enforced,
                reason             = EXCLUDED.reason,
                basis              = EXCLUDED.basis",
        )
        .bind(&record.line_id)
        .bind(record.bucket_start)
        .bind(record.evaluated_at)
        .bind(record.window_computed_at)
        .bind(record.mode.as_str())
        .bind(verdict)
        .bind(ineligible_reason)
        .bind(verdict_severity)
        .bind(severity_db(record.current_severity))
        .bind(record.would_escalate_to.map(severity_db))
        .bind(record.below_min_rank)
        .bind(record.in_allowlist)
        .bind(record.enforced)
        .bind(reason)
        .bind(record.basis.map(EscalationBasis::as_str))
        .execute(&mut *tx)
        .await?;
        written += result.rows_affected();
    }
    tx.commit().await?;
    Ok(written)
}

/// Deletes window buckets and verdicts older than `retention_days`.
///
/// Same shape as `queries::prune_history`: the cutoff is fixed once per
/// call, each table's `MIN(bucket_start)` is probed first (one descent of
/// its `*_bucket` index) and its `DELETE` skipped when nothing is due, and
/// each `DELETE` runs in its own transaction under
/// `queries::RETENTION_STATEMENT_TIMEOUT`.
pub(crate) async fn prune_full_coverage_window_stats(
    pool: &PgPool,
    retention_days: i64,
) -> anyhow::Result<u64> {
    let cutoff: DateTime<Utc> = sqlx::query_scalar("SELECT now() - make_interval(days => $1::int)")
        .bind(i32::try_from(retention_days).unwrap_or(i32::MAX))
        .fetch_one(pool)
        .await?;
    let mut pruned = 0;
    for table in [
        "full_coverage_line_window_stats",
        "full_coverage_window_verdicts",
    ] {
        let due: Option<bool> = sqlx::query_scalar(&format!(
            "SELECT (SELECT MIN(bucket_start) FROM {table}) < $1"
        ))
        .bind(cutoff)
        .fetch_one(pool)
        .await?;
        if due != Some(true) {
            continue;
        }
        let delete = format!("DELETE FROM {table} WHERE bucket_start < $1");
        let result =
            crate::queries::execute_retention_delete(pool, sqlx::query(&delete).bind(cutoff))
                .await?;
        pruned += result.rows_affected();
    }
    Ok(pruned)
}

/// Every counter an alert or dashboard would key on, at 0.
pub(crate) fn init_metrics() {
    for (verdict, basis) in [
        ("missing", None),
        ("good", None),
        ("escalate", Some(EscalationBasis::Rate)),
        ("escalate", Some(EscalationBasis::SparseAllCancelled)),
        ("below_threshold", None),
        ("partial", None),
        ("feed_stale", None),
        ("stale_row", None),
    ] {
        metrics::counter!(
            common::metrics::metric_name("aggregator_full_coverage_window_verdicts_total"),
            "verdict" => verdict,
            "basis" => basis_label(basis)
        )
        .increment(0);
    }
    metrics::counter!(common::metrics::metric_name(
        "aggregator_full_coverage_window_stats_pruned_total"
    ))
    .increment(0);
}

#[cfg(test)]
mod tests {
    use common::{
        FullCoverageWindowCounts, LineStatus, SampleAvailability, SampleStats, ValidityPeriod,
    };

    use super::*;

    fn now() -> DateTime<Utc> {
        "2026-09-27T12:00:00Z".parse().unwrap()
    }

    fn line(id: &str, enabled: bool) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "national-rail".to_string(),
            category: "regional".to_string(),
            operators: vec!["AW".to_string()],
            stations: vec![],
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: HashMap::new(),
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: enabled,
            pass_through: Vec::new(),
        }
    }

    fn status(severity: Severity, data_quality: DataQuality, reason: &str) -> LineStatus {
        LineStatus {
            severity,
            reason: reason.to_string(),
            validity: ValidityPeriod {
                from_date: now(),
                to_date: None,
                is_now: true,
            },
            disruption: None,
            data_quality,
            sample_stats: None,
            sample_availability: SampleAvailability::NoCoverage,
            full_coverage_stats: None,
            full_coverage_availability: FullCoverageAvailability::NotEnabled,
        }
    }

    fn report(id: &str, statuses: Vec<LineStatus>) -> LineStatusReport {
        LineStatusReport {
            id: id.to_string(),
            name: id.to_string(),
            mode_name: "national-rail".to_string(),
            operators: vec![],
            statuses,
        }
    }

    fn window(line_id: &str, total: u32, delayed: u32, explicit: u32) -> LineWindows {
        LineWindows {
            recent: Some(StoredWindow {
                bucket_start: "2026-09-27T11:45:00Z".parse().unwrap(),
                row: FullCoverageWindowStatsRow {
                    line_id: line_id.to_string(),
                    window_kind: FullCoverageWindowKind::Recent,
                    service_date: "2026-09-27".parse().unwrap(),
                    window_start: "2026-09-27T10:50:00Z".parse().unwrap(),
                    window_end: "2026-09-27T11:50:00Z".parse().unwrap(),
                    computed_at: now() - chrono::Duration::seconds(30),
                    counts: FullCoverageWindowCounts {
                        total,
                        on_time: total - delayed - explicit,
                        delayed,
                        cancelled_explicit: explicit,
                        ..Default::default()
                    },
                    relevance: "full".to_string(),
                    presumed_enabled: true,
                    partial: false,
                    feed_stale: false,
                    stats_version: 2,
                },
            }),
            day_to_date: None,
        }
    }

    fn settings(mode: WindowMode, allow: &str) -> WindowSettings {
        WindowSettings {
            mode,
            allowlist: Allowlist::parse(allow),
            sparse_allowlist: Allowlist::parse(""),
            min_rank: FULL_COVERAGE_WINDOW_DEFAULT_MIN_ESCALATION_RANK,
        }
    }

    /// A fixture plus a "branch" line whose window is 3 of 3 cancelled
    /// (a sparse all-cancelled verdict), showing Good Service.
    fn sparse_fixture() -> Fixture {
        let mut f = fixture();
        f.reports.insert(
            "branch".to_string(),
            report(
                "branch",
                vec![status(
                    Severity::GoodService,
                    DataQuality::LdbwsInferred,
                    "Good Service",
                )],
            ),
        );
        f.lines.insert("branch".to_string(), line("branch", false));
        f.windows
            .insert("branch".to_string(), window("branch", 3, 0, 3));
        f
    }

    /// The default: the sparse verdict is recorded, flagged as the sparse
    /// basis, and changes nothing -- even with the line in the main
    /// allowlist, where the rate tiers ARE enforced.
    #[test]
    fn a_sparse_verdict_is_shadow_only_without_the_sparse_allowlist() {
        let mut f = sparse_fixture();
        let records = run(&mut f, &settings(WindowMode::Enforce, "*"), true);
        let b = record(&records, "branch");
        assert_eq!(b.basis, Some(EscalationBasis::SparseAllCancelled));
        assert_eq!(b.would_escalate_to, Some(Severity::PartSuspended));
        assert!(!b.below_min_rank);
        assert!(!b.enforced && !b.in_allowlist);
        let s = &f.reports["branch"].statuses;
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].severity, Severity::GoodService);
        assert!(
            s[0].full_coverage_stats.is_some(),
            "the counts are still shown"
        );
        // The rate tiers on the same run are enforced as before.
        assert_eq!(
            record(&records, "severe").basis,
            Some(EscalationBasis::Rate)
        );
        assert!(record(&records, "severe").enforced);
        assert_eq!(
            f.reports["severe"].statuses[0].severity,
            Severity::SevereDelays
        );
    }

    #[test]
    fn the_sparse_allowlist_enforces_sparse_verdicts_only() {
        let mut f = sparse_fixture();
        let mut s = settings(WindowMode::Enforce, "");
        s.sparse_allowlist = Allowlist::parse("branch,severe");
        let records = run(&mut f, &s, true);
        let b = record(&records, "branch");
        assert!(b.enforced && b.in_allowlist);
        let st = &f.reports["branch"].statuses[0];
        assert_eq!(st.severity, Severity::PartSuspended);
        assert_eq!(st.data_quality, DataQuality::TrustInferred);
        assert_eq!(
            st.reason,
            "All 3 trains due in the last hour were cancelled."
        );
        assert!(
            st.full_coverage_stats.is_none(),
            "outside the main allowlist the legacy counts stay"
        );
        // "severe" is in the sparse list but its verdict is rate-based: the
        // sparse allowlist does not enforce it.
        let r = record(&records, "severe");
        assert!(!r.enforced && !r.in_allowlist);
        assert_eq!(
            f.reports["severe"].statuses[0].severity,
            Severity::GoodService
        );
    }

    #[test]
    fn shadow_records_the_sparse_basis_and_changes_nothing() {
        let mut f = sparse_fixture();
        let before = serde_json::to_value(&f.reports).unwrap();
        let mut s = settings(WindowMode::Shadow, "*");
        s.sparse_allowlist = Allowlist::All;
        let records = run(&mut f, &s, true);
        assert_eq!(serde_json::to_value(&f.reports).unwrap(), before);
        let b = record(&records, "branch");
        assert_eq!(b.basis, Some(EscalationBasis::SparseAllCancelled));
        assert!(!b.enforced);
        assert_eq!(record(&records, "minor").basis, Some(EscalationBasis::Rate));
    }

    struct Fixture {
        reports: HashMap<String, LineStatusReport>,
        lines: HashMap<String, LineDefinition>,
        windows: HashMap<String, LineWindows>,
    }

    /// Three lines: "severe" (7 of 12 late -> Severe Delays), "minor" (3 of
    /// 12 late -> Minor Delays), "incident" (an incident at Minor Delays,
    /// window Severe).
    fn fixture() -> Fixture {
        let mut reports = HashMap::new();
        reports.insert(
            "severe".to_string(),
            report(
                "severe",
                vec![status(
                    Severity::GoodService,
                    DataQuality::LdbwsInferred,
                    "Good Service",
                )],
            ),
        );
        reports.insert(
            "minor".to_string(),
            report(
                "minor",
                vec![status(
                    Severity::GoodService,
                    DataQuality::LdbwsInferred,
                    "Good Service",
                )],
            ),
        );
        reports.insert(
            "incident".to_string(),
            report(
                "incident",
                vec![status(
                    Severity::MinorDelays,
                    DataQuality::Knowledgebase,
                    "Signalling problems.",
                )],
            ),
        );
        let lines = ["severe", "minor", "incident"]
            .iter()
            .map(|id| ((*id).to_string(), line(id, false)))
            .collect();
        let mut windows = HashMap::new();
        windows.insert("severe".to_string(), window("severe", 12, 7, 0));
        windows.insert("minor".to_string(), window("minor", 12, 3, 0));
        windows.insert("incident".to_string(), window("incident", 12, 7, 0));
        Fixture {
            reports,
            lines,
            windows,
        }
    }

    fn run(f: &mut Fixture, s: &WindowSettings, enabled_default: bool) -> Vec<VerdictRecord> {
        apply_windows(
            &mut f.reports,
            &f.lines,
            &f.windows,
            &Defaults::default(),
            s,
            enabled_default,
            now(),
        )
    }

    fn record<'a>(records: &'a [VerdictRecord], line: &str) -> &'a VerdictRecord {
        records.iter().find(|r| r.line_id == line).unwrap()
    }

    #[test]
    fn off_changes_nothing_and_records_nothing() {
        let mut f = fixture();
        let before = serde_json::to_value(&f.reports).unwrap();
        assert!(run(&mut f, &settings(WindowMode::Off, "*"), true).is_empty());
        assert_eq!(serde_json::to_value(&f.reports).unwrap(), before);
    }

    /// Shadow: no `LineStatus` field changes, but every line's verdict --
    /// including the lower-tier would-escalate, flagged below the gate --
    /// is recorded.
    #[test]
    fn shadow_changes_no_status_but_records_would_escalate() {
        let mut f = fixture();
        let before = serde_json::to_value(&f.reports).unwrap();
        let records = run(&mut f, &settings(WindowMode::Shadow, "*"), true);
        assert_eq!(serde_json::to_value(&f.reports).unwrap(), before);
        assert_eq!(records.len(), 3);
        let severe = record(&records, "severe");
        assert_eq!(severe.would_escalate_to, Some(Severity::SevereDelays));
        assert!(!severe.below_min_rank && !severe.enforced);
        let minor = record(&records, "minor");
        assert_eq!(minor.would_escalate_to, Some(Severity::MinorDelays));
        assert!(
            minor.below_min_rank,
            "Minor Delays is below the Severe gate"
        );
        assert!(records.iter().all(|r| r.mode == WindowMode::Shadow));
    }

    #[test]
    fn enforce_with_the_default_empty_allowlist_enforces_nothing() {
        let mut f = fixture();
        let before = serde_json::to_value(&f.reports).unwrap();
        let records = run(&mut f, &settings(WindowMode::Enforce, ""), true);
        assert_eq!(serde_json::to_value(&f.reports).unwrap(), before);
        assert!(records.iter().all(|r| !r.enforced && !r.in_allowlist));
        assert_eq!(records.len(), 3, "still recorded");
    }

    #[test]
    fn enforce_escalates_ldbws_inferred_as_trust_inferred_and_annotates_incidents() {
        let mut f = fixture();
        let records = run(&mut f, &settings(WindowMode::Enforce, "*"), true);
        let s = &f.reports["severe"].statuses[0];
        assert_eq!(s.severity, Severity::SevereDelays);
        assert_eq!(s.data_quality, DataQuality::TrustInferred);
        assert_eq!(s.reason, "7 of 12 trains due in the last hour were late.");
        assert!(matches!(
            s.full_coverage_availability,
            FullCoverageAvailability::Available(SampleStats {
                total: 12,
                delayed: 7,
                ..
            })
        ));
        assert!(record(&records, "severe").enforced);

        let i = &f.reports["incident"].statuses[0];
        assert_eq!(i.severity, Severity::SevereDelays);
        assert_eq!(i.data_quality, DataQuality::Knowledgebase);
        assert_eq!(
            i.reason,
            "Signalling problems. (train-running data shows: 7 of 12 trains due in the last hour were late.)"
        );

        // Minor Delays is below the rank-4 gate: recorded, not applied,
        // though the counts are shown.
        let m = &f.reports["minor"].statuses[0];
        assert_eq!(m.severity, Severity::GoodService);
        assert_eq!(m.data_quality, DataQuality::LdbwsInferred);
        assert!(m.full_coverage_stats.is_some());
        let minor = record(&records, "minor");
        assert!(minor.below_min_rank && !minor.enforced);
    }

    #[test]
    fn enforce_lowers_nothing_and_leaves_lines_outside_the_allowlist_alone() {
        let mut f = fixture();
        f.reports.get_mut("severe").unwrap().statuses[0] = status(
            Severity::PartSuspended,
            DataQuality::Knowledgebase,
            "Landslip.",
        );
        let records = run(
            &mut f,
            &settings(WindowMode::Enforce, "severe,incident"),
            true,
        );
        let s = &f.reports["severe"].statuses[0];
        assert_eq!(s.severity, Severity::PartSuspended, "never demoted");
        assert_eq!(s.reason, "Landslip.");
        assert_eq!(record(&records, "severe").would_escalate_to, None);
        let m = &f.reports["minor"].statuses[0];
        assert!(
            m.full_coverage_stats.is_none(),
            "outside the allowlist: untouched"
        );
        assert!(!record(&records, "minor").in_allowlist);
    }

    #[test]
    fn enforce_needs_the_line_to_be_full_coverage_enabled() {
        let mut f = fixture();
        let records = run(&mut f, &settings(WindowMode::Enforce, "*"), false);
        assert_eq!(
            f.reports["severe"].statuses[0].severity,
            Severity::GoodService
        );
        assert!(records.iter().all(|r| !r.enforced));
        f.lines.get_mut("severe").unwrap().full_coverage_enabled = true;
        run(&mut f, &settings(WindowMode::Enforce, "*"), false);
        assert_eq!(
            f.reports["severe"].statuses[0].severity,
            Severity::SevereDelays
        );
    }

    #[test]
    fn a_lower_rank_gate_enforces_minor_delays() {
        let mut f = fixture();
        let mut s = settings(WindowMode::Enforce, "*");
        s.min_rank = 3;
        run(&mut f, &s, true);
        assert_eq!(
            f.reports["minor"].statuses[0].severity,
            Severity::MinorDelays
        );
    }

    #[test]
    fn a_stale_partial_or_feed_stale_window_is_pending_and_escalates_nothing() {
        for spoil in [
            |w: &mut FullCoverageWindowStatsRow| w.partial = true,
            |w: &mut FullCoverageWindowStatsRow| w.feed_stale = true,
            |w: &mut FullCoverageWindowStatsRow| w.computed_at -= chrono::Duration::minutes(5),
        ] {
            let mut f = fixture();
            spoil(
                &mut f
                    .windows
                    .get_mut("severe")
                    .unwrap()
                    .recent
                    .as_mut()
                    .unwrap()
                    .row,
            );
            let records = run(&mut f, &settings(WindowMode::Enforce, "*"), true);
            let s = &f.reports["severe"].statuses[0];
            assert_eq!(s.severity, Severity::GoodService);
            assert_eq!(
                s.full_coverage_availability,
                FullCoverageAvailability::Pending
            );
            assert!(matches!(
                record(&records, "severe").verdict,
                WindowVerdict::Ineligible(_)
            ));
        }
        // No window at all: Pending, nothing recorded.
        let mut f = fixture();
        f.windows.remove("severe");
        let records = run(&mut f, &settings(WindowMode::Enforce, "*"), true);
        assert!(records.iter().all(|r| r.line_id != "severe"));
        assert_eq!(
            f.reports["severe"].statuses[0].full_coverage_availability,
            FullCoverageAvailability::Pending
        );
    }

    /// A planned-works notice whose validity covers now but which is
    /// bounded (`is_now = false`): not in effect.
    fn planned_notice(severity: Severity) -> LineStatus {
        let mut s = status(
            severity,
            DataQuality::Planned,
            "Buses replace late night trains.",
        );
        s.validity = ValidityPeriod {
            from_date: now() - chrono::Duration::hours(10),
            to_date: Some(now() + chrono::Duration::days(3)),
            is_now: false,
        };
        s
    }

    /// 2026-10-02: a planned notice that is not in effect is neither the
    /// line's current severity nor raised. Alone on the line, the enforced
    /// verdict becomes its own `TrustInferred` status; the notice stays as
    /// published.
    #[test]
    fn enforce_never_raises_a_planned_notice_that_is_not_in_effect() {
        let mut f = fixture();
        // A Bus Service notice (rank 4) would otherwise hide the Severe
        // verdict entirely.
        f.reports.get_mut("severe").unwrap().statuses = vec![planned_notice(Severity::BusService)];
        let records = run(&mut f, &settings(WindowMode::Enforce, "*"), true);
        let r = record(&records, "severe");
        assert_eq!(r.current_severity, Severity::GoodService);
        assert_eq!(r.would_escalate_to, Some(Severity::SevereDelays));
        assert!(r.enforced);
        let statuses = &f.reports["severe"].statuses;
        assert_eq!(statuses.len(), 2, "{statuses:?}");
        assert_eq!(statuses[0].severity, Severity::BusService);
        assert_eq!(statuses[0].reason, "Buses replace late night trains.");
        assert_eq!(statuses[0].data_quality, DataQuality::Planned);
        assert!(statuses[0].full_coverage_stats.is_some(), "counts shown");
        assert_eq!(statuses[1].severity, Severity::SevereDelays);
        assert_eq!(statuses[1].data_quality, DataQuality::TrustInferred);
        assert_eq!(
            statuses[1].reason,
            "7 of 12 trains due in the last hour were late."
        );
        assert!(statuses[1].validity.is_now);

        // Below the rank gate, nothing is added.
        let mut f = fixture();
        f.reports.get_mut("minor").unwrap().statuses = vec![planned_notice(Severity::MinorDelays)];
        let records = run(&mut f, &settings(WindowMode::Enforce, "*"), true);
        assert_eq!(f.reports["minor"].statuses.len(), 1);
        assert!(!record(&records, "minor").enforced);
    }

    /// Beside an in-effect status, the in-effect one is raised and the
    /// not-in-effect notice left alone; shadow records the same current
    /// severity and changes nothing.
    #[test]
    fn only_in_effect_statuses_are_raised_and_counted_as_current() {
        let mut f = fixture();
        f.reports
            .get_mut("severe")
            .unwrap()
            .statuses
            .push(planned_notice(Severity::PartSuspended));
        let before = serde_json::to_value(&f.reports).unwrap();
        let records = run(&mut f, &settings(WindowMode::Shadow, "*"), true);
        assert_eq!(serde_json::to_value(&f.reports).unwrap(), before);
        let r = record(&records, "severe");
        assert_eq!(r.current_severity, Severity::GoodService);
        assert_eq!(r.would_escalate_to, Some(Severity::SevereDelays));

        run(&mut f, &settings(WindowMode::Enforce, "*"), true);
        let statuses = &f.reports["severe"].statuses;
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].severity, Severity::SevereDelays);
        assert_eq!(statuses[0].data_quality, DataQuality::TrustInferred);
        assert_eq!(statuses[1].severity, Severity::PartSuspended);
        assert_eq!(statuses[1].reason, "Buses replace late night trains.");
    }

    /// A custom line is not in the full-coverage set, so `enforce` with `*`
    /// neither marks it Pending nor counts it as a missing window.
    #[test]
    fn custom_lines_are_left_out_of_full_coverage() {
        let mut f = fixture();
        let custom = "custom-milton-keynes-drain";
        f.lines.insert(custom.to_string(), line(custom, false));
        f.reports.insert(
            custom.to_string(),
            report(
                custom,
                vec![status(
                    Severity::GoodService,
                    DataQuality::LdbwsInferred,
                    "Good Service",
                )],
            ),
        );
        let custom_ids: HashSet<String> = [custom.to_string()].into_iter().collect();
        let catalogue = full_coverage_lines(&f.lines, &custom_ids);
        assert!(!catalogue.contains_key(custom));
        assert_eq!(catalogue.len(), 3);
        let records = apply_windows(
            &mut f.reports,
            &catalogue,
            &f.windows,
            &Defaults::default(),
            &settings(WindowMode::Enforce, "*"),
            true,
            now(),
        );
        assert!(records.iter().all(|r| r.line_id != custom));
        assert_eq!(
            f.reports[custom].statuses[0].full_coverage_availability,
            FullCoverageAvailability::NotEnabled
        );
        // Without the filter, the same custom line is Pending ("missing").
        apply_windows(
            &mut f.reports,
            &f.lines,
            &f.windows,
            &Defaults::default(),
            &settings(WindowMode::Enforce, "*"),
            true,
            now(),
        );
        assert_eq!(
            f.reports[custom].statuses[0].full_coverage_availability,
            FullCoverageAvailability::Pending
        );
    }

    #[test]
    fn allowlist_parsing() {
        assert_eq!(Allowlist::parse(""), Allowlist::Lines(HashSet::new()));
        assert_eq!(Allowlist::parse(" * "), Allowlist::All);
        assert!(Allowlist::parse("a, b").contains("b"));
        assert!(!Allowlist::parse("a, b").contains("c"));
    }

    // --- DB-gated ---

    async fn pool() -> PgPool {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        sqlx::postgres::PgPoolOptions::new()
            .connect(&url)
            .await
            .expect("connect to postgres")
    }

    async fn insert_window(
        pool: &PgPool,
        line_id: &str,
        kind: &str,
        bucket: DateTime<Utc>,
        total: i32,
    ) {
        sqlx::query(
            "INSERT INTO full_coverage_line_window_stats
                (line_id, window_kind, bucket_start, service_date, window_start, window_end,
                 computed_at, total, on_time, delayed, cancelled_explicit, cancelled_presumed,
                 skipped, pending, unobserved, avg_delay_minutes, relevance, presumed_enabled,
                 partial, feed_stale, stats_version)
             VALUES ($1, $2, $3, $3::date, $3, $3, $3, $4, $4, 0, 0, 0, 0, 0, 0, 0, 'full',
                     true, false, false, 2)",
        )
        .bind(line_id)
        .bind(kind)
        .bind(bucket)
        .bind(total)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn cleanup(pool: &PgPool) {
        for table in [
            "full_coverage_line_window_stats",
            "full_coverage_window_verdicts",
        ] {
            sqlx::query(&format!(
                "DELETE FROM {table} WHERE line_id LIKE 'ztest-fcw-%'"
            ))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    /// The newest bucket per line and kind wins; a bucket more than 30
    /// minutes old is not read at all; verdicts upsert per bucket; prune
    /// deletes by age from both tables.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p aggregator \
                full_coverage_window -- --ignored --test-threads=1`"]
    async fn load_picks_the_newest_bucket_and_ignores_old_ones_and_prune_by_age() {
        let pool = pool().await;
        cleanup(&pool).await;
        let now = Utc::now();
        let b = |mins: i64| {
            let at = now - chrono::Duration::minutes(mins);
            at - chrono::Duration::seconds(at.timestamp() % 900)
        };
        insert_window(&pool, "ztest-fcw-a", "recent", b(20), 5).await;
        insert_window(&pool, "ztest-fcw-a", "recent", b(0), 9).await;
        insert_window(&pool, "ztest-fcw-a", "day_to_date", b(0), 50).await;
        insert_window(&pool, "ztest-fcw-old", "recent", b(60), 7).await;
        insert_window(
            &pool,
            "ztest-fcw-ancient",
            "recent",
            now - chrono::Duration::days(20),
            1,
        )
        .await;

        let windows = load_full_coverage_windows(&pool, now).await.unwrap();
        let a = &windows["ztest-fcw-a"];
        assert_eq!(a.recent.as_ref().unwrap().row.counts.total, 9);
        assert_eq!(a.day_to_date.as_ref().unwrap().row.counts.total, 50);
        assert!(!windows.contains_key("ztest-fcw-old"));

        let record = VerdictRecord {
            line_id: "ztest-fcw-a".to_string(),
            bucket_start: b(0),
            evaluated_at: now,
            window_computed_at: now,
            mode: WindowMode::Shadow,
            verdict: WindowVerdict::Escalate {
                severity: Severity::MinorDelays,
                reason: "3 of 12 trains due in the last hour were late.".to_string(),
                basis: EscalationBasis::Rate,
            },
            basis: Some(EscalationBasis::Rate),
            current_severity: Severity::GoodService,
            would_escalate_to: Some(Severity::MinorDelays),
            below_min_rank: true,
            in_allowlist: false,
            enforced: false,
        };
        assert_eq!(
            write_verdicts(&pool, std::slice::from_ref(&record))
                .await
                .unwrap(),
            1
        );
        let basis: Option<String> = sqlx::query_scalar(
            "SELECT basis FROM full_coverage_window_verdicts WHERE line_id = 'ztest-fcw-a'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(basis.as_deref(), Some("rate"));
        let mut sparse = record.clone();
        sparse.verdict = WindowVerdict::Escalate {
            severity: Severity::PartSuspended,
            reason: "All 2 trains due in the last hour were cancelled.".to_string(),
            basis: EscalationBasis::SparseAllCancelled,
        };
        sparse.basis = Some(EscalationBasis::SparseAllCancelled);
        write_verdicts(&pool, &[sparse]).await.unwrap();
        let basis: Option<String> = sqlx::query_scalar(
            "SELECT basis FROM full_coverage_window_verdicts WHERE line_id = 'ztest-fcw-a'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(basis.as_deref(), Some("sparse_all_cancelled"));
        let mut again = record.clone();
        again.verdict = WindowVerdict::Good;
        again.basis = None;
        again.would_escalate_to = None;
        again.below_min_rank = false;
        write_verdicts(&pool, &[again]).await.unwrap();
        let (verdict, would, basis): (String, Option<i16>, Option<String>) = sqlx::query_as(
            "SELECT verdict, would_escalate_to, basis FROM full_coverage_window_verdicts WHERE line_id = 'ztest-fcw-a'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            (verdict.as_str(), would, basis),
            ("good", None, None),
            "latest evaluation wins"
        );

        let pruned = prune_full_coverage_window_stats(&pool, 14).await.unwrap();
        assert!(pruned >= 1);
        let left: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM full_coverage_line_window_stats WHERE line_id = 'ztest-fcw-ancient'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(left, 0);
        cleanup(&pool).await;
    }
}
