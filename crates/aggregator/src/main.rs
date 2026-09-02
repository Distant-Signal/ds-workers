//! `aggregator`: periodically recomputes every line's status from
//! incidents + LDBWS samples and writes it to `line_status`/
//! `line_status_history`. See
//! `docs/superpowers/specs/2026-07-06-aggregator-read-api-design.md` for
//! the original design, and
//! `docs/superpowers/specs/2026-07-09-custom-lines-and-blended-stats-design.md`
//! for the custom-lines addition.

mod aggregation;
mod config;
mod dedup;
mod matcher;
mod queries;
mod segments;

use std::collections::HashMap;
use std::time::Duration;

use clap::Parser;
use common::{Defaults, LineDefinition, LineStatusReport};
use config::Config;
use dedup::SeenServiceLedger;
use segments::SegmentRegistry;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    if config.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
    }
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;

    let static_lines: HashMap<String, LineDefinition> = config
        .lines
        .iter()
        .map(|l| (l.id.clone(), l.clone()))
        .collect();
    tracing::info!(count = static_lines.len(), "loaded static line catalogue");

    let defaults = Defaults::default();

    // Lives for the whole process, threaded into every cycle -- this is
    // exactly what makes the dedup ledger "in-memory, restart-scoped"
    // rather than per-cycle: a service seen on cycle N must still be
    // recognized as already-counted on cycle N+1. See `dedup`'s module
    // docs for why a process-lifetime, non-persisted ledger is judged
    // sufficient.
    let mut dedup_ledger = SeenServiceLedger::new();

    let mut interval = tokio::time::interval(Duration::from_secs(config.poll_interval_secs));

    loop {
        interval.tick().await;

        let cycle_start = std::time::Instant::now();
        let result = run_cycle(
            &pool,
            &static_lines,
            &defaults,
            config.history_retention_days,
            config.daily_stats_retention_days,
            &mut dedup_ledger,
        )
        .await;
        metrics::histogram!(common::metrics::metric_name(
            "aggregator_cycle_duration_seconds"
        ))
        .record(cycle_start.elapsed().as_secs_f64());

        if let Err(err) = result {
            tracing::error!(error = ?err, "aggregation cycle failed; will retry next interval");
        }
    }
}

async fn run_cycle(
    pool: &sqlx::PgPool,
    static_lines: &HashMap<String, LineDefinition>,
    defaults: &Defaults,
    retention_days: i64,
    daily_stats_retention_days: i64,
    dedup_ledger: &mut SeenServiceLedger,
) -> anyhow::Result<()> {
    let custom_lines = queries::load_custom_lines(pool).await?;
    let lines = aggregation::merge_custom_lines(static_lines, custom_lines);
    let registry = SegmentRegistry::new(&lines);

    let incidents = queries::load_incidents(pool).await?;
    let samples = queries::load_station_samples(pool).await?;

    let reports = aggregation::aggregate(&lines, &incidents, &samples, &registry, defaults);

    for report in reports.values() {
        queries::write_line_status(pool, report).await?;
    }

    let current_line_ids: Vec<String> = lines.keys().cloned().collect();
    let removed = queries::prune_removed_lines(pool, &current_line_ids).await?;

    let pruned = queries::prune_history(pool, retention_days).await?;

    // Per-service dedup pass, folded together with the daily-stats write:
    // `dedup::dedup_new_sample_stats` is STATEFUL (it mutates `dedup_ledger`
    // via `mark_seen`), so it must be called AT MOST ONCE per line per
    // cycle -- calling it twice for the same line would make the second
    // call see everything as already-seen and silently under-report. The
    // gate for whether a line gets a `record_daily_stats` write at all
    // (independent of whether dedup finds anything NEW) is computed once
    // per line via `lines_with_sample_coverage`, not once per status --
    // see that function's doc for why iterating `report.statuses` would
    // double-count.
    let today = queries::london_calendar_day(chrono::Utc::now());
    let mut new_services_this_cycle: u64 = 0;
    let mut daily_stats_recorded = 0u64;
    for (line_id, line) in lines_with_sample_coverage(&reports, &lines) {
        let deduped =
            dedup::dedup_new_sample_stats(dedup_ledger, line_id, today, line, &samples, defaults);
        if let Some(ref stats) = deduped {
            new_services_this_cycle += stats.total as u64;
        }
        queries::record_daily_stats(pool, line_id, today, deduped.as_ref()).await?;
        daily_stats_recorded += 1;
    }
    dedup_ledger.prune_before(today);

    let daily_stats_pruned = queries::prune_daily_stats(pool, daily_stats_retention_days).await?;

    metrics::gauge!(common::metrics::metric_name("aggregator_lines_total"))
        .set(reports.len() as f64);
    metrics::gauge!(common::metrics::metric_name("aggregator_incidents_loaded"))
        .set(incidents.len() as f64);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_history_rows_pruned_total"
    ))
    .increment(pruned);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_deduped_new_services_total"
    ))
    .increment(new_services_this_cycle);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_daily_stats_recorded_total"
    ))
    .increment(daily_stats_recorded);
    metrics::counter!(common::metrics::metric_name(
        "aggregator_daily_stats_pruned_total"
    ))
    .increment(daily_stats_pruned);

    tracing::info!(
        lines = reports.len(),
        incidents = incidents.len(),
        removed_lines = removed,
        pruned_history_rows = pruned,
        deduped_new_services = new_services_this_cycle,
        daily_stats_recorded = daily_stats_recorded,
        daily_stats_pruned = daily_stats_pruned,
        "aggregation cycle complete"
    );

    Ok(())
}

/// Selects the `(line_id, &LineDefinition)` pairs that qualify for a
/// `queries::record_daily_stats` write this cycle: a line qualifies when its
/// `LineStatusReport` carries raw `sample_stats` this cycle, i.e. it had ANY
/// raw live coverage, independent of whether dedup later finds anything NEW
/// to contribute (that's `sample_cycles`'s signal -- see
/// `queries::record_daily_stats`'s doc).
///
/// Every status on a report carries an identical `Option<SampleStats>` clone
/// when `Some` (`aggregation.rs`'s Layer 2 and `infer_from_samples` both set
/// it this way), so checking only `report.statuses.first()` is correct and
/// sufficient -- iterating all of `report.statuses` here would call
/// `record_daily_stats` (and, worse, the stateful `dedup_new_sample_stats`)
/// once per status rather than once per line, silently double- (or
/// N-) counting any line with more than one concurrent incident. Pure and
/// synchronous so it's separately testable without a `PgPool`.
fn lines_with_sample_coverage<'a>(
    reports: &HashMap<String, LineStatusReport>,
    lines: &'a HashMap<String, LineDefinition>,
) -> Vec<(&'a str, &'a LineDefinition)> {
    reports
        .values()
        .filter(|report| {
            report
                .statuses
                .first()
                .and_then(|s| s.sample_stats.as_ref())
                .is_some()
        })
        .filter_map(|report| lines.get_key_value(report.id.as_str()))
        .map(|(id, line)| (id.as_str(), line))
        .collect()
}

#[cfg(test)]
mod tests {
    use common::{
        DataQuality, LineStatus, SampleAvailability, SampleStats, Severity, ValidityPeriod,
    };

    use super::*;

    fn line_def(id: &str) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "tube".to_string(),
            category: "tube".to_string(),
            operators: vec![],
            stations: vec![],
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: Default::default(),
            exclusive_segments: vec![],
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
        }
    }

    fn status_with_stats(stats: Option<SampleStats>) -> LineStatus {
        LineStatus {
            severity: Severity::GoodService,
            reason: "Good Service".to_string(),
            validity: ValidityPeriod {
                from_date: chrono::Utc::now(),
                to_date: None,
                is_now: true,
            },
            disruption: None,
            data_quality: DataQuality::default(),
            sample_stats: stats,
            sample_availability: SampleAvailability::NoCoverage,
        }
    }

    fn sample_stats() -> SampleStats {
        SampleStats {
            total: 10,
            delayed: 2,
            cancelled: 1,
            skipped: 0,
            avg_delay_minutes: 3.5,
        }
    }

    #[test]
    fn counts_a_line_with_two_concurrent_incidents_exactly_once() {
        let lines: HashMap<String, LineDefinition> =
            [("central".to_string(), line_def("central"))].into();
        let reports: HashMap<String, LineStatusReport> = [(
            "central".to_string(),
            LineStatusReport {
                id: "central".to_string(),
                name: "Central".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                // Two concurrent incidents on the same line -- both carry
                // the identical `Some(SampleStats)` clone, matching how
                // aggregation.rs actually populates multi-status reports.
                statuses: vec![
                    status_with_stats(Some(sample_stats())),
                    status_with_stats(Some(sample_stats())),
                ],
            },
        )]
        .into();

        let selected = lines_with_sample_coverage(&reports, &lines);

        assert_eq!(
            selected.len(),
            1,
            "expected exactly one entry per line regardless of status count"
        );
        assert_eq!(selected[0].0, "central");
    }

    #[test]
    fn excludes_a_line_with_no_sample_stats_on_any_status() {
        let lines: HashMap<String, LineDefinition> =
            [("victoria".to_string(), line_def("victoria"))].into();
        let reports: HashMap<String, LineStatusReport> = [(
            "victoria".to_string(),
            LineStatusReport {
                id: "victoria".to_string(),
                name: "Victoria".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                statuses: vec![status_with_stats(None), status_with_stats(None)],
            },
        )]
        .into();

        let selected = lines_with_sample_coverage(&reports, &lines);

        assert!(selected.is_empty());
    }

    #[test]
    fn skips_a_report_with_no_matching_line_definition_without_panicking() {
        // Report present but its line_id isn't in `lines` -- shouldn't panic,
        // should just be silently skipped (defensive; reports are normally
        // derived from lines).
        let lines: HashMap<String, LineDefinition> = HashMap::new();
        let reports: HashMap<String, LineStatusReport> = [(
            "jubilee".to_string(),
            LineStatusReport {
                id: "jubilee".to_string(),
                name: "Jubilee".to_string(),
                mode_name: "tube".to_string(),
                operators: vec![],
                statuses: vec![status_with_stats(Some(sample_stats()))],
            },
        )]
        .into();

        let selected = lines_with_sample_coverage(&reports, &lines);

        assert!(selected.is_empty());
    }

    #[test]
    fn handles_empty_reports_without_panicking() {
        let lines: HashMap<String, LineDefinition> =
            [("bakerloo".to_string(), line_def("bakerloo"))].into();
        let reports: HashMap<String, LineStatusReport> = HashMap::new();

        let selected = lines_with_sample_coverage(&reports, &lines);

        assert!(selected.is_empty());
    }
}
