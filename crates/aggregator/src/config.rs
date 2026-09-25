use clap::Parser;
use common::config::{LineCatalogue, parse_lines};

/// Rejects a negative retention value, loudly, at startup.
///
/// Signal Box Audit Low finding: every `*_retention_days`/
/// `*_retention_hours` field below feeds a `queries.rs` prune query shaped
/// like `WHERE <col> < (CURRENT_DATE - $1::int)` or
/// `NOW() - ($1 || ' days')::interval`. Subtracting a NEGATIVE value flips
/// the arithmetic into ADDITION -- `CURRENT_DATE - (-7)` is 7 days in the
/// FUTURE, so `day < CURRENT_DATE - (-7)` matches every row up to and
/// including today, deleting current data instead of the old/stale data
/// the cutoff is supposed to target. A negative value could only ever
/// reach here via a typo'd or deliberately hostile `--*-retention-days`/
/// env var -- there is no legitimate reason to configure one -- so this
/// fails startup outright rather than silently accepting it, the same
/// "fail loud on bad config" convention `parse_lines`/`parse_stanox_crs`
/// already use for THEIR own value_parsers (see `common::config::parse_lines`'s
/// doc comment).
fn non_negative_retention(s: &str) -> anyhow::Result<i64> {
    let value: i64 = s
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid retention value {s:?}: {e}"))?;
    anyhow::ensure!(
        value >= 0,
        "retention value must not be negative, got {value} -- a negative retention window \
         becomes a FUTURE cutoff that deletes today's data instead of old data"
    );
    Ok(value)
}

/// CLI/env configuration for the `aggregator` service.
#[derive(Debug, Parser)]
pub struct Config {
    #[arg(long, env)]
    pub database_url: String,

    /// Directory of line-catalogue TOML files, loaded once at startup.
    /// Same default as the `api` crate's `--lines-dir`, since both load
    /// the same catalogue independently (see the plan's Global
    /// Constraints on keeping this behind a narrow, swappable interface).
    #[arg(long = "lines-dir", env = "LINES_DIR", default_value = "/app/lines", value_parser = parse_lines)]
    pub lines: LineCatalogue,

    /// DESIGN.md §4 target cadence is "every 30-60s"; 60 is the
    /// conservative end.
    #[arg(long, env, default_value_t = 60)]
    pub poll_interval_secs: u64,

    /// How long to keep `line_status_history` rows before pruning them.
    #[arg(long, env, default_value_t = 7, value_parser = non_negative_retention)]
    pub history_retention_days: i64,

    /// How long to keep `line_status_daily_stats` rows before pruning them.
    /// `line_status_daily_stats` is fed by LDBWS-derived `StationSample`
    /// data (see `common::SampleStats`'s doc comment), and RDM's Live
    /// Departure Board licence (Schedule 1 §9) requires deleting all data
    /// received within 1 year. 300 leaves real margin under that 365-day
    /// ceiling to comfortably absorb poll/prune cadence, mirroring
    /// `history_retention_days`'s shape exactly. See
    /// docs/superpowers/plans/2026-09-01-ldbws-data-retention.md (Task 2)
    /// for the full finding and remediation plan -- the exact number below
    /// 365 remains a product/UX call (how far back the Trends tab should
    /// let a user scroll, docs/superpowers/specs/2026-08-31-line-history-graphics-design.md,
    /// Open question 1), this default just guarantees a ceiling exists.
    #[arg(long, env, default_value_t = 300, value_parser = non_negative_retention)]
    pub daily_stats_retention_days: i64,

    /// How long to keep `line_status_half_hourly_stats` rows before
    /// pruning them.
    ///
    /// Bumped from 48 hours to 840 (35 days) by
    /// docs/superpowers/specs/2026-09-05-configurable-trend-granularity-design.md
    /// Decision 3: this table is now also read directly (30-minute
    /// granularity) AND grouped into 1-hour/6-hour buckets
    /// (`crates/api/src/data/queries.rs`'s `sub_daily_stats_for_range`) by
    /// the History page's Trends tab, over the user's actual selected
    /// range -- up to the existing 30-day `RangePreset` ceiling, plus a
    /// 5-day buffer. 48 hours was sized only for the line-info page's
    /// fixed rolling-24h embed (`HalfHourlyTrendsResults`), which still
    /// only ever requests the most recent 24 hours regardless of this
    /// value -- this bump is purely additive for that view, unchanged
    /// behavior.
    ///
    /// This table is fed the SAME LDBWS-derived `SampleStats` value as
    /// `line_status_daily_stats` every cycle (`main.rs`'s `run_cycle`), so
    /// the same RDM Live Departure Board licence lineage applies: the
    /// repo owner confirmed directly that "half-hourly is still fine as
    /// long as we aren't retaining for more than 300 days" -- the same
    /// 300-day ceiling `daily_stats_retention_days` already uses. 840
    /// hours (35 days) clears that with enormous margin, mirroring
    /// `daily_stats_retention_days`'s own "real margin under a hard
    /// compliance ceiling, not a number picked to just barely clear it"
    /// reasoning.
    ///
    /// This field's UNIT is unchanged from the table's original
    /// 1-hour-bucket era: retention is measured in wall-clock hours, not
    /// bucket count. At 840 hours, storage is ~105 lines x 48 rows/day x
    /// 35 days ~= 176,400 rows -- trivial for Postgres, same order of
    /// magnitude this repo's specs have called "trivial" elsewhere.
    #[arg(long, env, default_value_t = 840, value_parser = non_negative_retention)]
    pub half_hourly_stats_retention_hours: i64,

    /// How long to keep `trust_event_backlog` rows before pruning them.
    ///
    /// DEFAULT IS 1 DAY, DELIBERATELY. The design spec this table
    /// implements
    /// (docs/superpowers/specs/2026-09-05-trust-event-backlog-design.md,
    /// Decision 5) found only a secondhand, imprecisely sourced citation
    /// that TRUST/Train Movements retention is unrestricted by licence --
    /// genuinely favorable evidence, but weaker than the quoted-clause
    /// standard this repo holds itself to for LDBWS
    /// (docs/superpowers/plans/2026-09-01-ldbws-data-retention.md). A
    /// human must confirm TRUST's real licence terms directly with RDM
    /// before this value is ever configured above 1 in a real production
    /// deployment -- do not bump this default, or any Helm values.yaml
    /// default derived from it, without that confirmation happening
    /// first. See
    /// docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md's
    /// own "Scope decision: retention tier and the licensing safeguard"
    /// section.
    #[arg(long, env, default_value_t = 1, value_parser = non_negative_retention)]
    pub trust_event_backlog_retention_days: i64,

    /// How long to keep `trains` (and, via CASCADE,
    /// `train_movement_events`/`train_current_state`) rows before pruning
    /// them. Reuses the past-dates sibling design's own 30-day figure
    /// (docs/superpowers/specs/2026-09-06-schedule-line-population-past-dates-design.md)
    /// rather than inventing a second number for a structurally similar
    /// concern -- see this plan's Global Constraints. Unlike
    /// `trust_event_backlog_retention_days`'s cautious default-1-until-licence-
    /// confirmed posture, this can default straight to 30 from day one: the
    /// RDM licensing question that caution exists to enforce has already been
    /// confirmed clear by the repo owner for this data.
    #[arg(long, env, default_value_t = 30, value_parser = non_negative_retention)]
    pub trains_retention_days: i64,

    /// How long to keep `schedule_destination_departures` rows before
    /// pruning them, in whole service dates.
    ///
    /// **8, not 1**, and the difference matters. 1 would match
    /// `trust_event_backlog_retention_days` above, but that default exists
    /// to enforce an RDM licensing safeguard for TRUST Train Movements
    /// data -- a constraint that does not apply to CIF SCHEDULE timetable
    /// data at all. Copying the number would copy a restriction that isn't
    /// real here while giving up the margin that is: `service_date` is a
    /// RAIL day, which crosses midnight, and a CIF delivery can land late,
    /// so a too-tight window can delete a still-searchable day shortly
    /// before its replacement arrives.
    ///
    /// `GET /public/trains/search` can now search up to 7 days INTO THE
    /// PAST (`crates/api/src/routes/trains.rs::SEARCH_WINDOW_BACKWARD_DAYS`)
    /// -- unlike the "nothing reads a past service date" reasoning this
    /// default used to be justified by, this window now has a real reader.
    /// 8, not 7: one extra day of safety margin beyond the search window,
    /// the same reasoning this field's default has always used (previously
    /// "2 rather than 1" for the identical reason), so a boundary date
    /// can't flake into a 404 if `aggregator`'s prune cycle runs against
    /// that date moments before a request for it lands. See
    /// docs/superpowers/specs/2026-09-09-trains-search-multi-day-design.md
    /// §1.2/§3.
    ///
    /// At ~377,000 rows per day, 8 days is ~3,016,000 rows and roughly
    /// 600MB with the index (§1.3 of the design doc above; that section's
    /// own ~675MB figure is the 9-day TOTAL resident size once today's own
    /// unretained row is counted alongside this 8-day backward window, not
    /// a second estimate of this field's own retained span).
    ///
    /// Unlike `trust_event_backlog_retention_days` there is deliberately NO
    /// warning emitted when this is configured higher: nothing legal is at
    /// stake, only disk.
    #[arg(long, env, default_value_t = 8, value_parser = non_negative_retention)]
    pub schedule_destination_departures_retention_days: i64,

    /// Retention window, in days of `service_date`, for the OTHER three
    /// CIF-derived published products: `schedule_calling_points_full`,
    /// `schedule_network_departures` and `schedule_line_population`.
    ///
    /// **None of the three had a pruning job anywhere in this repo until
    /// 2026-09-25, and all three genuinely accrue.** The reasoning that
    /// justified leaving them alone -- "their wholesale replace is scoped per
    /// `(crs, service_date)` / `(line_id, service_date)` over a bounded key
    /// space, so steady-state size is trivial" -- was only ever half true: the
    /// KEY space is bounded, but `service_date` is not one of the bounds.
    /// `schedule-reference` publishes a new `service_date` on every delivery
    /// (and, for `schedule_calling_points_full`, eight forward dates per
    /// cycle), so each product grows by roughly a day's worth of rows per day,
    /// forever. `schedule_calling_points_full` is the one that actually hurts:
    /// it is one row per calling point of every non-cancelled schedule --
    /// realistically 2-3x `schedule_destination_departures`' ~377,000 rows per
    /// date, since it includes the passing points and junction TIPLOCs that
    /// product excludes.
    ///
    /// 8, matching `schedule_destination_departures_retention_days` above, for
    /// the same reason: it is one day past the 7-day window
    /// `crates/api/src/routes/trains.rs`'s `SEARCH_WINDOW_BACKWARD_DAYS` and
    /// `schedule-reference`'s own `TRIP_PLANNING_FORWARD_DAYS` both work in, so
    /// a boundary date cannot flake into a 404 because a prune ran moments
    /// before a request for it landed.
    ///
    /// **One known consequence, stated rather than hidden:** beyond this
    /// window, `journey::build_journey_stops`' fallback read of
    /// `schedule_calling_points_full` for an old TRACKED train (`trains` rows
    /// live up to `trains_retention_days`, 30) finds nothing. That fallback is
    /// already the second source -- `trains.calling_points`, populated by
    /// schedule-matching, is the primary one -- and keeping ~1M rows/day for 30
    /// days to serve it would cost several GB. If that fallback ever needs to
    /// reach further back, raise this deliberately and budget the disk;
    /// `service_date` partitioning with a partition swap is the right
    /// mitigation before a longer unpartitioned window.
    ///
    /// Like `schedule_destination_departures_retention_days`, and unlike
    /// `trust_event_backlog_retention_days`, nothing legal is at stake in
    /// raising this -- only disk -- so no warning is emitted.
    #[arg(long, env, default_value_t = 8, value_parser = non_negative_retention)]
    pub schedule_derived_products_retention_days: i64,

    /// How long to keep a `trains` row (and its cascaded
    /// `train_movement_events`/`train_current_state` rows) when NO
    /// `train_subscriptions` row references it (`trains_id`) -- i.e.
    /// nobody ever pinned this journey. `trains_retention_days` above
    /// still governs a train with at least one subscription: a real user
    /// tracked that journey, and their history for it should not
    /// disappear sooner just because this shorter tier shipped. This
    /// field only shortens the window for the orphan case -- rows this
    /// service itself resolved from the schedule/TRUST feeds but that no
    /// one is actually watching, which make up the bulk of `trains` at
    /// national scale and have no per-user value once stale. 14 (2
    /// weeks) is comfortably under the existing 30-day
    /// `trains_retention_days` default, and reuses the same already-
    /// confirmed-clear RDM licensing posture that default's own doc
    /// comment cites -- this is a narrower cut of the same data, not a
    /// new licensing question.
    #[arg(long, env, default_value_t = 14, value_parser = non_negative_retention)]
    pub untracked_trains_retention_days: i64,

    /// Port for the aggregator's Prometheus `/metrics` endpoint. See
    /// docs/superpowers/plans/2026-08-29-metrics.md's Global Constraints
    /// for why this differs from api.service.port -- api reuses its
    /// existing HTTP listener, the aggregator has none, so it needs a new
    /// one.
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    /// Whether to start this service's Prometheus `/metrics` listener at
    /// all. Distinct from `metrics_port` (which port to use IF started) --
    /// this is what actually satisfies "metrics.enabled=false leaves the
    /// service working exactly as it does today" (see the Helm chart's
    /// `metrics.enabled` value and this branch's final whole-branch
    /// review, Important finding #2): omitting the containerPort/env/
    /// annotations in the chart alone does not stop the process from
    /// listening, since Kubernetes container ports are purely
    /// declarative.
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,

    /// Global override for `LineDefinition.full_coverage_enabled`
    /// (Decision 3's per-line TOML rollout gate, `crates/common/src/lib.rs`).
    /// When `true`, `aggregation::merge_full_coverage` treats EVERY
    /// catalogued line as full-coverage-enabled, regardless of what its
    /// own `lines/*.toml` entry sets -- a single runtime flag to flip on
    /// full coverage everywhere at once, instead of editing 100+ TOML
    /// files. Default `false` is deliberate: this flag must never
    /// silently change behavior for a deployment that doesn't explicitly
    /// set it, and `true` is never baked in here as the default (that
    /// would require a rebuild to ever revert) -- an operator opts in via
    /// this env var / the Helm chart's `aggregator.fullCoverageEnabledDefault`
    /// value.
    #[arg(long, env, default_value_t = false)]
    pub full_coverage_enabled_default: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines_dir() -> String {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../lines")
            .to_str()
            .expect("utf8 lines dir path")
            .to_string()
    }

    fn minimal_args(extra: &[&str]) -> Vec<String> {
        let mut args = vec![
            "aggregator".to_string(),
            "--database-url".to_string(),
            "postgres://user:pass@localhost/db".to_string(),
            "--lines-dir".to_string(),
            lines_dir(),
        ];
        args.extend(extra.iter().map(|s| s.to_string()));
        args
    }

    #[test]
    fn a_negative_retention_days_value_is_rejected_at_parse_time() {
        // Signal Box Audit Low finding: a negative retention value becomes
        // a FUTURE cutoff (`CURRENT_DATE - (-7)`), which would delete
        // TODAY's data instead of old data. This must fail startup loudly,
        // not silently parse.
        let result = Config::try_parse_from(minimal_args(&["--history-retention-days", "-7"]));
        assert!(
            result.is_err(),
            "a negative history_retention_days must be rejected at parse time"
        );
    }

    #[test]
    fn every_retention_field_rejects_a_negative_value() {
        for flag in [
            "--history-retention-days",
            "--daily-stats-retention-days",
            "--half-hourly-stats-retention-hours",
            "--trust-event-backlog-retention-days",
            "--trains-retention-days",
            "--schedule-destination-departures-retention-days",
            "--schedule-derived-products-retention-days",
            "--untracked-trains-retention-days",
        ] {
            let result = Config::try_parse_from(minimal_args(&[flag, "-1"]));
            assert!(
                result.is_err(),
                "{flag} must reject a negative value, but parsing succeeded"
            );
        }
    }

    #[test]
    fn a_non_negative_retention_days_value_still_parses_normally() {
        let config = Config::try_parse_from(minimal_args(&["--history-retention-days", "0"]))
            .expect("zero is a valid (if aggressive) retention window");
        assert_eq!(config.history_retention_days, 0);
    }

    #[test]
    fn default_retention_values_parse_with_no_overrides() {
        // Regression against accidentally requiring the new value_parser
        // args to be explicitly supplied: every default must still parse
        // cleanly on its own.
        let config =
            Config::try_parse_from(minimal_args(&[])).expect("defaults alone must still parse");
        assert_eq!(config.history_retention_days, 7);
        assert_eq!(config.daily_stats_retention_days, 300);
        assert_eq!(config.half_hourly_stats_retention_hours, 840);
        assert_eq!(config.trust_event_backlog_retention_days, 1);
        assert_eq!(config.trains_retention_days, 30);
        assert_eq!(config.schedule_destination_departures_retention_days, 8);
        assert_eq!(config.schedule_derived_products_retention_days, 8);
        assert_eq!(config.untracked_trains_retention_days, 14);
    }
}
