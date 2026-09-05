use clap::Parser;
use common::config::{LineCatalogue, parse_lines};

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
    #[arg(long, env, default_value_t = 7)]
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
    #[arg(long, env, default_value_t = 300)]
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
    #[arg(long, env, default_value_t = 840)]
    pub half_hourly_stats_retention_hours: i64,

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
