use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use common::LineDefinition;

fn parse_lines(path: &str) -> Result<LineCatalogue> {
    LineDefinition::from_dir(&PathBuf::from(path)).map(LineCatalogue)
}

/// Newtype around the parsed line catalogue.
///
/// `clap_derive` infers the type it downcasts an `ArgMatches` entry to from
/// the field's *syntactic* shape, not from the `value_parser`'s `Value`
/// type: a bare `Vec<LineDefinition>` field is always treated as "one
/// `LineDefinition` per CLI occurrence, collected via `ArgAction::Append`" —
/// this panics at runtime ("Mismatch between definition and access of
/// `lines`") the moment `--lines-dir`/`LINES_DIR`/`default_value` actually
/// supplies a value. `parse_lines` instead produces the *entire* vec from a
/// single `--lines-dir` occurrence, so the field type must not look like
/// `Vec<T>` to the derive macro. This newtype (plus `Deref`) sidesteps that;
/// see `crates/api/src/data/config.rs` for the same pattern applied to the
/// `api` crate's identical `--lines-dir` flag.
#[derive(Debug, Clone, Default)]
pub struct LineCatalogue(pub Vec<LineDefinition>);

impl std::ops::Deref for LineCatalogue {
    type Target = Vec<LineDefinition>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
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
    /// pruning them. Deliberately NOT a reuse of `history_retention_days`
    /// (governs a different table, `line_status_history`) or
    /// `daily_stats_retention_days` (sized for a weeks/months trend use
    /// case this half-hourly rolling-24h view does not have -- reusing its
    /// default of 300 would mean accumulating ~300 days x 48 rows/line of
    /// data only the most recent ~49 rows of which are ever read). 48
    /// hours is a 2x safety margin over the 48-49 rows the line-info-page
    /// embed actually needs at 30-minute granularity, per
    /// docs/superpowers/specs/2026-09-02-trend-chart-granularity-design.md
    /// Decision 5 -- a reasoned starting default, not empirically
    /// validated against real restart/deploy timing (see that spec's Open
    /// question 2).
    ///
    /// This field's UNIT is deliberately unchanged from the table's
    /// original 1-hour-bucket era: retention is measured in wall-clock
    /// hours, not bucket count, so halving the bucket size (1h -> 30min,
    /// alongside this field's own rename from `hourly_stats_retention_hours`)
    /// does not change the default value either -- 48 hours of real time
    /// is still 48 hours of real time. The only consequence is that this
    /// same window now holds roughly twice as many rows per line (~96
    /// instead of ~48) to cover it, which is a trivial row count for
    /// Postgres and not something that needs its own knob.
    #[arg(long, env, default_value_t = 48)]
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
