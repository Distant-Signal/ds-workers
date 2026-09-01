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
    /// Deliberately `Option`, defaulting to `None` (no pruning at all) --
    /// unlike `history_retention_days`, this rollup exists specifically to
    /// answer "how has this line trended over weeks/months," and the real
    /// retention ceiling is an unresolved product decision, not a technical
    /// one (storage is trivial at daily granularity either way -- see
    /// docs/superpowers/specs/2026-08-31-line-history-graphics-design.md,
    /// Open question 1). Set this via CLI/env once that's decided; until
    /// then rows accumulate indefinitely.
    #[arg(long, env)]
    pub daily_stats_retention_days: Option<i64>,

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
}
