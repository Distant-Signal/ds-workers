//! `enricher normalise-categories`: the one-off backfill that maps every
//! `incidents.extracted_category` stored before the fixed vocabulary
//! (`category.rs`, 14 Oct 2026) onto it, through the same
//! [`category::normalise`] the live path applies in `llm::parse_primary`.
//! No SQL copy of the rules exists, so the backfill can't drift from them.
//!
//! ```text
//! enricher normalise-categories --dry-run     # report only
//! enricher normalise-categories               # write
//! ```
//!
//! - **Safe.** It reads only rows whose category is not already one of
//!   [`CATEGORIES`], in `incident_id` (primary key) order with a keyset
//!   cursor, `--batch-size` at a time, and writes each batch with one
//!   autocommitted `UPDATE ... WHERE incident_id = ...`: row locks on at most
//!   one batch, held for one statement, and no table lock.
//! - **Compare-and-set.** Each write only fires if the row still holds the
//!   value the walk read, so a concurrent extraction (which already stores a
//!   normalised value) is never overwritten. Such rows count as `skipped`.
//! - **Idempotent and resumable.** A normalised value maps to itself and the
//!   walk skips it, so a re-run after a crash (or a finished run) only
//!   touches what is left; a second full run reports `updated=0`.
//!   `--after <incident_id>` (the `cursor` of the last logged batch) skips
//!   the part of the table already walked.
//! - Only `extracted_category` changes: not `extracted_at`, not the text
//!   hash or model version, so the sweep doesn't re-extract anything.
//!
//! Runs as the enricher role (it needs `UPDATE (extracted_category)` on
//! `incidents` and checks for it first), on one connection. Runbook:
//! docs/enricher-category-backfill.md.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use common::secret::Secret;
use ds_store::maintenance::Needs;
use sqlx::PgPool;

use crate::category::{self, CATEGORIES};

/// The command name, for errors and the refusal.
const TOOL: &str = "enricher normalise-categories";
/// The role the command runs as.
const ROLE: &str = "the enricher role (distant_signal_enricher)";
/// What the backfill reads and writes.
const NEEDS: &[Needs] = &[
    Needs::Table {
        table: "incidents",
        privilege: "SELECT",
    },
    Needs::Column {
        table: "incidents",
        column: "extracted_category",
        privilege: "UPDATE",
    },
];

/// `enricher normalise-categories`: map stored free-text categories onto
/// the fixed set.
#[derive(Debug, Parser)]
#[command(
    name = "enricher normalise-categories",
    bin_name = "enricher normalise-categories",
    version
)]
struct Args {
    /// The database, as the enricher role.
    #[arg(long, env, hide_env_values = true)]
    database_url: Option<Secret>,
    /// Report what would change, write nothing.
    #[arg(long)]
    dry_run: bool,
    /// Rows read, and at most updated, per batch.
    #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(i64).range(1..=5000))]
    batch_size: i64,
    /// Start after this `incident_id` (a previous run's last logged `cursor`).
    #[arg(long)]
    after: Option<String>,
    /// Stop after this `incident_id` (inclusive): a trial run over a slice.
    #[arg(long)]
    until: Option<String>,
    /// Pause between batches, in milliseconds.
    #[arg(long, default_value_t = 0)]
    pause_ms: u64,
}

/// The walk's settings.
#[derive(Debug, Clone, Default)]
pub(crate) struct Options {
    pub dry_run: bool,
    pub batch_size: i64,
    pub after: Option<String>,
    pub until: Option<String>,
    pub pause: Duration,
}

/// One stored value and what it becomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Change {
    pub incident_id: String,
    pub old: String,
    pub new: &'static str,
    /// `false`: the old value fell back to `other`.
    pub recognised: bool,
}

/// What a run saw and did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Report {
    /// Rows read whose category was not one of the fixed set.
    pub examined: u64,
    /// Rows written (0 on a dry run).
    pub updated: u64,
    /// Rows whose value changed between the read and the write (a
    /// concurrent extraction), left alone.
    pub skipped: u64,
    /// Rows whose old value isn't recognised and becomes `other`.
    pub unrecognised: u64,
    /// Every distinct (old value, new value) with its row count.
    pub mappings: BTreeMap<(String, &'static str), u64>,
    /// The last `incident_id` walked, for `--after`.
    pub cursor: Option<String>,
}

/// The rows of a batch that need a write: the mapping path, apart from the
/// database. A value already in the set is left alone (the walk's query
/// excludes those too).
pub(crate) fn plan(rows: Vec<(String, String)>) -> Vec<Change> {
    rows.into_iter()
        .filter_map(|(incident_id, old)| {
            let normalised = category::normalise(&old);
            (normalised.category != old).then_some(Change {
                incident_id,
                old,
                new: normalised.category,
                recognised: normalised.recognised,
            })
        })
        .collect()
}

/// Reads one batch: rows after `after` (up to `until`) whose category is
/// set and not one of [`CATEGORIES`], in primary-key order.
async fn load_batch(
    pool: &PgPool,
    after: Option<&str>,
    until: Option<&str>,
    limit: i64,
) -> sqlx::Result<Vec<(String, String)>> {
    sqlx::query_as(
        "SELECT incident_id, extracted_category FROM incidents \
         WHERE ($1::text IS NULL OR incident_id > $1) \
           AND ($4::text IS NULL OR incident_id <= $4) \
           AND extracted_category IS NOT NULL \
           AND NOT (extracted_category = ANY($2)) \
         ORDER BY incident_id \
         LIMIT $3",
    )
    .bind(after)
    .bind(CATEGORIES.as_slice())
    .bind(limit)
    .bind(until)
    .fetch_all(pool)
    .await
}

/// Writes one batch in one statement, each row only if it still holds the
/// value read. Returns the rows written.
async fn write_batch(pool: &PgPool, changes: &[Change]) -> sqlx::Result<u64> {
    let ids: Vec<&str> = changes.iter().map(|c| c.incident_id.as_str()).collect();
    let olds: Vec<&str> = changes.iter().map(|c| c.old.as_str()).collect();
    let news: Vec<&str> = changes.iter().map(|c| c.new).collect();
    let result = sqlx::query(
        "UPDATE incidents AS i SET extracted_category = v.new \
         FROM unnest($1::text[], $2::text[], $3::text[]) AS v(incident_id, old, new) \
         WHERE i.incident_id = v.incident_id AND i.extracted_category = v.old",
    )
    .bind(&ids)
    .bind(&olds)
    .bind(&news)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Walks the table and normalises every stored category (or, on a dry run,
/// only counts what it would do).
pub(crate) async fn run(pool: &PgPool, options: &Options) -> anyhow::Result<Report> {
    anyhow::ensure!(options.batch_size > 0, "the batch size must be positive");
    let mut report = Report {
        cursor: options.after.clone(),
        ..Report::default()
    };
    let mut batches = 0_u64;
    loop {
        let rows = load_batch(
            pool,
            report.cursor.as_deref(),
            options.until.as_deref(),
            options.batch_size,
        )
        .await
        .context("reading a batch of incidents")?;
        let Some((last, _)) = rows.last() else {
            break;
        };
        let last = last.clone();
        let full = i64::try_from(rows.len()).unwrap_or(i64::MAX) >= options.batch_size;
        let changes = plan(rows);
        let examined = changes.len() as u64;
        for change in &changes {
            *report
                .mappings
                .entry((change.old.clone(), change.new))
                .or_default() += 1;
            if !change.recognised {
                report.unrecognised += 1;
            }
        }
        let updated = if options.dry_run || changes.is_empty() {
            0
        } else {
            write_batch(pool, &changes)
                .await
                .with_context(|| format!("writing the batch ending at {last}"))?
        };
        report.examined += examined;
        report.updated += updated;
        if !options.dry_run {
            report.skipped += examined - updated;
        }
        report.cursor = Some(last);
        batches += 1;
        tracing::info!(
            batch = batches,
            examined,
            updated,
            dry_run = options.dry_run,
            cursor = report.cursor.as_deref(),
            "normalised a batch of categories"
        );
        if !full {
            break;
        }
        if !options.pause.is_zero() {
            tokio::time::sleep(options.pause).await;
        }
    }
    Ok(report)
}

/// The subcommand's entry point (`main.rs` dispatches here on
/// `enricher normalise-categories`).
pub(crate) async fn main() -> ExitCode {
    // `info` unless RUST_LOG says otherwise: the counts are the output.
    common::logging::init_with_filter(
        "enricher-normalise-categories",
        common::logging::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| common::logging::EnvFilter::new("info")),
    );
    // `skip(1)`: the binary's path; the subcommand stands in as the name.
    let args = Args::parse_from(std::env::args_os().skip(1));
    common::logging::exit_code(cli(args).await)
}

async fn cli(args: Args) -> anyhow::Result<()> {
    let url = args
        .database_url
        .as_ref()
        .map(Secret::expose)
        .filter(|url| !url.trim().is_empty())
        .context("set DATABASE_URL (or --database-url)")?;
    // One connection, whatever DB_MAX_CONNECTIONS the pod sets: the role's
    // CONNECTION LIMIT leaves one spare beside the service's own pool.
    let mut settings = common::pg::PoolSettings::from_env("enricher-normalise-categories", 1)?;
    settings.max_connections = 1;
    let pool = settings.connect(url).await?;
    let user = ds_store::maintenance::refuse_api_role(&pool, TOOL, ROLE).await?;
    ds_store::maintenance::require(&pool, TOOL, ROLE, NEEDS).await?;
    tracing::info!(
        user,
        dry_run = args.dry_run,
        batch_size = args.batch_size,
        after = args.after.as_deref(),
        until = args.until.as_deref(),
        "normalising incidents.extracted_category"
    );
    let report = run(
        &pool,
        &Options {
            dry_run: args.dry_run,
            batch_size: args.batch_size,
            after: args.after,
            until: args.until,
            pause: Duration::from_millis(args.pause_ms),
        },
    )
    .await?;
    for ((old, new), rows) in &report.mappings {
        tracing::info!(old = %old, new, rows, recognised = category::normalise(old).recognised, "mapping");
    }
    tracing::info!(
        examined = report.examined,
        updated = report.updated,
        skipped = report.skipped,
        unrecognised = report.unrecognised,
        distinct_values = report.mappings.len(),
        dry_run = args.dry_run,
        cursor = report.cursor.as_deref(),
        "category normalisation complete"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(values: &[(&str, &str)]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|(id, v)| ((*id).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn the_cli_is_well_formed() {
        use clap::CommandFactory;
        Args::command().debug_assert();
        let args = Args::try_parse_from([
            "normalise-categories",
            "--dry-run",
            "--batch-size",
            "50",
            "--after",
            "I9",
        ])
        .unwrap();
        assert!(args.dry_run);
        assert_eq!(args.batch_size, 50);
        assert_eq!(args.after.as_deref(), Some("I9"));
        assert!(Args::try_parse_from(["normalise-categories", "--batch-size", "0"]).is_err());
    }

    /// The backfill maps through the live normaliser: free text becomes its
    /// category, unknown text becomes `other` (flagged unrecognised), and a
    /// value already in the set is not rewritten.
    #[test]
    fn plan_maps_free_text_through_the_live_normaliser() {
        let planned = plan(rows(&[
            ("I1", "Signalling Fault"),
            ("I2", "signal_failure"),
            ("I3", "Rail Replacement Bus"),
            ("I4", "cyber"),
            ("I5", ""),
            ("I6", "other"),
            ("I7", "Unknown"),
        ]));
        let got: Vec<_> = planned
            .iter()
            .map(|c| (c.incident_id.as_str(), c.new, c.recognised))
            .collect();
        assert_eq!(
            got,
            [
                ("I1", "signal_failure", true),
                ("I3", "engineering_works", true),
                ("I4", "other", false),
                ("I5", "other", false),
                ("I7", "other", true),
            ]
        );
        for change in &planned {
            assert_eq!(change.new, category::normalise(&change.old).category);
        }
    }

    /// Idempotent: whatever the backfill writes, a second pass leaves alone.
    #[test]
    fn a_planned_value_plans_nothing_on_a_second_pass() {
        let first = plan(rows(&[
            ("I1", " Points Fault "),
            ("I2", "broken down train"),
            ("I3", "a goose"),
        ]));
        assert_eq!(first.len(), 3);
        let again = plan(
            first
                .iter()
                .map(|c| (c.incident_id.clone(), c.new.to_owned()))
                .collect(),
        );
        assert!(again.is_empty(), "{again:?}");
    }

    // -- DB-gated: the batched write --

    const PREFIX: &str = "TEST-ENRICHER-CATEGORY-BACKFILL-";

    async fn seed(pool: &PgPool, values: &[(&str, Option<&str>)]) {
        sqlx::query("DELETE FROM incidents WHERE incident_id LIKE $1")
            .bind(format!("{PREFIX}%"))
            .execute(pool)
            .await
            .expect("clear fixtures");
        for (suffix, category) in values {
            sqlx::query(
                "INSERT INTO incidents (incident_id, summary, description, operators, affected_stations, priority, \
                     extracted_category, extraction_model_version, extracted_at) \
                 VALUES ($1, 's', 'd', '{}', '{}', 3, $2, 'm@periods-v2', '2026-10-01T00:00:00Z')",
            )
            .bind(format!("{PREFIX}{suffix}"))
            .bind(category)
            .execute(pool)
            .await
            .expect("seed fixture incident row");
        }
    }

    async fn stored(pool: &PgPool) -> Vec<(String, Option<String>, chrono::DateTime<chrono::Utc>)> {
        sqlx::query_as(
            "SELECT incident_id, extracted_category, extracted_at FROM incidents \
             WHERE incident_id LIKE $1 ORDER BY incident_id",
        )
        .bind(format!("{PREFIX}%"))
        .fetch_all(pool)
        .await
        .expect("read fixtures")
    }

    fn options(dry_run: bool) -> Options {
        Options {
            dry_run,
            // Smaller than the fixture set, so the walk takes several batches.
            batch_size: 2,
            // Only the fixtures, whatever else the database holds.
            after: Some(PREFIX.to_owned()),
            until: Some(format!("{PREFIX}9")),
            pause: Duration::ZERO,
        }
    }

    /// A dry run writes nothing; a real run writes every free-text value
    /// through the normaliser in batches, leaves `extracted_at`, NULLs and
    /// canonical values alone; a second run finds nothing to do.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher category_backfill -- --ignored --test-threads=1`"]
    async fn category_backfill_normalises_in_batches_and_is_idempotent() {
        let pool = crate::test_db::pool().await;
        // As the role: the grants the subcommand checks are held.
        ds_store::maintenance::require(&pool, TOOL, ROLE, NEEDS)
            .await
            .expect("the enricher role holds the backfill's grants");
        let fixtures = [
            ("1", Some("Signalling Fault")),
            ("2", Some("signal_failure")),
            ("3", None),
            ("4", Some("Rail Replacement Bus")),
            ("5", Some("cyber")),
            ("6", Some("Points Fault")),
            ("7", Some("signal fault")),
        ];
        seed(&pool, &fixtures).await;
        let before = stored(&pool).await;

        let dry = run(&pool, &options(true)).await.unwrap();
        assert_eq!(dry.examined, 5);
        assert_eq!(dry.updated, 0);
        assert_eq!(dry.unrecognised, 1);
        assert_eq!(
            dry.mappings.get(&("cyber".to_owned(), "other")),
            Some(&1),
            "{:?}",
            dry.mappings
        );
        assert_eq!(stored(&pool).await, before, "a dry run writes nothing");

        let report = run(&pool, &options(false)).await.unwrap();
        assert_eq!((report.examined, report.updated, report.skipped), (5, 5, 0));
        assert_eq!(report.cursor.as_deref(), Some(&*format!("{PREFIX}7")));
        let after = stored(&pool).await;
        let categories: Vec<_> = after.iter().map(|(_, c, _)| c.as_deref()).collect();
        assert_eq!(
            categories,
            [
                Some("signal_failure"),
                Some("signal_failure"),
                None,
                Some("engineering_works"),
                Some("other"),
                Some("points_failure"),
                Some("signal_failure"),
            ]
        );
        let stamps = |rows: &[(String, Option<String>, chrono::DateTime<chrono::Utc>)]| {
            rows.iter().map(|(_, _, at)| *at).collect::<Vec<_>>()
        };
        assert_eq!(stamps(&after), stamps(&before), "extracted_at untouched");

        let again = run(&pool, &options(false)).await.unwrap();
        assert_eq!((again.examined, again.updated), (0, 0));

        // Resuming past a row leaves it for a later run.
        seed(
            &pool,
            &[
                ("1", Some("Signalling Fault")),
                ("2", Some("Signalling Fault")),
            ],
        )
        .await;
        let resumed = run(
            &pool,
            &Options {
                after: Some(format!("{PREFIX}1")),
                ..options(false)
            },
        )
        .await
        .unwrap();
        assert_eq!(resumed.updated, 1);
        let categories: Vec<_> = stored(&pool).await.into_iter().map(|(_, c, _)| c).collect();
        assert_eq!(
            categories,
            [
                Some("Signalling Fault".to_owned()),
                Some("signal_failure".to_owned())
            ]
        );

        sqlx::query("DELETE FROM incidents WHERE incident_id LIKE $1")
            .bind(format!("{PREFIX}%"))
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    /// A row changed between the read and the write (a concurrent
    /// extraction) is left alone and counted as skipped.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p enricher category_backfill -- --ignored --test-threads=1`"]
    async fn category_backfill_write_skips_a_row_that_moved() {
        let pool = crate::test_db::pool().await;
        seed(
            &pool,
            &[("1", Some("Signalling Fault")), ("2", Some("Points Fault"))],
        )
        .await;
        let changes = plan(
            stored(&pool)
                .await
                .into_iter()
                .map(|(id, c, _)| (id, c.unwrap()))
                .collect(),
        );
        sqlx::query(
            "UPDATE incidents SET extracted_category = 'track_fault' WHERE incident_id = $1",
        )
        .bind(format!("{PREFIX}2"))
        .execute(&pool)
        .await
        .expect("simulate a concurrent extraction");
        assert_eq!(write_batch(&pool, &changes).await.unwrap(), 1);
        let categories: Vec<_> = stored(&pool).await.into_iter().map(|(_, c, _)| c).collect();
        assert_eq!(
            categories,
            [
                Some("signal_failure".to_owned()),
                Some("track_fault".to_owned())
            ]
        );
        sqlx::query("DELETE FROM incidents WHERE incident_id LIKE $1")
            .bind(format!("{PREFIX}%"))
            .execute(&pool)
            .await
            .expect("cleanup");
    }
}
