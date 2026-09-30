//! Client-side expiry for the cold archive (`crate::archive`).
//!
//! OFF BY DEFAULT, and DRY-RUN BY DEFAULT when enabled. With
//! `ARCHIVE_EXPIRY_ENABLED` unset/false nothing in this module runs. With it
//! true and `ARCHIVE_EXPIRY_DRY_RUN` left at its default (true), the task
//! lists and matches the archive and reports what it *would* delete, but
//! never deletes.
//!
//! Some S3-compatible servers (Thoth on mine-bringer among them) have no
//! lifecycle API, so a bucket expiration rule cannot age archived objects
//! out. This task does it from the aggregator instead, because the
//! aggregator is what writes the archive and knows its key layout. Design:
//! `docs/superpowers/specs/2026-09-30-backup-and-observability-gaps-design.md`
//! section 3(b); operator notes: `docs/cold-archive.md`, "Object expiry".
//!
//! # What may be deleted
//!
//! The archive credentials may be able to delete much more than the archive
//! (on mine-bringer one Thoth key also covers the backups), so an object is
//! deleted only if ALL of these hold:
//!
//! - its key matches exactly
//!   `<prefix>/<table>/service_date=YYYY-MM-DD/part-<19 digits>.jsonl.zst`,
//!   with `<table>` one of [`EXPIRY_TABLES`] and the date a real calendar
//!   date ([`parse_key`]). Anything else under the listed prefixes is counted
//!   and logged, never deleted;
//! - the `service_date` taken FROM THE KEY (never the object's
//!   `LastModified`, which a copy or a restore can reset) is older than the
//!   current rail day (02:00 Europe/London) minus `ARCHIVE_EXPIRY_RETENTION_DAYS`;
//! - `ARCHIVE_EXPIRY_RETENTION_DAYS` is at least [`MIN_RETENTION_DAYS`], a
//!   floor fixed in code that no setting can lower;
//! - the key is not under any `ARCHIVE_PROTECTED_PREFIXES` entry.
//!
//! At startup the task also refuses an archive prefix that is empty, has
//! fewer than two path segments, or overlaps a protected prefix
//! ([`ExpirySettings::from_args`]). Each run deletes at most
//! `ARCHIVE_EXPIRY_MAX_DELETES_PER_RUN` objects, oldest service date first,
//! one single-object `DELETE` each (bulk `DeleteObjects` is disabled on the
//! archive client).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{NaiveDate, Utc};
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

/// The hard retention floor, in days. Not configurable: a typo'd or hostile
/// `ARCHIVE_EXPIRY_RETENTION_DAYS` below this refuses to start rather than
/// deleting recent history.
pub const MIN_RETENTION_DAYS: i64 = 90;

/// The table directories the archive writes (`archive.tables: [trains]`
/// writes all three) and therefore the only ones expiry lists.
pub const EXPIRY_TABLES: &[&str] = &["trains", "train_movement_events", "train_current_state"];

/// Stop a run after this many consecutive failed DELETEs: the store is
/// probably down, and the next run retries.
const MAX_CONSECUTIVE_DELETE_FAILURES: u32 = 10;

/// Unmatched keys logged individually per table per run; the rest are only
/// counted.
const UNMATCHED_LOG_LIMIT: u64 = 20;

/// CLI/env settings for archive expiry, flattened into
/// [`crate::archive::ArchiveArgs`]. Read only when
/// `ARCHIVE_EXPIRY_ENABLED` is true.
#[derive(Debug, Clone, clap::Args)]
pub struct ExpiryArgs {
    /// Master switch for client-side expiry. Needs `ARCHIVE_ENABLED`.
    #[arg(long, env, default_value_t = false, action = clap::ArgAction::Set)]
    pub archive_expiry_enabled: bool,

    /// Log and count what would be deleted, but delete nothing. On by
    /// default: switching it off is a deliberate second step.
    #[arg(long, env, default_value_t = true, action = clap::ArgAction::Set)]
    pub archive_expiry_dry_run: bool,

    /// Keep objects whose key `service_date` is at most this many rail days
    /// old. Must be at least [`MIN_RETENTION_DAYS`].
    #[arg(long, env, default_value_t = 730)]
    pub archive_expiry_retention_days: i64,

    /// Seconds between expiry runs. The first run starts at startup.
    #[arg(long, env, default_value_t = 86_400)]
    pub archive_expiry_interval_secs: u64,

    /// Circuit breaker: the most objects one run deletes (or, in dry-run,
    /// would delete).
    #[arg(long, env, default_value_t = 20_000)]
    pub archive_expiry_max_deletes_per_run: u64,

    /// Comma-separated key prefixes the archive prefix must never equal,
    /// contain or sit inside, e.g. `mine-bringer/backups/,mine-bringer/pgbackrest/`.
    #[arg(long, env, value_delimiter = ',')]
    pub archive_protected_prefixes: Vec<String>,
}

/// Validated expiry settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpirySettings {
    pub dry_run: bool,
    pub retention_days: i64,
    pub interval: Duration,
    pub max_deletes_per_run: u64,
    /// Normalised to path segments.
    pub protected_prefixes: Vec<Vec<String>>,
}

/// `"/a//b/"` -> `["a", "b"]`, the same normalisation `Archiver::new`
/// applies to the archive prefix.
pub fn segments(prefix: &str) -> Vec<String> {
    prefix
        .split('/')
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect()
}

/// Whether one segment list is a prefix of the other (equal, contains, or
/// is contained), compared segment by segment so `a/bc` does not overlap
/// `a/b`.
fn overlaps(a: &[String], b: &[String]) -> bool {
    let n = a.len().min(b.len());
    a[..n] == b[..n]
}

impl ExpirySettings {
    /// `Ok(None)` when expiry is disabled. Fails loudly on anything unsafe:
    /// retention under the floor, a zero cap, a too-short interval, an
    /// archive prefix with fewer than two segments, or one that overlaps a
    /// protected prefix. `archive_prefix` is `ARCHIVE_S3_PREFIX`.
    pub fn from_args(
        args: &ExpiryArgs,
        archive_enabled: bool,
        archive_prefix: &str,
    ) -> Result<Option<Self>> {
        if !args.archive_expiry_enabled {
            return Ok(None);
        }
        anyhow::ensure!(
            archive_enabled,
            "ARCHIVE_EXPIRY_ENABLED is true but ARCHIVE_ENABLED is not: expiry uses the archive's \
             S3 settings, so enable the archive too (or turn expiry off)"
        );
        anyhow::ensure!(
            args.archive_expiry_retention_days >= MIN_RETENTION_DAYS,
            "ARCHIVE_EXPIRY_RETENTION_DAYS is {}, below the hard floor of {MIN_RETENTION_DAYS} \
             days; the floor is fixed in code and cannot be lowered",
            args.archive_expiry_retention_days
        );
        anyhow::ensure!(
            args.archive_expiry_max_deletes_per_run >= 1,
            "ARCHIVE_EXPIRY_MAX_DELETES_PER_RUN must be at least 1 (turn expiry off instead)"
        );
        anyhow::ensure!(
            args.archive_expiry_interval_secs >= 60,
            "ARCHIVE_EXPIRY_INTERVAL_SECS must be at least 60, got {}",
            args.archive_expiry_interval_secs
        );
        let prefix = segments(archive_prefix);
        anyhow::ensure!(
            prefix.len() >= 2,
            "archive expiry needs ARCHIVE_S3_PREFIX to have at least two path segments (e.g. \
             <cluster>/distant-signal-archive), got {archive_prefix:?}: a short prefix would let \
             expiry list keys it does not own"
        );
        let mut protected_prefixes = Vec::new();
        for raw in &args.archive_protected_prefixes {
            if raw.trim().is_empty() {
                continue;
            }
            let p = segments(raw.trim());
            anyhow::ensure!(
                !p.is_empty(),
                "ARCHIVE_PROTECTED_PREFIXES entry {raw:?} names no path segment"
            );
            anyhow::ensure!(
                !overlaps(&prefix, &p),
                "ARCHIVE_S3_PREFIX {archive_prefix:?} overlaps protected prefix {raw:?} \
                 (ARCHIVE_PROTECTED_PREFIXES); refusing to run archive expiry there"
            );
            protected_prefixes.push(p);
        }
        Ok(Some(Self {
            dry_run: args.archive_expiry_dry_run,
            retention_days: args.archive_expiry_retention_days,
            interval: Duration::from_secs(args.archive_expiry_interval_secs),
            max_deletes_per_run: args.archive_expiry_max_deletes_per_run,
            protected_prefixes,
        }))
    }
}

/// A key the archive wrote: its table and the `service_date` in the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveKey {
    pub table: &'static str,
    pub service_date: NaiveDate,
}

/// Parses `key` as
/// `<prefix>/<table>/service_date=YYYY-MM-DD/part-<19 digits>.jsonl.zst`,
/// exactly: `None` for anything else, including a key under a different
/// prefix, an unknown table, extra or missing path segments, a malformed
/// or impossible date, or a part id that is not exactly 19 ASCII digits.
pub fn parse_key(prefix: &[String], key: &str) -> Option<ArchiveKey> {
    let mut rest = key;
    for segment in prefix {
        rest = rest.strip_prefix(segment.as_str())?.strip_prefix('/')?;
    }
    let mut parts = rest.split('/');
    let (table, date, part) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let table = *EXPIRY_TABLES.iter().find(|t| **t == table)?;

    let date = date.strip_prefix("service_date=")?;
    let b = date.as_bytes();
    let digit = |i: usize| b[i].is_ascii_digit();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' || !(0..4).chain(5..7).chain(8..10).all(digit)
    {
        return None;
    }
    let service_date = NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;

    let id = part.strip_prefix("part-")?.strip_suffix(".jsonl.zst")?;
    if id.len() != 19 || !id.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(ArchiveKey {
        table,
        service_date,
    })
}

/// Whether `service_date` is past retention on rail day `today`: strictly
/// older than `today - retention_days`, so with 730 days the date exactly
/// 730 days back is kept.
pub fn is_expired(service_date: NaiveDate, today: NaiveDate, retention_days: i64) -> bool {
    service_date < today - chrono::Duration::days(retention_days)
}

/// What one [`Expirer::run_once`] found and did. Per-table vectors follow
/// [`EXPIRY_TABLES`] order.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExpiryOutcome {
    /// Matching keys past retention (all of them, before the cap).
    pub candidates: Vec<(&'static str, u64)>,
    /// Dry-run only: what this run would have deleted (after the cap).
    pub would_delete: Vec<(&'static str, u64)>,
    pub deleted: Vec<(&'static str, u64)>,
    pub unmatched: Vec<(&'static str, u64)>,
    /// Oldest matching `service_date` seen per table, expired or not.
    pub oldest: Vec<(&'static str, NaiveDate)>,
    pub list_errors: u64,
    pub delete_errors: u64,
    pub cap_reached: bool,
}

fn bump(v: &mut Vec<(&'static str, u64)>, table: &'static str, n: u64) {
    match v.iter_mut().find(|(t, _)| *t == table) {
        Some((_, total)) => *total += n,
        None => v.push((table, n)),
    }
}

fn get(v: &[(&'static str, u64)], table: &str) -> u64 {
    v.iter().find(|(t, _)| *t == table).map_or(0, |(_, n)| *n)
}

/// Registers every expiry series at 0, so the chart's alerts see the first
/// increment with a plain `increase()` and dashboards show 0, not "no data".
/// `aggregator_archive_oldest_service_date_seconds` is deliberately left
/// unset until a run sees an archived object: 0 would read as 1970.
pub fn init_metrics(settings: &ExpirySettings) {
    use common::metrics::metric_name;
    for table in EXPIRY_TABLES {
        let table = *table;
        metrics::gauge!(metric_name("aggregator_archive_expiry_candidates"), "table" => table)
            .set(0.0);
        metrics::gauge!(metric_name("aggregator_archive_expiry_would_delete"), "table" => table)
            .set(0.0);
        metrics::counter!(metric_name("aggregator_archive_expiry_objects_deleted_total"), "table" => table)
            .increment(0);
        metrics::counter!(metric_name("aggregator_archive_expiry_skipped_unmatched_total"), "table" => table)
            .increment(0);
    }
    for stage in ["list", "delete"] {
        metrics::counter!(metric_name("aggregator_archive_expiry_errors_total"), "stage" => stage)
            .increment(0);
    }
    metrics::counter!(metric_name("aggregator_archive_expiry_cap_reached_total")).increment(0);
    metrics::gauge!(metric_name("aggregator_archive_expiry_dry_run")).set(if settings.dry_run {
        1.0
    } else {
        0.0
    });
    metrics::gauge!(metric_name("aggregator_archive_expiry_retention_days"))
        .set(settings.retention_days as f64);
}

/// Runs expiry over one archive: the archive's own store and prefix.
pub struct Expirer {
    store: Arc<dyn ObjectStore>,
    prefix: Vec<String>,
    settings: ExpirySettings,
}

impl std::fmt::Debug for Expirer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Expirer")
            .field("store", &self.store.to_string())
            .field("prefix", &self.prefix.join("/"))
            .field("settings", &self.settings)
            .finish()
    }
}

impl Expirer {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Vec<String>, settings: ExpirySettings) -> Self {
        Self {
            store,
            prefix,
            settings,
        }
    }

    pub fn settings(&self) -> &ExpirySettings {
        &self.settings
    }

    fn protected(&self, key: &str) -> bool {
        let key = segments(key);
        self.settings
            .protected_prefixes
            .iter()
            .any(|p| key.starts_with(p))
    }

    /// One expiry pass against rail day `today`. Never returns an error:
    /// list and delete failures are counted (and logged) in the outcome and
    /// in `aggregator_archive_expiry_errors_total`, and retried next run.
    pub async fn run_once(&self, today: NaiveDate) -> ExpiryOutcome {
        use common::metrics::metric_name;
        let retention = self.settings.retention_days;
        let mut out = ExpiryOutcome::default();
        // (service_date, key, table) for every expired, matching key.
        let mut expired: Vec<(NaiveDate, Path, &'static str)> = Vec::new();

        for &table in EXPIRY_TABLES {
            let dir = Path::from_iter(self.prefix.iter().map(String::as_str).chain([table]));
            let mut stream = self.store.list(Some(&dir));
            let mut table_expired = Vec::new();
            let mut oldest: Option<NaiveDate> = None;
            let mut unmatched = 0u64;
            let mut failed = false;
            while let Some(item) = stream.next().await {
                let meta = match item {
                    Ok(meta) => meta,
                    Err(err) => {
                        tracing::error!(error = ?err, table, prefix = %dir, "archive expiry: listing failed; skipping this table this run");
                        failed = true;
                        break;
                    }
                };
                let key = meta.location.as_ref();
                match parse_key(&self.prefix, key).filter(|k| k.table == table) {
                    Some(parsed) => {
                        oldest = Some(
                            oldest.map_or(parsed.service_date, |o| o.min(parsed.service_date)),
                        );
                        if is_expired(parsed.service_date, today, retention) {
                            table_expired.push((parsed.service_date, meta.location, table));
                        }
                    }
                    None => {
                        unmatched += 1;
                        if unmatched <= UNMATCHED_LOG_LIMIT {
                            tracing::warn!(
                                key,
                                table,
                                "archive expiry: key does not match the archive layout; never deleted"
                            );
                        }
                    }
                }
            }
            if unmatched > 0 {
                bump(&mut out.unmatched, table, unmatched);
                metrics::counter!(metric_name("aggregator_archive_expiry_skipped_unmatched_total"), "table" => table)
                    .increment(unmatched);
            }
            if failed {
                out.list_errors += 1;
                metrics::counter!(metric_name("aggregator_archive_expiry_errors_total"), "stage" => "list")
                    .increment(1);
                continue;
            }
            if let Some(oldest) = oldest {
                out.oldest.push((table, oldest));
                let secs = oldest
                    .and_time(chrono::NaiveTime::MIN)
                    .and_utc()
                    .timestamp();
                metrics::gauge!(metric_name("aggregator_archive_oldest_service_date_seconds"), "table" => table)
                    .set(secs as f64);
            }
            bump(&mut out.candidates, table, table_expired.len() as u64);
            expired.extend(table_expired);
        }

        for &table in EXPIRY_TABLES {
            metrics::gauge!(metric_name("aggregator_archive_expiry_candidates"), "table" => table)
                .set(get(&out.candidates, table) as f64);
        }

        // Oldest service date first, then key order.
        expired.sort_by(|a, b| (a.0, a.1.as_ref()).cmp(&(b.0, b.1.as_ref())));
        let cap = usize::try_from(self.settings.max_deletes_per_run).unwrap_or(usize::MAX);
        if expired.len() > cap {
            out.cap_reached = true;
            metrics::counter!(metric_name("aggregator_archive_expiry_cap_reached_total"))
                .increment(1);
            tracing::warn!(
                candidates = expired.len(),
                cap,
                "archive expiry: more expired objects than ARCHIVE_EXPIRY_MAX_DELETES_PER_RUN; \
                 handling the oldest {cap} this run"
            );
            expired.truncate(cap);
        }

        if self.settings.dry_run {
            for (date, key, table) in &expired {
                tracing::debug!(%date, key = key.as_ref(), table, "archive expiry (dry run): would delete");
                bump(&mut out.would_delete, table, 1);
            }
        } else {
            let mut consecutive_failures = 0;
            for (date, key, table) in &expired {
                // Re-check right before the DELETE; cheap, and the only
                // thing between a bug above and a deleted backup.
                let recheck = parse_key(&self.prefix, key.as_ref());
                if recheck.is_none_or(|k| !is_expired(k.service_date, today, retention))
                    || self.protected(key.as_ref())
                {
                    tracing::error!(
                        key = key.as_ref(),
                        "archive expiry: refusing to delete a key that failed the pre-delete check"
                    );
                    out.delete_errors += 1;
                    metrics::counter!(metric_name("aggregator_archive_expiry_errors_total"), "stage" => "delete")
                        .increment(1);
                    continue;
                }
                match self.store.delete(key).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        consecutive_failures = 0;
                        bump(&mut out.deleted, table, 1);
                        metrics::counter!(metric_name("aggregator_archive_expiry_objects_deleted_total"), "table" => *table)
                            .increment(1);
                        tracing::debug!(%date, key = key.as_ref(), "archive expiry: deleted");
                    }
                    Err(err) => {
                        consecutive_failures += 1;
                        out.delete_errors += 1;
                        metrics::counter!(metric_name("aggregator_archive_expiry_errors_total"), "stage" => "delete")
                            .increment(1);
                        tracing::error!(error = ?err, key = key.as_ref(), "archive expiry: DELETE failed; retrying next run");
                        if consecutive_failures >= MAX_CONSECUTIVE_DELETE_FAILURES {
                            tracing::error!(
                                "archive expiry: too many consecutive DELETE failures; stopping this run"
                            );
                            break;
                        }
                    }
                }
            }
        }
        for &table in EXPIRY_TABLES {
            metrics::gauge!(metric_name("aggregator_archive_expiry_would_delete"), "table" => table)
                .set(get(&out.would_delete, table) as f64);
        }
        if out.list_errors == 0 && out.delete_errors == 0 {
            metrics::gauge!(metric_name(
                "aggregator_archive_expiry_last_success_timestamp_seconds"
            ))
            .set(Utc::now().timestamp() as f64);
        }
        tracing::info!(
            dry_run = self.settings.dry_run,
            retention_days = retention,
            %today,
            candidates = ?out.candidates,
            would_delete = ?out.would_delete,
            deleted = ?out.deleted,
            unmatched = ?out.unmatched,
            oldest = ?out.oldest,
            list_errors = out.list_errors,
            delete_errors = out.delete_errors,
            cap_reached = out.cap_reached,
            "archive expiry run complete"
        );
        out
    }
}

/// The expiry task: one run at startup, then every `interval`. Runs on its
/// own task so neither a slow LIST nor a store outage touches aggregation
/// or the retention prunes.
pub async fn expiry_loop(expirer: Expirer) {
    let mut interval = tokio::time::interval(expirer.settings.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let today = common::rail_day::current_rail_day(Utc::now());
        expirer.run_once(today).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicU64, Ordering};

    const PREFIX: &str = "mine-bringer/distant-signal-archive";

    fn prefix() -> Vec<String> {
        segments(PREFIX)
    }

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn key(table: &str, date: &str, id: i64) -> String {
        format!("{PREFIX}/{table}/service_date={date}/part-{id:019}.jsonl.zst")
    }

    fn args() -> ExpiryArgs {
        ExpiryArgs {
            archive_expiry_enabled: true,
            archive_expiry_dry_run: true,
            archive_expiry_retention_days: 730,
            archive_expiry_interval_secs: 86_400,
            archive_expiry_max_deletes_per_run: 20_000,
            archive_protected_prefixes: vec![
                "mine-bringer/backups/".into(),
                "mine-bringer/pgbackrest/".into(),
            ],
        }
    }

    fn settings(dry_run: bool, cap: u64) -> ExpirySettings {
        let a = ExpiryArgs {
            archive_expiry_dry_run: dry_run,
            archive_expiry_max_deletes_per_run: cap,
            ..args()
        };
        ExpirySettings::from_args(&a, true, PREFIX)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn parses_exactly_the_archive_layout() {
        let p = prefix();
        for table in EXPIRY_TABLES {
            assert_eq!(
                parse_key(&p, &key(table, "2024-02-29", 42)),
                Some(ArchiveKey {
                    table,
                    service_date: d("2024-02-29")
                })
            );
        }
        let bad = [
            // wrong prefix / protected area / prefix only partly matching
            "mine-bringer/backups/trains/service_date=2020-01-01/part-0000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive2/trains/service_date=2020-01-01/part-0000000000000000001.jsonl.zst",
            "distant-signal-archive/trains/service_date=2020-01-01/part-0000000000000000001.jsonl.zst",
            // unknown or excluded table
            "mine-bringer/distant-signal-archive/trust_event_backlog/service_date=2020-01-01/part-0000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/Trains/service_date=2020-01-01/part-0000000000000000001.jsonl.zst",
            // extra or missing segments
            "mine-bringer/distant-signal-archive/trains/x/service_date=2020-01-01/part-0000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/service_date=2020-01-01/part-0000000000000000001.jsonl.zst/x",
            "mine-bringer/distant-signal-archive/trains/part-0000000000000000001.jsonl.zst",
            // date shape and validity
            "mine-bringer/distant-signal-archive/trains/service_date=2020-1-01/part-0000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/service_date=2021-02-29/part-0000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/service_date=2020-13-01/part-0000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/service_date=+2020-01-0/part-0000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/service_date=20200101/part-0000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/date=2020-01-01/part-0000000000000000001.jsonl.zst",
            // part id shape and suffix
            "mine-bringer/distant-signal-archive/trains/service_date=2020-01-01/part-000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/service_date=2020-01-01/part-00000000000000000001.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/service_date=2020-01-01/part-000000000000000000a.jsonl.zst",
            "mine-bringer/distant-signal-archive/trains/service_date=2020-01-01/part-0000000000000000001.jsonl",
            "mine-bringer/distant-signal-archive/trains/service_date=2020-01-01/part-0000000000000000001.jsonl.zst.bak",
            "mine-bringer/distant-signal-archive/trains/service_date=2020-01-01/0000000000000000001.jsonl.zst",
            "",
        ];
        for k in bad {
            assert_eq!(parse_key(&p, k), None, "{k}");
        }
    }

    #[test]
    fn expiry_uses_the_key_date_and_keeps_the_boundary_day() {
        let today = d("2028-10-01");
        assert!(
            !is_expired(d("2026-10-02"), today, 730),
            "730 days back is kept"
        );
        assert!(
            is_expired(d("2026-10-01"), today, 730),
            "731 days back goes"
        );
        assert!(!is_expired(today, today, 730));
    }

    #[test]
    fn the_retention_floor_cannot_be_undercut() {
        for days in [0, 1, 30, MIN_RETENTION_DAYS - 1, -730] {
            let a = ExpiryArgs {
                archive_expiry_retention_days: days,
                ..args()
            };
            let err = ExpirySettings::from_args(&a, true, PREFIX)
                .unwrap_err()
                .to_string();
            assert!(err.contains("floor"), "{days}: {err}");
        }
        let at_floor = ExpiryArgs {
            archive_expiry_retention_days: MIN_RETENTION_DAYS,
            ..args()
        };
        assert!(
            ExpirySettings::from_args(&at_floor, true, PREFIX)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn the_floor_has_no_config_knob() {
        // Only the retention itself is configurable; no flag or env var
        // lowers the floor.
        use clap::CommandFactory;
        #[derive(clap::Parser)]
        struct Probe {
            #[command(flatten)]
            expiry: ExpiryArgs,
        }
        let cmd = Probe::command();
        for arg in cmd.get_arguments() {
            let id = arg.get_id().as_str();
            assert!(!id.contains("min"), "unexpected knob {id}");
        }
    }

    #[test]
    fn disabled_expiry_checks_nothing() {
        let a = ExpiryArgs {
            archive_expiry_enabled: false,
            archive_expiry_retention_days: 1,
            ..args()
        };
        assert_eq!(ExpirySettings::from_args(&a, false, "").unwrap(), None);
    }

    #[test]
    fn unsafe_prefixes_and_settings_refuse_to_start() {
        let refuse = |a: &ExpiryArgs, enabled: bool, prefix: &str| {
            ExpirySettings::from_args(a, enabled, prefix)
                .unwrap_err()
                .to_string()
        };
        assert!(refuse(&args(), false, PREFIX).contains("ARCHIVE_ENABLED"));
        for short in ["", "/", "archive", "/archive/"] {
            assert!(
                refuse(&args(), true, short).contains("two path segments"),
                "{short:?}"
            );
        }
        for overlapping in [
            "mine-bringer/backups",
            "/mine-bringer/backups/",
            "mine-bringer/backups/distant-signal",
            "mine-bringer/pgbackrest/archive",
        ] {
            assert!(
                refuse(&args(), true, overlapping).contains("protected"),
                "{overlapping}"
            );
        }
        // A protected prefix inside the archive prefix also overlaps.
        let inner = ExpiryArgs {
            archive_protected_prefixes: vec![format!("{PREFIX}/trains")],
            ..args()
        };
        assert!(refuse(&inner, true, PREFIX).contains("protected"));
        // Segment-aware: a sibling that shares a string prefix is fine.
        assert!(ExpirySettings::from_args(&args(), true, "mine-bringer/backups-archive").is_ok());
        let slash_only = ExpiryArgs {
            archive_protected_prefixes: vec!["/".into()],
            ..args()
        };
        assert!(refuse(&slash_only, true, PREFIX).contains("no path segment"));
        let zero_cap = ExpiryArgs {
            archive_expiry_max_deletes_per_run: 0,
            ..args()
        };
        assert!(refuse(&zero_cap, true, PREFIX).contains("MAX_DELETES"));
        let fast = ExpiryArgs {
            archive_expiry_interval_secs: 0,
            ..args()
        };
        assert!(refuse(&fast, true, PREFIX).contains("INTERVAL"));
    }

    /// `InMemory` that counts DELETE calls, so a test can assert "no DELETE
    /// was sent" rather than only "the objects are still there".
    #[derive(Debug, Default)]
    struct CountingStore {
        inner: InMemory,
        deletes: AtomicU64,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
            // `ObjectStoreExt::delete` makes one call per key.
            self.deletes.fetch_add(1, Ordering::SeqCst);
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    const TODAY: &str = "2028-10-01";
    const PROTECTED_KEY: &str = "mine-bringer/backups/postgres/2020-01-01.sql.zst.age";
    const STRAY_KEY: &str =
        "mine-bringer/distant-signal-archive/trains/service_date=2020-01-01/notes.txt";

    /// Three expired keys (two tables), two kept (one on the boundary day),
    /// one unmatched stray under the archive, one protected backup.
    async fn seeded() -> Arc<CountingStore> {
        let store = Arc::new(CountingStore::default());
        for k in [
            key("trains", "2026-09-29", 1),
            key("trains", "2026-09-30", 2),
            key("train_movement_events", "2026-09-28", 3),
            key("trains", "2026-10-02", 4),
            key("train_current_state", "2028-09-30", 5),
            STRAY_KEY.to_string(),
            PROTECTED_KEY.to_string(),
        ] {
            store
                .put(&Path::from(k), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        store
    }

    async fn keys(store: &CountingStore) -> Vec<String> {
        use futures_util::TryStreamExt;
        let mut all: Vec<String> = store
            .list(None)
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap();
        all.sort();
        all
    }

    #[tokio::test]
    async fn dry_run_deletes_nothing_but_counts_what_it_would() {
        let store = seeded().await;
        let before = keys(&store).await;
        let e = Expirer::new(store.clone(), prefix(), settings(true, 20_000));
        let out = e.run_once(d(TODAY)).await;

        assert_eq!(
            store.deletes.load(Ordering::SeqCst),
            0,
            "no DELETE may be sent"
        );
        assert_eq!(keys(&store).await, before);
        assert_eq!(get(&out.candidates, "trains"), 2);
        assert_eq!(get(&out.candidates, "train_movement_events"), 1);
        assert_eq!(get(&out.candidates, "train_current_state"), 0);
        assert_eq!(get(&out.would_delete, "trains"), 2);
        assert_eq!(get(&out.would_delete, "train_movement_events"), 1);
        assert!(out.deleted.is_empty());
        assert_eq!(get(&out.unmatched, "trains"), 1);
        assert_eq!(
            (out.list_errors, out.delete_errors, out.cap_reached),
            (0, 0, false)
        );
        assert!(out.oldest.contains(&("trains", d("2026-09-29"))));
    }

    #[tokio::test]
    async fn live_run_deletes_only_expired_matching_keys() {
        let store = seeded().await;
        let e = Expirer::new(store.clone(), prefix(), settings(false, 20_000));
        let out = e.run_once(d(TODAY)).await;

        assert_eq!(get(&out.deleted, "trains"), 2);
        assert_eq!(get(&out.deleted, "train_movement_events"), 1);
        assert!(out.would_delete.is_empty());
        let mut expected = vec![
            key("trains", "2026-10-02", 4),
            key("train_current_state", "2028-09-30", 5),
            STRAY_KEY.to_string(),
            PROTECTED_KEY.to_string(),
        ];
        expected.sort();
        assert_eq!(keys(&store).await, expected);

        // A second run finds nothing more to do.
        let again = e.run_once(d(TODAY)).await;
        assert!(again.deleted.is_empty());
        assert_eq!(get(&again.candidates, "trains"), 0);
    }

    #[tokio::test]
    async fn the_cap_limits_a_run_to_the_oldest_dates() {
        let store = seeded().await;
        let e = Expirer::new(store.clone(), prefix(), settings(false, 2));
        let out = e.run_once(d(TODAY)).await;
        assert!(out.cap_reached);
        assert_eq!(
            get(&out.candidates, "trains"),
            2,
            "candidates are counted before the cap"
        );
        // Oldest first: 2026-09-28 (movement events), then 2026-09-29.
        assert_eq!(get(&out.deleted, "train_movement_events"), 1);
        assert_eq!(get(&out.deleted, "trains"), 1);
        assert!(keys(&store).await.contains(&key("trains", "2026-09-30", 2)));

        // In dry-run the cap bounds would_delete the same way.
        let store = seeded().await;
        let dry = Expirer::new(store.clone(), prefix(), settings(true, 1));
        let out = dry.run_once(d(TODAY)).await;
        assert!(out.cap_reached);
        assert_eq!(get(&out.would_delete, "train_movement_events"), 1);
        assert_eq!(get(&out.would_delete, "trains"), 0);
        assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
    }
}
