//! Optional "archive, then delete" cold storage for retention prunes.
//!
//! OFF BY DEFAULT. With `ARCHIVE_ENABLED` unset/false, nothing in this
//! module runs: `main::run_retention` calls the plain delete-only prunes in
//! `queries.rs` exactly as before, and no S3 setting is read or required.
//!
//! When enabled, each opted-in table's prune streams the rows it is about
//! to delete into zstd-compressed JSON Lines objects in S3-compatible
//! storage, confirms each object landed (PUT, then a HEAD whose size must
//! match), and only then deletes exactly those rows -- all inside one
//! database transaction per batch, with the batch's rows locked
//! (`SELECT ... FOR UPDATE`) from selection to delete. See
//! `docs/cold-archive.md` for the object layout and how to read an archive
//! offline; nothing in the app reads archived data back.
//!
//! # Which tables can be archived
//!
//! Only [`ARCHIVABLE_TABLES`] -- today just `trains`, which archives the
//! `trains` row together with the `train_movement_events` and
//! `train_current_state` rows that `ON DELETE CASCADE` would otherwise
//! remove with it. Everything else is rejected at startup, and two groups
//! are rejected with a specific explanation because archiving them would
//! break a licensing safeguard rather than merely being unimplemented:
//!
//! - `trust_event_backlog`: its 1-day retention is a deliberate TRUST
//!   licensing safeguard (see `Config::trust_event_backlog_retention_days`).
//!   A copy in object storage would defeat it.
//! - LDBWS-derived tables (`line_status_history`, the daily/half-hourly
//!   stats and coverage stats, `station_samples`): RDM's 300-day ceiling
//!   applies to every copy, so archiving them would need an expiry policy
//!   on the bucket this app cannot verify. Left out entirely instead.
//!
//! # Idempotency
//!
//! A batch is "the lowest-id eligible `trains` rows of the oldest eligible
//! `service_date`", and its objects are keyed by that date and the batch's
//! first `trains.id`:
//! `<prefix>/<table>/service_date=YYYY-MM-DD/part-<first id, 19 digits>.jsonl.zst`.
//! Once a batch commits its first id no longer exists, so no later batch
//! can reuse its key. If an upload succeeds but the transaction then rolls
//! back (a later upload failed, verification failed, the DELETE or COMMIT
//! failed, the pod died), the rows are still in the database and the next
//! cycle selects the same batch again and overwrites the same keys -- no
//! loss, no duplicate. The one residual case: if the eligible set for that
//! date changes between the failed attempt and the retry (e.g. a
//! subscription is added to a 14-day-old train), the retry's first id can
//! differ, leaving the stale object behind; every row carries its primary
//! key `id`, so an offline reader dedupes on it (`docs/cold-archive.md`).

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt, PutPayload, RetryConfig};
use sqlx::{PgConnection, PgPool};

/// Tables an operator may list in `ARCHIVE_TABLES`.
pub const ARCHIVABLE_TABLES: &[&str] = &["trains"];

/// Tables that must never be archived because a licensing safeguard
/// depends on the data actually disappearing. Rejected at startup with an
/// explanation rather than the generic "unknown table" message.
const LICENSING_EXCLUDED_TABLES: &[(&str, &str)] = &[
    (
        "trust_event_backlog",
        "its 1-day retention is a deliberate TRUST licensing safeguard \
         (see Config::trust_event_backlog_retention_days); an archived copy would defeat it",
    ),
    (
        "line_status_history",
        "it is LDBWS-derived, and RDM's 300-day ceiling applies to every copy",
    ),
    (
        "line_status_daily_stats",
        "it is LDBWS-derived, and RDM's 300-day ceiling applies to every copy",
    ),
    (
        "line_status_half_hourly_stats",
        "it is LDBWS-derived, and RDM's 300-day ceiling applies to every copy",
    ),
    (
        "line_coverage_daily_stats",
        "it is LDBWS-derived, and RDM's 300-day ceiling applies to every copy",
    ),
    (
        "line_coverage_half_hourly_stats",
        "it is LDBWS-derived, and RDM's 300-day ceiling applies to every copy",
    ),
    (
        "station_samples",
        "it is LDBWS-derived, and RDM's 300-day ceiling applies to every copy",
    ),
];

/// `trains` rows per archive batch (one transaction, one object per table).
/// Same size as `queries::PRUNE_TRAINS_BATCH`. At ~25 movement events per
/// train this is ~25k event rows per batch -- a few MB once compressed,
/// which is what is held in memory before the upload.
pub const ARCHIVE_TRAINS_BATCH: i64 = 1000;

/// zstd level: 3 is zstd's own default, a good speed/ratio trade-off for
/// JSON text.
const ZSTD_LEVEL: i32 = 3;

/// What to do when an upload fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum FailurePolicy {
    /// Keep the rows (roll back the batch) and retry next retention cycle.
    /// The table grows past its window while storage is unreachable.
    Retain,
    /// Delete the rows anyway, without archiving them -- the same outcome
    /// as running with archiving disabled. For operators who care more
    /// about the retention window than about the archive being complete.
    Delete,
}

/// A secret string that never prints its value via `Debug`.
#[derive(Clone)]
pub struct Secret(String);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(***)")
    }
}

fn parse_secret(s: &str) -> Result<Secret, std::convert::Infallible> {
    Ok(Secret(s.to_string()))
}

/// CLI/env settings for the cold archive, flattened into `Config`.
///
/// Every field is optional or defaulted so a deployment that leaves
/// `ARCHIVE_ENABLED` unset needs none of them; [`Archiver::from_args`]
/// validates the combination only when archiving is enabled.
#[derive(Debug, Clone, clap::Args)]
pub struct ArchiveArgs {
    /// Master switch. False (the default) keeps pruning delete-only.
    #[arg(long, env, default_value_t = false)]
    pub archive_enabled: bool,

    /// Comma-separated tables to archive before pruning. Must be a subset
    /// of `ARCHIVABLE_TABLES`.
    #[arg(long, env, value_delimiter = ',')]
    pub archive_tables: Vec<String>,

    /// S3 endpoint URL, e.g. `https://thoth.example.ts.net`. Unset means
    /// AWS's own regional endpoint.
    #[arg(long, env)]
    pub archive_s3_endpoint: Option<String>,

    #[arg(long, env)]
    pub archive_s3_bucket: Option<String>,

    /// Key prefix inside the bucket (no leading/trailing slash needed).
    #[arg(long, env, default_value = "")]
    pub archive_s3_prefix: String,

    /// Most non-AWS S3 servers ignore the region but still need one for
    /// request signing.
    #[arg(long, env, default_value = "us-east-1")]
    pub archive_s3_region: String,

    #[arg(long, env, value_parser = parse_secret, hide_env_values = true)]
    pub archive_s3_access_key_id: Option<Secret>,

    #[arg(long, env, value_parser = parse_secret, hide_env_values = true)]
    pub archive_s3_secret_access_key: Option<Secret>,

    /// Path-style addressing (`https://endpoint/bucket/key`), which most
    /// non-AWS S3 servers need. Set false for virtual-hosted style.
    #[arg(long, env, default_value_t = true, action = clap::ArgAction::Set)]
    pub archive_s3_path_style: bool,

    /// Permit a plain `http://` endpoint. Off by default.
    #[arg(long, env, default_value_t = false, action = clap::ArgAction::Set)]
    pub archive_s3_allow_http: bool,

    #[arg(long, env, value_enum, default_value_t = FailurePolicy::Retain)]
    pub archive_failure_policy: FailurePolicy,
}

/// Validates `tables` against [`ARCHIVABLE_TABLES`], with a specific
/// message for the licensing-excluded ones.
fn validate_tables(tables: &[String]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for raw in tables {
        let table = raw.trim();
        if table.is_empty() {
            continue;
        }
        if let Some((_, why)) = LICENSING_EXCLUDED_TABLES.iter().find(|(t, _)| *t == table) {
            anyhow::bail!("ARCHIVE_TABLES must not include {table:?}: {why}");
        }
        anyhow::ensure!(
            ARCHIVABLE_TABLES.contains(&table),
            "ARCHIVE_TABLES entry {table:?} is not archivable; supported: {ARCHIVABLE_TABLES:?} \
             (\"trains\" also covers its cascaded train_movement_events and train_current_state)"
        );
        if !out.iter().any(|t| t == table) {
            out.push(table.to_string());
        }
    }
    Ok(out)
}

/// A configured archive target: where objects go and what to do when an
/// upload fails.
pub struct Archiver {
    store: Arc<dyn ObjectStore>,
    prefix: Vec<String>,
    tables: Vec<String>,
    policy: FailurePolicy,
}

impl std::fmt::Debug for Archiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Archiver")
            .field("store", &self.store.to_string())
            .field("prefix", &self.prefix.join("/"))
            .field("tables", &self.tables)
            .field("policy", &self.policy)
            .finish()
    }
}

impl Archiver {
    /// `Ok(None)` when archiving is disabled (nothing else is inspected).
    /// Fails loudly on an enabled-but-incomplete or unsafe configuration,
    /// the same "fail loud on bad config" posture as the retention knobs.
    pub fn from_args(args: &ArchiveArgs) -> Result<Option<Self>> {
        if !args.archive_enabled {
            return Ok(None);
        }
        let tables = validate_tables(&args.archive_tables)?;
        let bucket = args
            .archive_s3_bucket
            .as_deref()
            .filter(|b| !b.is_empty())
            .context("ARCHIVE_ENABLED is true but ARCHIVE_S3_BUCKET is not set")?;
        let key_id = args
            .archive_s3_access_key_id
            .as_ref()
            .filter(|s| !s.0.is_empty())
            .context("ARCHIVE_ENABLED is true but ARCHIVE_S3_ACCESS_KEY_ID is not set")?;
        let secret = args
            .archive_s3_secret_access_key
            .as_ref()
            .filter(|s| !s.0.is_empty())
            .context("ARCHIVE_ENABLED is true but ARCHIVE_S3_SECRET_ACCESS_KEY is not set")?;

        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(&args.archive_s3_region)
            .with_access_key_id(&key_id.0)
            .with_secret_access_key(&secret.0)
            .with_virtual_hosted_style_request(!args.archive_s3_path_style)
            .with_allow_http(args.archive_s3_allow_http)
            // Bounded, so an unreachable endpoint fails a batch in about a
            // minute (and, under `retain`, ends this cycle's trains prune)
            // instead of object_store's default 3-minute retry window.
            .with_retry(RetryConfig {
                max_retries: 3,
                retry_timeout: Duration::from_secs(60),
                ..RetryConfig::default()
            })
            .with_client_options(
                ClientOptions::new()
                    .with_timeout(Duration::from_secs(120))
                    .with_connect_timeout(Duration::from_secs(10)),
            );
        if let Some(endpoint) = args
            .archive_s3_endpoint
            .as_deref()
            .filter(|e| !e.is_empty())
        {
            builder = builder.with_endpoint(endpoint);
        }
        let store = builder
            .build()
            .context("building the archive S3 client from ARCHIVE_S3_* settings")?;
        if tables.is_empty() {
            tracing::warn!(
                "ARCHIVE_ENABLED is true but ARCHIVE_TABLES is empty -- nothing will be archived"
            );
        }
        Ok(Some(Self::new(
            Arc::new(store),
            &args.archive_s3_prefix,
            tables,
            args.archive_failure_policy,
        )))
    }

    /// Builds an archiver over any `ObjectStore` (tests use `InMemory`).
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        tables: Vec<String>,
        policy: FailurePolicy,
    ) -> Self {
        Self {
            store,
            prefix: prefix
                .split('/')
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect(),
            tables,
            policy,
        }
    }

    pub fn archives(&self, table: &str) -> bool {
        self.tables.iter().any(|t| t == table)
    }

    /// `<prefix>/<table>/service_date=YYYY-MM-DD/part-<first_id>.jsonl.zst`.
    /// `first_id` is zero-padded to 19 digits (i64's width) so a listing
    /// sorts parts in id order.
    pub fn object_path(&self, table: &str, service_date: NaiveDate, first_id: i64) -> Path {
        let date = format!("service_date={}", service_date.format("%Y-%m-%d"));
        let part = format!("part-{first_id:019}.jsonl.zst");
        Path::from_iter(self.prefix.iter().map(String::as_str).chain([
            table,
            date.as_str(),
            part.as_str(),
        ]))
    }

    /// PUTs `bytes` to `path`, then HEADs it and checks the size, so the
    /// caller only deletes rows once the object is confirmed present in
    /// full. A PUT overwrites any stale object at the same key.
    async fn put_verified(&self, path: &Path, bytes: Vec<u8>) -> Result<()> {
        let len = bytes.len() as u64;
        self.store
            .put(path, PutPayload::from(bytes))
            .await
            .with_context(|| format!("uploading archive object {path}"))?;
        let meta = self
            .store
            .head(path)
            .await
            .with_context(|| format!("verifying archive object {path}"))?;
        anyhow::ensure!(
            meta.size == len,
            "archive object {path} has size {} after upload, expected {len}",
            meta.size
        );
        Ok(())
    }
}

/// Accumulates JSON Lines into a zstd-compressed in-memory buffer, so only
/// the compressed form of a batch is ever held.
pub struct JsonlZstWriter {
    encoder: zstd::stream::write::Encoder<'static, Vec<u8>>,
    rows: u64,
}

impl JsonlZstWriter {
    pub fn new() -> Result<Self> {
        Ok(Self {
            encoder: zstd::stream::write::Encoder::new(Vec::new(), ZSTD_LEVEL)?,
            rows: 0,
        })
    }

    pub fn push(&mut self, json_line: &str) -> Result<()> {
        debug_assert!(
            !json_line.contains('\n'),
            "JSON Lines rows must be single-line"
        );
        self.encoder.write_all(json_line.as_bytes())?;
        self.encoder.write_all(b"\n")?;
        self.rows += 1;
        Ok(())
    }

    /// `(compressed bytes, row count)`.
    pub fn finish(self) -> Result<(Vec<u8>, u64)> {
        Ok((self.encoder.finish()?, self.rows))
    }
}

/// Streams `sql`'s single `text` column (one JSON document per row) into a
/// [`JsonlZstWriter`]. `sql` must take the batch's `trains.id` array as `$1`.
async fn encode_rows(conn: &mut PgConnection, sql: &str, ids: &[i64]) -> Result<(Vec<u8>, u64)> {
    let mut writer = JsonlZstWriter::new()?;
    let mut rows = sqlx::query_scalar::<_, String>(sql)
        .bind(ids)
        .fetch(&mut *conn);
    while let Some(line) = rows.try_next().await? {
        writer.push(&line)?;
    }
    drop(rows);
    writer.finish()
}

/// One archived table's per-batch export query. Each serialises whole rows
/// with `to_jsonb(t)::text`, so every column (current and future) is
/// captured without a hand-maintained column list; Postgres renders
/// timestamps as ISO-8601 and `jsonb` columns as nested JSON.
const TRAINS_GROUP_EXPORTS: &[(&str, &str)] = &[
    (
        "train_movement_events",
        "SELECT to_jsonb(t)::text FROM train_movement_events t \
         WHERE t.trains_id = ANY($1) ORDER BY t.trains_id, t.id",
    ),
    (
        "train_current_state",
        "SELECT to_jsonb(t)::text FROM train_current_state t \
         WHERE t.trains_id = ANY($1) ORDER BY t.trains_id, t.id",
    ),
    (
        "trains",
        "SELECT to_jsonb(t)::text FROM trains t WHERE t.id = ANY($1) ORDER BY t.id",
    ),
];

/// Outcome of one [`archive_and_prune_trains`] run.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TrainsArchiveOutcome {
    /// `trains` rows deleted (same meaning as `queries::prune_trains`'s
    /// return value).
    pub pruned: u64,
    /// Rows written to the archive, per table.
    pub archived_rows: Vec<(String, u64)>,
    pub objects_written: u64,
    /// An upload failed this run. Under `retain` the failing batch was
    /// rolled back and the run stopped there.
    pub upload_failed: bool,
}

impl TrainsArchiveOutcome {
    fn add_rows(&mut self, table: &str, n: u64) {
        match self.archived_rows.iter_mut().find(|(t, _)| t == table) {
            Some((_, total)) => *total += n,
            None => self.archived_rows.push((table.to_string(), n)),
        }
    }
}

/// Archive-then-delete replacement for `queries::prune_trains`, with the
/// same two-tier eligibility: an untracked train (no `train_subscriptions`
/// row) past `untracked_retention_days`, or a tracked one past
/// `retention_days`. Used only when `trains` is in `ARCHIVE_TABLES`.
///
/// Per batch, in ONE transaction: lock the batch's `trains` rows, export
/// them and their `train_movement_events`/`train_current_state` rows to
/// objects, verify each object, then `DELETE` exactly those `trains` ids
/// (the children go by the same `ON DELETE CASCADE` as today) and commit.
/// Each batch is its own transaction, as in `prune_trains`.
///
/// An upload failure is not an `Err`: under [`FailurePolicy::Retain`] the
/// batch rolls back and this returns with `upload_failed` set (retried
/// next cycle); under [`FailurePolicy::Delete`] the rows are deleted
/// anyway and the rest of this run skips archiving. Returning `Ok` keeps
/// an object-storage outage from aborting the LDBWS-ceiling prunes that
/// `run_retention` runs after this one. Database errors still propagate
/// as `Err`, exactly as `prune_trains`'s do.
pub async fn archive_and_prune_trains(
    pool: &PgPool,
    archiver: &Archiver,
    retention_days: i64,
    untracked_retention_days: i64,
    batch_size: i64,
) -> Result<TrainsArchiveOutcome> {
    let mut outcome = TrainsArchiveOutcome::default();
    let mut skip_uploads = false;

    // One MIN(service_date) probe: when neither tier has anything old
    // enough (almost every cycle) skip the FOR UPDATE candidate scan.
    let (untracked_due, tracked_due) =
        crate::queries::trains_prune_due(pool, retention_days, untracked_retention_days).await?;
    if !untracked_due && !tracked_due {
        return Ok(outcome);
    }

    loop {
        let mut tx = pool.begin().await?;
        // Oldest eligible date first, lowest ids first: deterministic, so a
        // retried batch re-selects the same rows and overwrites the same
        // keys. `FOR UPDATE OF t` also blocks a concurrent
        // train_subscriptions/movement-event insert referencing these rows
        // (their FK takes a KEY SHARE lock) until we commit or roll back.
        let candidates: Vec<(i64, NaiveDate)> = sqlx::query_as(
            "SELECT t.id, t.service_date FROM trains t \
             WHERE t.service_date < CURRENT_DATE - ($4 || ' days')::interval \
               AND ((t.service_date < CURRENT_DATE - ($1 || ' days')::interval \
                     AND NOT EXISTS (SELECT 1 FROM train_subscriptions s WHERE s.trains_id = t.id)) \
                 OR (t.service_date < CURRENT_DATE - ($2 || ' days')::interval \
                     AND EXISTS (SELECT 1 FROM train_subscriptions s WHERE s.trains_id = t.id))) \
             ORDER BY t.service_date, t.id \
             LIMIT $3 \
             FOR UPDATE OF t",
        )
        .bind(untracked_retention_days.to_string())
        .bind(retention_days.to_string())
        .bind(batch_size)
        // The later of the two cutoffs, as a plain range the planner can
        // serve from `trains_service_date` (the OR alone cannot be).
        .bind(retention_days.min(untracked_retention_days).to_string())
        .fetch_all(&mut *tx)
        .await?;

        let Some(&(first_id, service_date)) = candidates.first() else {
            tx.rollback().await?;
            break;
        };
        // One batch never spans two service dates, so every object lives
        // under exactly one `service_date=` partition.
        let ids: Vec<i64> = candidates
            .iter()
            .take_while(|(_, d)| *d == service_date)
            .map(|(id, _)| *id)
            .collect();

        if !skip_uploads {
            match export_batch(&mut tx, archiver, service_date, first_id, &ids).await {
                Ok((rows, objects)) => {
                    for (table, n) in rows {
                        outcome.add_rows(table, n);
                        metrics::counter!(
                            common::metrics::metric_name("aggregator_archive_rows_total"),
                            "table" => table
                        )
                        .increment(n);
                    }
                    outcome.objects_written += objects;
                    metrics::counter!(common::metrics::metric_name(
                        "aggregator_archive_objects_total"
                    ))
                    .increment(objects);
                }
                Err(err) => {
                    outcome.upload_failed = true;
                    metrics::counter!(common::metrics::metric_name(
                        "aggregator_archive_upload_failures_total"
                    ))
                    .increment(1);
                    match archiver.policy {
                        FailurePolicy::Retain => {
                            tracing::error!(
                                error = ?err,
                                %service_date,
                                first_id,
                                "archiving a trains batch failed (export, upload or verification); \
                                 keeping these rows and retrying next retention cycle \
                                 (ARCHIVE_FAILURE_POLICY=retain)"
                            );
                            tx.rollback().await?;
                            break;
                        }
                        FailurePolicy::Delete => {
                            tracing::error!(
                                error = ?err,
                                %service_date,
                                first_id,
                                "archiving a trains batch failed (export, upload or verification); \
                                 deleting rows WITHOUT archiving them for the rest of this \
                                 cycle (ARCHIVE_FAILURE_POLICY=delete)"
                            );
                            skip_uploads = true;
                        }
                    }
                }
            }
        }

        let deleted = sqlx::query("DELETE FROM trains WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        outcome.pruned += deleted;

        // A short candidate list that this batch consumed whole means
        // nothing eligible is left: stop without another candidate scan.
        // (A batch cut at a service-date boundary is not "whole" and loops.)
        if (candidates.len() as i64) < batch_size && ids.len() == candidates.len() {
            break;
        }
    }

    Ok(outcome)
}

/// Exports one batch's three tables and uploads them. Returns the per-table
/// row counts and the number of objects written. A table with no rows in
/// this batch gets no object (an empty file only trips up offline readers).
/// Any error here means "do not delete this batch".
async fn export_batch(
    conn: &mut PgConnection,
    archiver: &Archiver,
    service_date: NaiveDate,
    first_id: i64,
    ids: &[i64],
) -> Result<(Vec<(&'static str, u64)>, u64)> {
    let mut rows = Vec::with_capacity(TRAINS_GROUP_EXPORTS.len());
    let mut objects = 0;
    for &(table, sql) in TRAINS_GROUP_EXPORTS {
        let (bytes, n) = encode_rows(conn, sql, ids).await?;
        if n > 0 {
            let path = archiver.object_path(table, service_date, first_id);
            archiver.put_verified(&path, bytes).await?;
            objects += 1;
        }
        rows.push((table, n));
    }
    Ok((rows, objects))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn archiver(store: Arc<dyn ObjectStore>, policy: FailurePolicy) -> Archiver {
        Archiver::new(
            store,
            "/cold/distant-signal/",
            vec!["trains".into()],
            policy,
        )
    }

    fn decode(bytes: &[u8]) -> Vec<serde_json::Value> {
        let raw = zstd::stream::decode_all(bytes).expect("valid zstd");
        String::from_utf8(raw)
            .expect("utf8")
            .lines()
            .map(|l| serde_json::from_str(l).expect("each line is one JSON document"))
            .collect()
    }

    fn base_args() -> ArchiveArgs {
        ArchiveArgs {
            archive_enabled: true,
            archive_tables: vec!["trains".into()],
            archive_s3_endpoint: Some("https://s3.example.invalid".into()),
            archive_s3_bucket: Some("bucket".into()),
            archive_s3_prefix: "p".into(),
            archive_s3_region: "us-east-1".into(),
            archive_s3_access_key_id: Some(Secret("id".into())),
            archive_s3_secret_access_key: Some(Secret("secret".into())),
            archive_s3_path_style: true,
            archive_s3_allow_http: false,
            archive_failure_policy: FailurePolicy::Retain,
        }
    }

    #[test]
    fn object_path_layout() {
        let a = archiver(Arc::new(InMemory::new()), FailurePolicy::Retain);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        assert_eq!(
            a.object_path("trains", date, 42).as_ref(),
            "cold/distant-signal/trains/service_date=2026-09-01/part-0000000000000000042.jsonl.zst"
        );
        let bare = Archiver::new(Arc::new(InMemory::new()), "", vec![], FailurePolicy::Retain);
        assert_eq!(
            bare.object_path("trains", date, 7).as_ref(),
            "trains/service_date=2026-09-01/part-0000000000000000007.jsonl.zst"
        );
    }

    #[test]
    fn jsonl_zst_round_trips() {
        let mut w = JsonlZstWriter::new().unwrap();
        w.push(r#"{"id":1,"raw_body":{"a":"x"}}"#).unwrap();
        w.push(r#"{"id":2,"raw_body":null}"#).unwrap();
        let (bytes, n) = w.finish().unwrap();
        assert_eq!(n, 2);
        let rows = decode(&bytes);
        assert_eq!(rows[0]["raw_body"]["a"], "x");
        assert_eq!(rows[1]["id"], 2);
    }

    #[test]
    fn disabled_needs_no_s3_settings() {
        let args = ArchiveArgs {
            archive_enabled: false,
            archive_tables: vec!["trust_event_backlog".into()],
            archive_s3_endpoint: None,
            archive_s3_bucket: None,
            archive_s3_access_key_id: None,
            archive_s3_secret_access_key: None,
            ..base_args()
        };
        assert!(Archiver::from_args(&args).unwrap().is_none());
    }

    #[test]
    fn enabled_with_complete_settings_builds() {
        let a = Archiver::from_args(&base_args()).unwrap().expect("enabled");
        assert!(a.archives("trains"));
        assert!(!a.archives("train_movement_events"));
    }

    #[test]
    fn licensing_excluded_tables_are_rejected_with_a_reason() {
        for (table, _) in LICENSING_EXCLUDED_TABLES {
            let args = ArchiveArgs {
                archive_tables: vec!["trains".into(), (*table).into()],
                ..base_args()
            };
            let err = Archiver::from_args(&args).unwrap_err().to_string();
            assert!(err.contains(table), "{err}");
            assert!(err.contains("licens") || err.contains("300-day"), "{err}");
        }
    }

    #[test]
    fn unknown_tables_and_missing_settings_are_rejected() {
        let unknown = ArchiveArgs {
            archive_tables: vec!["schedule_destination_departures".into()],
            ..base_args()
        };
        assert!(Archiver::from_args(&unknown).is_err());
        let no_bucket = ArchiveArgs {
            archive_s3_bucket: None,
            ..base_args()
        };
        assert!(Archiver::from_args(&no_bucket).is_err());
        let no_secret = ArchiveArgs {
            archive_s3_secret_access_key: Some(Secret(String::new())),
            ..base_args()
        };
        assert!(Archiver::from_args(&no_secret).is_err());
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let rendered = format!("{:?}", base_args());
        assert!(!rendered.contains("secret\""), "{rendered}");
        assert!(rendered.contains("Secret(***)"), "{rendered}");
    }

    #[tokio::test]
    async fn put_verified_writes_and_overwrites() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = archiver(store.clone(), FailurePolicy::Retain);
        let path = a.object_path("trains", NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 1);
        a.put_verified(&path, b"first".to_vec()).await.unwrap();
        a.put_verified(&path, b"second!".to_vec()).await.unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"second!");
    }

    #[tokio::test]
    async fn put_verified_fails_against_an_unreachable_endpoint() {
        // A real S3 client (the production code path) pointed at a closed
        // port: the upload must surface as an error, never as success.
        let args = ArchiveArgs {
            archive_s3_endpoint: Some("http://127.0.0.1:9".into()),
            archive_s3_allow_http: true,
            ..base_args()
        };
        let a = Archiver::from_args(&args).unwrap().unwrap();
        let path = a.object_path("trains", NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 1);
        assert!(a.put_verified(&path, b"x".to_vec()).await.is_err());
    }

    /// Wraps `InMemory`, failing PUTs on demand or failing the post-PUT
    /// HEAD (after the object has landed -- the "uploaded, then the batch
    /// rolled back anyway" case).
    #[derive(Debug, Default)]
    struct FlakyStore {
        inner: InMemory,
        fail_put: std::sync::atomic::AtomicBool,
        fail_head: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("FlakyStore")
        }
    }

    fn injected() -> object_store::Error {
        object_store::Error::Generic {
            store: "FlakyStore",
            source: "injected failure".into(),
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if self.fail_put.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(injected());
            }
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
            if options.head && self.fail_head.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(injected());
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
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

    // ---- DB-gated: archive_and_prune_trains against a real Postgres ----

    async fn pool() -> PgPool {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect")
    }

    /// Seeds `count` trains on `date` (uid prefix `tag`), each with two
    /// movement events and one current-state row. Returns the ids.
    async fn seed(pool: &PgPool, tag: &str, date: NaiveDate, count: i64) -> Vec<i64> {
        sqlx::query("DELETE FROM trains WHERE train_uid LIKE $1 || '%'")
            .bind(tag)
            .execute(pool)
            .await
            .unwrap();
        let ids: Vec<i64> = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date, origin_crs, calling_points) \
             SELECT $1 || gs, $2, 'EUS', '[{\"crs\":\"EUS\"}]'::jsonb \
             FROM generate_series(1, $3) gs ORDER BY gs RETURNING id",
        )
        .bind(tag)
        .bind(date)
        .bind(count)
        .fetch_all(pool)
        .await
        .unwrap();
        for &id in &ids {
            for n in 0..2 {
                sqlx::query(
                    "INSERT INTO train_movement_events \
                         (trains_id, event_type, msg_type, dedup_key, raw_body, actual_timestamp) \
                     VALUES ($1, 'departure', '0003', $2, jsonb_build_object('n', $3::int, 'loc', 'EUS'), NOW())",
                )
                .bind(id)
                .bind(format!("{tag}-{id}-{n}"))
                .bind(n)
                .execute(pool)
                .await
                .unwrap();
            }
            sqlx::query(
                "INSERT INTO train_current_state (trains_id, status) VALUES ($1, 'completed')",
            )
            .bind(id)
            .execute(pool)
            .await
            .unwrap();
        }
        ids
    }

    async fn remaining(pool: &PgPool, ids: &[i64]) -> (i64, i64, i64) {
        let q = |sql: &'static str| {
            let ids = ids.to_vec();
            async move {
                sqlx::query_scalar::<_, i64>(sql)
                    .bind(ids)
                    .fetch_one(pool)
                    .await
                    .unwrap()
            }
        };
        (
            q("SELECT COUNT(*) FROM trains WHERE id = ANY($1)").await,
            q("SELECT COUNT(*) FROM train_movement_events WHERE trains_id = ANY($1)").await,
            q("SELECT COUNT(*) FROM train_current_state WHERE trains_id = ANY($1)").await,
        )
    }

    async fn objects_under(store: &dyn ObjectStore, prefix: &str) -> Vec<Path> {
        let mut paths: Vec<Path> = store
            .list(Some(&Path::from(prefix)))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .unwrap();
        paths.sort();
        paths
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p aggregator -- --ignored"]
    async fn archives_every_row_to_expected_keys_then_deletes() {
        let pool = pool().await;
        let date = NaiveDate::from_ymd_opt(2001, 2, 3).unwrap();
        let ids = seed(&pool, "TEST-ARCHIVE-OK-", date, 5).await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = archiver(store.clone(), FailurePolicy::Retain);

        // Batch of 2 -> parts starting at ids[0], ids[2], ids[4].
        let outcome = archive_and_prune_trains(&pool, &a, 30, 14, 2)
            .await
            .unwrap();
        assert!(!outcome.upload_failed);
        assert!(outcome.pruned >= 5);
        assert_eq!(remaining(&pool, &ids).await, (0, 0, 0));

        for table in ["trains", "train_movement_events", "train_current_state"] {
            let dir = format!("cold/distant-signal/{table}/service_date=2001-02-03");
            let paths = objects_under(store.as_ref(), &dir).await;
            let expected: Vec<Path> = [ids[0], ids[2], ids[4]]
                .iter()
                .map(|&id| a.object_path(table, date, id))
                .collect();
            assert_eq!(paths, expected, "{table} parts");

            let mut rows = Vec::new();
            for p in &paths {
                let bytes = store.get(p).await.unwrap().bytes().await.unwrap();
                rows.extend(decode(&bytes));
            }
            let per_train = if table == "train_movement_events" {
                2
            } else {
                1
            };
            assert_eq!(rows.len(), 5 * per_train, "{table} row count");
            if table == "trains" {
                let got: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
                assert_eq!(got, ids);
                assert_eq!(rows[0]["service_date"], "2001-02-03");
                assert_eq!(rows[0]["origin_crs"], "EUS");
                assert_eq!(rows[0]["calling_points"][0]["crs"], "EUS");
            }
            if table == "train_movement_events" {
                assert_eq!(rows[1]["raw_body"]["n"], 1);
                assert_eq!(rows[1]["raw_body"]["loc"], "EUS");
                assert_eq!(rows[1]["trains_id"].as_i64().unwrap(), ids[0]);
            }
            if table == "train_current_state" {
                assert_eq!(rows[0]["status"], "completed");
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p aggregator -- --ignored"]
    async fn failed_upload_keeps_rows_under_retain() {
        let pool = pool().await;
        let date = NaiveDate::from_ymd_opt(2001, 3, 4).unwrap();
        let ids = seed(&pool, "TEST-ARCHIVE-FAIL-", date, 3).await;
        let flaky = Arc::new(FlakyStore::default());
        flaky
            .fail_put
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let a = archiver(flaky.clone(), FailurePolicy::Retain);

        let outcome = archive_and_prune_trains(&pool, &a, 30, 14, 1000)
            .await
            .unwrap();
        assert!(outcome.upload_failed);
        assert_eq!(
            remaining(&pool, &ids).await,
            (3, 6, 3),
            "nothing may be deleted"
        );
        assert!(objects_under(flaky.as_ref(), "cold").await.is_empty());

        sqlx::query("DELETE FROM trains WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p aggregator -- --ignored"]
    async fn failed_verification_rolls_back_and_retry_overwrites_same_keys() {
        let pool = pool().await;
        let date = NaiveDate::from_ymd_opt(2001, 4, 5).unwrap();
        let ids = seed(&pool, "TEST-ARCHIVE-RETRY-", date, 3).await;
        let flaky = Arc::new(FlakyStore::default());
        // The PUT lands but the HEAD check fails: objects exist, rows must
        // stay.
        flaky
            .fail_head
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let a = archiver(flaky.clone(), FailurePolicy::Retain);
        let first = archive_and_prune_trains(&pool, &a, 30, 14, 1000)
            .await
            .unwrap();
        assert!(first.upload_failed);
        assert_eq!(remaining(&pool, &ids).await, (3, 6, 3));

        flaky
            .fail_head
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let second = archive_and_prune_trains(&pool, &a, 30, 14, 1000)
            .await
            .unwrap();
        assert!(!second.upload_failed);
        assert_eq!(remaining(&pool, &ids).await, (0, 0, 0));

        let dir = "cold/distant-signal/trains/service_date=2001-04-05";
        let paths = objects_under(flaky.as_ref(), dir).await;
        assert_eq!(
            paths,
            vec![a.object_path("trains", date, ids[0])],
            "one part, no duplicate"
        );
        let bytes = flaky.get(&paths[0]).await.unwrap().bytes().await.unwrap();
        assert_eq!(decode(&bytes).len(), 3);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p aggregator -- --ignored"]
    async fn failed_upload_deletes_anyway_under_delete_policy() {
        let pool = pool().await;
        let date = NaiveDate::from_ymd_opt(2001, 5, 6).unwrap();
        let ids = seed(&pool, "TEST-ARCHIVE-DEL-", date, 3).await;
        let flaky = Arc::new(FlakyStore::default());
        flaky
            .fail_put
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let a = archiver(flaky.clone(), FailurePolicy::Delete);

        let outcome = archive_and_prune_trains(&pool, &a, 30, 14, 1000)
            .await
            .unwrap();
        assert!(outcome.upload_failed);
        assert_eq!(remaining(&pool, &ids).await, (0, 0, 0));
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p aggregator -- --ignored"]
    async fn keeps_rows_inside_their_retention_tier() {
        let pool = pool().await;
        // 20 days old: past the 14-day untracked tier, inside the 30-day
        // tracked tier.
        let date = chrono::Utc::now().date_naive() - chrono::Duration::days(20);
        let ids = seed(&pool, "TEST-ARCHIVE-TIER-", date, 2).await;
        let user_id = "test-archive-tier-user";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $1) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("archive-tier@example.com")
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO train_subscriptions \
                 (user_id, trains_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, 'EUS', NOW())",
        )
        .bind(user_id)
        .bind(ids[1])
        .bind(date)
        .execute(&pool)
        .await
        .unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = archiver(store.clone(), FailurePolicy::Retain);
        archive_and_prune_trains(&pool, &a, 30, 14, 1000)
            .await
            .unwrap();

        assert_eq!(
            remaining(&pool, &ids[..1]).await,
            (0, 0, 0),
            "untracked goes"
        );
        assert_eq!(
            remaining(&pool, &ids[1..]).await,
            (1, 2, 1),
            "tracked stays"
        );
        let dir = format!("cold/distant-signal/trains/service_date={date}");
        let paths = objects_under(store.as_ref(), &dir).await;
        let bytes = store.get(&paths[0]).await.unwrap().bytes().await.unwrap();
        let archived: Vec<i64> = decode(&bytes)
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(archived, vec![ids[0]]);

        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM trains WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .unwrap();
    }
}
