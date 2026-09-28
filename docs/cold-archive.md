# Cold archive for retention prunes

The aggregator can archive rows to S3-compatible object storage before
deleting them as part of a retention prune. The feature is **off by
default**: with `archive.enabled: false` (the default Helm value, or
`ARCHIVE_ENABLED` unset) pruning only deletes rows, and no S3 setting is
read or required.

Code: `crates/aggregator/src/archive.rs`. Chart: `archive.*` in
`charts/distant-signal/values.yaml`.

## What can be archived

| `archive.tables` entry | Archived objects |
| --- | --- |
| `trains` | `trains`, `train_movement_events` and `train_current_state` (the two child tables that `ON DELETE CASCADE` removes along with each `trains` row). `train_movement_events.raw_body` is left out (see below). |

The two retention tiers stay as they are. An untracked train is archived
and pruned after `untrackedTrainsRetentionDays` (14 days by default). A
train with a subscription is archived and pruned after
`trainsRetentionDays` (30 days by default).

Some tables can never be archived. The binary refuses to start, and the
chart refuses to render, if one of them is listed:

- **`trust_event_backlog`**: its 1-day retention is a deliberate TRUST
  licensing safeguard (see `Config::trust_event_backlog_retention_days`),
  and an archived copy would defeat it.
- **`train_movement_events.raw_body`**, the verbatim TRUST message, is
  stripped from every archived movement event, which keeps only the derived
  columns (event type, location, planned and actual times, variation
  status). Keeping the licensed feed's raw messages indefinitely in a bucket
  is what `trust_event_backlog`'s 1-day retention exists to avoid, so the
  archive does not do it either (triage decision DQ14, 2026-09-27).
- **LDBWS-derived tables** (`line_status_history`,
  `line_status_daily_stats`, `line_status_half_hourly_stats`,
  `line_coverage_*_stats`, `station_samples`): RDM's 300-day ceiling
  applies to every copy. Archiving them safely would also need an expiry
  rule on the bucket that deletes objects older than 300 days, and the
  app cannot check that such a rule exists. These tables are left out
  entirely.

The app never reads archived data back. History routes read only the
live (hot) tables in Postgres.

## Object expiry

The app never deletes an archived object, and it cannot see the bucket's
lifecycle rules. Expiry is therefore the bucket's job, and enabling the
archive requires you to confirm it:

- The chart refuses to render with `archive.enabled: true` unless
  `archive.s3.lifecycleConfirmed: true` is also set.
- The aggregator refuses to start with `ARCHIVE_ENABLED=true` unless
  `ARCHIVE_S3_LIFECYCLE_CONFIRMED=true` is also set. The chart sets it for
  you once `lifecycleConfirmed` is true.

Before you set it, add an expiration rule covering the archive prefix. The
expiry age is a licensing decision, not a technical one: the archived rows
are derived from TRUST (Network Rail's movement feed, via the Rail Data
Marketplace), so pick an age your review of that licence allows. With the
AWS CLI (most S3-compatible servers accept the same call):

```sh
aws --endpoint-url https://thoth.<tailnet>.ts.net s3api put-bucket-lifecycle-configuration \
  --bucket distant-signal-archive --lifecycle-configuration '{
    "Rules": [{
      "ID": "distant-signal-archive-expiry",
      "Filter": {"Prefix": "prod/"},
      "Status": "Enabled",
      "Expiration": {"Days": 365}
    }]
  }'
aws --endpoint-url https://thoth.<tailnet>.ts.net s3api get-bucket-lifecycle-configuration \
  --bucket distant-signal-archive
```

The `365` above is only an example. S3-compatible servers differ in how
much of the lifecycle API they implement, so check that yours actually
enforces the expiration rule.

## Configuration

```yaml
archive:
  enabled: true
  tables: [trains]
  failurePolicy: retain        # or: delete
  s3:
    endpoint: https://thoth.<tailnet>.ts.net
    bucket: distant-signal-archive
    prefix: prod
    region: us-east-1          # needed for request signing; most non-AWS servers accept any value
    pathStyle: true            # https://endpoint/bucket/key
    allowHttp: false
    existingSecret: thoth-archive-creds
    accessKeyIdKey: access-key-id
    secretAccessKeyKey: secret-access-key
    lifecycleConfirmed: true   # only after adding an expiry rule; see "Object expiry"
```

The chart never creates the credentials Secret. Create it yourself:

```sh
kubectl create secret generic thoth-archive-creds -n <ns> \
  --from-literal=access-key-id=... --from-literal=secret-access-key=...
```

The chart turns these values into `ARCHIVE_*` environment variables on the
aggregator Deployment. You can set the same variables directly, for
example in docker-compose: `ARCHIVE_ENABLED`, `ARCHIVE_TABLES`
(comma-separated), `ARCHIVE_FAILURE_POLICY`, `ARCHIVE_S3_ENDPOINT`,
`ARCHIVE_S3_BUCKET`, `ARCHIVE_S3_PREFIX`, `ARCHIVE_S3_REGION`,
`ARCHIVE_S3_PATH_STYLE`, `ARCHIVE_S3_ALLOW_HTTP`,
`ARCHIVE_S3_LIFECYCLE_CONFIRMED`, `ARCHIVE_S3_ACCESS_KEY_ID` and
`ARCHIVE_S3_SECRET_ACCESS_KEY`.

## How it works

The retention prunes run on their own task in the aggregator, on the same
interval as aggregation but independent of it, so a slow or failing upload
never delays line-status aggregation.

Every retention run, the prune processes batches of up to 1,000 `trains`
rows. Each batch covers a single `service_date`: the oldest eligible date
is taken first, and within that date the lowest ids are taken first. No
database transaction or row lock is held while object storage is being
talked to. Each batch goes through three steps:

1. **Export.** A short read-only transaction (one `REPEATABLE READ`
   snapshot) streams the batch's rows from each table through
   `to_jsonb(row)::text` into a zstd-compressed JSON Lines buffer, and takes
   a fingerprint of each table's rows (row count plus a sum of row hashes).
   Only the compressed bytes are held in memory, a few MB per batch.
2. **Upload.** Each object is uploaded with a PUT, and a HEAD then confirms
   the stored size and ETag (see "Upload verification" below).
3. **Delete.** A short transaction locks the batch's `trains` rows
   (`SELECT ... FOR UPDATE`), checks that every row is still eligible and
   that every table's fingerprint is unchanged since the export, then runs
   `DELETE FROM trains WHERE id = ANY(batch)` (the child tables cascade)
   and commits. If anything changed in between (for example, someone
   subscribed to one of the trains), nothing is deleted and the run stops.
   The next run exports that batch again and overwrites the same keys.
   `distant_signal_aggregator_archive_batches_changed_total` counts these.

Object keys:

```
<prefix>/<table>/service_date=YYYY-MM-DD/part-<first trains.id in batch, 19 digits>.jsonl.zst
```

If a table has no rows in a batch, no object is written for it.

**Upload verification.** After each PUT, a HEAD must report the same size
as the body sent, and both the PUT's and the HEAD's ETag must equal the
hex MD5 of the body. For a single-part PUT (every archive object is one),
AWS S3 and most S3-compatible servers use the body's MD5 as the ETag, so
this catches a stored object that differs from what was sent even when
the length matches. A bucket
whose ETags are not content MD5s, such as AWS SSE-KMS or SSE-C encryption,
fails verification on every upload, so under `retain` nothing is ever
pruned. Use SSE-S3 or no server-side encryption for the archive bucket.

**Failure behaviour** (`failurePolicy`):

- `retain` (default): if an upload or its verification fails, the batch
  is not deleted. The trains prune stops for this run and runs again next
  time. The table grows past its retention window for as long as storage
  stays unreachable. A failed upload does not stop the other prunes that
  run later in the same pass, including the LDBWS 300-day-ceiling prunes.
- `delete`: the failed batch and every later batch in that run are
  deleted without being archived, just as if archiving were disabled.

**Idempotency.** Once a batch commits, its first id no longer exists, so
no later batch can reuse its key. Suppose an upload lands but the batch
is then not deleted (verification failed, the batch changed before the delete,
the DELETE or COMMIT failed, or the pod restarted). The rows are still in
Postgres. The next run selects the same batch and overwrites the same
keys, so no row is lost or
duplicated. One rare case can still leave a duplicate. It happens when the
set of eligible trains for that date changes between the failed attempt
and the retry, for example when someone subscribes to a 14-day-old train.
The retry can then start at a different id and leave the stale object in
place. Every row carries its primary key `id`, so readers should
deduplicate on it (see below).

**Metrics:**

- `distant_signal_aggregator_archive_rows_total{table}`
- `distant_signal_aggregator_archive_objects_total`
- `distant_signal_aggregator_archive_upload_failures_total`
- `distant_signal_aggregator_archive_batches_changed_total`
- `distant_signal_aggregator_retention_duration_seconds` (the whole retention pass)

**Alert:** with `metrics.prometheusRule.enabled` and `archive.enabled`, the
chart renders `DistantSignalArchiveUploadFailures`, which fires when at
least `metrics.prometheusRule.archiveUploadFailures.minFailures` (3) batch
uploads fail within its `window` (1h).

`distant_signal_aggregator_trains_rows_pruned_total` keeps its existing meaning.

## Reading an archive offline

Each object is newline-delimited JSON, one row per line, compressed with
zstd. Column names match the Postgres table. Timestamps are ISO-8601
strings, and `jsonb` columns such as `calling_points` are
nested JSON.

With [DuckDB](https://duckdb.org/) (its `httpfs` extension autoloads):

```sql
CREATE SECRET thoth (
  TYPE s3, KEY_ID '...', SECRET '...',
  ENDPOINT 'thoth.<tailnet>.ts.net', URL_STYLE 'path', REGION 'us-east-1'
);

-- Every movement event for one service date, deduplicated on id.
SELECT DISTINCT ON (id) *
FROM read_json('s3://distant-signal-archive/prod/train_movement_events/service_date=2026-09-01/*.jsonl.zst',
               format = 'newline_delimited', compression = 'zstd', hive_partitioning = true)
ORDER BY id;

-- Trains joined to their final state over a month.
SELECT t.train_uid, t.service_date, s.status, s.delay_minutes
FROM read_json('s3://distant-signal-archive/prod/trains/service_date=2026-09-*/*.jsonl.zst',
               format = 'newline_delimited', compression = 'zstd') t
JOIN read_json('s3://distant-signal-archive/prod/train_current_state/service_date=2026-09-*/*.jsonl.zst',
               format = 'newline_delimited', compression = 'zstd') s
  ON s.trains_id = t.id;
```

Without DuckDB, download the objects and decompress them:

```sh
aws --endpoint-url https://thoth.<tailnet>.ts.net s3 cp --recursive \
  s3://distant-signal-archive/prod/trains/service_date=2026-09-01/ ./trains-2026-09-01/
zstdcat ./trains-2026-09-01/*.jsonl.zst | jq -c 'select(.matched_line_id == "some-line")'
```
