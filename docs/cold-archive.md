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
| `trains` | `trains`, `train_movement_events`, `train_current_state` and `train_reasons` (the child tables that `ON DELETE CASCADE` removes along with each `trains` row). `train_movement_events.raw_body` is left out (see below). `notifier_forward_queue` also cascades but is not archived: it is a transient push-forwarding signal, not history. Its rows leave only with their `trains` row, which `DistantSignalNotifierForwardQueueRetentionOverdue` watches ([alerts](https://github.com/Distant-Signal/ds-deploy/blob/main/docs/alerts.md#distantsignalnotifierforwardqueueretentionoverdue)). `train_service_labels` (ds-core v0.15.0, DS-513) also cascades and is not archived: it is an internal-only label whose `service_class` is derivable from the archived `trains.train_id` headcode, and whose `toc_id` is deliberately not kept. |

`train_reasons` was added on 2026-10-08. Before that, the archive deleted
its rows through the cascade without archiving them, so archived batches
from earlier dates have no `train_reasons/` objects. The aggregator's DB
test `every_cascading_child_of_trains_is_archived` reads `pg_constraint`
and fails if a migration adds another cascading child of `trains` that is
neither exported nor excluded with a reason.

The two retention tiers stay as they are. An untracked train is archived
and pruned after `untrackedTrainsRetentionDays` (14 days by default). A
train with a subscription is archived and pruned after
`trainsRetentionDays` (30 days by default).

**Journey hold (all `trains` prunes, archived or not).** A train that a
journey leg is bound to is not deleted for as long as that leg exists,
however old it is: the journey keeps the train's whole scheduled and tracked
data (movement events, current state, reasons, service labels) and the train
stays found by its uid. Tracking a train is a one-leg journey, so this covers
every tracked train. The test is `train_subscriptions.trains_id = trains.id`
joined to `journey_legs.train_subscription_id`; whether the leg has a final
snapshot (`journey_legs.final_snapshot_at`) no longer matters, and the
snapshot is a fallback, not a reason to delete. The hold ends when the api's
journey retention deletes the leg's `train_subscriptions` row
(`PAST_TRAVEL_RETENTION_DAYS`, 548 days from the service date; the journey
goes with it), after which the train is an ordinary tracked train and goes
at `trainsRetentionDays`, archived first. A train with a subscription but no
leg is not held.

The hold has a hard bound, `TRAINS_SNAPSHOT_GUARD_MAX_DAYS` (default 600,
counted from the train's `service_date`; 0 turns the hold off; the variable
keeps its first name). It is above the 548-day journey retention on purpose:
past it the row is archived and pruned anyway, so a journey retention that
stopped running cannot grow `trains` without bound. Each retention run logs a
WARN with `held` (kept past retention) and `forced` (pruned by the bound with
a leg still bound), and exports
`distant_signal_aggregator_trains_snapshot_guard_held` (gauge) and
`distant_signal_aggregator_trains_snapshot_guard_forced_total` (counter).
While rows are held, the cheap "nothing old enough" probe no longer
short-circuits, so every run does the candidate scan; the scan walks the
held rows (one index probe each) before the rows behind them. The hold
reads `journey_legs (train_subscription_id)` and
`train_subscriptions (id, trains_id)`, which the aggregator role has from
ds-core v0.17.6: the grants setup Job must have run before an aggregator
with the hold rolls out, or the prune fails with permission denied. No chart
change is needed (the default applies, and ds-deploy's
`check-config-defaults-chart.py` already lists the variable as unwired); a
chart value for `TRAINS_SNAPSHOT_GUARD_MAX_DAYS` is optional. The hold keeps
the retention windows' other rule: `trainsRetentionDays` still decides when
an unbound tracked train goes.

**Pinned schedule, Darwin and board rows.** Tracking a train pins the rest of
what the train page and leg detail read about it, not only the `trains` row.
The retention run keeps, for each train a journey leg is bound to (the same
test and the same hard bound as the journey hold above), only the rows keyed
to that train and service date, past their own windows:

| Table | Key to the train | Own window | What it gives the page | Estimated size per train |
| --- | --- | --- | --- | --- |
| `schedule_services` | `(service_date, uid)` | `trainsRetentionDays` (min 8 days) | service mode (bus, ferry), operator code, headcode | 1 row, about 100 bytes |
| `schedule_destination_departures` | `(service_date, train_uid)` | 8 days | operator, headcode, "known scheduled train" | one row per departure, about 10 to 40 rows, a few KB |
| `schedule_calling_points_full` | `(service_date, uid)` | 8 days | booked stops and platforms, when `trains.calling_points` is empty | about 20 to 60 rows, a few KB |
| `schedule_turnarounds` | `(service_date, uid)` | 8 days | the previous working's platform and times | 1 row |
| `schedule_associations` | the main or the associated `(service_date, uid)` | 8 days | joins and divides | 0 to 4 rows |
| `darwin_services`, `darwin_calls`, `darwin_associations` | RID, from `darwin_services (uid, ssd)` | 2 days past the start date | forecast times, platform changes and their confirmed flags, cancellations | 1 service, about 20 to 80 calls (about 200 bytes each), under 20 KB |
| `tracked_stop_boards`, `tracked_stop_arrival_boards` | `(train_uid, service_date)` | 24 hours from the last poll; held to at most 300 days from the row's own `polled_at` (licence cap, below) | the station board last polled for each of the train's stops | one row per stop with a JSON board, about 2 to 20 KB each, so up to a few hundred KB |

**Boards are capped at 300 days (decision of 2026-11-22, DS-638).** The two
board tables are LDBWS-derived (ds-core: `tracked_stop_arrival_boards` is the
"Latest LDBWS arrival board per tracked train and alighting stop";
`tracked_stop_boards` is filled by the same poller's departure-board lane),
and RDM's 300-day ceiling applies to every copy of LDBWS data (see "What can
be archived", below). So the journey hold does not keep them for the journey's
life: a board whose own `polled_at` is more than 300 days old is deleted by
the next retention run even while its train is bound to a journey leg. A board
polled 300 days ago or less is held under the pin's usual rules. The cap runs
from `polled_at`, the capture time of the board (not the service date, which
can be earlier than the last poll), and is a separate bound,
`LDBWS_PINNED_BOARDS_MAX_DAYS` (default 300, so no chart value is needed). A
larger value is refused at startup, because the ceiling is a licence rule, not
a tuning knob; 0 holds no board past the ordinary 24-hour rule. Every other
pinned table keeps the 600-day bound above: `schedule_*` is the CIF timetable
and `darwin_*` is the Darwin Push Port feed, and neither is LDBWS data. This
repository states no licence ceiling for either feed.

**Raw TRUST messages stay on pinned live rows (decision of 2026-11-22).** The
journey hold keeps a pinned train's `train_movement_events` whole, including
`raw_body`, for the journey's lifetime; nothing scrubs it while the row is
held. The archive still strips `raw_body` from every archived movement event
(triage decision DQ14, below), and `trust_event_backlog` keeps its 1-day
window; neither changes.

Every one of these is a single anti-join against the set of bound trains
(`PINNED_TRAINS_CTE`, evaluated once per statement from the legs, never from
a scan of `trains`), so a prune costs what it did plus one hash probe per row
it would have deleted; no row is deleted and re-inserted, and the first run
after this ships deletes nothing more than before, so there is no bulk write
(until the storage migration the node's kubelet eviction threshold is 5%
free, but the pin only adds the few KB above per bound train, never a whole
day). The pin keeps what exists: a row that was already pruned, or never
recorded (a Darwin call for a train bound after its start date plus two days,
a board for a stop the train had already passed), is not recovered. The pin
ends with the journey hold: when the journey (so the leg) is deleted or the
bound passes, the rows go on the next run by the ordinary rule. These tables
are not archived; `ARCHIVABLE_TABLES` is still `trains` and the archive's
order (upload, then delete) is untouched.

Not pinned, because no safe per-train key exists or a rule forbids it:
`trust_event_backlog` (1-day TRUST licensing window; the tracked train's own
`train_movement_events` hold the same facts), `notifier_forward_queue`
(transient push signals, gone with the `trains` row), `station_samples`,
`schedule_line_population`, `schedule_network_departures` and
`line_train_summaries` (per station or line, not per train),
`station_full_coverage_samples` and the full-coverage statistics (aggregates).
The Island of Ireland board history (`island_of_ireland_trains`,
`island_of_ireland_train_calls`) is pruned by ingest-writer, not here.

**Loss alarm.** Each retention run exports
`distant_signal_aggregator_pinned_trains_missing_data{kind}` (gauge): bound,
schedule-matched GB trains from 2026-11-22 on, old enough for the ordinary
retention to have removed the rows, that have no `schedule_services` row
(`kind="schedule_services"`), no `schedule_destination_departures` row
(`schedule_departures`), neither `trains.calling_points` nor a
`schedule_calling_points_full` row (`calling_points`) or no
`train_current_state` row (`current_state`). All zero while the pin works;
the retention run also logs an ERROR per non-zero kind.

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

The archiver never deletes an archived object. Something else must, so
enabling the archive requires one of two things:

- **A bucket lifecycle rule** that you confirm with
  `archive.s3.lifecycleConfirmed: true` (`ARCHIVE_S3_LIFECYCLE_CONFIRMED`).
  The app cannot see the bucket's rules, so this is your word for it.
- **Client-side expiry** by the aggregator, with `archive.expiry.enabled:
  true` (`ARCHIVE_EXPIRY_ENABLED`). Use this when the store has no
  lifecycle API, as on Thoth (`GET ?lifecycle` returns 501). See
  "Client-side expiry" below.

The chart refuses to render, and the aggregator refuses to start, with the
archive enabled and neither set. The expiry age is a licensing decision,
not a technical one: the archived rows are derived from TRUST (Network
Rail's movement feed, via the Rail Data Marketplace), so pick an age your
review of that licence allows. In production it is 730 days (decided
2026-09-30).

### Client-side expiry

Code: `crates/aggregator/src/archive_expiry.rs`. Off by default, and
**dry-run by default** once enabled.

```yaml
archive:
  expiry:
    enabled: true
    dryRun: true              # log and count only; set false once the metrics look right
    retentionDays: 730        # the floor of 90 is fixed in code
    intervalSecs: 86400
    maxDeletesPerRun: 20000
    protectedPrefixes: [prod-cluster/backups/, prod-cluster/pgbackrest/]
```

A task in the aggregator, separate from the retention prunes, runs at
startup and then every `intervalSecs`. For each of `trains`,
`train_movement_events`, `train_current_state` and `train_reasons` it lists
`<prefix>/<table>/` (ListObjectsV2). It deletes an object only if all of
these hold:

- its key matches exactly
  `<prefix>/<table>/service_date=YYYY-MM-DD/part-<19 digits>.jsonl.zst`,
  with a real calendar date;
- the `service_date` **in the key** is older than the current rail day
  (02:00 Europe/London) minus `retentionDays`. The object's
  `LastModified` is never used. With 730 days, the date exactly 730 days
  back is kept and the one before it goes;
- `retentionDays` is at least 90. That floor has no setting: a lower value
  fails the render and the aggregator's startup;
- the key is not under a `protectedPrefixes` entry.

Keys that don't match are counted and logged (the first 20 per table per
run), never deleted. Each run handles at most `maxDeletesPerRun` objects,
oldest date first, and sends one single-object `DELETE` per object (bulk
`DeleteObjects` is disabled on the archive client). After 10 consecutive
failed DELETEs a run stops, and the next run retries.

The archive's S3 key may be able to delete other things in the same bucket
(in production, the backups). So, at startup, the aggregator refuses to
run expiry if `archive.s3.prefix` is empty, has fewer than two path
segments, or equals, contains or sits inside any `protectedPrefixes` entry.

Metrics, all registered at 0 at startup (names carry the
`distant_signal_` prefix):

| Metric | Meaning |
| --- | --- |
| `aggregator_archive_expiry_candidates{table}` | Gauge: matching keys past retention in the last run, before the cap. |
| `aggregator_archive_expiry_would_delete{table}` | Gauge: what the last dry run would have deleted, after the cap. 0 when live. |
| `aggregator_archive_expiry_objects_deleted_total{table}` | Objects deleted. |
| `aggregator_archive_expiry_errors_total{stage}` | `list`: a listing failed (that table is skipped for the run). `delete`: a DELETE failed or the pre-delete check refused a key. |
| `aggregator_archive_expiry_skipped_unmatched_total{table}` | Keys under a table prefix that don't match the layout. Counted every run they are seen. |
| `aggregator_archive_expiry_cap_reached_total` | Runs that hit `maxDeletesPerRun`. |
| `aggregator_archive_oldest_service_date_seconds{table}` | Gauge: the oldest archived `service_date` (midnight UTC, Unix seconds). Unset until a run sees an object. |
| `aggregator_archive_expiry_dry_run` | Gauge: 1 in dry-run, 0 live. |
| `aggregator_archive_expiry_retention_days` | Gauge: the configured retention. |
| `aggregator_archive_expiry_last_success_timestamp_seconds` | Gauge: when the last run with no errors finished. |

The chart's PrometheusRule adds `DistantSignalArchiveExpiryErrors`,
`DistantSignalArchiveExpiryUnmatchedKeys`,
`DistantSignalArchiveExpiryCapReached` and
`DistantSignalArchiveExpiryOverdue` (the oldest date is more than
`retentionDays` + 7 days old: expiry isn't running, or is still in
dry-run).

**Going live.** Leave `dryRun: true` until the metrics show that listing
and matching are right:

- `aggregator_archive_expiry_last_success_timestamp_seconds` is recent;
- `aggregator_archive_expiry_errors_total` and
  `aggregator_archive_expiry_skipped_unmatched_total` stay at 0;
- `aggregator_archive_oldest_service_date_seconds` matches the oldest
  `service_date=` directory you can see in the bucket;
- `aggregator_archive_expiry_candidates` and `…_would_delete` are what you
  expect. With 730 days they stay at 0 until about two years after the
  first upload.

Then set `dryRun: false`, well before the first object comes due.

### Bucket lifecycle rule

If the store supports lifecycle rules, add an expiration rule covering
the archive prefix before you set `lifecycleConfirmed`. With the AWS CLI
(most S3-compatible servers accept the same call):

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
  # or, instead of lifecycleConfirmed (see "Client-side expiry"):
  # expiry:
  #   enabled: true
  #   dryRun: true
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
`ARCHIVE_S3_SECRET_ACCESS_KEY`. Client-side expiry adds
`ARCHIVE_EXPIRY_ENABLED`, `ARCHIVE_EXPIRY_DRY_RUN` (default `true`),
`ARCHIVE_EXPIRY_RETENTION_DAYS` (default `730`),
`ARCHIVE_EXPIRY_INTERVAL_SECS` (default `86400`),
`ARCHIVE_EXPIRY_MAX_DELETES_PER_RUN` (default `20000`) and
`ARCHIVE_PROTECTED_PREFIXES` (comma-separated).

The journey hold's two bounds are aggregator variables with defaults, not
chart-driven: `TRAINS_SNAPSHOT_GUARD_MAX_DAYS` (default 600; the hold on
`trains`, schedule and Darwin rows, counted from the service date) and
`LDBWS_PINNED_BOARDS_MAX_DAYS` (default 300; the hold on the two LDBWS board
tables, counted from each row's `polled_at`; values above 300 are refused at
startup, 0 turns the board hold off). ds-deploy needs no chart value for the
new variable while the default stands. Its `check-config-defaults-chart.py`
should list `LDBWS_PINNED_BOARDS_MAX_DAYS` as unwired, on the same footing as
`TRAINS_SNAPSHOT_GUARD_MAX_DAYS`, if it fails on an env the chart does not set.

### Network access

The aggregator needs egress to the S3 endpoint. The chart renders no
egress policy for the aggregator, so on a cluster with default-deny egress
add one yourself. The endpoint is often at a private or tailnet address
that a generic "public internet" allow excludes (in production,
`allow-egress-internet` excludes `100.64.0.0/10`, where Thoth lives). A
narrow rule, for example:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-egress-aggregator-thoth
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/instance: distant-signal
      app.kubernetes.io/component: aggregator
  policyTypes: [Egress]
  egress:
    - to:
        - ipBlock:
            cidr: <S3 endpoint address>/32
      ports:
        - protocol: TCP
          port: <S3 endpoint port>
```

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
place. Every row carries its primary key, so readers should
deduplicate on it (see below). That key is `id`, except for `train_reasons`,
whose key is `(trains_id, msg_type)`.

**Metrics:**

- `distant_signal_aggregator_archive_rows_total{table}`
- `distant_signal_aggregator_archive_objects_total`
- `distant_signal_aggregator_archive_upload_failures_total`
- `distant_signal_aggregator_archive_batches_changed_total`
- `distant_signal_aggregator_archive_cycles_total{cycle="trains",result}` and
  `distant_signal_aggregator_archive_last_success_timestamp_seconds{cycle="trains"}`
  (a cycle succeeds when it neither errors nor fails an upload; the gauge
  starts at the aggregator's start time)
- `distant_signal_aggregator_retention_errors_total{task}` (every retention
  prune, `task="trains"` included, runs in its own error scope: one failing
  never skips another)
- `distant_signal_aggregator_retention_duration_seconds` (the whole retention pass)

**Alerts:** with `metrics.prometheusRule.enabled` and `archive.enabled`, the
chart renders:

- `DistantSignalArchiveUploadFailures`: at least
  `metrics.prometheusRule.archiveUploadFailures.minFailures` (3) batch
  uploads fail within its `window` (1h).
- `DistantSignalArchiveStale`: no successful archive cycle for
  `archiveStale.maxAgeSeconds` (6h).
- `DistantSignalArchiveBatchChurn`: more than `archiveBatchChurn.threshold`
  (10) batches changed before their delete within its `window` (6h).

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
