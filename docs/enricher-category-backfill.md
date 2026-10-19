# Backfilling `incidents.extracted_category`

Since 791feee3 (14 Oct 2026, merged in 6c6d0911) the enricher stores
`incidents.extracted_category` as one of a fixed set of 22 values
(`crates/enricher/src/category.rs`; anything unrecognised becomes `other`).
Rows extracted before that still hold the model's free text: `signal_failure`
next to `Signalling Fault` and `signal fault`. `enricher normalise-categories`
maps those rows onto the set, through the same `category::normalise` the live
path uses, so the two can't disagree.

It is optional. Nothing outside the enricher reads the column, and churn
measurement already normalises a stored baseline before comparing. Run it so
the column holds one vocabulary for whoever reads it next.

## Which rows, and how many

A row needs it when `extracted_category` is set and not one of the 22 values.
791feee3 didn't change the extraction model version
(`<model>@periods-v2`), so the sweep doesn't re-extract old rows. An old row
only gets a normalised value when its text changes. Expect most rows extracted
before the deploy of 6c6d0911 to still need it.

The count and the values below come from the code and the fixtures, not
production (this work didn't query prod). The dry run reports the real count
and every distinct value.

- **Production:** unknown until the dry run. The table held about 1,500
  incidents in mid-September (docs/incident-affected-lines-backfill.md). At
  most every extracted row written before the deploy needs it.
- **What the old values look like:** `category` was free text with no prompt
  guidance. The eval dataset's accepted spellings
  (`crates/enricher/eval/dataset.jsonl`, 26 values) show the spread: for
  example `signalling_fault`, `points_fault`, `strike_action`,
  `broken_down_train`, `rail_replacement_bus` and
  `emergency_engineering_work`. All of them map to one category each, and a
  unit test checks that. The enricher's test fixtures also store
  `Signalling Fault` and `cyber`. `cyber` isn't recognised, so it becomes
  `other`.
- **Dev seed:** `scripts/dev-seed.sql` sets no `extracted_category`, so a dev
  database has nothing to backfill.

## Running it

It runs as the **enricher role** (`distant_signal_enricher`, the enricher
pod's own `DATABASE_URL`). It refuses the api's role. It checks for
`SELECT` on `incidents` and `UPDATE (extracted_category)` before reading
anything. It uses one connection, and the role's `CONNECTION LIMIT` leaves
one spare next to the service's pool of 5.

Dry run first, then the real run, from the running enricher pod:

```sh
kubectl -n distant-signal exec deploy/distant-signal-enricher -- \
  /usr/local/bin/enricher normalise-categories --dry-run
kubectl -n distant-signal exec deploy/distant-signal-enricher -- \
  /usr/local/bin/enricher normalise-categories
```

From a checkout, against a local database:

```sh
DATABASE_URL=postgres://... cargo run -p enricher -- normalise-categories --dry-run
```

| Flag | Default | |
|---|---|---|
| `--dry-run` | off | Read and report only; write nothing |
| `--batch-size N` | 500 | Rows read, and at most updated, per batch (1 to 5000) |
| `--after ID` | none | Start after this `incident_id`, for resuming from a logged `cursor` |
| `--until ID` | none | Stop after this `incident_id` (inclusive), for a trial run over a slice |
| `--pause-ms N` | 0 | Sleep between batches |

Exit status is 0 on success and 1 on any error. The error is logged as one
`fatal` line.

### Why it is safe to run live

- **Short locks only.** It walks `incidents` in primary-key order with a
  keyset cursor, reading only rows whose value isn't already in the set. It
  writes each batch with one autocommitted `UPDATE ... FROM unnest(...)`
  keyed by `incident_id`. That takes row locks on at most one batch, for one
  statement, and no table lock.
- **Compare-and-set.** A write only applies if the row still holds the value
  the walk read. A row the enricher re-extracts mid-run already has a
  normalised value, so it is left alone and counted as `skipped`.
- **Only the category changes.** `extracted_at`, `source_text_hash` and
  `extraction_model_version` stay as they are, so the sweep re-extracts
  nothing and no LLM calls are made.
- **Idempotent and resumable.** A normalised value maps to itself, and the
  walk skips it. Re-running after a crash only touches what is left, and a
  finished run's re-run reports `examined=0`. `--after` with the last logged
  `cursor` saves rescanning the part already done.

## Reading the output

One JSON line per batch (`normalised a batch of categories`: `examined`,
`updated`, `cursor`). Then one `mapping` line per distinct old value (`old`,
`new`, `rows`, `recognised`). Then the totals (`category normalisation
complete`):

- **examined:** rows whose value wasn't in the set.
- **updated:** rows written. This is 0 on a dry run. Otherwise it equals
  `examined` unless something else wrote a row mid-run.
- **skipped:** `examined - updated` on a real run. A few are expected while
  the enricher is running. If `updated` is 0 while `examined` isn't, the
  writes are being lost: stop and investigate.
- **unrecognised:** rows becoming `other` because no rule matched. These are
  the `mapping` lines with `recognised=false`.

**Review the dry run's `mapping` lines before the real run.** An
unrecognised value that names a real cause is a missing rule in
`category.rs`, not something to accept as `other`. Add the rule, with a test
in `category.rs`, deploy, and dry-run again. The live path picks up the new
rule too. A recognised value that maps to the wrong category is a rule bug,
fixed the same way.

## Operator note

- **Who and when:** whoever operates the cluster (Ranma-Config), once an
  image with `normalise-categories` is deployed.
- **What:** run the dry run, check the `mapping` lines, then run the real
  command. Both commands are in [Running it](#running-it).
- **Done when:** a second run reports `examined=0`.
- **Risk:** low. Nothing outside the enricher reads the column, and the
  writes are small batches by primary key.
- **Rollback:** none needed. The old free text isn't kept, and it isn't
  needed: the column is advisory, and re-extraction writes a normalised
  value anyway.
- **Re-run:** after a `category.rs` change that should move stored rows.
  Only values outside the set are read, so a rule change that moves one
  canonical value to another isn't backfilled by this command.
