# Enricher model evaluation

How to compare LLMs for the enricher (`crates/enricher`), which extracts a
category plus per-period dates, schedule windows, resolution status,
severity and impact type from Knowledgebase incident text through any
OpenAI-compatible endpoint (`LLM_BASE_URL` / `LLM_MODEL`).

Choosing a model asks two separate questions, and the harness keeps them
apart:

| | Quality eval | Performance benchmark |
| --- | --- | --- |
| Question | Does this model extract the right data? | Does this model, served from this environment, fit the service? |
| Depends on | The model (and prompt) | The model *and* the hardware/host/network/provider serving it |
| Timeout | Generous (`quality_timeout_secs`, default 1800 s) | The real `LLM_REQUEST_TIMEOUT_SECS` |
| Needs gold labels | Yes | No |
| Needs the model | Only to record outputs; scoring is offline-capable | Yes |
| Runner | `eval::quality::live_eval_quality`, `eval::quality::replay_quality_score` | `eval::perf::live_eval_perf` |
| Reports | `target/enricher-eval/quality/<stamp>/` | `target/enricher-eval/perf/<stamp>/` |

Run quality once per model (on any host that can serve it, however slowly),
and perf once per model *per environment* you might deploy to.

For OpenAI's own API (`gpt-6-luna` at reasoning effort `none`), see
[Using the OpenAI API](enricher-openai.md): settings, schema strictness,
cost, data handling and the checklist to complete before switching
production. Its eval target is `openai-gpt-6-luna-none` in
`targets.example.toml`.

## How it works

Code: `crates/enricher/src/eval/`. It is test-only, like the existing
`replay_eval` and `llm::tests::live_eval_*` evals. Each runner is an
`#[ignore]`d test, so neither `cargo test` nor CI ever calls a model.

- `pipeline.rs` runs the service's real pipeline minus the database: the
  primary pass, the resolution-adversarial pass, the severity-adversarial
  pass, then `combine::combine_periods`. It uses the same order and the same
  stop-on-failure points as `process_incident`. The prompts, JSON schemas,
  parsers and provider policy are the service's own (`LlmClient::*_raw` and
  `llm::parse_*` in `llm.rs`), so the harness can't drift from production.
- Every run is saved as a **record**: one JSONL line per (case,
  repetition), holding a short hash of the case's input (`input_hash`), a
  short hash of the prompts and schemas that produced it
  (`prompt_fingerprint`), and for each pass its raw output or error, end-to-end latency, final
  outcome label, in-call retry count and every HTTP attempt (`attempts`:
  outcome, time queued for a `max_in_flight` permit, send time, and any 429
  or 502/503/504 back-off slept after it). When the provider reports token
  usage, each call also keeps it (`usage`: `prompt_tokens`,
  `completion_tokens`, and OpenAI's `reasoning_tokens` and
  `cached_tokens`). Providers that send none simply have no `usage`.
- What the service would have written is derived from a record by
  replaying it through the same parsers. Quality scoring is a pure function
  of records, so saved records can be re-scored without the model.
- `quality.rs` scores records against gold labels. `perf.rs` computes
  latency, outcome and throughput statistics. The two share only the
  pipeline, the dataset loader and the target config.

## Quick start

```sh
cp crates/enricher/eval/targets.example.toml crates/enricher/eval/targets.toml
$EDITOR crates/enricher/eval/targets.toml     # git-ignored; one [[targets]] per model x environment

# Quality: records every target's outputs, scores them, writes reports
EVAL_TARGETS=crates/enricher/eval/targets.toml \
  cargo test -p enricher --bin enricher eval::quality::live_eval_quality -- --ignored --nocapture

# Quality, offline: re-score saved records (e.g. after fixing a gold label)
EVAL_RECORDS=target/enricher-eval/quality/<stamp>/<target>.records.jsonl \
  cargo test -p enricher --bin enricher eval::quality::replay_quality_score -- --ignored --nocapture

# Performance: run from a host with the same network path to the endpoint as the enricher
EVAL_TARGETS=crates/enricher/eval/targets.toml \
  cargo test -p enricher --release --bin enricher eval::perf::live_eval_perf -- --ignored --nocapture
```

The crate is bin-only, so the `--bin enricher` flag is required. Relative
paths in the `EVAL_*` variables resolve from the workspace root. Each run
prints its Markdown reports and writes them, with JSON copies and the raw
records, to a new timestamped directory.

Without `EVAL_TARGETS`, the runners evaluate a single target built from
the service's own env vars: `LLM_BASE_URL`, `LLM_MODEL`, `LLM_API_KEY`,
`LLM_REQUEST_TIMEOUT_SECS` and the `LLM_*` provider-policy knobs. Name it
with `EVAL_TARGET_NAME` and describe its environment with
`EVAL_ENVIRONMENT`, and set its quality-eval request timeout with
`EVAL_QUALITY_TIMEOUT_SECS` (default 1800). This is the quickest way to
check an existing deployment's `.env`.

## Targets

A target is one model served from one endpoint in one environment. The
same model on a laptop and on a GPU host is two targets. The format is in
`crates/enricher/eval/targets.example.toml`. The keys mirror the service's
env vars, and every optional key defaults to the service's own default
(read from `Config`'s definitions, not copied), so an unset key measures
the deployment as it would really run.

- `environment`: free text (hardware, host, provider, quantization,
  network path). Fill it in, because perf numbers mean nothing without it.
- `api_key_env`: the *name* of the env var holding the key. Keys never go
  in the file.
- `request_timeout_secs`: used by the perf benchmark only.
  `quality_timeout_secs` (default 1800) is used by the quality eval. For
  the env-var target, set it with `EVAL_QUALITY_TIMEOUT_SECS`.
- `EVAL_TARGET=a,b`: runs only the named targets.

## Running the quality eval

| Variable | Default | Meaning |
| --- | --- | --- |
| `EVAL_DATASET` | `crates/enricher/eval/dataset.jsonl` | Dataset to use |
| `EVAL_CASES` | all | Comma-separated case ids |
| `EVAL_REPEATS` | 1 | Repetitions per case. Use 3 or more to measure consistency (the service sends `temperature: 0`, but many backends are still non-deterministic) |
| `EVAL_CONCURRENCY` | 1 | Documents in flight at once. Only for speed: quality doesn't depend on it |
| `EVAL_DATE_TOLERANCE_MINS` | 0 | Treat predicted and gold instants this close as equal |
| `EVAL_OUT_DIR` | `target/enricher-eval` | Report root |
| `EVAL_QUALITY_TIMEOUT_SECS` | 1800 | Request timeout of the env-var target (no `EVAL_TARGETS`); a targets file uses `quality_timeout_secs` |
| `EVAL_RECORDS` | (replay only) | Comma-separated `*.records.jsonl` files to re-score |
| `EVAL_ACCEPT_UNHASHED` | 0 | Replay only. `1` scores records that have no `input_hash` (saved before records had one) instead of skipping them |

Only labelled cases are run. Per target, the run writes
`<target>.records.jsonl`, `<target>.json` and `<target>.md`, plus a
`comparison.md` with one row per target.

**Offline re-scoring** (`replay_quality_score`) groups the records by the
target and model named on each line, and scores them against the
*current* dataset. Use it when a gold label changes, when you add scoring
logic, or to score records produced elsewhere. Records are skipped and
counted in the report when:

- their case id is no longer in the dataset;
- their `input_hash` doesn't match the case's current `summary`,
  `description` and `reference_date` (the text was edited after the run,
  so the saved output answers a different question; re-run the case).
  Gold labels, tags and notes aren't hashed, so fixing a label never
  invalidates records;
- they have no `input_hash` at all, because they were saved before
  records had one. Set `EVAL_ACCEPT_UNHASHED=1` to score them anyway,
  unchecked: only do that if you know their case text hasn't changed. A
  hash that is present but doesn't match is skipped either way;
- they repeat a (case, repetition) already read for the same target and
  model, which happens when several record files for one target are
  passed. The record from the file listed **first** in `EVAL_RECORDS` is
  kept. To combine runs, give them distinct target names, or list the run
  you want to win first.

Skips are reported on stderr as one line per target and reason, with the
count and the first few `case_id#repetition` examples, for example:

```text
warning: target "local-qwen": skipped 4 stale record(s): their input hash doesn't match the case's current text (re-run those cases); e.g. eta-a#0, eta-a#1, bst-b#0, ...
```

The report's `repetitions` is the most repetitions any one case had
scored, counting only the records actually used: if a case's #0 and #2
are used and #1 is skipped as stale, that's 2.

**Prompt changes.** Each record also stores `prompt_fingerprint`, a short
hash over the three passes' system prompts, schema names and JSON schemas
(`llm::prompt_fingerprint`; the user-message wrappers around the case text
aren't covered). When records were produced by a different fingerprint
than the current code's, or have none (saved before it existed), the
replay prints one warning line per target with the fingerprints it saw,
and the report notes the count. Those records are still scored:
re-scoring old outputs against corrected labels or new scoring logic is
legitimate, but they measure the old prompts, not the current ones.

## Running the performance benchmark

| Variable | Default | Meaning |
| --- | --- | --- |
| `EVAL_PERF_REPEATS` | 3 | Repetitions per case (more gives steadier percentiles) |
| `EVAL_PERF_CONCURRENCY` | 1 | Documents in flight at once |
| `EVAL_PERF_WARMUP` | 1 | Untimed runs of the first case first, so a cold model load doesn't skew the numbers |
| `EVAL_DATASET`, `EVAL_CASES`, `EVAL_OUT_DIR` | as above | Cases do not need labels here |

Choose the concurrency to match the deployment. The service's stream loop
processes one incident at a time (concurrency 1). The hourly sweep and
the reclaim loop can overlap with it, so up to 3 documents can be in
flight, capped across all callers by `max_in_flight` (`LLM_MAX_IN_FLIGHT`).
Use 1 for steady-state latency. Use 3 for a burst, such as the full
re-extraction after a prompt version bump.

Run the benchmark from a host with the same network path to the endpoint
as the enricher pod. Use `--release`: client-side overhead is negligible
either way, but there's no reason to measure a debug build. Every record
is saved, and the saved records can be quality-scored offline; transport
failures (timeouts included) are then left out of the quality rates, but
the run used the real, tighter timeout, so prefer the quality eval for
quality.

Each document stops where production stops: if the primary pass fails or
its output can't be parsed, the adversarial passes are never sent. A model
that often produces invalid primary output therefore makes fewer calls per
document; the report keeps invalid outputs apart from transport failures
so this doesn't read as an environment problem.

## The dataset

`crates/enricher/eval/dataset.jsonl` holds one case per line (point
`EVAL_DATASET` at another file to use your own):

```json
{"id": "flat-eta-signal-failure", "tags": ["single_period", "eta"], "notes": "why/how labelled",
 "reference_date": "2026-04-01T07:45:00Z", "summary": "...", "description": "...",
 "expected": {"category_any_of": ["signal_failure"], "periods": [
   {"scope_hint": "only shown in reports",
    "date_range": {"from_date": null, "to_date": "2026-04-01T17:00:00Z"},
    "schedule_window": null, "resolution_status": "ongoing",
    "apparent_severity": "moderate_disruption", "impact_type": null}]}}
```

| Field | Meaning |
| --- | --- |
| `id` | Unique and stable. Records refer to it, so renaming a case orphans its recordings |
| `summary`, `description` | The incident text, sent to the model exactly as given. Keep it exactly as production would see it. Records store a hash of these and `reference_date`, so editing them makes existing records stale. NUL characters are rejected (the hash separates fields with NUL) |
| `reference_date` | What `process_incident` passes as `first_seen_at`. The model resolves year-less dates and bare times against it |
| `tags`, `notes` | Optional. Tags slice the report (a "By tag" table). Notes record why the case exists and how it was labelled |
| `expected` | Optional. Without it, the case is perf-only |
| `expected.category_any_of` | Accepted categories. `category` is free text in the schema, so list synonyms. They're compared lowercased, with punctuation runs folded to `_` |
| `expected.periods` | Gold periods in text order, at most 12. Leave it out to leave segmentation unlabelled |
| Per period: `date_range`, or `from_date` / `to_date` on their own | Full range, or just one side when the other is a judgment call |
| `schedule_window` | `{days_of_week (ISO 1-7), start_time, end_time ("HH:MM")}` or `null`. Day order doesn't matter |
| `resolution_status`, `apparent_severity`, `impact_type` | A value, or a list of acceptable values. `impact_type` lists may include `null` |

**Absent is not null.** A field left out is not scored. A field given as
`null` is scored: the model must return null. Label only what the text
settles, and use an accepted-values list where reasonable readers
disagree, for example `["moderate_disruption", "severe_disruption"]`.

**Date conventions** (from `PRIMARY_PROMPT`). Gold dates are UTC instants:

1. A year-less date resolves to the occurrence closest to
   `reference_date`.
2. A stated end day is inclusive, so `to_date` is the *following* day's
   00:00.
3. A bare date is a Europe/London day boundary converted to UTC: 23:00Z the
   previous day in BST, 00:00Z in GMT. Watch the clock changes, as in the
   `bst-to-gmt-crossover-two-periods` case.
4. An ETA ("expected to resume from 18:00") is `to_date` with
   `from_date: null`.

**Adding a case:**

1. Label it against the rules above. Write `notes` explaining anything
   non-obvious.
2. Add tags. Reuse the existing ones (`single_period`, `multi_period`,
   `negation`, `eta`, `schedule_window`, `bst`, `gmt`,
   `over_segmentation_trap`, `observational`, ...) so the "By tag" table
   stays useful.
3. Run `cargo test -p enricher eval::dataset`. The
   `shipped_dataset_is_valid` test rejects unknown enum values, bad
   weekdays or times, duplicate ids and unknown keys (typos). A dataset
   loaded through `EVAL_DATASET` gets the same checks when a runner
   starts.
4. Run the quality eval to score models on it. Existing records have no
   output for a new case, so re-scoring them doesn't cover it. (Once it
   has been run, fixing its gold labels only needs an offline re-score.)

The 15 seed cases cover the design doc's golden-corpus list (ongoing,
resolved, residual, an ETA, a schedule window narrower than the date
range, and the negation pairs "has now ended" / "not expected to end soon"
and "until 18:00" / "began at 18:00"). They also cover the existing
live-eval fixtures with their specs' expected segmentation, a GMT-season
case and a BST-to-GMT crossover. They are a starting point, not a
benchmark: real model choices need more cases, ideally drawn from real
incident history.

## Reading the quality report

A **completed** case is one the service would have written. Failures are
split in two:

- **Transport failure**: the call failed (timeout, gateway or HTTP error,
  429). That's an environment problem, so look at the perf report. It is
  shown as its own count and left out of the denominator of every rate:
  valid output, exact match (overall and per tag) and the comparison
  table's columns.
- **Invalid output**: the model answered, but with something the service
  rejects: empty content (typically a reasoning model that used up
  `max_tokens`, which depends on the model and its config, not the
  environment), a safety refusal (`refused`: OpenAI's `message.refusal`),
  malformed or schema-violating JSON, an empty `periods`
  array, or adversarial verdicts that don't align with the primary
  periods. That's a quality problem.

**Period matching.** A predicted and a gold period can only be paired if
they agree enough to be the same period: at least one date bound
(`from_date` or `to_date`) is correct and non-null (both being null
doesn't count), or strictly more than half of the gold period's scored
fields are correct (`date_range`, `from_date` and `to_date` each count as
a field; exactly half is not enough). A gold period with no scored fields
matches any prediction. Among the eligible pairs, the matcher finds the
pairing with the most pairs; among those, the most agreeing fields in
total; then the smallest total position difference; any remaining tie
goes to the first predicted period taking the lowest-indexed gold period
that still allows an optimal pairing, then the second, and so on. It is
exact (dynamic programming over the set of gold periods used), so an
early high-agreement pair can't strand a gold period another prediction
could have matched; that's why a case has at most 12 gold periods (the
service keeps at most 8 predicted periods anyway). A pair below the bar
stays unmatched, so it counts as one hallucinated (extra) period plus one
missed period, and its fields are not scored. Period precision
is matched / predicted, and period recall is matched / gold.

**Field verdicts**, per matched pair and scored field:

| Verdict | Meaning | Counts against |
| --- | --- | --- |
| correct | non-null and accepted | |
| correct_null | null, and null is accepted | |
| wrong | non-null, wrong value | precision and recall |
| hallucinated | non-null where gold is null | precision |
| missed | null where gold is non-null | recall |

Accuracy is (correct + correct_null) / scored. Precision and recall treat
a non-null value as a positive. They matter most for `date_range`,
`schedule_window` and `impact_type`, where returning null is a real
answer. Fields scored per period: `date_range` (whole range), `from_date`,
`to_date`, `schedule_window`, `impact_type`, `resolution_status`,
`apparent_severity`. `scope_description` is free text and never scored.

Other sections:

- **Exact case match**: completed, every scored field correct, and the
  right period count, out of the attempts that got an answer.
  Transport failures and observational cases (nothing labelled) are
  excluded; an invalid output counts as not exact.
- **Wrong dates by kind** shows how the dates went wrong: `off_by_1h`
  (Europe/London to UTC conversion ignored), `off_by_1d` (end day not
  treated as inclusive), `wrong_year` (year inference) or `other`.
- **Confusions** lists gold value to predicted value for the enum fields.
  A false `resolved` is the costliest mistake, because the aggregator can
  demote a status on it.
- **Low-confidence rates** show how often the adversarial passes disagreed
  with the primary pass. That isn't an error, but low-confidence fields
  are ignored downstream, so a very high rate means the extraction rarely
  takes effect.
- **Consistency** (with repetitions) shows whether repeats of a case gave
  the same output, using `churn::compare`. `scope_description` wording is
  ignored.
- **By tag**: per tag, attempts, transport failures, completed (with the
  valid-output rate) and exact / scored, with the same denominators as
  the headline rates.
- **Cases** lists every mismatch for every attempt, with `scope_hint`
  labels.

## Reading the performance report

- **Calls**: per pass and overall, giving the final outcome counts (the
  service's `enricher_llm_call_total` labels: `success`, `timeout`,
  `gateway_error`, `rate_limited`, `quota_exhausted`, `http_error`,
  `empty_content`, `refused`, `auth_error`, `unauthorized`, `error`), timeout and error rates, in-call retries, timed-out attempts
  (including timeouts a gateway retry recovered) and three kinds of
  latency. Percentiles use the nearest-rank method.
  - End-to-end call latency (p50 to "Max incl. failures"): successful
    calls, with retries, `max_in_flight` queueing and 429/gateway back-off included,
    like `enricher_llm_call_duration_seconds`. "Max incl. failures" is
    how long a failing call held the loop.
  - Send latency: one successful HTTP attempt. (The timeout fit uses a
    wider set, below.)
  - Wait: per call, time queued for a `max_in_flight` permit plus 429 and
    502/503/504 back-off sleeps.
- **Documents**: all three passes, end to end. Shows completion,
  transport failures and invalid outputs separately, failures by stage,
  latency of completed documents, and throughput at the chosen
  concurrency: documents per minute over every *attempted* document (the
  headline, independent of answer quality) and completed documents per
  minute.
- **Timeout fit**: judged per attempt, because the request timeout bounds
  each HTTP attempt. `exceeds` if any attempt hit the client timeout, even
  if a gateway retry then recovered the call, or any call ended in a
  timeout (the report says how many attempts timed out, how many of those
  were recovered and how many calls ended in a timeout). `tight` if the
  send p95 of every attempt that got a response is at least 80% of
  `request_timeout_secs`. That set is every outcome except `timeout` and
  `error` (a connection-level failure that never got a response, which
  would only pull the p95 down): successes, but also `empty_content`, HTTP
  and gateway errors and 429s, because a slow answer the service rejects
  came just as close to the timeout as a slow success. Otherwise `fits`
  (`no data` without any such attempt). Queue and back-off wait is
  reported next to it but not part of the verdict. The report also gives completed-document p95 as a share of
  `RECLAIM_MIN_IDLE_SECS`: a document slower than that gets reclaimed and
  skipped while it is still in flight. That's churn, not an error, but
  worth avoiding.
- A gateway that cuts requests itself (returning 504) shows up as
  `gateway_error`, not `timeout`. As `config.rs` advises, set the timeout
  just above the gateway's cutoff, and consider `gateway_retries`.

## Choosing a model

1. Shortlist models from the quality comparison: valid-output rate
   (with the transport-failure count beside it),
   `resolution_status` with no false `resolved`, segmentation (period
   precision/recall and the `multi_period` tag), and consistency across
   repeats.
2. For each shortlisted model, run perf in each candidate environment, at
   concurrency 1 and 3, with the timeouts you intend to deploy.
3. Deploy a model and environment pair that is both good enough and
   `fits`. Set `LLM_REQUEST_TIMEOUT_SECS` with headroom over the measured
   p95, and `RECLAIM_MIN_IDLE_SECS` above the document p95.
4. Keep the records. When the prompt, the dataset or the scoring changes,
   re-score offline first, and re-run models only when the prompt changes.

Changing `PRIMARY_PROMPT` or a schema changes every model's behaviour, so
re-run the quality eval for each candidate afterwards.

## Tests

The scoring, matching, statistics, dataset validation and pipeline logic
are unit-tested with a scripted fake backend (`pipeline::FakeBackend`).
They run in the normal `cargo test -p enricher`, with no network. The
real-model runners are `#[ignore]`d and named `live_eval_*` / `replay_*`,
the prefixes CI's ignored-tests step already skips
(`--skip live_eval --skip replay_`).

## Limitations and open questions

- **Category is free text** (the schema has no enum), so category accuracy
  is only as good as each case's synonym list.
- **Period matching** maximises the number of pairs, then field
  agreement, so with two near-identical periods the pairing it picks may
  not be the one a human would. It is deterministic, and the per-case
  mismatch list shows the pairing. The minimum-agreement bar is a
  heuristic: a prediction that is the right period but gets almost every
  field wrong counts as extra plus missed.
- **Prompt fingerprints** cover the system prompts and schemas, not the
  user-message wrappers in `llm.rs`'s `*_raw` methods; changing only a
  wrapper won't trigger the replay warning.
- **The seed labels are judgments.** Contested fields use accepted-value
  lists or are left unscored. Review them as the dataset grows.
- **Small seed set.** 15 cases can separate a broken model from a good one,
  but not two good ones. Grow the set from real incident history first.
- **Throughput is client-side.** It reflects the endpoint as seen from the
  host running the benchmark, including any other load on a shared
  endpoint. Note that load in the target's `environment`.
