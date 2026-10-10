# Using the Claude API for the enricher

How to point the enricher (`crates/enricher`) at Anthropic's Claude API,
through either the normal (synchronous) route or the Message Batches route,
what prompt caching does for this workload, what it costs, and how to switch
back.

**Status (2026-10-09): not deployed, off by default.** `LLM_PROVIDER`
defaults to `openai` (any OpenAI-compatible endpoint, including the current
self-hosted model), and an unset deployment sends exactly the requests it
sent before. Nothing here has run against the real Claude API: there was no
key. Every request and response shape below is unit-tested against mocked
HTTP fixtures taken from the API reference. Run the
[quality eval](#evaluating-before-switching) before switching production.

Claude API documentation this page relies on (read 2026-10-09; re-check
before relying on prices):

- Models overview: <https://platform.claude.com/docs/en/about-claude/models/overview>
- Claude Haiku 5.5: <https://platform.claude.com/docs/en/models/haiku-5-5/overview>
- Claude Sonnet 5.5: <https://platform.claude.com/docs/en/models/sonnet-5-5/overview>
- Structured outputs: <https://platform.claude.com/docs/en/build-with-claude/structured-outputs>
- Prompt caching: <https://platform.claude.com/docs/en/build-with-claude/prompt-caching>
- Message Batches: <https://platform.claude.com/docs/en/build-with-claude/batch-processing>
- Errors: <https://platform.claude.com/docs/en/api/errors>
- Data retention: <https://platform.claude.com/docs/en/manage-claude/api-and-data-retention>

## How it fits in

`llm.rs` has one client and two wire providers. Everything above the wire is
shared: the three prompts and schemas, the three calls per incident
(primary, resolution-adversarial, severity-adversarial), the provider policy
(`LLM_MAX_IN_FLIGHT`, `LLM_RATE_LIMIT_*`, `LLM_GATEWAY_RETRIES`), the typed
errors and their outcome labels, the per-text backoff, and the metrics.

| | `openai` (default) | `anthropic` |
| --- | --- | --- |
| Endpoint | `POST {LLM_BASE_URL}/chat/completions` | `POST {LLM_BASE_URL}/messages` (default base `https://api.anthropic.com/v1`) |
| Credential | `Authorization: Bearer` (or keyless WIF) | `x-api-key` (or keyless WIF: `Authorization: Bearer`) + `anthropic-version: 2023-06-01` |
| System prompt | a `system` message | one `system` text block with `cache_control` (`LLM_PROMPT_CACHE`) |
| Structured output | `response_format: json_schema, strict: true` | `output_config.format: json_schema` (schema rewritten, below) |
| Temperature | `0` | not sent: the current Claude models reject a non-default one |
| `LLM_REASONING_EFFORT` | `reasoning_effort` | `output_config.effort` |
| `LLM_MAX_TOKENS` | `max_tokens`, omitted when unset | `max_tokens`, 16000 when unset (required by the API) |
| Thinking | n/a | `LLM_THINKING` → `thinking.type`; unset = model default (adaptive) |
| Bulk path | none | Message Batches for the sweep (`LLM_SWEEP_MODE=batch`), or for everything ([`LLM_MODE=batch`/`batch-only`](#llm-modes)) |

The code: `crates/enricher/src/llm/anthropic.rs` (request, response,
errors, batch client), `crates/enricher/src/batch.rs` (batch mode),
`crates/enricher/src/config.rs` (`resolve_llm`, the settings).

**Schemas.** The three schemas are written in OpenAI's strict subset. Claude
rejects two things in them with a 400: type unions (`"type": ["string",
"null"]`) and numeric bounds (`minimum`/`maximum` on `days_of_week`). For
Claude they are rewritten on the way out (`anthropic::claude_schema`): a
nullable scalar becomes `anyOf: [{"type": "string", ...}, {"type": "null"}]`
(an enum's `null` moves to the null branch), and a bound becomes description
text. The JSON the model returns has the same shape, so the parsers are
unchanged. `every_schema_is_rewritten_into_claudes_subset` checks all three.

**No temperature.** Sonnet 5.5 returns a 400 for a non-default
`temperature`, and Haiku 5.5's and Opus 5.5's docs say to omit it. So repeat
runs over the same text are not guaranteed identical, unlike the `openai`
path's `temperature: 0`. The per-text backoff and the combine-mismatch
tracker still work, but a "deterministic" persistent mismatch may now
sometimes clear on retry. (The `claude-default` profile, below, is what
sends none; setting one for these models fails at startup.)

## Tuning per model

Generation settings and prompts come from a **profile**
(`crates/enricher/src/profile.rs`), picked by provider plus a model glob,
first match wins, or named with `LLM_PROFILE` / `enricher.llm.profile`:

| Profile | Provider | Models | Defaults |
| --- | --- | --- | --- |
| `openai-gpt-6-luna` | openai | `gpt-6-luna*` | `temperature: 0`, effort `none` |
| `openai-default` | openai | `*` | `temperature: 0` (the pre-profile behaviour) |
| `claude-default` | anthropic | `*` | nothing: no `temperature`/`top_p`, the model's effort and adaptive thinking |

On top of the profile, explicit settings win, and `omit` means "don't send
the profile's value"; unset means the profile's value, and a value nobody
sets is never sent:

| Setting | Env | Chart |
| --- | --- | --- |
| temperature | `LLM_TEMPERATURE` (number or `omit`) | `enricher.llm.temperature` |
| top_p | `LLM_TOP_P` (number or `omit`) | `enricher.llm.topP` |
| max_tokens | `LLM_MAX_TOKENS` | `enricher.extraEnv` |
| reasoning effort | `LLM_REASONING_EFFORT` (or `omit`) | `enricher.llm.reasoningEffort` |
| thinking (Claude) | `LLM_THINKING` (or `omit`) | `enricher.llm.anthropic.thinking` |

**Startup checks (best effort).** A small capability table knows which
models reject sampling parameters: Claude Haiku 5.5, Sonnet 5 / 5.5, Opus
4.7 and later, Fable/Mythos 5 reject `temperature` and `top_p`;
`gpt-6-luna` takes `temperature` only at effort `none`. A resolved
configuration that would send one fails at startup with the setting to
change, instead of every request answering 400. Models not in the table
(any self-hosted one, Haiku 4.5) aren't checked. The table also knows each
Claude model's minimum cacheable prefix: with caching on, a primary prompt
below it logs a startup warning (caching would silently not apply).

**Prompts.** The three system prompts (`primary`, `adversarial`,
`severity_adversarial`) default to the shared built-in ones. To tune them
per profile, put overrides in a ConfigMap and name it in
`enricher.llm.prompts.configMap`; the chart mounts it at
`/etc/enricher/prompts` (`LLM_PROMPTS_DIR`). Per call the enricher reads
`<profile>.<call>.txt`, else `<call>.txt`, else the built-in prompt; an
empty file fails startup. The schemas and parsers stay shared, so whatever
the prompt, the output has the same shape. For example:

```bash
kubectl create configmap enricher-prompts \
  --from-file=claude-default.primary.txt=./primary-for-claude.txt
```

The active prompt set's 12-character hash (`prompt_version`) is logged at
startup with the profile, the resolved settings and where each prompt came
from; `enricher_llm_model_info` carries a `profile` label. When the prompts
aren't the built-in ones, the hash is appended to `model_version`
(`<model>@periods-v2+prompts-<hash>`), so a prompt change re-extracts every
live incident, like a model change, and going back to the built-in prompts
restores the old `model_version`. Keep caching in mind: an edited primary
prompt below ~2,000 characters drops under the Claude cache minimum.

**Evaluating a tuning.** Eval targets take the same keys (`profile`,
`temperature`, `top_p`, `reasoning_effort`, `thinking`, `prompts_dir`; see
`crates/enricher/eval/targets.example.toml`) and resolve exactly like the
service, and each record's prompt fingerprint is the prompts actually sent.
Compare a tuned target with the untuned one before deploying.

**Rolling back a tuning.** Clear the values (`profile`, `temperature`,
`topP`, `prompts.configMap`). With the prompts back to built-in,
`model_version` returns to `<model>@periods-v2` and the sweep re-extracts
once more.

## Setup

1. Create a Claude API key in a dedicated workspace with a monthly spend
   limit (a few dollars a month covers the expected load, see [Cost](#cost)).
   A spend limit surfaces as HTTP 400, a billing problem as 402
   `billing_error` (`quota_exhausted`).
2. Store it in a Secret:
   `kubectl create secret generic enricher-anthropic --from-literal=anthropic-api-key=...`
3. Chart values:

   ```yaml
   enricher:
     llm:
       provider: anthropic
       anthropic:
         existingSecret: enricher-anthropic
         # model: claude-haiku-5-5        # the default
         # promptCache: 1h                # the default
       # reasoningEffort: ""              # optional, sent as output_config.effort
     llmRequestTimeoutSecs: 120
     extraEnv:
       - { name: LLM_MAX_IN_FLIGHT, value: "3" }
       - { name: LLM_RATE_LIMIT_RETRIES, value: "3" }
       - { name: LLM_GATEWAY_RETRIES, value: "2" }
   ```

   `enricher.llm.baseUrl`/`model`/`apiKey`/`existingSecret` are not used
   with `provider: anthropic`, and `enricher.llm.auth` must stay `apiKey`.
   If the opt-in egress NetworkPolicies are on, the enricher's internet rule
   already opens `enricher.llm.anthropic.baseUrl`'s port (443); batches use
   the same host.

Service env vars (the chart sets them; listed for local runs):

| Env var | Default | Notes |
| --- | --- | --- |
| `LLM_PROVIDER` | `openai` | `anthropic` for the Claude API |
| `LLM_BASE_URL` | `https://api.anthropic.com/v1` (anthropic) | required for `openai` |
| `LLM_MODEL` | `claude-haiku-5-5` (anthropic) | required for `openai`; also part of `model_version` |
| `LLM_API_KEY` | none | required for `anthropic` (sent as `x-api-key`) |
| `LLM_ANTHROPIC_VERSION` | `2023-06-01` | the `anthropic-version` header |
| `LLM_PROMPT_CACHE` | `1h` | `1h`, `5m` or `off`; see [Prompt caching](#prompt-caching) |
| `LLM_THINKING` | unset | `thinking.type`, e.g. `disabled` (Haiku 5.5), `between_tools` (Sonnet 5.5) |
| `LLM_SWEEP_MODE` | `sync` | `batch`: see [Batch mode](#batch-mode) |
| `LLM_BATCH_MIN_ITEMS` | `20` | a smaller sweep stays synchronous |
| `LLM_BATCH_MAX_ITEMS` | `2000` | incidents per batch, capped at 5000 |
| `LLM_BATCH_POLL_SECS` | `60` | poll interval for in-flight batches |

Startup refuses `anthropic` without `LLM_API_KEY` (unless
`LLM_AUTH=anthropic-wif-authentik`, which in turn refuses a key), with an
`openai-wif-*` mode, `anthropic-wif-authentik` with `openai`, and
`LLM_SWEEP_MODE=batch` with `openai`; the chart refuses the same
combinations at render time. For keyless auth see
[Keyless auth](#keyless-auth).

### Errors and what the enricher does with them

Same outcome labels and retry budgets as the `openai` path
(docs/enricher-openai.md, "Errors"):

| Response | Outcome label | Retried in-call? | Per-text backoff? |
| --- | --- | --- | --- |
| 429 `rate_limit_error` | `rate_limited` | yes, `LLM_RATE_LIMIT_RETRIES`, honouring `retry-after` (≤ 600 s) | no |
| 402 `billing_error` | `quota_exhausted` | no | no |
| 529 `overloaded_error`, 500 `api_error`, 504 `timeout_error`, 502/503 | `gateway_error` | yes, `LLM_GATEWAY_RETRIES`, `retry-after` when sent, else 2 s, 4 s, ... | no (504 only with gateway retries on) |
| other 4xx (400 invalid request or spend limit, 401, 403, 404, 413) | `http_error` | no | yes |
| 200, `stop_reason: "refusal"` | `refused` (`stop_details.category: explanation` in the log) | no | yes |
| 200, `stop_reason: "max_tokens"`, or no text block | `empty_content` (`finish_reason` = the stop reason) | no | yes |

Failures are logged with the response's `request-id`, which Anthropic
support asks for. Responses also carry `thinking` blocks (adaptive thinking;
their text is empty by default): they are skipped, and only `text` blocks are
parsed.

## LLM modes

`LLM_MODE` (chart `enricher.llm.batch.mode`) decides where extractions run.
`normal` is the default and today's behaviour; `batch` and `batch-only` are
Claude only (with `openai` the pod refuses to start and the chart refuses
to render).

| | `normal` (default) | `batch` | `batch-only` |
| --- | --- | --- | --- |
| Stream loop and reclaim | extract synchronously, seconds after the text change | no LLM call: ACK, incident left stale | no LLM call: ACK, incident left stale |
| Sweep | every `SWEEP_INTERVAL_SECS` (1 h); `LLM_SWEEP_MODE` picks sync or batch | every `LLM_BATCH_SWEEP_INTERVAL_SECS` (120 s); ≥ `LLM_BATCH_MIN_ITEMS` (20) stale incidents: a batch; fewer: synchronously, right away | every `LLM_BATCH_SWEEP_INTERVAL_SECS` (300 s); everything batched, even one incident |
| `LLM_SWEEP_MODE` | used | ignored | ignored |
| Latency of a text change | the three calls (tens of seconds) | quiet: ≤ 2 min + the three calls; a burst of ≥ 20: two sequential batches (below) | ≤ 5 min + two sequential batches: usually well under 2 h, at worst ~48 h |
| Cost (Haiku 5.5, [Cost](#cost)) | ~$6-8/month | about the same in steady state; bursts and re-extractions at half price | half of everything: ~$3-4/month |
| Use it when | users should see an LLM reading within a minute (the default) | you want bursts (a re-extraction, a big disruption day) at half price but quiet periods near-live | cost matters more than freshness |

Both new modes share these rules:

- **The stream loop and reclaim still do everything that needs no LLM.**
  Unchanged text is skipped and, with `CARRY_FORWARD_SEMANTIC_NOOPS`, a
  semantic no-op is carried forward. Only a text that needs the LLM is
  deferred: the entry is ACKed and counted in
  `distant_signal_enricher_deferred_to_batch_total{path="stream"|"reclaim"}`,
  and the incident stays stale (its stored `source_text_hash` doesn't match)
  for the next sweep. A preflight that couldn't finish (database error,
  backed-off text) leaves the entry pending, as in `normal`.
- **No double submission.** The sweep skips an incident only when a batch
  is already in flight for its *current* text. If the text changed after
  its batch was submitted, the next sweep extracts the new text straight
  away (batched or, in `batch`, synchronously below the minimum); when the
  old batch lands, `write_extraction`'s stale-text guard drops its result,
  because it no longer matches the incident's text. The alternative, waiting
  for the old batch to end, could hold a changed text back for a day, since
  nothing else extracts it in these modes. The cost is the superseded
  request, which is what `normal` pays for every edit anyway. (In `normal`
  with `LLM_SWEEP_MODE=batch`, the sweep keeps skipping every incident in a
  batch, as before: there the stream loop extracts the new text.)
- **Everything else is [batch mode](#batch-mode)**: the two-stage flow,
  `LLM_BATCH_MAX_ITEMS`, the poll loop, restart resume from
  `enricher_llm_batches`, the batch metrics and the three batch alerts
  (rendered for both modes).

**Why 120 s and 300 s.** In `batch`, the interval is the worst-case wait of
a quiet period's text change, so it is short. It must also be long enough
for a burst to reach the minimum: 20 text changes in 2 minutes is a real
disruption day or a re-extraction. Each sweep is one scan of the live
incidents (a few hundred rows). In `batch-only`, two batches (minutes to an
hour each) dominate the latency; 5 minutes adds little and keeps the batches
fewer and larger (better prompt-cache reuse inside each).

### Batch-only mode

`enricher.llm.batch.mode: batch-only` (`LLM_MODE=batch-only`). No
synchronous LLM call anywhere: every extraction is half price.

**Latency.** A text change waits for the next sweep (up to
`LLM_BATCH_SWEEP_INTERVAL_SECS`, 300 s), then goes through two batches in
sequence: the primary batch, then (once it has ended and been polled, up to
`LLM_BATCH_POLL_SECS`, 60 s, later) the adversarial batch. Anthropic's
documentation (Message Batches, read 2026-10-09): "most batches completing
within 1 hour", and "Batches expire if processing does not complete within
24 hours". So expect minutes to an hour or two per text change, and plan for
the worst case: a request that expires is dropped and the next sweep
resubmits it, so a day or two is possible when the API is busy.

**What users see meanwhile.** The incident's previous extraction no longer
counts: the aggregator only uses an extraction whose `source_text_hash`
matches the current text (`LoadedIncident::new` in
`crates/aggregator/src/queries.rs`). Until the batch result is written, the
incident is shown as if never enriched: severity from the keyword rules,
without the LLM's periods, impact type or annotations. A brand-new incident
is shown the same way until its first extraction. That is the real cost of
this mode.

**Cost.** Half of everything: on Haiku 5.5 about **$3-4 a month** instead
of $6-8 (the [Cost](#cost) assumptions, all at the batch price). Prompt
caching still applies inside batches, best effort.

**Retention.** Batch results stay downloadable for 29 days after creation
(the same page: "Batch results are available for 29 days after creation").
The enricher reads each batch as soon as it has ended, so this only matters
for a batch left unread (an enricher down for weeks, or rolled back to
`normal`, below); such a batch is abandoned with a 404 and its incidents go
into a later sweep.

**Judging it.** Once the extraction latency histogram
(`distant_signal_enricher_enrichment_latency_seconds{path}`) is merged, its
`path="batch"` series against `path="stream"` in `normal` is the answer:

```promql
histogram_quantile(0.95, sum by (le) (rate(distant_signal_enricher_enrichment_latency_seconds_bucket{path="batch"}[1d])))
```

Until then: `distant_signal_enricher_llm_batch_oldest_age_seconds` (how
long the oldest in-flight batch has been running), the rate of
`distant_signal_enricher_llm_batches_total{event="ended"}`, and
`distant_signal_enricher_deferred_to_batch_total` for how many text changes
went this way. If the p95 is longer than users tolerate, `batch` keeps
quiet periods near-live and still halves the bursts.

**Rollback.** Set `enricher.llm.batch.mode: normal` (or unset it). The
stream loop extracts synchronously again at once. Incidents deferred before
the switch are not re-published to the stream, so the next hourly sweep
picks them up (or set `SWEEP_INTERVAL_SECS` lower for a while). Batches
still in flight are not polled with batches off (no `LLM_SWEEP_MODE=batch`):
their rows stay in `enricher_llm_batches`, unread. Their incidents are
extracted synchronously anyway, and if batches come back on later, a result
for a text that has since changed is dropped by the stale-text guard. Delete
the rows by hand (`DELETE FROM enricher_llm_batches`) once you won't switch
back; or keep `sweepMode: batch` during the rollback so the poll loop drains
them.

## Batch mode

Message Batches run asynchronously at **50% of the synchronous price**, most
within an hour and all within 24 hours (results stay downloadable for 29
days).

This section is `LLM_MODE=normal` with `LLM_SWEEP_MODE=batch`; the
[LLM modes](#llm-modes) `batch` and `batch-only` reuse all of its machinery
but also take the stream loop's work.

**Where it applies: the sweep only.** The enricher has three loops: the
stream loop (a text change, seconds after `api` publishes it), reclaim (its
retries) and the hourly sweep (anything stale: missed events and, above all,
every live incident after a `model_version` change). Only the sweep is bulk
and latency-tolerant, so only the sweep batches; the other two always stay
synchronous. A sweep that finds fewer than `LLM_BATCH_MIN_ITEMS` (20)
incidents runs them synchronously too: in steady state the sweep finds a
handful, and half the price of a handful of calls isn't worth an hour's
delay. So in practice batch mode pays off on the **re-extraction after a
model or prompt change** (including the switch to Claude itself), which is
also when the volume is largest.

**How it runs.** An incident is three calls, and the two adversarial calls
need the primary call's periods, so a batch run has two stages:

1. The sweep drops every incident already in a batch, runs the usual
   preflight (unchanged text is skipped, semantic no-ops carried forward,
   backed-off texts skipped), and submits one primary request per incident
   (`custom_id` `p-<n>`), up to `LLM_BATCH_MAX_ITEMS` per batch.
2. The poll loop polls each batch every `LLM_BATCH_POLL_SECS`. When one has
   ended it downloads `results_url` (JSONL) and matches results by
   `custom_id`, never by position. Each primary output that parses goes into
   an adversarial batch (`r-<n>` and `s-<n>`).
3. When that ends, each incident whose two verdict lists parse is combined
   and written by the synchronous path's own `finish_extraction` (same
   combine checks, same stale-text guard in `write_extraction`).

`errored`, `canceled`, `expired` and missing results drop that incident from
the run; the next sweep finds it again, because its stored hash still
doesn't match. An unusable success (refusal, truncated or unparseable JSON)
also feeds the per-text backoff, as on the synchronous path.

**Restarts resume.** Every in-flight batch is a row of
`enricher_llm_batches` (migration `20261010120000`): batch id, stage,
`model_version`, and the exact text each incident was submitted with (plus
the primary output in the adversarial stage). The poll loop works from the
table, so a restarted pod resumes polling instead of re-submitting. Moving
to the adversarial stage inserts the new row and deletes the old one in one
transaction, after the new batch exists. Known gaps, all bounded:

- A crash between creating a batch and recording it orphans that batch: it
  runs and is billed, nobody reads it, and its incidents go into a later
  batch.
- A batch whose `model_version` is no longer the service's (a model change
  mid-run) is canceled and dropped.
- Batch mode records no churn measurement (the `churn` baseline isn't
  persisted).
- Batches belong to the Claude workspace, not to the key, so in-flight batch
  ids stay valid across a key rotation (or a switch to [keyless auth](#keyless-auth))
  in the same workspace.

**OpenAI batch.** The enricher has no OpenAI Batch API path, and
`LLM_SWEEP_MODE=batch` with `openai` is refused. The design leaves room for
one (the table has a `provider` column, the stage planning in `batch.rs` is
provider-neutral, and `custom_id` matching is the same idea), but OpenAI's
flow is different enough (upload a JSONL file, create a batch on it, download
an output file) to need its own client. **Recommendation: don't build it
yet.** On gpt-6-luna the whole workload is about $4.50 a month
(docs/enricher-openai.md), and the sweep is a small share of it outside
re-extractions; half of that is cents. Revisit only if re-extractions become
frequent or large.

### Is the latency acceptable?

`distant_signal_enricher_enrichment_latency_seconds{path, outcome}` is the
end-to-end delay a reader sees: from when the incident's current text was
first observed to when its extraction was committed.

- **Start:** the earliest `incident_history` row of the current text's
  latest unbroken run (the same `reference_date` the prompt gets), falling
  back to `first_seen_at`. The poller writes that row, stamped with its
  transaction's `NOW()`, in the transaction that changes the text, and
  publishes the stream entry only after it commits. So it is the earliest
  record of the text anywhere, and it exists on every path. A stream entry
  id would time the publish, not the change, would be missing on the sweep
  and batch paths, and a reclaimed entry can predate a later edit. The
  poll interval (how late the poller saw a Knowledgebase edit) is not
  included.
- **End:** the moment `write_extraction` (or the carry-forward write)
  committed, on the enricher's clock (clamped at 0 against clock skew).
- `path`: `stream`, `reclaim`, `sweep` (synchronous, including a
  carry-forward the batch-mode sweep does before submitting) or `batch`
  (written when an adversarial batch ends).
- `outcome`: `stored` (three LLM passes) or `carried_forward` (a semantic
  no-op edit, `CARRY_FORWARD_SEMANTIC_NOOPS`).
- Recorded **once per new text**. Not recorded: a text that was already
  extracted (counted when it was stored; a redelivery would count it twice),
  a re-extraction of unchanged text after a model or prompt change (its text
  can be days old, which says nothing about how fast a change reaches
  users), a failed attempt and a stale result discarded because the text
  moved (each retry would add a sample; the eventual success carries the
  delay the failures caused).
- Buckets: 1 s, 5 s, 15 s, 30 s, 1, 2, 5, 10, 30 min, 1, 2, 4, 12, 24 and
  30 h.

How to read it:

1. `stream` is what users get today: seconds to a few minutes. Its p95 is
   the baseline.
2. `reclaim` and `sweep` are the slow tails of the synchronous design (a
   failed call retried after `RECLAIM_MIN_IDLE_SECS`, a missed event found
   by the hourly sweep). Their share of the total says how often the fast
   path already misses.
3. `batch` is what a text gets when it goes through a Message Batch: two
   stages, each up to 24 h. It only has samples when a sweep found at least
   `LLM_BATCH_MIN_ITEMS` stale texts; model-version re-extractions are
   excluded (see above). For the batch round trip alone, watch
   `distant_signal_enricher_llm_batch_oldest_age_seconds`.
4. Compare the `batch` p50/p95 with how long incident texts stay current.
   A text replaced or cleared before its batch ends is never shown
   enriched (`write_extraction` discards it), so batches are only
   acceptable for text changes if their p95 is well below the usual text
   lifetime. Batching the stream path is not supported; this metric is the
   evidence for or against building it.

p50 and p95 by path over the last day:

```promql
histogram_quantile(0.5, sum by (path, le) (
  increase(distant_signal_enricher_enrichment_latency_seconds_bucket[1d])))
histogram_quantile(0.95, sum by (path, le) (
  increase(distant_signal_enricher_enrichment_latency_seconds_bucket[1d])))
```

Share of new texts enriched within 10 minutes, all paths (`le` must be a
bucket boundary):

```promql
sum(increase(distant_signal_enricher_enrichment_latency_seconds_bucket{le="600"}[1d]))
  / sum(increase(distant_signal_enricher_enrichment_latency_seconds_count[1d]))
```

Samples by path and outcome:

```promql
sum by (path, outcome) (increase(distant_signal_enricher_enrichment_latency_seconds_count[1d]))
```

There is no latency alert: the stream path's failure modes already alert
(stream lag, LLM call failures, `DistantSignalEnricherBatchStuck`), and the
metric is for judging the design.

## Prompt caching

**Verdict: use it, with the 1-hour TTL, on the primary call's system
prompt. On by default (`LLM_PROMPT_CACHE=1h`).**

### The prompt structure

Measured from `llm.rs` (characters, and tokens at 4 characters per token;
the current tokenizer may count up to ~1.35x that):

| Part | Static? | Size |
| --- | --- | --- |
| Primary system prompt (`PRIMARY_PROMPT`) | yes | 9,574 chars, ~2,400 tokens |
| Primary schema | yes | 1,216 chars, ~300 tokens |
| Resolution-adversarial system prompt | yes | 988 chars, ~250 tokens |
| Severity-adversarial system prompt | yes | 943 chars, ~240 tokens |
| Adversarial schemas | yes | ~410-450 chars, ~100 tokens each |
| Primary user content: reference date + summary + description | no | typically ~500 tokens |
| Adversarial user content: the text + the period list | no | ~650 tokens |

So about 80% of a primary call's input is a fixed prefix, while an
adversarial call is mostly per-incident. Only the system prompt carries the
`cache_control` marker; the schema travels in `output_config.format`, which
the API compiles into a grammar with its own 24-hour cache, and changing it
invalidates the prompt cache (it never changes between calls of one kind).

### Minimum cacheable length

A prefix shorter than the model's minimum silently doesn't cache (no error,
`cache_creation_input_tokens: 0`):

| Model | Minimum | Primary (~2,400) | Adversarial (~250) |
| --- | --- | --- | --- |
| Haiku 5.5, Sonnet 5.5, Opus 5.5 | 512 | caches | doesn't |
| Sonnet 5 / 4.6, Opus 4.8 | 1,024 | caches | doesn't |
| Haiku 4.5 | 4,096 | **doesn't** | doesn't |

So only the primary call benefits. The adversarial calls carry the marker
anyway: it costs nothing below the minimum and starts working if a prompt
grows past it. `prompt_sizes_match_the_caching_analysis` fails if a prompt
edit moves either side of the line. (Haiku 4.5 never caching this prompt is
one more reason the default is Haiku 5.5.)

### Pricing and break-even

Cache writes cost 1.25x the input price (5-minute TTL) or 2x (1-hour TTL);
reads cost 0.1x (Haiku 5.5: $0.01 per million tokens) or 0.05x (Sonnet 5.5
and Opus 5.5). With *h* the share of primary calls that find the prefix still
cached, the prefix's cost relative to no caching is
(1 − *h*) × write + *h* × read, so caching pays when:

- 5-minute TTL: *h* > 0.25 / (1.25 − read) ≈ **21%**;
- 1-hour TTL: *h* > 1 / (2 − read) ≈ **52%**.

### The enricher's request rate

From the code and the existing estimate (docs/enricher-openai.md, "Cost"):
about 200 extractions a day, i.e. about 200 primary calls a day, ~8 an hour
on average, in daytime clusters (a poll of the feed publishes several
changed incidents at once; the hourly sweep runs its finds back to back).
Measure the real rate with
`sum(increase(distant_signal_enricher_llm_call_total{call="primary"}[1d]))`.

- 5-minute TTL: at ~8 calls an hour spread evenly, the chance that the
  previous primary call was within 5 minutes is 1 − e^(−8.3/12) ≈ **50%**.
  Clustering raises it, quiet hours lower it.
- 1-hour TTL: a miss needs an hour with no primary call at all; that is a
  few times a day (overnight), so *h* ≈ **95%**.

Expected cost of the 2,400-token prefix relative to no caching:

| | No caching | 5m, *h* = 50% | 1h, *h* = 95% |
| --- | --- | --- | --- |
| Haiku 5.5 | 1.00 | 0.68 | **0.20** |
| Sonnet 5.5 | 1.00 | 0.65 | **0.15** |

In dollars (200 primary calls a day, synchronous):

| | Prefix input, no caching | With 1h caching | Saved per month |
| --- | --- | --- | --- |
| Haiku 5.5 | $0.048/day | ~$0.010/day | ~$1.15 (about 15% of the whole Haiku bill) |
| Sonnet 5.5 | $0.96/day | ~$0.14/day | ~$25 |

The downside is bounded and small: if the traffic were far sparser than
assumed (under one primary call an hour), every call would pay the 2x write,
about +$0.00024 a call on Haiku 5.5. Hence 1 hour by default; `5m` is there
for a much busier deployment, `off` for debugging.

### Batches

Caching works inside Message Batches, and the 50% batch discount stacks with
it (a Haiku 5.5 cache read in a batch is $0.005 per million tokens). A batch
runs its requests concurrently and can take longer than 5 minutes, so the
1-hour TTL (the default) is also what the Claude docs recommend for batches.
A batch of N incidents should then pay roughly one or a few cache writes and
N reads of the primary prefix. Cache hits in a batch are best effort.

### Watching it

`cached` and `cache_write` in the token counter (below). The primary call's
read ratio over a day:

```promql
sum(increase(distant_signal_enricher_llm_tokens_total{call="primary", kind="cached"}[1d]))
/
(
    sum(increase(distant_signal_enricher_llm_tokens_total{call="primary", kind="cached"}[1d]))
  + sum(increase(distant_signal_enricher_llm_tokens_total{call="primary", kind="cache_write"}[1d]))
)
```

Near 1 means the prefix is read from cache; near 0 with a nonzero
`cache_write` means the traffic is too sparse for the TTL; both 0 means the
prefix isn't cached at all (a model whose minimum is above the prompt).

## Model

**Default: `claude-haiku-5-5`** (Claude Haiku 5.5, released 2026-10-07).
Configurable with `enricher.llm.anthropic.model` / `LLM_MODEL`.

| Model | Input / output per MTok | Cache read | Notes |
| --- | --- | --- | --- |
| `claude-haiku-5-5` | $0.10 / $0.50 (prompts ≤ 100K tokens) | $0.01 | positioned for high-volume extraction and classification; adaptive thinking, default effort `medium`; 512-token cache minimum |
| `claude-sonnet-5-5` | $2 / $10 | $0.10 | 20x Haiku 5.5; default effort `high` |
| `claude-opus-5-5` | $4 / $20 | $0.20 | far more than this task needs |
| `claude-haiku-4-5` (`claude-haiku-4-5-20251001`) | $1 / $5 | $0.10 | previous Haiku: 10x Haiku 5.5's price, and its 4,096-token cache minimum means the primary prompt never caches |

Why Haiku 5.5: the job is structured extraction from short public text, the
kind of workload the Claude docs name for it, and it costs the same as the
OpenAI candidate (gpt-6-luna at $0.10/$0.50), so the two can be compared on
quality alone. Sonnet 5.5 is the step-up if the eval shows Haiku 5.5 missing
the hard cases (multi-leg segmentation, the BST date arithmetic): at ~$120-150
a month it is affordable, but 20x the price should be earned in the eval
first. The model the brief listed, `claude-haiku-4-5-20251001`, is the
previous Haiku and the worst value here (above).

Thinking: every current Claude model thinks adaptively by default, and its
tokens are billed as output (`completion` in the metrics; the API doesn't
report them separately). For a cheaper, faster run, measure `reasoning_effort
= "low"` and, on Haiku 5.5, `thinking = "disabled"` as extra eval targets.

## Metrics and cost

Unchanged metrics keep their meaning (`enricher_llm_call_total`,
`enricher_llm_call_duration_seconds`, `enricher_llm_model_info`, which
names the model and `base_url_host` `api.anthropic.com`). Changed or new:

- `distant_signal_enricher_enrichment_latency_seconds{path, outcome}`
  (histogram, all modes): from a text first being observed to its
  extraction being committed; see
  [Is the latency acceptable?](#is-the-latency-acceptable).
- `distant_signal_enricher_llm_tokens_total{call, kind}` gains a fifth kind,
  `cache_write` (15 series, all registered at 0). For the Claude API:
  `prompt` is every input token (`input_tokens` +
  `cache_creation_input_tokens` + `cache_read_input_tokens`), `cached` the
  cache reads, `cache_write` the cache writes, `completion` `output_tokens`
  (thinking included), `reasoning` always 0. OpenAI never sends
  `cache_write`.
- `distant_signal_enricher_deferred_to_batch_total{path}`
  (`LLM_MODE=batch`/`batch-only` only, registered at 0 then): text changes
  the stream loop (`path="stream"`) or reclaim (`path="reclaim"`) left for
  the sweep instead of extracting.
- Batch mode only (registered when `LLM_SWEEP_MODE=batch` or `LLM_MODE` is
  `batch`/`batch-only`):
  - `distant_signal_enricher_llm_batch_tokens_total{call, kind}`: the same
    kinds for batch results, kept apart because they are billed at half
    price;
  - `distant_signal_enricher_llm_batches_total{stage, event}`: `submitted`,
    `submit_failed`, `ended`, `poll_failed`, `abandoned`;
  - `distant_signal_enricher_llm_batch_requests_total{call, result}`:
    `succeeded`, `invalid`, `errored`, `canceled`, `expired`, `missing`;
  - `distant_signal_enricher_llm_batches_in_flight`;
  - `distant_signal_enricher_llm_batch_oldest_age_seconds`: the oldest
    in-flight batch's age.

Batch mode (either way) renders three alerts (docs/alerts.md):
`DistantSignalEnricherBatchFailing` (batches failing to submit or
abandoned), `DistantSignalEnricherBatchResultsFailing` (a high share of
errored, expired or canceled results) and `DistantSignalEnricherBatchStuck`
(a batch in flight for over 26 h). Thresholds are under
`metrics.prometheusRule.rules.enricherBatches`.

Spend for Haiku 5.5 over the last day (1-hour cache writes at $0.20; use
$0.125 for `LLM_PROMPT_CACHE=5m`), in USD:

```promql
(
    (
        sum(increase(distant_signal_enricher_llm_tokens_total{kind="prompt"}[1d]))
      - sum(increase(distant_signal_enricher_llm_tokens_total{kind="cached"}[1d]))
      - sum(increase(distant_signal_enricher_llm_tokens_total{kind="cache_write"}[1d]))
    ) * 0.10
  + sum(increase(distant_signal_enricher_llm_tokens_total{kind="cached"}[1d])) * 0.01
  + sum(increase(distant_signal_enricher_llm_tokens_total{kind="cache_write"}[1d])) * 0.20
  + sum(increase(distant_signal_enricher_llm_tokens_total{kind="completion"}[1d])) * 0.50
) / 1e6
and on() count(distant_signal_enricher_llm_model_info{model="claude-haiku-5-5"})
```

Add the same expression over `enricher_llm_batch_tokens_total`, times 0.5,
for batch mode. The Claude Console's usage page stays the source of truth.

### Cost

Assumptions as in docs/enricher-openai.md: ~200 extractions a day, ~5,000
input tokens each (2,400 of them the cacheable primary prefix), plus output.
Output is the uncertain part: ~450 visible tokens per incident plus adaptive
thinking, assumed here at ~1,000 more (measure it in the eval records).

| | Input/day | Output/day | Per month |
| --- | --- | --- | --- |
| Haiku 5.5, 1h caching | ~$0.06 | ~$0.15 | **~$6-8** |
| Sonnet 5.5, 1h caching | ~$1.1 | ~$3.0 | **~$120-150** |

Switching provider or model re-extracts every live incident once
(`model_version` is `<model>@periods-v2`): a few hundred incidents, cents on
Haiku 5.5, and half that with batch mode. Set the workspace spend limit at
about twice the expected month.

## Switching and rolling back

Switch: set the chart values in [Setup](#setup). Expect one re-extraction
pass on the next sweep (synchronous, or batched with
`enricher.llm.batch.sweepMode: batch`, which finishes within hours instead
of in one long sweep). Watch `enricher_llm_call_total` by outcome,
`enricher_llm_call_duration_seconds`, and the cache read ratio above.

Roll back: restore the previous `enricher.llm.*` values (or just
`provider: openai`). `model_version` changes back, so incidents re-extract
with the previous model. In-flight batches are then canceled and dropped by
the next poll (their `model_version` no longer matches), or, if batch mode is
off, left in `enricher_llm_batches` unread; delete those rows by hand
(`DELETE FROM enricher_llm_batches`) once you are sure you won't switch back.

## Evaluating before switching

The model-eval harness (docs/enricher-model-eval.md) runs the service's own
client, so it runs the Claude API with the same requests:

- `crates/enricher/eval/targets.example.toml` has
  `anthropic-claude-haiku-5-5` and `anthropic-claude-sonnet-5-5` targets
  (`provider = "anthropic"`, key from `ANTHROPIC_API_KEY`), next to the
  current ones; or set `LLM_PROVIDER=anthropic` and `LLM_API_KEY` for a
  single env-var target. The live evals and `replay_eval` read the same
  variables.
- Look for: a 100% valid-output rate (no `refused`, no HTTP 400, which would
  mean a schema the API rejects), no false `resolved`, segmentation at least
  as good as the current model's, and consistent repeats (there is no
  `temperature: 0` here).
- Check the token records: `cache_write_tokens` on the first primary call,
  `cached_tokens` on the following ones, and how many output tokens thinking
  adds.

## Data handling

What leaves the cluster: the incident summary and description (public
National Rail Knowledgebase text), the reference date and the prompts; no
user data. Per Anthropic's data retention page (read 2026-10-09; re-check
before switching), API data is not used for model training without
permission, and Message Batches are stored for 29 days by design (outside
zero data retention). The privacy notice and the UK legal review assume a
self-hosted model, so the same follow-up as for OpenAI applies
(docs/enricher-openai.md, "Legal follow-up before switching"), with Anthropic
as the processor.

## Keyless auth

`enricher.llm.auth: anthropicWifAuthentik` (`LLM_AUTH=anthropic-wif-authentik`)
replaces the API key with the Claude API's workload identity federation,
through Authentik, the same way `openaiWifAuthentik` works for OpenAI
(docs/enricher-openai.md, "Keyless auth"). Off by default. There is no
Kubernetes-direct variant: the cluster's issuer isn't publicly reachable,
and Authentik is the identity the Claude rule trusts.

Claude docs (read 2026-10-09):
<https://platform.claude.com/docs/en/manage-claude/workload-identity-federation>
and <https://platform.claude.com/docs/en/manage-claude/wif-reference>.

### The flow

1. The kubelet projects a service-account token for the dedicated
   `enricher` ServiceAccount, with audience = the Authentik client ID, at
   `/var/run/secrets/llm-identity/token` (`LLM_IDENTITY_TOKEN_FILE`; a
   provider-neutral path, unlike the OpenAI modes' `/var/run/secrets/openai`).
2. The enricher sends it to Authentik's token endpoint as a JWT client
   assertion (`client_credentials`, as `openaiWifAuthentik` does) and gets an
   Authentik access token (a JWT).
3. That JWT is the `assertion` of
   `POST https://api.anthropic.com/v1/oauth/token`, JSON body, RFC 7523:
   `grant_type: urn:ietf:params:oauth:grant-type:jwt-bearer`, `assertion`,
   `federation_rule_id`, `organization_id`, `service_account_id`, and
   `workspace_id` when set. The answer is `access_token` (`sk-ant-oat01-...`),
   `token_type: Bearer`, `expires_in`.
4. Every Messages and Message Batches request carries
   `Authorization: Bearer <token>` and no `x-api-key`.

**One Authentik token per exchange.** The Claude API accepts a JWT that
carries a `jti` only once per issuer; a second exchange with it fails
(`jti_reused`). So this mode never caches the Authentik token: every Claude
exchange, including the one after a 401 and every refresh, first fetches a
fresh one. The Claude token itself is cached and refreshed before expiry
with the usual margin (the larger of `LLM_TOKEN_REFRESH_SKEW_SECS`, 60 s,
and 10% of its life).

**Lifetime.** The minted token lives min(the rule's token lifetime, 2 × the
presented JWT's remaining life). With the rule at 3600 s and a fresh
Authentik token of 30 minutes, that is the full hour, so the enricher
exchanges about once an hour.

**Batches.** Batches belong to the workspace, not the credential: batch ids
submitted under an API key stay valid after switching to keyless auth in
the same workspace, and the reverse.

### Console runbook (Claude side)

Done by hand in the Claude Console by an org admin. Write down every ID.

1. **Workspace.** Settings → Workspaces → Create workspace, e.g.
   `distant-signal`. Set its spend limit (Limits) to about twice the expected
   month (see [Cost](#cost)) and, if you want, a lower rate limit. Note its ID
   (`wrkspc_...`).
2. **Service account.** Settings → Workload identity → Service accounts →
   Create, name `ds-enricher`, organization role `developer`. Note its ID
   (`svac_...`). Open the `distant-signal` workspace → Members → add
   `ds-enricher` (a service account acts only in workspaces it is a member
   of, plus the Default workspace).
3. **Issuer.** Workload identity → Issuers → Create (Custom OIDC):
   - Issuer URL: the Authentik application's issuer, **exactly** as it
     appears in Authentik tokens' `iss`, trailing slash included, e.g.
     `https://sso.example.com/application/o/ds-enricher-anthropic/`.
     Decode a token (below) and copy `iss` byte for byte.
   - JWKS source: `discovery` (Anthropic fetches
     `<issuer>/.well-known/openid-configuration`, which Authentik serves).
     The issuer must be reachable from the internet on https/443 with a
     public DNS name. Use **Verify issuer**.
   - Leave `check_jti` on (the default).
4. **Rule.** Workload identity → Rules → Create:
   - Issuer: the one above. Target: `ds-enricher`. Workspace:
     `distant-signal` only (then `workspaceId` can stay empty in the chart).
   - Match: `subject_prefix` = the Authentik token's `sub` (exact; decode a
     token to read it) and `audience` = the Authentik provider's client ID
     (the Anthropic one, below). At least the subject must be set; add
     `claims` if your Authentik mapping emits a group claim you want pinned.
   - Scope: `workspace:developer`. Not `workspace:inference`: its documented
     endpoint list (Messages, Models) doesn't include Message Batches, which
     batch mode needs. `developer` matches what a workspace API key can do.
   - **Token lifetime: 3600.** The Connect-workload wizard pre-fills 600;
     change it, or the enricher re-exchanges every few minutes.
   - Note the rule ID (`fdrl_...`).
5. **Organization ID.** Settings → Organization: the UUID.

### Authentik side (Ranma-Config)

Create a **separate** OAuth2/OpenID provider and application for Claude,
not the OpenAI one:

- its own client ID (e.g. `ds-enricher-anthropic`), so its access tokens
  carry their own audience and can't be replayed at OpenAI (or OpenAI's at
  Claude);
- the same JWT-federation (machine-to-machine) setup as the OpenAI provider:
  the k3s Generic OAuth Source as a federated source, an expression policy
  binding that checks the projected token's audience and service account;
- access token validity `minutes=30` (at most 60: the Claude issuer
  rejects JWTs whose `exp − iat` exceeds 1 hour by default, and the minted
  token lives at most twice what is left of it);
- signing key: an RS256/ES256 certificate (the Claude API refuses HMAC);
- **check that its access tokens carry a `jti`** (decode one). If they do,
  the "fresh token per exchange" rule above matters and is what the
  enricher does; if they don't, nothing breaks, but there is no replay
  protection at Anthropic.

### Chart values

```yaml
enricher:
  serviceAccount:
    create: true
  llm:
    provider: anthropic
    auth: anthropicWifAuthentik
    anthropic:
      existingSecret: ""            # keyless: no key Secret
    workloadIdentity:
      anthropic:
        organizationId: <org-uuid>
        serviceAccountId: svac_...
        federationRuleId: fdrl_...
        # workspaceId: wrkspc_...   # only if the rule spans several workspaces
      authentik:
        tokenUrl: https://sso.example.com/application/o/token/
        clientId: ds-enricher-anthropic
      # tokenAudience: ""           # empty: the Authentik client ID
```

The chart refuses to render without the three IDs, the Authentik URL and
client ID, a dedicated ServiceAccount, with a Claude key Secret set, or with
`provider: openai`; it opens the token URLs' ports in the egress
NetworkPolicy, and renders `DistantSignalEnricherTokenExchangeFailing`.

### Test exchange

From a pod running as the enricher's ServiceAccount (or with a token minted
by `kubectl create token`):

```bash
K8S=$(kubectl -n <ns> create token <enricher-sa> --audience ds-enricher-anthropic --duration 10m)
claims() { python3 -c 'import base64,json,sys; p=sys.argv[1].split(".")[1]; print(json.dumps(json.loads(base64.urlsafe_b64decode(p+"="*(-len(p)%4))),indent=2))' "$1"; }
# 1. Authentik (a NEW token each time: a jti is single-use at Anthropic).
AK=$(curl -sS https://sso.example.com/application/o/token/ \
  -d grant_type=client_credentials -d client_id=ds-enricher-anthropic \
  -d client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer \
  --data-urlencode "client_assertion=$K8S" | jq -r .access_token)
claims "$AK"   # iss must equal the Claude issuer URL byte for byte; note sub, aud, jti
# 2. Claude.
CT=$(jq -n --arg a "$AK" '{grant_type:"urn:ietf:params:oauth:grant-type:jwt-bearer",
  assertion:$a, federation_rule_id:"fdrl_...", organization_id:"<org-uuid>",
  service_account_id:"svac_..."}' \
  | curl -sS https://api.anthropic.com/v1/oauth/token -H 'content-type: application/json' -d @- \
  | jq -r .access_token)
# 3. One tiny request.
curl -sS https://api.anthropic.com/v1/messages -H "authorization: Bearer $CT" \
  -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
  -d '{"model":"claude-haiku-5-5","max_tokens":16,"messages":[{"role":"user","content":"ping"}]}'
```

Re-running step 2 with the same `$AK` must fail (that is the `jti` check
working); fetch a new one.

### Troubleshooting keyless auth

- Every denial is the same `401 authentication_error` ("Authentication
  failed"), counted as `enricher_llm_token_exchange_total{stage="anthropic",
  outcome="authentication_failed"}`. **The reason is only on the Console's
  Workload identity → Authentication history page** (for example
  `match_subject_prefix`, `jti_reused`, `workspace_id_required`, an `iss`
  mismatch, a JWKS fetch failure). A 401 with no history entry usually means
  the rule ID itself is wrong.
- `iss` must match the issuer URL byte for byte (scheme, host, path,
  trailing slash).
- A 400 `invalid_request_error` is a malformed request (a missing field, a
  bad `workspace_id`); its message names the problem.
- A token that works for Messages but gets 403 on batches means the rule's
  scope is too narrow: use `workspace:developer`.
- `stage="authentik"` failures are Authentik's side, as in the OpenAI mode
  (docs/alerts.md, DistantSignalEnricherTokenExchangeFailing).
- Rolling back: set `auth: apiKey` and `anthropic.existingSecret` again; no
  data or batch state changes.
