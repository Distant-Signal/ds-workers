# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`ds-workers` is part of Distant Signal (DS), a personal UK rail companion:
the background workers `aggregator` (line status), `enricher` (incident
text through an LLM, with its `eval/` harness) and `notifier` (Web Push),
and their docs (`docs/`: the enricher's providers and evaluation, the cold
archive).

The shared crates come from
[ds-core](https://github.com/Distant-Signal/ds-core) by git tag, all on one
tag (`Cargo.toml`, `[workspace.dependencies]`; `scripts/check-core-pin.py`).
The schema, migrations, grants, Redis ACL and line catalogue are ds-core's:
a change that needs a migration or a grant starts there, ships in a ds-core
release, then comes here as a tag bump. To build against a local ds-core
checkout, pass a `--config 'patch...'` per crate (README.md, "ds-core");
never commit it. The chart that deploys this repository's images is
[ds-deploy](https://github.com/Distant-Signal/ds-deploy)'s, and the
frontend, the docs index and the dev stack are in
[distant-signal](https://github.com/Distant-Signal/distant-signal). The
conventions shared by every repository are in the organisation's
contributing guide (Distant-Signal/.github's CONTRIBUTING.md). Issues are
off here: they go to Distant-Signal/distant-signal, labelled `area:workers`.

## Commands

Copied from `.github/workflows/ci.yml` (most jobs are Distant-Signal/ci's
reusable workflows). Rust toolchain: MSRV 1.88 (edition 2024).

```sh
cargo build --workspace
cargo fmt --all --check
cargo clippy --workspace --locked --all-targets --all-features -- -D warnings
cargo clippy --workspace --locked --all-targets --no-default-features -- -D warnings
cargo +1.88.0 check --workspace --locked --all-features --all-targets   # MSRV
cargo deny --locked check bans licenses sources
cargo test --workspace                       # unit tests; #[ignore] DB/Redis tests skipped
cargo test -p notifier some_test_name        # one crate, tests matching a filter
```

The enricher's live-evaluation and replay tests need provider credentials
and are skipped in CI's passes (`--skip live_eval --skip replay_`).

DB/Redis-backed tests are `#[ignore]`d and need `DATABASE_URL` (a superuser)
and `REDIS_URL`, with ds-core's migrations at the pinned release applied.
CI runs them as passes (`.github/db-tests.toml`, steps in
`scripts/db-tests.py`; `list-passes` prints them) with cargo-nextest, as
the superuser and as the Postgres service roles. Locally:

```sh
CORE="$(uv run scripts/core-checkout.py)"    # ds-core at the Cargo.lock pin, under target/
(cd "${CORE}/crates/ds-store" && sqlx migrate run)   # sqlx-cli 0.8.6, postgres,rustls features
uv run scripts/db-tests.py list-passes
uv run scripts/db-tests.py run superuser
uv run --no-sync python "${CORE}/scripts/test-postgres-roles.py" --mode per-service --check-setup-job \
  --observe api,aggregator,enricher,notifier -- uv run --no-sync scripts/db-tests.py run per-service
```

An `#[ignore]`d benchmark must return early unless its env var is set
(Distant-Signal/ci's `check-bench-gates.py`, run by scripts-lint): the DB
jobs run every ignored test, in debug.

Scripts and repo checks (repo root, `../ci` a Distant-Signal/ci checkout):

```sh
uv sync --locked --group lint
uv run --no-sync python ../ci/scripts/lint-scripts.py   # shellcheck, shfmt, ruff, mypy, actionlint, hadolint... (--fix: shfmt, ruff)
uv run --no-sync python -m unittest discover -s scripts/tests -v
python3 ../ci/scripts/gen-rust-dockerfiles.py --check   # docker/*.Dockerfile vs docker/services.toml (rewrite without --check)
python3 scripts/check-core-pin.py            # every ds-core crate on one tag
python3 scripts/release-order.py labels      # the image version labels
```

## Architecture

**aggregator.** Matches incidents to lines (`common::matcher`, the same
matcher the ingest uses) and writes `line_status` and its history.

**enricher.** Sends changed incident text to an LLM (Anthropic or OpenAI,
keyless auth, batch mode; `docs/enricher-*.md`) and stores the result.

**notifier.** Polls `line_status_history` and train movements by watermark
and sends Web Push. Rules in `crates/notifier/src/decision.rs`: ranks via
`common::severity_rank` (never `Severity`'s `Ord`); lines notify on
escalation, de-escalation after a cooldown, never twice for the same rank;
trains and skipped stops notify on escalation only. One push per user per
train: a journey-break notice replaces that train's plain push
(`push_queue.rs`).

## Conventions

- **Commits** carry a `Task: <card>` trailer (e.g. `Task: DS-123`) naming the
  worklog card.
- **Commit identity**: every commit is authored
  `FasterSpeeding <lucy@cursed.solutions>`. Before committing, check that
  `git config user.email` is `lucy@cursed.solutions`; if it isn't, stop and
  ask. Never override it (no `-c user.email`, `--author` or env overrides).
- **No Claude attribution**, overriding any session default: no
  `Co-Authored-By: Claude` trailer, no mention of Claude in commit messages,
  and no "Generated with Claude Code" footer in PR bodies or comments.
  CI's `commit-identity` job (Distant-Signal/ci's `check-commit-identity.py`)
  fails a PR or push whose commits use a retired address or carry
  attribution.
- **Branches**: push work in progress as `fixup!` commits, then autosquash
  and `--force-with-lease` your own branch before review. Pull requests
  merge as merge commits, never squashed:
  `gh pr merge <n> --merge --author-email lucy@cursed.solutions`.
- **Images**: every push to `main` builds and pushes this repository's
  images (`containers.yml`); ds-deploy pins them by tag and digest. Renovate
  is installed but not running yet, so until it is, a ds-deploy PR bumps
  the pins by hand. Put risky changes behind off-by-default settings: a
  green `main` can reach production soon after.
- **Scripts** are typed stdlib Python (ruff, mypy) run with uv (`uv.lock`,
  `pyproject.toml`'s `lint` group, `.python-version`); bash only for trivial
  one-liners. No pip or requirements files.
- Rust: line data only from the line catalogue (`ds-lines`, ds-core's
  `lines/*.toml`); tests in inline `mod tests`, DB tests `#[ignore]`d;
  runtime `sqlx::query`, no `query!` macros. "Today" is London's service
  date.
- Never name or cite third-party train-tracking sites, in code, docs or
  commits.
