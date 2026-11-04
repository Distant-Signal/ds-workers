# ds-workers

Part of [Distant Signal](https://github.com/Distant-Signal/distant-signal).
The background workers: `aggregator` (line status), `enricher` (incident enrichment, with its `eval/` harness) and `notifier` (push notifications).

## Images

Every push to `main` builds and publishes (`.github/workflows/containers.yml`):

- `ghcr.io/distant-signal/images/aggregator`
- `ghcr.io/distant-signal/images/enricher`
- `ghcr.io/distant-signal/images/notifier`

The deployed versions are pinned by digest in
[ds-deploy](https://github.com/Distant-Signal/ds-deploy). Renovate is
installed there but not running yet, so until it is, a ds-deploy pull
request bumps a pin by hand.

## ds-core

The shared crates (`common`, `ds-runtime`, `health-http`, `ds-store`, `ds-lines`) come from
[ds-core](https://github.com/Distant-Signal/ds-core) as git dependencies,
all on one tag (`Cargo.toml`, `[workspace.dependencies]`). Once Renovate
runs here it opens one `renovate/ds-core` PR per ds-core release; until
then, bump every crate's tag together by hand.

To build against a local ds-core checkout beside this one, without
committing anything, pass a patch for each crate, e.g.:

```sh
cargo --config 'patch."https://github.com/Distant-Signal/ds-core".common.path="../ds-core/crates/common"' test
```

## Development

```sh
cargo test --workspace                         # the tests that need no database
uv run scripts/db-tests.py list-passes         # the DB test passes (DATABASE_URL, REDIS_URL)
python3 ../ci/scripts/gen-rust-dockerfiles.py  # regenerate docker/*.Dockerfile
```

The schema, grants and Redis ACL belong to ds-core: a change that needs a
migration or a grant starts there.

## Issues

File them in
[Distant-Signal/distant-signal](https://github.com/Distant-Signal/distant-signal/issues),
with the `area:workers` label: this repository has Issues turned off. The
contributing guide for every repository is the organisation's
[CONTRIBUTING.md](https://github.com/Distant-Signal/.github/blob/main/CONTRIBUTING.md);
[CLAUDE.md](CLAUDE.md) lists this repository's commands and conventions.

## Licence

AGPL-3.0-or-later (`LICENSE`).
