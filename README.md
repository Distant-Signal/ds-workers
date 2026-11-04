# ds-workers

Part of [Distant Signal](https://github.com/Distant-Signal/distant-signal).
The background workers: `aggregator` (line status), `enricher` (incident enrichment, with its `eval/` harness) and `notifier` (push notifications).

## Images

Every push to `main` builds and publishes (`.github/workflows/containers.yml`):

- `ghcr.io/distant-signal/images/aggregator`
- `ghcr.io/distant-signal/images/enricher`
- `ghcr.io/distant-signal/images/notifier`

The deployed versions are pinned by digest in
[ds-deploy](https://github.com/Distant-Signal/ds-deploy), whose Renovate
picks up new builds.

## ds-core

The shared crates (`common`, `ds-runtime`, `health-http`, `ds-store`, `ds-lines`) come from
[ds-core](https://github.com/Distant-Signal/ds-core) as git dependencies,
all on one tag (`Cargo.toml`, `[workspace.dependencies]`). Renovate opens
one `renovate/ds-core` PR per ds-core release.

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

## Licence

AGPL-3.0-or-later (`LICENSE`).
