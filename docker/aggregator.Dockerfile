# syntax=docker/dockerfile:1
# Multi-stage build for the `aggregator` service.
#
# Builder pin: matches `api` at rust:1.88-bookworm (this crate pulls in
# sqlx-postgres, whose transitive `home` crate requires 1.88+, same as
# `api` — confirmed by `api`'s own Dockerfile comment).
#
# Build from the repo root so the workspace's Cargo.toml/Cargo.lock and
# crates/common path dependency are all in the build context:
#   docker build -f docker/aggregator.Dockerfile .
#
# CARGO_PROFILE picks the cargo build profile (and matching target/<profile>
# output dir): "release" (default) for optimized builds, "debug" for fast
# unoptimized dev builds. Set to "debug" by docker-compose.dev.yml, the
# override that `dev.env` selects via COMPOSE_FILE; docker-compose.yml on
# its own leaves it at "release".
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
# BuildKit cache mounts: the cargo registry, the git checkouts and the
# target/ dir all live in caches that persist across builds, so a rebuild
# recompiles only what actually changed instead of the whole dependency
# tree. Requires the `# syntax=` directive at the top of this file.
#
# The target cache id is keyed by rustc version (`cargo-target-1.88`). Every
# Rust service in this workspace now builds with the same rustc version, so
# this id is shared across all of them -- see docker-compose.yml's top-of-file
# comment for the full list. The registry and git caches hold only downloaded
# sources, so sharing those across all of them is safe too.
#
# `sharing=locked` because docker-compose builds services in parallel, and
# concurrent cargo invocations must not share one target dir unserialised.
#
# The trailing `cp` is the non-obvious part: a cache mount is NOT part of the
# resulting image layer, so /app/target ceases to exist the moment this RUN
# finishes and a later `COPY --from=builder /app/target/...` would find
# nothing. The binary has to be copied out to a normal path within the same
# RUN — which is why the runtime stage below copies from /usr/local/bin/aggregator.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin aggregator; \
    else \
      cargo build --bin aggregator; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/aggregator /usr/local/bin/aggregator

FROM debian:bookworm-slim

# sqlx's tls-native-tls feature verifies the Postgres connection's cert
# (when TLS is in play) against the system store, so the runtime image
# needs a CA bundle even though it otherwise only carries the one binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin aggregator

COPY --from=builder /usr/local/bin/aggregator /usr/local/bin/aggregator
COPY --chown=aggregator:aggregator lines/ /app/lines/

USER aggregator

ENTRYPOINT ["/usr/local/bin/aggregator"]
