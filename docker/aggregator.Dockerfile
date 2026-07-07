# Multi-stage build for the `aggregator` service.
#
# Builder pin: matches `api` at rust:1.88-bookworm (this crate pulls in
# sqlx-postgres, whose transitive `home` crate requires 1.88+, same as
# `api` — confirmed by `api`'s own Dockerfile comment).
#
# Build from the repo root so the workspace's Cargo.toml/Cargo.lock and
# crates/common path dependency are all in the build context:
#   docker build -f docker/aggregator.Dockerfile .
FROM rust:1.88-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release --bin aggregator

FROM debian:bookworm-slim

# sqlx's tls-native-tls feature verifies the Postgres connection's cert
# (when TLS is in play) against the system store, so the runtime image
# needs a CA bundle even though it otherwise only carries the one binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin aggregator

COPY --from=builder /app/target/release/aggregator /usr/local/bin/aggregator
COPY --chown=aggregator:aggregator lines/ /app/lines/

USER aggregator

ENTRYPOINT ["/usr/local/bin/aggregator"]
