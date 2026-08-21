# syntax=docker/dockerfile:1
# Multi-stage build for the `enricher` service. Same builder pin as
# `api`/`aggregator` (rust:1.88-bookworm) -- this crate pulls in
# sqlx-postgres, same transitive `home` crate version requirement.
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin enricher; \
    else \
      cargo build --bin enricher; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/enricher /usr/local/bin/enricher

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin enricher

COPY --from=builder /usr/local/bin/enricher /usr/local/bin/enricher

USER enricher

ENTRYPOINT ["/usr/local/bin/enricher"]
