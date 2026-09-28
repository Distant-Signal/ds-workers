# syntax=docker/dockerfile:1
# Multi-stage build for the `enricher` service. Same builder pin as
# `api`/`aggregator` (rust:1.88-bookworm) -- this crate pulls in
# sqlx-postgres, same transitive `home` crate version requirement.
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder
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

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 enricher \
    && useradd --system --no-create-home --shell /usr/sbin/nologin --uid 1000 --gid 1000 enricher

COPY --from=builder /usr/local/bin/enricher /usr/local/bin/enricher

# Numeric USER, not the `enricher` name useradd created above: Kubernetes'
# runAsNonRoot admission check (this chart's podSecurityContext sets
# runAsNonRoot: true with no explicit runAsUser) resolves the image's
# config purely from its manifest -- it does NOT read /etc/passwd inside
# the image -- so a symbolic USER fails admission with "container has
# runAsNonRoot and image has non-numeric user, cannot verify user is
# non-root". Pinned to the same uid/gid useradd was given above so this
# stays in sync with the group ownership set via COPY --chown/groupadd.
USER 1000:1000

ENTRYPOINT ["/usr/local/bin/enricher"]
