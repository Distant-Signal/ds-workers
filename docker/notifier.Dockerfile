# syntax=docker/dockerfile:1
# Multi-stage build for the `notifier` service. Mirrors
# docker/aggregator.Dockerfile's shape exactly (no HTTP surface, same
# rust:1.88-bookworm builder pin, same cache-mount pattern) with one
# addition: libssl-dev/pkg-config in the builder stage, needed at build
# time by the web-push crate's OpenSSL dependency (confirmed by this
# plan's own Status note) -- the same requirement docker/trust-consumer.Dockerfile
# already documents for its own, unrelated reason (rdkafka's sasl2-sys).
#
# libssl3 in the runtime stage: unlike docker/aggregator.Dockerfile (which
# gets away with `ca-certificates` alone -- Debian's `ca-certificates`
# package transitively depends on `openssl`, which pulls in `libssl3`
# anyway), this Dockerfile lists `libssl3` explicitly, matching
# docker/trust-consumer.Dockerfile's precedent: `ldd` against a locally
# built (non-container) `notifier` binary on this build host confirmed a
# real dynamic dependency on `libssl`/`libcrypto` (native-tls, used by both
# sqlx's tls-native-tls feature and web-push's hyper-tls). No Docker
# daemon was available in the sandbox this Dockerfile was authored in, so
# the container-image-specific `ldd` check this plan's Task 7 Step 2
# describes (`docker run --rm --entrypoint ldd <tag> /usr/local/bin/notifier`)
# should still be run once in a real Docker environment to confirm the
# exact Debian bookworm package name/version resolves the same way.
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm AS builder
ARG CARGO_PROFILE

RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin notifier; \
    else \
      cargo build --bin notifier; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/notifier /usr/local/bin/notifier

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin notifier

COPY --from=builder /usr/local/bin/notifier /usr/local/bin/notifier

USER notifier

ENTRYPOINT ["/usr/local/bin/notifier"]
