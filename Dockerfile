# syntax=docker/dockerfile:1.7

# ---- builder ----
FROM rust:1-bookworm AS builder
WORKDIR /workspace

# Sibling crates injected via buildx `--build-context` (see justfile and
# ci.yml). The container layout mirrors a developer Mac — siblings live
# as sister directories of the service — so [patch] paths in Cargo.toml
# resolve identically in both environments (`path = "../bridge-types"`
# works for native cargo and for this build).
COPY --from=bridge-types . ./bridge-types/
COPY --from=bridge-encodings . ./bridge-encodings/
COPY --from=bridge-rulebot . ./bridge-rulebot/

# Cache deps separately from source. Service goes in a bridge-table-service
# subdirectory so siblings can reference its parent (../<sibling>).
COPY Cargo.toml Cargo.lock ./bridge-table-service/
WORKDIR /workspace/bridge-table-service
RUN mkdir src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/bridge_table_service*

# Real service source last so service-only edits leave sibling layers cached.
COPY src/ ./src/
COPY wwwroot/ ./wwwroot/
RUN cargo build --release

# ---- runtime ----
FROM debian:bookworm-slim

# Base runtime packages — ca-certificates is required for outbound TLS,
# wget is required by HEALTHCHECK. Don't remove either.
#
# Add service-specific apt packages by either:
#   (a) editing the default below (e.g. RUNTIME_PACKAGES="mdbtools ffmpeg")
#   (b) passing --build-arg RUNTIME_PACKAGES="…" at build time
ARG RUNTIME_PACKAGES=""
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget ${RUNTIME_PACKAGES} \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 1000 -m service \
    && mkdir -p /data && chown service:service /data

USER service
WORKDIR /app

COPY --from=builder /workspace/bridge-table-service/target/release/bridge-table-service /app/bridge-table-service
COPY --from=builder /workspace/bridge-table-service/wwwroot /app/wwwroot

ENV PORT=8004
ENV DATABASE_PATH=/data/bridge-table-service.db
EXPOSE 8004

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -q --spider http://localhost:8004/healthz || exit 1

CMD ["/app/bridge-table-service"]
