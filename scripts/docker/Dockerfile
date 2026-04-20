# Multi-stage build for smaller final image
FROM docker.io/debian:13-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    systemd \
    curl \
    binutils \
    && rm -rf /var/lib/apt/lists/*

# Final stage with minimal runtime dependencies
FROM docker.io/debian:13-slim

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    systemd \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

ARG TARGETARCH
COPY vector-$TARGETARCH /usr/bin/vector
# COPY vector.toml /etc/vector/vector.toml

ENV VECTOR_LOG="info"

RUN ["chmod", "+x", "/usr/bin/vector"]
# Smoke test
RUN ["vector", "--version"]

ENTRYPOINT ["/usr/bin/vector"]
