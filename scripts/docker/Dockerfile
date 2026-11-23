# Multi-stage build for smaller final image
FROM docker.io/debian:13-slim as builder

# Install build dependencies.
# Pull/upgrade zlib1g for CVE-2026-85091 (DSA-6613-1 → …+deb13u1 on trixie).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    systemd \
    curl \
    binutils \
    zlib1g \
    && apt-get upgrade -y --no-install-recommends zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Final stage with minimal runtime dependencies
FROM docker.io/debian:13-slim

# Install only runtime dependencies; upgrade zlib1g for CVE-2026-85091.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    systemd \
    zlib1g \
    && apt-get upgrade -y --no-install-recommends zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

ARG TARGETARCH
COPY vector-$TARGETARCH /usr/bin/vector
# COPY vector.toml /etc/vector/vector.toml

# Nextgen vs legacy is a runtime switch (same binary). Set to "true" for -ng images.
ARG VECTOR_NEXTGEN=false
ENV VECTOR_NEXTGEN=${VECTOR_NEXTGEN}
ENV VECTOR_LOG="info"

RUN ["chmod", "+x", "/usr/bin/vector"]
# Smoke test
RUN ["vector", "--version"]

ENTRYPOINT ["/usr/bin/vector"]
