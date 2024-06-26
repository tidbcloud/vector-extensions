FROM docker.io/debian:bullseye
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata systemd curl binutils

ARG TARGETARCH
COPY vector-$TARGETARCH /usr/bin/vector
# COPY vector.toml /etc/vector/vector.toml

ENV VECTOR_LOG="info"

RUN ["chmod", "+x", "/usr/bin/vector"]
# Smoke test
RUN ["vector", "--version"]

ENTRYPOINT ["/usr/bin/vector"]
