FROM docker.io/debian:bullseye-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata systemd

ARG TARGETARCH
COPY vector-$TARGETARCH /usr/bin/vector
COPY vector.toml /etc/vector/vector.toml

ENV VECTOR_LOG="info"

# Smoke test
RUN ["vector", "--version"]

ENTRYPOINT ["/usr/bin/vector"]
