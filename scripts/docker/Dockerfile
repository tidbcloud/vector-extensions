FROM docker.io/alpine:3.14
RUN apk --no-cache add ca-certificates tzdata

COPY vector /usr/local/bin/
COPY vector.toml /etc/vector/vector.toml

# Smoke test
RUN ["vector", "--version"]

ENTRYPOINT ["/usr/local/bin/vector"]
