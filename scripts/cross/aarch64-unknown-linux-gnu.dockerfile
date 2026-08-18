FROM ghcr.io/cross-rs/aarch64-unknown-linux-gnu:edge

COPY bootstrap-ubuntu.sh .
COPY install-protoc.sh .
RUN ./bootstrap-ubuntu.sh
RUN ./install-protoc.sh
