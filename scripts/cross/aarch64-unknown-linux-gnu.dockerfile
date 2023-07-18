FROM ghcr.io/cross-rs/aarch64-unknown-linux-gnu:0.2.5

COPY bootstrap-ubuntu.sh .
COPY install-protoc.sh .
RUN ./bootstrap-ubuntu.sh
RUN ./install-protoc.sh
