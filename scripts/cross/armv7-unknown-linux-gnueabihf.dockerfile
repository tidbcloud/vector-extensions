FROM ghcr.io/cross-rs/armv7-unknown-linux-gnueabihf:edge

COPY bootstrap-ubuntu.sh .
COPY install-protoc.sh .
RUN ./bootstrap-ubuntu.sh
RUN ./install-protoc.sh
