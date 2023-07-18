FROM ghcr.io/cross-rs/armv7-unknown-linux-gnueabihf:0.2.5

COPY bootstrap-ubuntu.sh .
COPY install-protoc.sh .
RUN ./bootstrap-ubuntu.sh
RUN ./install-protoc.sh
