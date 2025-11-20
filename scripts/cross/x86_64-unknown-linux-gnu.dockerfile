FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:edge

COPY bootstrap-ubuntu.sh .
COPY install-protoc.sh .
RUN ./bootstrap-ubuntu.sh
RUN ./install-protoc.sh

RUN apt-get update && \
    apt-get remove --assume-yes gcc-9 && \
    apt-get --assume-yes install clang && \
    rm -rf /var/lib/apt/lists/*
