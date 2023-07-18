FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:0.2.4

COPY bootstrap-ubuntu.sh .
COPY bootstrap-ubuntu-x64.sh .
RUN ./bootstrap-ubuntu.sh
RUN ./bootstrap-ubuntu-x64.sh
