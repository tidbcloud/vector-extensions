ARG RUST_VERSION
FROM docker.io/rust:${RUST_VERSION}-slim-bullseye

COPY . .

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    g++ \
    libclang1-9 \
    libsasl2-dev \
    libssl-dev \
    llvm-9 \
    pkg-config \
    zlib1g-dev

RUN cargo build
