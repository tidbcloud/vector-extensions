#!/usr/bin/env sh

sleep 600 # wait for data
cargo test --test ${CASE}
