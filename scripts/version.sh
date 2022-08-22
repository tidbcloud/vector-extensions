#!/usr/bin/env bash
set -euo pipefail

VERSION="${VERSION:-"$(awk -F ' = ' '$1 ~ /^version/ { gsub(/["]/, "", $2); printf("%s",$2) }' Cargo.toml)"}"
echo "$VERSION"
