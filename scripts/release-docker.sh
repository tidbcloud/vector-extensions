#!/usr/bin/env bash
set -euo pipefail

## set up working directory

# the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# the temp directory used, within $DIR
# omit the -p parameter to create a temporal directory in the default location
WORK_DIR=`mktemp -d -p "$DIR"`

# check if tmp dir was created
if [[ ! "$WORK_DIR" || ! -d "$WORK_DIR" ]]; then
  echo "Could not create temp dir"
  exit 1
fi

# deletes the temp directory
function cleanup {      
  rm -rf "$WORK_DIR"
  echo "Deleted temp working directory $WORK_DIR"
}

# register the cleanup function to be called on the EXIT signal
trap cleanup EXIT

# Prepare in the setup directory

# map of docker platform-TARGETARCH:
#  linux/amd64 -> amd64
#  linux/arm64 -> arm64
#  linux/arm/v7 -> arm
cp target/x86_64-unknown-linux-gnu/release/vector "$WORK_DIR"/vector-amd64
cp target/aarch64-unknown-linux-gnu/release/vector "$WORK_DIR"/vector-arm64
cp target/armv7-unknown-linux-gnueabihf/release/vector "$WORK_DIR"/vector-arm
cp config/vector.toml "$WORK_DIR"

VERSION="${VECTOR_VERSION:-"$(scripts/version.sh)"}"
REPO="${REPO:-"tidbcloud/vector"}"
BASE=debian

TAG="${TAG:-$REPO:$VERSION-$BASE}"
DOCKERFILE="scripts/docker/Dockerfile"

PLATFORMS="linux/amd64,linux/arm64,linux/arm/v7"
echo "Building docker image: $TAG for $PLATFORMS"
docker buildx build --push --platform="$PLATFORMS" -t "$TAG" -f "$DOCKERFILE" "$WORK_DIR"
