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


## build docker image in the setup directory

cp target/x86_64-unknown-linux-gnu/release/vector "$WORK_DIR"
cp config/vector.toml "$WORK_DIR"

VERSION="${VECTOR_VERSION:-"$(scripts/version.sh)"}"
REPO="${REPO:-"tidbcloud/vector"}"
BASE=debian

TAG="${TAG:-$REPO:$VERSION-$BASE}"
DOCKERFILE="scripts/docker/Dockerfile"

echo "Building docker image: $TAG"
docker build -t "$TAG" -f "$DOCKERFILE" "$WORK_DIR"
