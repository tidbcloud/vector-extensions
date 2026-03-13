#!/usr/bin/env bash
set -euo pipefail

## Build perl-nice variant from existing base image
## Builds multi-platform image from specified base image

# Base image
BASE_IMAGE="${BASE_IMAGE:-385595570414.dkr.ecr.us-west-2.amazonaws.com/tidbcloud/vector:0.37.1-2d79df-debian}"

# Target image tag
# If TAG not set, extract repo and tag from BASE_IMAGE, add -perl-nice suffix
if [ -z "${TAG:-}" ]; then
  # Extract repo path (without tag)
  REPO=$(echo "$BASE_IMAGE" | sed 's/:.*$//')
  # Extract tag part; use latest if none
  IMAGE_TAG=$(echo "$BASE_IMAGE" | sed 's/^.*://')
  if [ "$IMAGE_TAG" = "$BASE_IMAGE" ]; then
    IMAGE_TAG="latest"
  fi
  TAG="${REPO}:${IMAGE_TAG}-chrt"
fi

# Dockerfile path
DOCKERFILE="scripts/docker/Dockerfile.perl-nice"

# Supported platforms
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"

echo "Building docker image: $TAG for $PLATFORMS"
echo "Base image: $BASE_IMAGE"
echo "Dockerfile: $DOCKERFILE"

# Get project root (parent of script dir)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Verify paths
echo "Current directory: $(pwd)"
echo "Dockerfile path: $DOCKERFILE"
if [ ! -f "$DOCKERFILE" ]; then
  echo "ERROR: Dockerfile not found at $DOCKERFILE (from $PROJECT_ROOT)" >&2
  exit 1
fi
echo "Dockerfile found, proceeding with build..."

# Use buildx for multi-platform build
# Note: multi-platform requires --push, or --load for current platform only
if [ "${PUSH:-false}" = "true" ]; then
  echo "Building and pushing multi-platform image..."
  docker buildx build --push \
    --platform="$PLATFORMS" \
    --build-arg BASE_IMAGE="$BASE_IMAGE" \
    -t "$TAG" \
    -f "$DOCKERFILE" \
    .
else
  # Local test: build current platform only (uses --load)
  CURRENT_PLATFORM=$(docker version --format '{{.Server.Arch}}')
  if [ "$CURRENT_PLATFORM" = "amd64" ]; then
    PLATFORM="linux/amd64"
  elif [ "$CURRENT_PLATFORM" = "arm64" ] || [ "$CURRENT_PLATFORM" = "aarch64" ]; then
    PLATFORM="linux/arm64"
  else
    PLATFORM="linux/amd64"  # default
  fi
  echo "Building single-platform image for local testing: $PLATFORM"
  echo "Use PUSH=true to build and push multi-platform image"
  docker buildx build \
    --platform="$PLATFORM" \
    --build-arg BASE_IMAGE="$BASE_IMAGE" \
    -t "$TAG" \
    -f "$DOCKERFILE" \
    --load \
    .
fi

echo "Done building docker image: $TAG"

