#!/usr/bin/env bash
set -euo pipefail

## 构建基于现有镜像的 perl-nice 版本
## 这个脚本会基于指定的基础镜像构建多平台镜像

# 基础镜像
BASE_IMAGE="${BASE_IMAGE:-385595570414.dkr.ecr.us-west-2.amazonaws.com/tidbcloud/vector:0.37.1-2d79df-debian}"

# 目标镜像标签
# 如果未指定 TAG，则从 BASE_IMAGE 提取仓库和标签，然后添加 -perl-nice 后缀
if [ -z "${TAG:-}" ]; then
  # 提取仓库路径（去掉标签部分）
  REPO=$(echo "$BASE_IMAGE" | sed 's/:.*$//')
  # 提取标签部分，如果没有标签则使用 latest
  IMAGE_TAG=$(echo "$BASE_IMAGE" | sed 's/^.*://')
  if [ "$IMAGE_TAG" = "$BASE_IMAGE" ]; then
    IMAGE_TAG="latest"
  fi
  TAG="${REPO}:${IMAGE_TAG}-chrt"
fi

# Dockerfile 路径
DOCKERFILE="scripts/docker/Dockerfile.perl-nice"

# 支持的平台
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"

echo "Building docker image: $TAG for $PLATFORMS"
echo "Base image: $BASE_IMAGE"
echo "Dockerfile: $DOCKERFILE"

# 获取脚本所在目录的父目录（项目根目录）
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# 验证路径
echo "Current directory: $(pwd)"
echo "Dockerfile path: $DOCKERFILE"
if [ ! -f "$DOCKERFILE" ]; then
  echo "ERROR: Dockerfile not found at $DOCKERFILE (from $PROJECT_ROOT)" >&2
  exit 1
fi
echo "Dockerfile found, proceeding with build..."

# 使用 buildx 构建多平台镜像
# 注意：多平台构建时，必须使用 --push 推送到仓库，或者使用 --load 只构建当前平台
if [ "${PUSH:-false}" = "true" ]; then
  echo "Building and pushing multi-platform image..."
  docker buildx build --push \
    --platform="$PLATFORMS" \
    --build-arg BASE_IMAGE="$BASE_IMAGE" \
    -t "$TAG" \
    -f "$DOCKERFILE" \
    .
else
  # 本地测试：只构建当前平台（可以使用 --load）
  CURRENT_PLATFORM=$(docker version --format '{{.Server.Arch}}')
  if [ "$CURRENT_PLATFORM" = "amd64" ]; then
    PLATFORM="linux/amd64"
  elif [ "$CURRENT_PLATFORM" = "arm64" ] || [ "$CURRENT_PLATFORM" = "aarch64" ]; then
    PLATFORM="linux/arm64"
  else
    PLATFORM="linux/amd64"  # 默认
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

