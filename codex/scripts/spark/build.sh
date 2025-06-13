#!/bin/bash
set -e

# Spark 컨테이너 이미지를 빌드하여 GHCR로 푸시합니다.
# `.env` 파일에 다음 변수를 포함해야 합니다:
#   GHCR_TOKEN=<TOKEN>

# 스크립트 위치와 관계없이 동작하도록 .env 경로를 계산합니다.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../.env"

if [ -f "$ENV_FILE" ]; then
  set -a
  source "$ENV_FILE"
  set +a
else
  echo ".env file not found. Please create one with GHCR_TOKEN defined."
  exit 1
fi

if [ -z "$GHCR_TOKEN" ]; then
  echo "GHCR_TOKEN not found in .env."
  exit 1
fi

TAG="4.0.0"
REPO="ghcr.io/jmhwang-dev"
PLATFORMS="linux/amd64,linux/arm64"

# GitHub Container Registry 로그인
echo "GHCR에 로그인합니다..."
echo "$GHCR_TOKEN" | docker login ghcr.io -u jmhwang-dev --password-stdin

# buildx 빌더가 존재하는지 확인
if ! docker buildx inspect multiarch > /dev/null 2>&1; then
  docker buildx create --name multiarch --use
fi

docker buildx inspect --bootstrap

# 베이스 이미지 빌드
docker buildx build \
  --platform $PLATFORMS \
  -t $REPO/spark-minio-base:$TAG \
  -f spark/image/Dockerfile.base \
  --push .

# 개발용 이미지 빌드
docker buildx build \
  --platform $PLATFORMS \
  -t $REPO/spark-minio-jupyter-dev:$TAG \
  -f spark/image/Dockerfile.dev \
  --push .

# 운영용 이미지 빌드
docker buildx build \
  --platform $PLATFORMS \
  -t $REPO/spark-minio-jupyter-prod:$TAG \
  -f spark/image/Dockerfile.prod \
  --push .

echo "모든 이미지를 성공적으로 빌드하고 푸시했습니다."

