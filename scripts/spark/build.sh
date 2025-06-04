#!/bin/bash
set -e

# Build Spark container images and push to GHCR.
# Requires an .env file containing:
#   GHCR_TOKEN=<TOKEN>

# Resolve .env relative to this script so it works from any directory
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

# Login to GitHub Container Registry
echo "Logging into GHCR..."
echo "$GHCR_TOKEN" | docker login ghcr.io -u jmhwang-dev --password-stdin

# Ensure buildx builder exists
if ! docker buildx inspect multiarch > /dev/null 2>&1; then
  docker buildx create --name multiarch --use
fi

docker buildx inspect --bootstrap

# Build base image
docker buildx build \
  --platform $PLATFORMS \
  -t $REPO/spark-minio-base:$TAG \
  -f spark/image/Dockerfile.base \
  --push .

# Build dev image
docker buildx build \
  --platform $PLATFORMS \
  -t $REPO/spark-minio-jupyter-dev:$TAG \
  -f spark/image/Dockerfile.dev \
  --push .

# Build prod image
docker buildx build \
  --platform $PLATFORMS \
  -t $REPO/spark-minio-jupyter-prod:$TAG \
  -f spark/image/Dockerfile.prod \
  --push .

echo "All images built and pushed successfully."

