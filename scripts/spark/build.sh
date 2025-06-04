#!/bin/bash
set -e

# Load only GHCR token from .env
if [ -f .env ]; then
  GHCR_TOKEN=$(grep '^ghcr=' .env | cut -d '=' -f2-)
else
  echo ".env file not found. Please create one with ghcr=<TOKEN> defined."
  exit 1
fi

if [ -z "$GHCR_TOKEN" ]; then
  echo "GHCR token not found in .env (expected format: ghcr=...)."
  exit 1
fi

# Constants
TAG="4.0.0"
REPO="ghcr.io/jmhwang-dev"
PLATFORMS="linux/amd64,linux/arm64"

# Login to GitHub Container Registry
echo "Logging into GHCR..."
echo "$GHCR_TOKEN" | docker login ghcr.io -u jmhwang-dev --password-stdin

# Ensure buildx builder exists
if ! docker buildx inspect multiarch > /dev/null 2>&1; then
  echo "Creating buildx builder 'multiarch'..."
  docker buildx create --name multiarch --use
fi

docker buildx inspect --bootstrap

# Step 1: Build base image
echo "Building base image: spark-minio-base:${TAG}"
docker buildx build \
  --platform ${PLATFORMS} \
  -t ${REPO}/spark-minio-base:${TAG} \
  -f spark/image/Dockerfile.base \
  --push .

# Step 2: Build dev image
echo "Building dev image: spark-minio-jupyter-dev:${TAG}"
docker buildx build \
  --platform ${PLATFORMS} \
  -t ${REPO}/spark-minio-jupyter-dev:${TAG} \
  -f spark/image/Dockerfile.dev \
  --push .

# Step 3: Build prod image
echo "Building prod image: spark-minio-jupyter-prod:${TAG}"
docker buildx build \
  --platform ${PLATFORMS} \
  -t ${REPO}/spark-minio-jupyter-prod:${TAG} \
  -f spark/image/Dockerfile.prod \
  --push .

echo "All images built and pushed successfully."