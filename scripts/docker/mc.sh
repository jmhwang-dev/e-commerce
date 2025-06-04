#!/bin/bash

# Run the MinIO Client with credentials from the environment.
# The script assumes MinIO is exposed on NodePort 30900.
docker run --rm -it \
  --network host \
  -v "$HOME/.mc:/root/.mc" \
  minio/mc \
  alias set localminio http://localhost:30900 "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin123}"
