#!/bin/bash
# Deploy both MinIO and Spark components.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

bash ./deploy_minio.sh
bash ./deploy_spark.sh

