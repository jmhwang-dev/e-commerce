#!/bin/bash
# Bash 스크립트: 전체 데이터 플랫폼을 자동으로 배포하는 스크립트

# 스크립트 실행 중 오류 발생시 즉시 중단
set -e

# Load environment variables if .env exists next to this script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

bash ./deploy_minio.sh
bash ./deploy_spark.sh
