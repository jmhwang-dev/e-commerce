#!/bin/bash
set -e

PYTHON_SCRIPT=$1

if [ -z "$PYTHON_SCRIPT" ]; then
  echo "Usage: $0 <python_script_path>"
  exit 1
fi

docker compose -f docker-compose.yml exec spark-client \
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  "$PYTHON_SCRIPT"
