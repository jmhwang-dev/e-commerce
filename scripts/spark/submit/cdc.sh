#!/bin/bash
set -e

CHECKPOINT_BASE="data/minio/warehousedev/bronze"
CHECKPOINT_DIRS=$(find "$CHECKPOINT_BASE" -type d -path "*/checkpoint" 2>/dev/null || true)

if [ -n "$CHECKPOINT_DIRS" ]; then
  echo "Found checkpoint directories:"
  echo "$CHECKPOINT_DIRS"
  while IFS= read -r dir; do
    echo "Removing checkpoint directory: $dir"
    rm -rf "$dir" && echo "Successfully removed: $dir" || echo "Failed to remove: $dir"
  done <<< "$CHECKPOINT_DIRS"
else
  echo "No checkpoint directories found in $CHECKPOINT_BASE/*/checkpoint"
fi

SRC_ZIP="src.zip"
if [ -f "$SRC_ZIP" ]; then
  rm -f "$SRC_ZIP"
fi

PYTHON_SCRIPT="${1:-jobs/cdc.py}"
echo "실행할 파이썬 스크립트: $PYTHON_SCRIPT"

# zip 생성
cd src
SRC_ZIP="src.zip"
zip -r ../$SRC_ZIP service config schema > /dev/null
cd ..
# 컨테이너에 복사
docker cp "$SRC_ZIP" spark-client:/opt/spark/work-dir/$SRC_ZIP
  
# Spark 실행: -T 옵션을 추가하여 TTY 할당 비활성화
docker compose exec spark-client spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.extraJavaOptions="-Daws.region=us-east-1" \
  --conf spark.executor.extraJavaOptions="-Daws.region=us-east-1" \
  --deploy-mode client \
  --py-files /opt/spark/work-dir/$SRC_ZIP \
  "$PYTHON_SCRIPT"