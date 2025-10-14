#!/bin/bash
set -e

CHECKPOINT_DIR="data/minio/warehousedev/bronze/checkpoints"
if [ -d "$CHECKPOINT_DIR" ]; then
  echo "Checkpoint directory found at '$CHECKPOINT_DIR'. Removing it..."
  sudo rm -r "$CHECKPOINT_DIR"
  echo "Directory successfully removed."
else
  echo "Checkpoint directory not found. No action taken."
fi

SRC_ZIP="src.zip"
if [ -f "$SRC_ZIP" ]; then
  sudo rm -f "$SRC_ZIP"
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