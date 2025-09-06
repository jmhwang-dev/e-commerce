#!/bin/bash
set -e

SRC_ZIP="src.zip"

# -d 옵션으로 해당 경로가 디렉터리인지 확인합니다.
if [ -d "$CHECKPOINT_DIR" ]; then
  echo "Checkpoint directory found at '$CHECKPOINT_DIR'. Removing it..."
  sudo rm -r "$CHECKPOINT_DIR"
  sudo rm -f "$SRC_ZIP"
  echo "Directory successfully removed."
else
  echo "Checkpoint directory not found. No action taken."
fi

PYTHON_SCRIPT="${1:-jobs/load.py}"
echo "실행할 파이썬 스크립트: $PYTHON_SCRIPT"

# zip 생성
cd src
SRC_ZIP="src.zip"
zip -r ../$SRC_ZIP service config schema > /dev/null
cd ..
# 컨테이너에 복사
docker cp "$SRC_ZIP" spark-client:/opt/spark/work-dir/$SRC_ZIP

# Spark 실행
docker compose exec spark-client spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.extraJavaOptions="-Daws.region=us-east-1" \
  --conf spark.executor.extraJavaOptions="-Daws.region=us-east-1" \
  --deploy-mode client \
  --py-files /opt/spark/work-dir/$SRC_ZIP \
  "$PYTHON_SCRIPT"