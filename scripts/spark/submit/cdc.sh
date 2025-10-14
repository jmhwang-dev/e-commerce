#!/bin/bash
set -e

BRONZE_LAYER_DIR="data/minio/warehousedev/cdc/checkpoints"
if [ -d "$BRONZE_LAYER_DIR" ]; then
  # 테스트 실행을 위해 이전 체크포인트 내용을 모두 삭제합니다.
  echo "Checkpoint directory found. Removing all contents within '$BRONZE_LAYER_DIR'..."
  # `find` 명령어를 사용하여 하위 모든 파일과 디렉토리를 안전하게 삭제합니다.
  sudo find "$BRONZE_LAYER_DIR" -mindepth 1 -delete
  echo "Contents successfully removed."
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