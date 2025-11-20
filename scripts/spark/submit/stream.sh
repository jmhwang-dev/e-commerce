#!/bin/bash
set -e

# MinIO 임시 디렉토리 삭제
rm -rf "data/minio/tmp/*" 2>/dev/null || true

# 체크포인트 기본 경로
BASE_PATHS=("data/minio/warehousedev/stream/dev")

for CHECKPOINT_BASE in "${BASE_PATHS[@]}"; do
  echo "=== Cleaning checkpoints in $CHECKPOINT_BASE ==="
  
  # 체크포인트 디렉토리 검색 및 삭제
  if CHECKPOINT_DIRS=$(find "$CHECKPOINT_BASE" -type d -path "*" 2>/dev/null); then
    if [ -n "$CHECKPOINT_DIRS" ]; then
      echo "Found checkpoint directories, removing..."
      # find로 직접 삭제, 성공/실패 로깅 간소화
      find "$CHECKPOINT_BASE" -type d -path "*" -exec rm -rf {} \; 2>/dev/null && \
        echo "All checkpoint directories removed successfully" || \
        echo "Failed to remove some checkpoint directories"
    else
      echo "No checkpoint directories found"
    fi
  else
    echo "Error accessing $CHECKPOINT_BASE"
  fi
  echo
done

echo "Checkpoint cleanup completed."

SRC_ZIP="src.zip"
if [ -f "$SRC_ZIP" ]; then
  rm -f "$SRC_ZIP"
fi

PYTHON_SCRIPT="${1:-jobs/stream.py}"
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
  --master spark://192.168.45.191:7077 \
  --conf spark.driver.extraJavaOptions="-Daws.region=us-east-1 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9082 -javaagent:/mnt/jmx_exporter/jmx_prometheus_javaagent-1.3.0.jar=9080:/mnt/configs/jmx/spark_driver.yml" \
  --conf spark.executor.extraJavaOptions="-Daws.region=us-east-1 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9082 -javaagent:/mnt/jmx_exporter/jmx_prometheus_javaagent-1.3.0.jar=9080:/mnt/configs/jmx/spark_executor.yml" \
  --deploy-mode client \
  --py-files /opt/spark/work-dir/$SRC_ZIP \
  "$PYTHON_SCRIPT"