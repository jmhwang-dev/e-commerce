#!/bin/bash
set -e

rm -rf "data/minio/tmp/*"
# 대상 베이스 디렉터리 (silver / gold)
BASE_PATHS=(
  "data/minio/warehousedev/silver"
  "data/minio/warehousedev/gold"
)

for CHECKPOINT_BASE in "${BASE_PATHS[@]}"; do
  echo "=== Checking $CHECKPOINT_BASE ==="

  # */checkpoint 형태의 디렉터리 전부 찾기
  CHECKPOINT_DIRS=$(find "$CHECKPOINT_BASE" -type d -path "*/checkpoint" 2>/dev/null || true)

  if [ -n "$CHECKPOINT_DIRS" ]; then
    echo "Found checkpoint directories:"
    printf '%s\n' "$CHECKPOINT_DIRS"

    while IFS= read -r dir; do
      echo "Removing checkpoint directory: $dir"
      if rm -rf "$dir"; then
        echo "Successfully removed: $dir"
      else
        echo "Failed to remove: $dir"
      fi
    done <<< "$CHECKPOINT_DIRS"
  else
    echo "No checkpoint directories found in $CHECKPOINT_BASE/*/checkpoint"
  fi
  echo
done

echo "All checkpoint clean-up finished."

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
  --master spark://spark-master:7077 \
  --conf spark.driver.extraJavaOptions="-Daws.region=us-east-1 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9082 -javaagent:/mnt/jmx_exporter/jmx_prometheus_javaagent-1.3.0.jar=9080:/mnt/configs/jmx/spark_driver.yml" \
  --conf spark.executor.extraJavaOptions="-Daws.region=us-east-1 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9082 -javaagent:/mnt/jmx_exporter/jmx_prometheus_javaagent-1.3.0.jar=9080:/mnt/configs/jmx/spark_executor.yml" \
  --deploy-mode client \
  --py-files /opt/spark/work-dir/$SRC_ZIP \
  "$PYTHON_SCRIPT"