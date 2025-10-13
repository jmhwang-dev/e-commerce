#!/bin/bash
set -e

SRC_ZIP="src.zip"
sudo rm -f "$SRC_ZIP"
sudo rm -rf data/minio/warehousedev/checkpoint

CHECKPOINT_DIR="./data/minio/warehousedev/silver/checkpoints"
# -d 옵션으로 해당 경로가 디렉터리인지 확인합니다.
if [ -d "$CHECKPOINT_DIR" ]; then
  echo "Checkpoint directory found at '$CHECKPOINT_DIR'. Removing it..."
  sudo rm -r "$CHECKPOINT_DIR"
  echo "Directory successfully removed."
else
  echo "Checkpoint directory not found. No action taken."
fi

PYTHON_SCRIPT="${1:-jobs/batch_test.py}"
echo "실행할 파이썬 스크립트: $PYTHON_SCRIPT"

# zip 생성
cd src
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