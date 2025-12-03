#!/bin/bash
set -e


SRC_ZIP="src.zip"
if [ -f "$SRC_ZIP" ]; then
  rm -f "$SRC_ZIP"
fi

PYTHON_SCRIPT="${1:-jobs/batch_test.py}"
echo "실행할 파이썬 스크립트: $PYTHON_SCRIPT"

# zip 생성
cd src
SRC_ZIP="src.zip"
zip -r ../$SRC_ZIP service config schema > /dev/null
cd ..
# 컨테이너에 복사
docker cp "$SRC_ZIP" spark-client:/opt/spark/work-dir/$SRC_ZIP
  
# Spark 실행: -T 옵션을 추가하여 TTY 할당 비활성화
docker compose -f docker-compose.spark-control-plane.yml exec spark-client spark-submit \
  --master spark://192.168.45.192:7078 \
  --conf spark.driver.extraJavaOptions="-Daws.region=us-east-1" \
  --conf spark.executor.extraJavaOptions="-Daws.region=us-east-1" \
  --deploy-mode client \
  --py-files /opt/spark/work-dir/$SRC_ZIP \
  "$PYTHON_SCRIPT"