#!/bin/bash
set -e

PYTHON_SCRIPT=$1

if [ -z "$PYTHON_SCRIPT" ]; then
  echo "Usage: $0 <python_script_path>"
  exit 1
fi

# zip 생성
cd src
SRC_ZIP="src.zip"
zip -r ../$SRC_ZIP service config > /dev/null
cd ..
# 컨테이너에 복사
docker cp "$SRC_ZIP" spark-client:/opt/spark/work-dir/$SRC_ZIP

# Spark 실행
docker compose exec spark-client \
spark-submit \
  --deploy-mode client \
  --py-files /opt/spark/work-dir/$SRC_ZIP \
  "$PYTHON_SCRIPT"
  # --conf spark.driver.extraJavaOptions="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9082 -javaagent:/mnt/jmx_exporter/jmx_prometheus_javaagent-1.3.0.jar=9080:/mnt/configs/jmx/spark_driver.yml" \
  # --conf spark.executor.extraJavaOptions="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9082 -javaagent:/mnt/jmx_exporter/jmx_prometheus_javaagent-1.3.0.jar=9080:/mnt/configs/jmx/spark_executor.yml" \