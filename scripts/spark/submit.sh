#!/bin/bash

# 첫 번째 인자를 PYTHON_SCRIPT 변수로 받음
PYTHON_SCRIPT=$1

# 인자가 비어 있는 경우 에러 메시지
if [ -z "$PYTHON_SCRIPT" ]; then
  echo "Usage: $0 <python_script_path>"
  exit 1
fi

# Spark 실행
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  "$PYTHON_SCRIPT"
