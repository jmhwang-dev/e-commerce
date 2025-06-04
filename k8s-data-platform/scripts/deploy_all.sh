#!/bin/bash
# Bash 스크립트: 전체 데이터 플랫폼을 자동으로 배포하는 스크립트

# 스크립트 실행 중 오류 발생시 즉시 중단
set -e

bash ./deploy_minio.sh
bash ./deploy_spark.sh