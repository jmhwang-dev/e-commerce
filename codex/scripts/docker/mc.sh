#!/bin/bash

# 환경변수에 설정된 자격 정보로 MinIO 클라이언트를 실행합니다.
# 이 스크립트는 MinIO가 NodePort 30900으로 노출되어 있다고 가정합니다.
docker run --rm -it \
  --network host \
  -v "$HOME/.mc:/root/.mc" \
  minio/mc \
  alias set localminio http://localhost:30900 "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin123}"
