#!/bin/bash

REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8082}"

# 모든 subject 목록 가져오기
subjects=$(curl -s "$REGISTRY_URL/subjects" | jq -r '.[]')

# 각 subject에 대해 latest 스키마 출력
for subject in $subjects; do
  echo "Subject: $subject"
  curl -s "$REGISTRY_URL/subjects/$subject/versions/latest" | jq .
  echo -e "\n"
done