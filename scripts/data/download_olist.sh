#!/bin/bash

# Kaggle에서 Olist 데이터셋을 내려받습니다.
# 실행 전 Kaggle CLI에 API 자격 정보를 설정해야 합니다.
# 데이터는 저장소 루트의 downloads/ 디렉터리에 저장됩니다.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$REPO_ROOT/downloads/olist" "$REPO_ROOT/downloads/marketing-funnel-olist"

kaggle datasets download -d olistbr/brazilian-ecommerce \
  -p "$REPO_ROOT/downloads/olist" --unzip
kaggle datasets download -d olistbr/marketing-funnel-olist \
  -p "$REPO_ROOT/downloads/marketing-funnel-olist" --unzip

