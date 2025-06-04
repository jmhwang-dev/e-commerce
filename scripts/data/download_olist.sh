#!/bin/bash

# Download Olist datasets from Kaggle.
# Requires Kaggle CLI configured with your API credentials.
# Data will be stored under the repository root in the downloads/ directory.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$REPO_ROOT/downloads/olist" "$REPO_ROOT/downloads/marketing-funnel-olist"

kaggle datasets download -d olistbr/brazilian-ecommerce \
  -p "$REPO_ROOT/downloads/olist" --unzip
kaggle datasets download -d olistbr/marketing-funnel-olist \
  -p "$REPO_ROOT/downloads/marketing-funnel-olist" --unzip

