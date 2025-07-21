# curl -X POST http://localhost:8000/produce/sample

#!/usr/bin/env bash
# scripts/test_publish_sample.sh
set -e

API_HOST=${API_HOST:-localhost}
API_PORT=${API_PORT:-8000}

echo "▶ POST  http://${API_HOST}:${API_PORT}/produce/sample"
curl -X POST "http://${API_HOST}:${API_PORT}/produce/sample" \
     -H "Content-Type: application/json"

echo -e "\n✅  sample message queued"
