#!/usr/bin/env bash
set -e

API_HOST=${API_HOST:-localhost}
API_PORT=${API_PORT:-8000}
URL="http://${API_HOST}:${API_PORT}/produce/sample"

echo "▶ POST $URL"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL" -H "Content-Type: application/json")

if [[ "$STATUS" =~ ^2 ]]; then
  echo -e "✅  sample message queued"
else
  echo -e "❌  failed to queue sample message (HTTP $STATUS)" >&2
  exit 1
fi
