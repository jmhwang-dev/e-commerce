#!/usr/bin/env bash
set -e

API_HOST=${API_HOST:-localhost}
API_PORT=${API_PORT:-8003}
URL="http://${API_HOST}:${API_PORT}/consume"

echo "▶ GET $URL"

RESP=$(curl -s -w "\n%{http_code}" "$URL")
BODY=$(echo "$RESP" | head -n1)
CODE=$(echo "$RESP" | tail -n1)

if [[ "$CODE" == "204" ]]; then
  echo "ℹ️  No messages available right now."
elif [[ "$CODE" =~ ^2 ]]; then
  echo "✅  Received message:"
  echo "$BODY" | jq
else
  echo "❌  Error fetching message (HTTP $CODE)" >&2
  echo "$BODY" >&2
  exit 1
fi
