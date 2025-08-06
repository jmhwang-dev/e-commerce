#!/usr/bin/env bash
set -e
SCHEMA_FILE="./infra/shared/schemas/review.avsc"
SUBJECT="review-value"
REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

echo "▶ Registering $SUBJECT"

# build JSON payload
cat <<EOF > /tmp/register_payload.json
{
  "schema": $(jq -Rs . < "$SCHEMA_FILE")
}
EOF

curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST "$REGISTRY_URL/subjects/$SUBJECT/versions" \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d @/tmp/register_payload.json | grep -q '^200$' \
  && echo "✅  schema registered"