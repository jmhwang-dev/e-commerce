REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8082}"
SUBJECT="review-value"

curl -s "$REGISTRY_URL/subjects/$SUBJECT/versions/latest" | jq .