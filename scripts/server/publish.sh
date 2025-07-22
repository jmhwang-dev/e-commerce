set -e

API_HOST=${API_HOST:-localhost}
API_PORT=${API_PORT:-8000}

if curl -s -o /dev/null -w "%{http_code}" -X POST \
  "http://${API_HOST}:${API_PORT}/produce/sample" \
  -H "Content-Type: application/json" | grep -q '^2'; then
  echo -e "✅  sample message queued"
else
  echo -e "❌  failed to queue sample message" >&2
  exit 1
fi