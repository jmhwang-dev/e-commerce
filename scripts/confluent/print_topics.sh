COMPOSE_FILE="docker-compose.override.yml"
TOPIC="review"
SCHEMA_REGISTRY="http://schema-registry:8081"

echo -e "\n📌 토픽 '$TOPIC' 에 들어간 메시지 (최대 5건)"
docker compose -f "$COMPOSE_FILE" exec schema-registry \
  kafka-avro-console-consumer \
  --bootstrap-server kafka1:9092 \
  --topic "$TOPIC" \
  --property schema.registry.url=$SCHEMA_REGISTRY \
  --from-beginning --max-messages 5 2>&1 | grep -E '^\{.*\}$'
