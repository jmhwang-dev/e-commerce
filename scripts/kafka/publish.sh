#!/usr/bin/env bash

COMPOSE_FILE="infra/docker-compose.override.yml"
TOPIC="event-review-received"

docker compose -f "$COMPOSE_FILE" exec kafka2 \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka1:9092 \
    --property schema.registry.url=http://schema-registry:8081 \
    --topic ${TOPIC} \
    --from-beginning --max-messages 1 \
    --timeout-ms 5000

