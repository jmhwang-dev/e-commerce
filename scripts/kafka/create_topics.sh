#!/usr/bin/env bash
set -e

TOPIC="event-review-received"
BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092"
COMPOSE_FILE="infra/docker-compose.override.yml"

echo "▶ creating topic: ${TOPIC}"

docker compose -f $COMPOSE_FILE exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic ${TOPIC} \
  --partitions 3 \
  --replication-factor 1