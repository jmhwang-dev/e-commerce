#!/usr/bin/env bash
set -e

SERVICE="kafka1"                     # compose.yml 의 broker 서비스명
BOOTSTRAP="kafka1:9092"              # 컨테이너 내부 접근 (호스트면 localhost:9092)
TOPIC="event-review-received"

echo "▶ creating topic: ${TOPIC}"
docker compose -f infra/docker-compose.override.yml exec "$SERVICE" \
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic ${TOPIC} \
    --partitions 3 \
    --replication-factor 1
