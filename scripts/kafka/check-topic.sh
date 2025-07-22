#!/usr/bin/env bash

COMPOSE_FILE="infra/docker-compose.override.yml"
TOPIC="review"
BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092"
SCHEMA_REGISTRY="http://schema-registry:8081"

echo -e "\n토픽 상세 정보 조회"
docker compose -f "$COMPOSE_FILE" exec kafka1 \
    /opt/kafka/bin/kafka-topics.sh \
    --describe \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC"

docker compose -f "$COMPOSE_FILE" exec kafka1 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list