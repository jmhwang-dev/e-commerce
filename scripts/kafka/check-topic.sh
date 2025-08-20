#!/usr/bin/env bash
set -e

TOPIC=$1
if [ -z "$TOPIC" ]; then
  echo "Usage: $0 <topic_name>"
  exit 1
fi

BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092"
SCHEMA_REGISTRY="http://schema-registry:8081"

echo -e "\n📌 토픽 상세 정보 조회 (파티션, 리더, ISR 등)"
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC"

echo -e "\n📌 현재 존재하는 Kafka 토픽 목록"
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --list

echo -e "\n📌 ${TOPIC} 토픽에 저장된 메시지"
docker compose exec kafka1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka1:9092 \
    --topic $TOPIC \
    --from-beginning
    # --property print.value=true | jq . # message가 json인 경우
    # --max-messages 1 \
