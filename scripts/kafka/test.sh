#!/usr/bin/env bash

# Kafka 테스트 스크립트
# 사용법: chmod +x kafka_test_commands.sh && ./kafka_test_commands.sh

COMPOSE_FILE="infra/kafka/docker-compose.yml"
BOOTSTRAP="kafka1:9092"
CONTROLLER="kafka1:9093"
TOPIC="test-topic"
GROUP="my-test-group"

# 1. kafka1: 브로커 API 및 토픽 관리
echo -e "\n1. 브로커 API 버전 확인"
docker compose -f "$COMPOSE_FILE" exec kafka1 \
    /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$BOOTSTRAP"

echo -e "\n2. 컨트롤러 쿼럼 상태 확인"
docker compose -f "$COMPOSE_FILE" exec kafka1 \
    /opt/kafka/bin/kafka-metadata-quorum.sh \
    --bootstrap-controller "$CONTROLLER" describe \
    --status

echo -e "\n3. 테스트 토픽 생성"
docker compose -f "$COMPOSE_FILE" exec kafka1 \
    /opt/kafka/bin/kafka-topics.sh \
    --create \
    --bootstrap-server "$BOOTSTRAP" \
    --replication-factor 3 \
    --partitions 1 \
    --topic "$TOPIC" || echo "토픽이 이미 존재하거나 생성 오류"

echo -e "\n4. 토픽 목록 조회"
docker compose -f "$COMPOSE_FILE" exec kafka1 \
    /opt/kafka/bin/kafka-topics.sh \
    --list \
    --bootstrap-server "$BOOTSTRAP"

echo -e "\n5. 토픽 상세 정보 조회"
docker compose -f "$COMPOSE_FILE" exec kafka1 \
    /opt/kafka/bin/kafka-topics.sh \
    --describe \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC"

# 2. 메시지 송신 (kafka1에서 실행, STDIN 파이프 사용)
# -T 옵션은 TTY 없이 STDIN 파이프를 동작시키기 위해 사용
echo -e "\n6. 메시지 송신: Hello from script!"
docker compose -f "$COMPOSE_FILE" exec -T kafka1 bash -c "\
echo 'Hello from script!' | \
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC""

# 3. 메시지 수신 및 로그 확인 (kafka2에서 실행)
echo -e "\n7. 메시지 수신 (콘솔 소비자)"
docker compose -f "$COMPOSE_FILE" exec kafka2 \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --from-beginning \
    --group "$GROUP" \
    --timeout-ms 5000

echo -e "\n8. 오프셋 로그 덤프 (토픽 경로는 환경에 맞게 조정)"
docker compose -f "$COMPOSE_FILE" exec kafka2 \
    /opt/kafka/bin/kafka-dump-log.sh \
    --files /var/lib/kafka/data/__consumer_offsets-0/00000000000000000000.log \
    --print-data-log \
    --deep-iteration

# 4. 소비자 그룹 관리 확인
echo -e "\n9. 소비자 그룹 목록 조회"
docker compose -f "$COMPOSE_FILE" exec kafka2 \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --list

echo -e "\n10. 소비자 그룹 상세 정보 조회"
docker compose -f "$COMPOSE_FILE" exec kafka2 \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --group "$GROUP"
