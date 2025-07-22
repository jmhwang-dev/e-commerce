#!/bin/bash

set -e

BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092"
TOPIC_REVIEW="review"
TOPIC_REVIEW_DLQ="review-dlq"

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$TOPIC_REVIEW" \
    --replication-factor 3 --partitions 6

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$TOPIC_REVIEW_DLQ" \
    --replication-factor 3 --partitions 6