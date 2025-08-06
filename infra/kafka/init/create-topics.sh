#!/bin/bash

set -e

BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092"

TOPIC_REVIEWS="review-value"
TOPIC_PROMPT="reviews.translation-prompts"

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$TOPIC_REVIEWS" \
    --replication-factor 3 --partitions 1