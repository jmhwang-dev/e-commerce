docker compose \
    -f docker-compose.kafka.yml \
    -f docker-compose.metric.yml \
    up -d --force-recreate