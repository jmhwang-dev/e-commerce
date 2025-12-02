docker compose \
    -f docker-compose.kafka.yml \
    -f docker-compose.spark-control-plane.yml \
    up -d --force-recreate