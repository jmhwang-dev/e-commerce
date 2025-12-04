docker compose \
    -f docker-compose.kafka.yml \
    -f docker-compose.metric.yml \
    down -v

sudo rm -r ./data/kafka