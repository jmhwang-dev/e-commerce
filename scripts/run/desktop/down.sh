docker compose \
    -f docker-compose.storage.yml \
    -f docker-compose.spark-worker-etl.yml \
    down -v