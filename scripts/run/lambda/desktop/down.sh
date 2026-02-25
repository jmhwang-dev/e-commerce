docker compose \
    -f docker-compose.spark-worker.yml \
    --env-file ./configs/spark/.env.desktop \
    down -v