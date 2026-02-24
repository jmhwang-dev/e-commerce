docker compose \
    -f docker-compose.spark-worker.yml \
    --env-file ./configs/spark/batch/.env.pi2 \
    down -v