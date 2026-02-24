docker compose \
    -f docker-compose.spark-worker.yml \
    --env-file ./configs/spark/batch/.env.pi1 \
    up -d --force-recreate