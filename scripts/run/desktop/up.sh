docker compose \
    -f docker-compose.storage.yml \
    up -d --force-recreate

docker compose \
    -f docker-compose.spark-worker.yml \
    --env-file ./configs/spark/.env.desktop \
    up -d --force-recreate