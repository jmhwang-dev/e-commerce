docker compose \
    -f docker-compose.spark-worker.yml \
    --env-file ./configs/spark/.env.mini_pc \
    up -d --force-recreate