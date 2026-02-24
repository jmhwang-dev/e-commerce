docker compose \
    -f docker-compose.spark-master.yml \
    --env-file ./configs/spark/batch/.env.mini_pc \
    up -d --force-recreate