docker compose \
    -f docker-compose.storage.yml \
    -f docker-compose.spark-master.yml \
    --env-file ./configs/spark/batch/.env.mini_pc \
    down -v