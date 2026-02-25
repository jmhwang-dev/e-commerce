docker compose \
    -f docker-compose.storage.yml \
    down -v
    
docker compose \
    -f docker-compose.spark-worker.yml \
    --env-file ./configs/spark/.env.desktop \
    down -v