docker compose \
    -f docker-compose.storage.yml \
    down -v
    
docker compose \
    -f docker-compose.spark-worker.yml \
    down -v

sudo rm -r ./data/minio ./data/iceberg