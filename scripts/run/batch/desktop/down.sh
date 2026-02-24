# docker compose \
#     -f docker-compose.storage.yml \
#     down -v
    
docker compose \
    -f docker-compose.spark-driver.yml \
    --env-file ./configs/spark/batch/.env.desktop \
    down -v