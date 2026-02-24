# docker compose \
#     -f docker-compose.storage.yml \
#     up -d --force-recreate

docker compose \
    -f docker-compose.spark-driver.yml \
    --env-file ./configs/spark/batch/.env.desktop \
    up -d --force-recreate