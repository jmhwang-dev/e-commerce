docker compose \
    -f docker-compose.kafka.yml \
    -f docker-compose.metric.yml \
    up -d --force-recreate

docker compose \
    -f docker-compose.spark-control-plane.yml \
    up -d --force-recreate

docker compose \
    -f docker-compose.airflow.yml \
    --env-file ./configs/airflow/.env.airflow \
    up -d --force-recreate