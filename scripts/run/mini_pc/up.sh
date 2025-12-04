docker compose \
    -f docker-compose.airflow.yml \
    -f docker-compose.spark-control-plane.yml \
    up -d --force-recreate