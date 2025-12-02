docker compose \
    -f docker-compose.airflow.yml \
    -f docker-compose.metric.yml \
    up -d --force-recreate