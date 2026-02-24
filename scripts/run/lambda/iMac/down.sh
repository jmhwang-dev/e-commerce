docker compose \
    -f docker-compose.kafka.yml \
    -f docker-compose.metric.yml \
    -f docker-compose.thrift.yml \
    -f docker-compose.spark-control-plane.yml \
    down -v

docker compose \
    -f docker-compose.airflow.yml \
    --env-file ./configs/airflow/.env.airflow \
    down -v