# -f docker-compose.spark-worker-cdc.yml \
docker compose \
    -f docker-compose.airflow.yml \
    -f docker-compose.inference.yml \
    -f docker-compose.kafka.yml \
    -f docker-compose.metric.yml \
    -f docker-compose.pipeline.yml \
    -f docker-compose.spark-control-plane.yml \
    -f docker-compose.storage.yml \
    -f docker-compose.thrift.yml \
    down -v --remove-orphans
    