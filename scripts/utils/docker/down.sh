docker compose \
    -f docker-compose.yml \
    -f docker-compose.override.yml \
    -f docker-compose.pipeline.yml \
    -f docker-compose.inference.yml \
    -f docker-compose.thrift.yml \
    down -v --remove-orphans