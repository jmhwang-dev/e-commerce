docker compose \
    -f docker-compose.yml \
    -f docker-compose.override.yml \
    -f docker-compose.pipeline.yml \
    -f docker-compose.inference.yml \
    up -d --force-recreate