docker compose \
    -f docker-compose.inference.yml \
    -f docker-compose.storage.yml \
    up -d --force-recreate