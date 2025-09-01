docker compose \
    -f docker-compose.yml \
    -f docker-compose.override.yml \
    -f docker-compose.bronze.yml \
    down -v --remove-orphans