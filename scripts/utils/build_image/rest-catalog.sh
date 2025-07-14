DOCKER_BUILDKIT=1 docker build --no-cache \
  -t catalog:ice1.9.1-psql16 \
  --build-arg ICEBERG_VERSION=1.9.1 \
  --build-arg PG_JDBC_VERSION=42.7.3 \
  ./infra/iceberg-rest \
&& docker image prune -f
