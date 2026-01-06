DOCKER_BUILDKIT=1 docker build --no-cache \
  -t warehouse:sp3.5.6-ice1.9.1 \
  ./infra/spark \
&& docker image prune -f
