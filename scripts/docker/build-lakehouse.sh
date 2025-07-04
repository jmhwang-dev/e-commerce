DOCKER_BUILDKIT=1 docker build --no-cache \
  -t ecommerce-lakehouse:sp3.5.6-ice1.9.1 \
  --build-arg ICEBERG_VERSION=1.9.1 \
  --build-arg SPARK_VERSION=3.5.6 \
  --build-arg ICEBERG_VERSION=1.9.1 \
  --build-arg ICEBERG_SPARK_RUNTIME_VERSION=3.5_2.12 \
  --build-arg SCALA_BINARY=2.12 \
  --build-arg JAVA_VERSION=17 \
  ./infra/spark \
&& docker image prune -f
