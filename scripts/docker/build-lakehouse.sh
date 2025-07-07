DOCKER_BUILDKIT=1 docker build --no-cache \
  -t lakehouse:sp3.5.6-ice1.9.1 \
  --build-arg ICEBERG_VERSION=1.9.1 \
  --build-arg SPARK_VERSION=3.5.6 \
  --build-arg ICEBERG_VERSION=1.9.1 \
  --build-arg ICEBERG_SPARK_RUNTIME_VERSION=3.5_2.12 \
  --build-arg SCALA_BINARY=2.12 \
  --build-arg JAVA_VERSION=17 \
  --build-arg HADOOP_AWS_VERSION=3.3.4 \
  --build-arg AWS_SDK_VERSION=1.12.262 \
  ./infra/spark \
&& docker image prune -f
