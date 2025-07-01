docker build -t ecommerce-lakehouse:sp3.5.6-ice1.9.1 ./infra/spark

# echo $GHCR_TOKEN | docker login ghcr.io -u jmhwang-dev --password-stdin

# docker buildx create --name multiarch --use
# docker buildx inspect --bootstrap

# docker buildx build \
#   --platform linux/amd64,linux/arm64 \
#   -t ghcr.io/jmhwang-dev/spark-minio:4.0.0 \
#   --push .
