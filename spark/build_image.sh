# For arm64
sudo apt-get install -y qemu-user-static
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

docker buildx create --name arm64-builder --use
docker buildx inspect --bootstrap

docker buildx build --platform linux/arm64 \
  -t my-spark-hadoop:3.5.5-arm64 \
  --load .

# For amd64
# to check current builder: docker buildx ls
docker buildx use default
docker build -t my-spark-hadoop:3.5.5 .