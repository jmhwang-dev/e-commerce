# login
echo <token> | docker login ghcr.io -u jmhwang-dev --password-stdin

# ghcr 용 태그 생성
docker tag my-spark-hadoop:3.5.5-arm64 ghcr.io/jmhwang-dev/spark-hadoop:3.5.5-arm64
docker tag my-spark-hadoop:3.5.5-amd64 ghcr.io/jmhwang-dev/spark-hadoop:3.5.5-amd64

# push
docker push ghcr.io/jmhwang-dev/spark-hadoop:3.5.5-arm64
docker push ghcr.io/jmhwang-dev/spark-hadoop:3.5.5-amd64

# 실패시
docker logout ghcr.io