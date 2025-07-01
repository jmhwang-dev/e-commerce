# default: --deploy-mode client
docker exec infra-spark-client-1 spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  test/iceberg.py
