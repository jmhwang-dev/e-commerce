# 1. Docker 컨테이너 실행 (호스트 네트워크 사용)
docker run --rm -it \
  --network host \
  ghcr.io/jmhwang-dev/spark-minio-base:4.0.0 \
  bash

# 2. 환경변수 설정 (Spark 4.0.0부터는 환경변수 방식 권장)
export AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD:-minioadmin123}"

# 3. PySpark 세션 시작 (필요한 JAR 명시)
# hadoop-aws 와 AWS SDK v2 번들 jar 포함
/opt/spark/bin/pyspark \
  --jars "/opt/spark/jars/hadoop-aws-3.4.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.783.jar" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true

# 4. PySpark 세션 안에서 실행
s3a_path = "s3a://olist-data/raw/olist/olist_customers_dataset.csv"
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(s3a_path)
df.show()

