from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinIO CSV Reader") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# CSV 파일들 읽기
df = spark.read.csv("s3a://bronze/*.csv", header=True, inferSchema=True)