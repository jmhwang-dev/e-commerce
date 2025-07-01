# jobs/simple_test.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinIOExample") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

try:
    df = spark.read.csv("s3a://bronze/olist_customers_dataset.csv", header=True)
    df.show()
    print("✅ 읽기 성공!")
except Exception as e:
    print(f"❌ 오류: {e}")
finally:
    spark.stop()
