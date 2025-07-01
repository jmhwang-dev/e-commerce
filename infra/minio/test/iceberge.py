from pyspark.sql import SparkSession

# Spark 세션 초기화
spark = SparkSession.builder \
    .appName("LoadOlistCustomers") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://my-bucket/warehouse/") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Iceberg 테이블 생성
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.olist_customers (
        customer_id STRING,
        customer_unique_id STRING,
        customer_zip_code_prefix STRING,
        customer_city STRING,
        customer_state STRING
    ) USING iceberg
    PARTITIONED BY (customer_state)
""")

# MinIO에서 CSV 읽기 및 Iceberg 테이블에 삽입
input_path = "s3a://bronze/olist_customers_dataset.csv"
df = spark.read.option("header", "true").csv(input_path)
df.writeTo("local.db.olist_customers").append()

# 데이터 조회
spark.sql("SELECT * FROM local.db.olist_customers LIMIT 5").show()

# 결과 저장 (Parquet 형식)
output_path = "s3a://my-bucket/output/olist_customers_processed"
df.write.mode("overwrite").parquet(output_path)

# Spark 세션 종료
spark.stop()
