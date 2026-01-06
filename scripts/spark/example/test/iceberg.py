from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bronze to Silver Dedup").getOrCreate()

# 데이터 로딩 및 중복제거
bronze_df = (
    spark.read
        .option("header", "true")
        .csv("s3a://bronze/olist_customers_dataset.csv")
)
dedup_df = bronze_df.dropDuplicates(["customer_id"])

# (1) 테이블을 반드시 CREATE TABLE로 먼저 만듦
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.db.olist_customers (
        customer_id STRING,
        customer_unique_id STRING,
        customer_zip_code_prefix STRING,
        customer_city STRING,
        customer_state STRING
    ) USING iceberg
    PARTITIONED BY (customer_state)
""")

# (2) 그 다음 overwritePartitions()
dedup_df.writeTo("silver.db.olist_customers").overwritePartitions()

spark.sql("SELECT * FROM silver.db.olist_customers LIMIT 5").show()
