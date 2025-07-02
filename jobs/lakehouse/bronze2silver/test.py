from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# 테이블 생성
spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.default.products (
  id INT,
  name STRING
) USING iceberg
""")

# 데이터 삽입
spark.sql("""
INSERT INTO nessie.default.products VALUES (1, 'apple'), (2, 'banana')
""")

# 데이터 조회
spark.sql("SELECT * FROM nessie.default.products").show()


# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .appName("Bronze to Silver Dedup")
#     .getOrCreate()
# )

# bronze_df = (
#     spark.read.option("header", "true")
#               .csv("s3a://bronze/olist_customers_dataset.csv")
# )
# dedup_df = bronze_df.dropDuplicates(["customer_id"])

# spark.sql("""
#     CREATE TABLE IF NOT EXISTS nessie.silver.olist_customers (
#         customer_id          STRING,
#         customer_unique_id   STRING,
#         customer_zip_code_prefix STRING,
#         customer_city        STRING,
#         customer_state       STRING
#     ) USING iceberg
#     PARTITIONED BY (customer_state)
# """)

# dedup_df.writeTo("nessie.silver.olist_customers").overwritePartitions()
