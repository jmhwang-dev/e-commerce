from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("check")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("DEBUG")

# 테이블 위치 확인
# spark.sql("DESCRIBE EXTENDED nessie.silver.olist_customers").show(200, truncate=False)
# spark.sql("SELECT _file FROM nessie.silver.olist_customers.files").show(50, truncate=False)

df = spark.read.table("nessie.silver.olist_customers")
print(df.inputFiles())
