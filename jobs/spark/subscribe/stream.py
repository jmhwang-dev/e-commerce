from service.init.kafka import *
from service.init.iceberg import *

from service.io.iceberg import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaAvroConsumer") \
    .getOrCreate()

# Olist 스키마 예시 (JSON 형식 가정)
schema = StructType() \
    .add("review_creation_date", StringType()) \
    .add("review_answer_timestamp", TimestampType()) \
    .add("review_id", StringType()) \
    .add("order_id", StringType()) \
    .add("review_score", IntegerType()) \
    .add("review_comment_title", StringType()) \
    .add("review_comment_message", StringType())


# Kafka에서 실시간 읽기 (Kafka 3.9.1 호환)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
    .option("subscribe", "review") \
    .option("startingOffsets", "earliest") \
    .load()

# 데이터 처리 (JSON 파싱)
processed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
print(processed_df)


DST_QUALIFIED_NAMESPACE = "warehouse_dev.silver.review"
DST_TABLE_NAME = "raw"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

# writer = (
#     processed_df.writeTo(full_table_name)
#     .tableProperty(
#         "comment",
#         "test"
#     )
# )

# if not spark.catalog.tableExists(full_table_name):
#     writer.create()
# else:
#     writer.overwritePartitions()


# Iceberg 테이블에 실시간 쓰기 (append 모드)
query = processed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/olist_stream") \
    .toTable(full_table_name)

query.awaitTermination()