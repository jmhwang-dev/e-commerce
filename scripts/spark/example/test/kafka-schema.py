from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP = "kafka1:9092"
TOPIC = "review"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Spark 세션 생성 (Avro 패키지 포함)
spark = SparkSession.builder \
    .appName("KafkaAvroConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka에서 데이터 읽기
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 스키마 레지스트리에서 스키마 가져오기
sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
schema_str = sr.get_latest_version(f"{TOPIC}-value").schema.schema_str

print(f"Using schema: {schema_str}")

# 방법 1: Schema Registry 옵션 추가 (권장)
decoded = (
    df_raw
      .select(from_avro(col("value"), schema_str, {
          "mode": "PERMISSIVE",
          "columnNameOfCorruptRecord": "_corrupt_record"
        }).alias("data"))
      .select("data.*")
)

# # 방법 2: 만약 위 방법이 안 되면, 직접 바이너리 처리
# decoded = (
#     df_raw
#       .select(
#           from_avro(
#               expr("substring(value, 6, length(value)-5)"), # Magic byte + schema id 제거
#               schema_str
#           ).alias("data")
#       )
#       .select("data.*")
# )

# 디버깅: raw 데이터 확인
print("Raw Kafka data sample:")
df_raw.select("key", "value", "topic", "partition", "offset").writeStream \
    .outputMode("append") \
    .format("console") \
    .option("numRows", 1) \
    .option("truncate", "false") \
    .trigger(once=True) \
    .start() \
    .awaitTermination()

print("\nDecoded Avro data:")
# 콘솔에 출력
query = decoded.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()