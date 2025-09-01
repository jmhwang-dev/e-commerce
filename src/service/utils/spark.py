from typing import Iterable

from config.kafka import *

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr

from config.spark import *
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

def get_spark_session(app_name: str) -> SparkSession:
    """
    confs

    - spark.conf.set("spark.sql.shuffle.partitions", "50")
        : 셔플 작업의 파티션 수. 스트리밍 병렬 처리 성능에 영향.
    
    - spark.conf.set("spark.sql.streaming.commitIntervalMs", "5000")
        : Kafka 오프셋 커밋 주기(ms). 빠른 커밋 vs 안정성 조절.

    - spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "268435456") // 256MB
        : minIO에 업로드 시 멀티파트 크기. 큰 데이터 전송 성능 최적화.

    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def start_console_stream(decoded_stream_df: DataFrame) -> StreamingQuery:
    return decoded_stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

def get_kafka_stream_df(spark_session: SparkSession, topic_names: Iterable[str]) -> DataFrame:
    """
    - .option("maxOffsetsPerTrigger", "10000")
        : 배치당 처리할 Kafka 오프셋 최대 수. 처리량 제어.
    """
    # The type of `spark_session.readStream` is `DataStreamReader`
    # .option("maxOffsetsPerTrigger", "20000")  # 배치당 최대 오프셋, 조정 필요
    # 추가: spark.streaming.kafka.maxRatePerPartition (파티션당 초당 메시지 수 제한, 예: 1000) 설정으로 입력 속도 제어.
    # .option("groupId", "bronze2silver") \
    return spark_session.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
        .option("subscribe", ','.join(topic_names)) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "20000") \
        .load()

def get_decoded_stream_df(kafka_stream_df: DataFrame, schema_str) -> DataFrame:
    deserialized_column = from_avro(
              expr("substring(value, 6, length(value)-5)"), # Magic byte + schema id 제거
              schema_str,
              DESERIALIZE_OPTIONS
          ).alias("data")

    return kafka_stream_df.select(deserialized_column).select("data.*")

