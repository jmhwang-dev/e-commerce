from service.init.kafka import *
from service.init.spark import *

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import DataStreamReader, StreamingQuery
from pyspark.sql.functions import col, expr

from config.spark import *

def get_kafka_stream_df(spark_session: SparkSession, topic_name: str) -> DataFrame:
    """
    - .option("maxOffsetsPerTrigger", "10000")
        : 배치당 처리할 Kafka 오프셋 최대 수. 처리량 제어.

    """
    # The type of `spark_session.readStream` is `DataStreamReader`
    return spark_session.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

def get_decoded_stream_df(kafka_stream_df: DataFrame, schema_str) -> DataFrame:
    deserialized_column = from_avro(
              expr("substring(value, 6, length(value)-5)"), # Magic byte + schema id 제거
              schema_str,
              DESERIALIZE_OPTIONS
          ).alias("data")

    return kafka_stream_df.select(deserialized_column).select("data.*")

def start_console_stream(decoded_stream_df: DataFrame) -> StreamingQuery:
    return decoded_stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()