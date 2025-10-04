from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from service.utils.spark import get_spark_session
from service.stream.topic import BronzeTopic
from service.utils.schema.reader import AvscReader
from service.utils.spark import *
from service.producer.bronze import *

def load_kafka_topic(spark: SparkSession, topic_name: str, key_column: str) -> DataFrame:
    topic_stream_df: DataFrame = get_kafka_stream_df(spark, topic_name)
    avsc_reader = AvscReader(topic_name)
    return get_deserialized_stream_df(topic_stream_df, avsc_reader.schema_str, key_column)

if __name__ == "__main__":
    # SparkSession 생성
    spark: SparkSession = get_spark_session(app_name='stream')

    # 네임스페이스 생성
    silver_namespace: str = 'warehousedev.silver'
    gold_namespace: str = 'warehousedev.gold'
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {silver_namespace}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gold_namespace}")

    order_status_df: DataFrame = load_kafka_topic(spark, BronzeTopic.ORDER_STATUS, OrderStatusBronzeProducer.key_column)
    query = start_console_stream(order_status_df)

    
    spark.streams.awaitAnyTermination()