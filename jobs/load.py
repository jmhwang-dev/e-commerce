from pyspark.sql.functions import col
from pyspark.sql import DataFrame

from service.common.topic import BronzeTopic, SilverTopic
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.utils.schema.reader import AvscReader
from service.utils.spark import get_spark_session, get_kafka_stream_df
from service.pipeline.load import load_medallion_layer

TARGET_TOPIC_NAMES = BronzeTopic.get_all_topics() + SilverTopic.get_all_topics()

def process_micro_batch(micro_batch_df: DataFrame, batch_id: int):
    load_medallion_layer(TARGET_TOPIC_NAMES, micro_batch_df, batch_id)

if __name__ == "__main__":
    spark_session = get_spark_session("LoadAllTopicsJob")
    client = SchemaRegistryManager._get_client(use_internal=True)
    target_topics_stream_df = get_kafka_stream_df(spark_session, TARGET_TOPIC_NAMES)
    base_stream_df = target_topics_stream_df.select(col("key").cast("string"), col("value"), col("topic"))

    avsc_reader = AvscReader(TARGET_TOPIC_NAMES[0])
    query = base_stream_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .queryName("load_all_topics") \
        .option("checkpointLocation", f"s3a://{avsc_reader.s3_uri}/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()