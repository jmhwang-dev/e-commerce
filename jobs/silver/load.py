from service.common.topic import SilverTopic
from service.utils.iceberg import *
from service.utils.spark import *
from service.pipeline.bronze import process_micro_batch
from service.utils.schema.reader import AvscReader

if __name__ == "__main__":
    spark_session = get_spark_session("LoadSilverJob")
    client = SchemaRegistryManager._get_client(use_internal=True)
    target_topic_names = SilverTopic.get_all_topics()
    target_topics_stream_df = get_kafka_stream_df(spark_session, target_topic_names)
    base_stream_df = target_topics_stream_df.select(col("key").cast("string"), col("value"), col("topic"))

    avsc_reader = AvscReader(target_topic_names[0])
    query = base_stream_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .queryName("load_silver_layer") \
        .option("checkpointLocation", f"s3a://{avsc_reader.s3_uri}/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()