from service.common.topic import BronzeTopic
from service.utils.iceberg import *
from service.utils.spark import *
from service.pipeline.bronze import process_micro_batch
from service.utils.schema.reader import AvscReader

if __name__ == "__main__":
    spark_session = get_spark_session("BronzeJob")
    client = SchemaRegistryManager._get_client(use_internal=True)
    all_topic_names = BronzeTopic.get_all_topics()
    all_topic_stream_df = get_kafka_stream_df(spark_session, all_topic_names)
    base_stream_df = all_topic_stream_df.select(col("key").cast("string"), col("value"), col("topic"))

    avsc_reader = AvscReader(all_topic_names[0])
    query = base_stream_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .queryName("Bronze_Loader") \
        .option("checkpointLocation", f"s3a://{avsc_reader.s3_uri}/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()