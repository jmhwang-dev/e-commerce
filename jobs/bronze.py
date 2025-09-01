from service.common.topic import BronzeTopic
from service.utils.iceberg.spark import *
from service.utils.spark import *
from service.pipeline.bronze import process_micro_batch

if __name__ == "__main__":
    spark_session = get_spark_session("BronzeJob")
    client = SchemaRegistryManager._get_client(use_internal=True)
    all_topic_names = BronzeTopic.get_all_topics()

    tmp_topic = all_topic_names[0]
    schema_str = client.get_latest_version(tmp_topic).schema.schema_str
    # # TODO: 여기서는 `s3_uri` 만 필요
    s3_uri, table_identifier, table_name = get_iceberg_destination(schema_str)
    
    all_topic_stream_df = get_kafka_stream_df(spark_session, all_topic_names)
    base_stream_df = all_topic_stream_df.select(col("key").cast("string"), col("value"), col("topic"))

    query = base_stream_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .queryName("Bronze_Loader") \
        .option("checkpointLocation", f"s3a://{s3_uri}/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()