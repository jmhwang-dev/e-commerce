from service.common.topic import BronzeTopic, SilverTopic, DeadLetterQueuerTopic, InferenceTopic
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.utils.spark import get_spark_session, get_kafka_stream_df
from service.pipeline.load import load_medallion_layer

SRC_TOPIC_NAMES = BronzeTopic.get_all_topics() + SilverTopic.get_all_topics() + DeadLetterQueuerTopic.get_all_topics() + InferenceTopic.get_all_topics()

if __name__ == "__main__":
    spark_session = get_spark_session("LoadAllTopicsJob")
    client = SchemaRegistryManager._get_client(use_internal=True)
    src_stream_df = get_kafka_stream_df(spark_session, SRC_TOPIC_NAMES)

    query = src_stream_df.writeStream \
        .foreachBatch(load_medallion_layer) \
        .queryName("load_all_topics") \
        .option("checkpointLocation", f"s3a://warehousedev/checkpoints/load") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()