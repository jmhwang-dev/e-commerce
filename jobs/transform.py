from functools import partial

from service.common.topic import BronzeTopic, SilverTopic, DeadLetterQueuerTopic
from service.utils.spark import get_spark_session, get_kafka_stream_df
from service.utils.schema.reader import AvscReader
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.pipeline.transform import transform_topic_stream, get_confluent_serializer_udf

SRC_TOPIC_NAMES = BronzeTopic.get_all_topics()
DST_TOPIC_NAMES = SilverTopic.get_all_topics() + DeadLetterQueuerTopic.get_all_topics()

if __name__ == "__main__":
    spark_session = get_spark_session("TransformBronzeTopicsJob")
    client = SchemaRegistryManager._get_client(use_internal=True)
    src_stream_df = get_kafka_stream_df(spark_session, SRC_TOPIC_NAMES)

    serializer_udf_dict = {}
    for topic_name in DST_TOPIC_NAMES:
        serializer_udf_dict[topic_name] = get_confluent_serializer_udf(topic_name)
        print(f"Created UDF for subject: {topic_name}")
    process_function = partial(transform_topic_stream, serializer_udfs=serializer_udf_dict)
    
    query = src_stream_df.writeStream \
        .foreachBatch(process_function) \
        .queryName("transform_bronze_topics") \
        .option("checkpointLocation", f"s3a://warehousedev/checkpoints/transform") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()