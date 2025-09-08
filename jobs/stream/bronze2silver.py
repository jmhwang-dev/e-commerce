from functools import partial
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
from confluent_kafka.serialization import SerializationContext, MessageField

from service.stream.topic import BronzeTopic, SilverTopic, DeadLetterQueuerTopic
from service.utils.spark import get_spark_session, get_kafka_stream_df, get_serialized_df, get_decoded_stream_df
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.utils.schema.reader import AvscReader
from service.utils.kafka import get_confluent_serializer_conf
from service.stream.helper import *

SRC_TOPIC_NAMES = BronzeTopic.get_all_topics()
DST_TOPIC_NAMES = SilverTopic.get_all_topics() + DeadLetterQueuerTopic.get_all_topics()

def get_confluent_serializer_udf(subject: Optional[str] = None, use_internal=True):
    def serialize_logic(row_struct: Row) -> bytes:
        try:
            # 초기화를 한 번에 처리
            if not hasattr(serialize_logic, '_initialized'):
                topic_name = subject.rsplit('-value', 1)[0] if subject else None
                serializer, _ = get_confluent_serializer_conf(topic_name, use_internal)
                
                serialize_logic.topic_name = topic_name
                serialize_logic.serializer = serializer
                serialize_logic._initialized = True
            
            # json 직렬화 수행
            row_dict = row_struct.asDict(recursive=True)
            if serialize_logic.topic_name is None:
                return serialize_logic.serializer(row_dict)
            else:
                ctx = SerializationContext(serialize_logic.topic_name, MessageField.VALUE)
                return serialize_logic.serializer(row_dict, ctx)
                
        except Exception as e:
            raise RuntimeError(f"Serialization failed: {str(e)}")
    
    return udf(serialize_logic, BinaryType())

def transform_topic_stream(micro_batch_df:DataFrame, batch_id: int, serializer_udfs: dict):
    topics_in_batch = [row.topic for row in micro_batch_df.select("topic").distinct().collect()]
    print(f"Processing Batch ID: {batch_id}, Topics: {topics_in_batch}")

    for topic_name in topics_in_batch:
        try:
            # debug
            # BronzeTopic.CUSTOMER
            # if topic_name not in [BronzeTopic.ORDER_ITEM]:
            #     continue

            topic_df = micro_batch_df.filter(col("topic") == topic_name)
            avsc_reader = AvscReader(topic_name)            
            deserialized_df = get_decoded_stream_df(topic_df, avsc_reader.schema_str)

            job_instance = get_job(topic_name) # key: dst_table_name(topic_name), value: DataFrame
            destination_dfs = job_instance.transform(deserialized_df)

            for dst_topic_name, transformed_df in destination_dfs.items():
                producer_class = get_producer(dst_topic_name)
                serialized_df = get_serialized_df(serializer_udfs, transformed_df, producer_class)
                producer_class.publish(serialized_df.select("key", "value"))
                
        except Exception as e:
            print(f"{topic_name} in batch {batch_id}: {e}")

if __name__ == "__main__":
    spark_session = get_spark_session("TransformBronzeTopicToSilverTopicJob")
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