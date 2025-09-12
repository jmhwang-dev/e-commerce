from typing import Optional
from functools import partial
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
from confluent_kafka.serialization import SerializationContext, MessageField

from service.stream.topic import BronzeTopic, SilverTopic
from service.utils.spark import get_spark_session, get_kafka_stream_df, get_serialized_df, get_deserialized_stream_df, start_console_stream
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.utils.schema.reader import AvscReader
from service.utils.kafka import get_confluent_serializer_conf
from service.stream.helper import *

SRC_TOPIC_NAMES = [BronzeTopic.ORDER_STATUS, BronzeTopic.PAYMENT]
DST_TOPIC_NAMES = [SilverTopic.STREAM_ORDER_STATUS, SilverTopic.STREAM_PAYMENT, SilverTopic.STREAM_PAYMENT_DLQ]

def get_confluent_serializer_udf(subject: Optional[str] = None, use_internal=True):
    def serialize_logic(row_struct: Row) -> bytes:
        try:
            # 초기화를 한 번에 처리
            if not hasattr(serialize_logic, '_initialized'):
                src_topic_name = subject.rsplit('-value', 1)[0] if subject else None
                serializer, _ = get_confluent_serializer_conf(src_topic_name, use_internal)
                
                serialize_logic.src_topic_name = src_topic_name
                serialize_logic.serializer = serializer
                serialize_logic._initialized = True
            
            # json 직렬화 수행
            row_dict = row_struct.asDict(recursive=True)
            if serialize_logic.src_topic_name is None:
                return serialize_logic.serializer(row_dict)
            else:
                ctx = SerializationContext(serialize_logic.src_topic_name, MessageField.VALUE)
                return serialize_logic.serializer(row_dict, ctx)
                
        except Exception as e:
            raise RuntimeError(f"Serialization failed: {str(e)}")
    
    return udf(serialize_logic, BinaryType())

def transform_topic_stream(micro_batch_df:DataFrame, batch_id: int, serializer_udfs: dict):
    micro_batch_df.cache()
    print(f"Processing Batch ID: {batch_id}")

    # TODO: solve falling behind
    for src_topic_name in SRC_TOPIC_NAMES:
        try:
            topic_df = micro_batch_df.filter(col("topic") == src_topic_name)
            
            avsc_reader = AvscReader(src_topic_name)
            deserialized_df = get_deserialized_stream_df(topic_df, avsc_reader.schema_str)

            job_instance = get_bronze2silver_job(src_topic_name)
            destination_dfs = job_instance.transform(deserialized_df)   # key: dst_table_name(src_topic_name), value: DataFrame

            for dst_topic_name, transformed_df in destination_dfs.items():
                producer_class = get_bronze2silver_producer(dst_topic_name)
                serialized_df = get_serialized_df(serializer_udfs, transformed_df, producer_class)
                producer_class.publish(serialized_df.select("key", "value"))
                
        except Exception as e:
            print(f"{src_topic_name} in batch {batch_id}: {e}")

if __name__ == "__main__":
    spark_session = get_spark_session("Process stream")
    client = SchemaRegistryManager._get_client(use_internal=True)
    src_stream_df = get_kafka_stream_df(spark_session, SRC_TOPIC_NAMES)
    
    serializer_udf_dict = {}
    for src_topic_name in DST_TOPIC_NAMES:
        serializer_udf_dict[src_topic_name] = get_confluent_serializer_udf(src_topic_name)
        print(f"Created UDF for subject: {src_topic_name}")
    
    process_function = partial(transform_topic_stream, serializer_udfs=serializer_udf_dict)
    
    query = src_stream_df.writeStream \
        .foreachBatch(process_function) \
        .queryName("process_stream") \
        .option("checkpointLocation", f"s3a://warehousedev/silver/checkpoints/stream") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()