
from typing import Optional
from pyspark.sql import Row
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BinaryType
from functools import partial
from confluent_kafka.serialization import SerializationContext, MessageField

from service.common.topic import BronzeTopic
from service.utils.spark import get_spark_session, get_kafka_stream_df, start_console_stream
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.producer.silver import *
from service.pipeline.transform import transform_topic_stream
from service.utils.schema.reader import AvscReader
from service.utils.kafka import get_confluent_serializer_conf

import time
TARGET_TOPIC_NAMES = BronzeTopic.get_all_topics() + SilverTopic.get_all_topics()

def get_confluent_serializer_udf(subject: Optional[str] = None, use_internal=True):
    def serialize_logic(row_struct: Row) -> bytes:
        try:
            # 초기화를 한 번에 처리
            if not hasattr(serialize_logic, '_initialized'):
                topic_name = subject.rsplit('-value', 1)[0] if subject else None
                serializer_result = get_confluent_serializer_conf(topic_name, use_internal)
                serializer = serializer_result[0] if isinstance(serializer_result, (list, tuple)) else serializer_result
                
                serialize_logic.topic_name = topic_name
                serialize_logic.serializer = serializer
                serialize_logic._initialized = True
            
            # 직렬화 수행
            if serialize_logic.topic_name is None:
                return serialize_logic.serializer(str(row_struct))
            else:
                row_dict = row_struct.asDict(recursive=True)
                ctx = SerializationContext(serialize_logic.topic_name, MessageField.VALUE)
                return serialize_logic.serializer(row_dict, ctx)
                
        except Exception as e:
            raise RuntimeError(f"Serialization failed: {str(e)}")
    
    return udf(serialize_logic, BinaryType())

if __name__ == "__main__":
    spark_session = get_spark_session("TransformAllTopicsJob")
    client = SchemaRegistryManager._get_client(use_internal=True)
    target_topics_stream_df = get_kafka_stream_df(spark_session, TARGET_TOPIC_NAMES)

    serializer_udf_dict = {}

    for topic_name in TARGET_TOPIC_NAMES:
        print(f"Creating UDF for subject: {topic_name}")
        if topic_name in ["review_conflict_sentiment", 'review_consistent_sentiment']:
            continue
        if '_dlq' not in topic_name:
            serializer_udf_dict[topic_name] = get_confluent_serializer_udf(topic_name)
        else:
            serializer_udf_dict[topic_name] = get_confluent_serializer_udf(None)

    base_stream_df = target_topics_stream_df.select(col("key").cast("string"), col("value"), col("topic"))
    process_function = partial(transform_topic_stream, serializer_udfs=serializer_udf_dict)

    avsc_reader = AvscReader(TARGET_TOPIC_NAMES[0])
    query = base_stream_df.writeStream \
        .foreachBatch(process_function) \
        .queryName("transform_all_topics") \
        .option("checkpointLocation", f"s3a://{avsc_reader.s3_uri}/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()