
from service.common.topic import BronzeTopic
from service.utils.spark import get_spark_session, get_kafka_stream_df, start_console_stream
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.producer.silver import *
from service.pipeline.transform import transform_topic_stream
from service.utils.schema.reader import AvscReader

from pyspark.sql.functions import col, udf
from service.utils.kafka import get_confluent_serializer_conf
from pyspark.sql.types import BinaryType
from functools import partial # 👈 functools.partial import


TARGET_TOPIC_NAMES = BronzeTopic.get_all_topics() + SilverTopic.get_all_topics()

# def process_micro_batch(micro_batch_df: DataFrame, batch_id: int):
#     transform_topic_stream(TARGET_TOPIC_NAMES, micro_batch_df, batch_id)

# src/service/utils/kafka.py

from typing import Tuple, List, Optional, Union
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from config.kafka import BOOTSTRAP_SERVERS_EXTERNAL, BOOTSTRAP_SERVERS_INTERNAL
from service.utils.schema.registry_manager import SchemaRegistryManager
from confluent_kafka.serialization import SerializationContext, MessageField


def get_confluent_serializer_udf(subject: Optional[str] = None, use_internal=True):
    def serialize_logic(row_struct):
        if not hasattr(serialize_logic, 'serializer'):
            if subject is None:
                serialize_logic.serializer = StringSerializer('utf_8')
            else:
                client = SchemaRegistryManager._get_client(use_internal)
                schema_str = client.get_latest_version(subject).schema.schema_str
                serialize_logic.serializer = AvroSerializer(client, schema_str)

        if subject is None:
            return serialize_logic.serializer(str(row_struct))
        else:
            row_dict = row_struct.asDict(recursive=True)

            # 👇👇👇 ** 여기가 바로 수정된 핵심 부분입니다! ** 👇👇👇
            # subject 이름(예: "silver.payment-value")에서 토픽 이름("silver.payment")을 추출합니다.
            topic_name = subject.replace("-value", "")
            # 올바른 SerializationContext 객체를 생성합니다.
            ctx = SerializationContext(topic_name, MessageField.VALUE)
            
            # 이제 None이 아닌, 올바른 context 객체를 전달하여 직렬화기를 호출합니다.
            return serialize_logic.serializer(row_dict, ctx)
    # -------------------------------------------------------------------

    # ⭐️ 실제 로직을 담은 함수를 UDF로 등록하여 반환합니다.
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

    # for k, v in serializer_udf_dict.items():
    #     print(k, v)

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