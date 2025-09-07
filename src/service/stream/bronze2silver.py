from typing import List, Optional
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import BinaryType
from pyspark.sql.functions import col, struct, concat_ws, udf
from confluent_kafka.serialization import SerializationContext, MessageField

from service.stream.topic import BronzeTopic
from service.producer.silver import *
from service.utils.schema.reader import AvscReader
from service.utils.spark import get_decoded_stream_df
from service.utils.kafka import get_confluent_serializer_conf
from service.stream.common import *

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

def get_transform_result(src_topic_name: str, deserialized_df: DataFrame) -> dict[str, DataFrame]:
    transformer: BronzeToSilverJob
    if src_topic_name == BronzeTopic.REVIEW:
        transformer = ReviewBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.PAYMENT:
        transformer = PaymentBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.ORDER_STATUS:
        transformer = OrderStatusBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.PRODUCT:
        transformer = ProductBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.CUSTOMER:
        transformer = CustomerBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.SELLER:
        transformer = SellerBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.GEOLOCATION:
        transformer = GeolocationBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.ESTIMATED_DELIVERY_DATE:
        transformer = EstimatedDeliveryDateBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.ORDER_ITEM:
        transformer = OrderItemBronzeToSilverJob()

    else:
        raise ValueError("There is no job for bronze")

    destination_dfs = transformer.transform(deserialized_df)
    if len(destination_dfs) == 0:
        raise ValueError("The # of transform: 0")
    return destination_dfs

def get_producer(dst_topic_name: str) -> SparkProducer:
    if dst_topic_name == SilverTopic.REVIEW_CLEAN_COMMENT:
        producer = ReviewCleanCommentSilverProducer

    elif dst_topic_name == SilverTopic.REVIEW_METADATA:
        producer = ReviewMetadataSilverProducer

    elif dst_topic_name == SilverTopic.PAYMENT:
        producer = PaymentSilverProducer

    elif dst_topic_name == DeadLetterQueuerTopic.PAYMENT_DLQ:
        producer = PaymentDeadLetterQueueSilverProducer

    elif dst_topic_name == SilverTopic.ORDER_STATUS:
        producer = OrderStatusSilverProducer

    elif dst_topic_name == SilverTopic.PRODUCT:
        producer = ProductSilverProducer

    elif dst_topic_name == SilverTopic.CUSTOMER:
        producer = CustomerSilverProducer

    elif dst_topic_name == SilverTopic.SELLER:
        producer = SellerSilverProducer

    elif dst_topic_name == SilverTopic.GEOLOCATION:
        producer = GeolocationSilverProducer

    elif dst_topic_name == SilverTopic.ESTIMATED_DELIVERY_DATE:
        producer = EstimatedDeliveryDateSilverProducer

    elif dst_topic_name == SilverTopic.ORDER_ITEM:
        producer = OrderItemSilverProducer
    else:
        raise ValueError("There is no producer for silver")

    return producer

def get_serialized_df(transformed_df: DataFrame, serializer_udf, key_columns: List[str]):
    # DataFrame의 스키마를 순회하며 TimestampType인 컬럼을 모두 찾아서 string 변환: TimestampType는 직렬화 할 수 없음
    # df_to_serialize = transformed_df
    # for field in transformed_df.schema:
    #     if isinstance(field.dataType, TimestampType):
    #         print(f"Casting timestamp column '{field.name}' to string.")
    #         df_to_serialize = df_to_serialize.withColumn(field.name, col(field.name).cast("string"))
    
    df_to_publish = transformed_df.select(
        concat_ws("-", *[col(c).cast("string") for c in key_columns]).alias("key"),
        # struct('*')를 사용하여 데이터프레임의 모든 컬럼을 value_struct로 만듬
        struct(*transformed_df.columns).alias("value_struct")
    )

    return df_to_publish.withColumn(
        "value", serializer_udf(col("value_struct"))
    )

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

            destination_dfs = get_transform_result(topic_name, deserialized_df) # key: dst_table_name(topic_name), value: DataFrame

            for dst_topic_name, transformed_df in destination_dfs.items():

                producer_class = get_producer(dst_topic_name)
                serializer_udf = serializer_udfs.get(producer_class.topic)

                if not serializer_udf:
                    print(f"Warning: Serializer UDF for destination topic '{producer_class.topic}' not found. Skipping.")
                    continue

                serialized_df = get_serialized_df(transformed_df, serializer_udf, producer_class.pk_column)
                producer_class.publish(serialized_df.select("key", "value"))
                print(f"Published {transformed_df.count()} records for destination topic {dst_topic_name}")

        except Exception as e:
            print(f"Error processing source topic {topic_name} in batch {batch_id}: {e}")