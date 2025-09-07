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
from service.job.base import *

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