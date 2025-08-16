from typing import Iterable
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka import KafkaProducer

from service.init.confluent import *
from service.consumer.utils import *
from config.kafka import *
from enum import Enum

import json

class IngestionType(Enum):
    CDC = 'cdc'
    STREAM = 'stream'

class BaseTopic:
    """Base class for Kafka topic definitions with common methods."""
    TOPIC_PREFIX: str = ""

    @classmethod
    def get_all_topics(cls) -> list[str]:
        """모든 토픽 이름을 prefix와 함께 반환"""
        topics = []
        for attr_name in dir(cls):
            # 메타클래스에서 이미 값을 변환했으므로, 값 자체를 추가하기만 하면 됨
            attr_value = getattr(cls, attr_name)
            if (isinstance(attr_value, str) and
                    attr_name.isupper() and
                    not attr_name.startswith('_') and
                    not attr_name.startswith('TOPIC_')):
                topics.append(attr_value)
        return topics

class RawToBronzeTopic(BaseTopic):
    """Topics for raw data ingestion (raw to bronze)."""
    TOPIC_PREFIX = "bronze"
    
    # CDC Topics
    PRODUCT = "cdc_product" 
    CUSTOMER = "cdc_customer"
    SELLER = "cdc_seller"
    GEOLOCATION = "cdc_geolocation"
    
    # Stream Topics
    ORDER_ITEM = "stream_order_item"
    ORDER_STATUS = "stream_order_status"
    PAYMENT = "stream_payment"
    ESTIMATED_DELIVERY_DATE = "stream_estimated_delivery_date"
    REVIEW = "stream_review"

class BronzeToSilverTopic(BaseTopic):
    """Topics for bronze to silver processing."""
    TOPIC_PREFIX = "silver"
    
    ORDER_ITEM = "order_item"
    PRODUCT = "product"
    CUSTOMER = "customer" 
    SELLER = "seller"
    GEOLOCATION = "geolocation"
    ORDER_STATUS = "order_status"
    PAYMENT = "payment"
    ESTIMATED_DELIVERY_DATE = "estimated_delivery_date"
    
    REVIEW_METADATA = "review_metadata"
    REVIEW_CLEAN_COMMENT = "review_clean_comment"
    REVIEW_INFERENCED = "review_inferenced"

class SilverToGoldTopic(BaseTopic):
    """Topics for silver to gold processing."""
    TOPIC_PREFIX = "gold"
    
    AGGREGATED_ORDER = "aggregated_order"

def get_kafka_producer(bootstrp_servers: Iterable[str]) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrp_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )

def get_kafka_admin_client(bootstrp_servers: str) -> KafkaAdminClient:
    bootstrp_server_list = bootstrp_servers.split(",")
    return KafkaAdminClient(
        bootstrap_servers=bootstrp_server_list
    )

def get_kafka_consumer(bootstrp_servers: Iterable[str], topic_name: Iterable[str]) -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrp_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    consumer.subscribe(topic_name)
    wait_for_partition_assignment(consumer)
    return consumer

