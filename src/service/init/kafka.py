from typing import Iterable, Iterator
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka import KafkaProducer

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer

from service.init.confluent import *
from config.kafka import *
from enum import Enum

import json
import time

class IngestionType(Enum):
    CDC = 'cdc'
    STREAM = 'stream'


# 1. 클래스 생성 과정을 제어할 메타클래스 정의
class TopicMeta(type):
    """
    클래스 변수를 `prefix.value` 형태로 자동 변환하는 메타클래스
    """
    def __new__(mcs, name, bases, attrs):
        # 클래스에 정의된 TOPIC_PREFIX 값을 가져옴
        prefix = attrs.get("TOPIC_PREFIX", "")

        # 클래스 속성들을 순회하며 TOPIC 변수들을 찾아 값을 변환
        for attr_name, attr_value in attrs.items():
            if (isinstance(attr_value, str) and
                    attr_name.isupper() and
                    not attr_name.startswith('_') and
                    not attr_name.startswith('TOPIC_')):
                # 'prefix.value' 형태로 값을 새로 할당
                attrs[attr_name] = f"{prefix}.{attr_value}"

        # 수정된 속성들로 새로운 클래스를 생성하여 반환
        return super().__new__(mcs, name, bases, attrs)

# 2. BaseTopic에 메타클래스 지정
class BaseTopic(metaclass=TopicMeta):
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
    ORDER_ITEM = "order_item"
    PRODUCT = "product" 
    CUSTOMER = "customer"
    SELLER = "seller"
    GEOLOCATION = "geolocation"
    
    # Stream Topics
    ORDER_STATUS = "order_status"
    PAYMENT = "payment"
    ESTIMATED_DELIVERY_DATE = "estimated_delivery_date"
    REVIEW = "review"

class BronzeToSilverTopic(BaseTopic):
    """Topics for bronze to silver processing."""
    TOPIC_PREFIX = "silver"
    
    # 기존 bronze 토픽들 상속
    ORDER_ITEM = "order_item"
    PRODUCT = "product"
    CUSTOMER = "customer" 
    SELLER = "seller"
    GEOLOCATION = "geolocation"
    ORDER_STATUS = "order_status"
    PAYMENT = "payment"
    ESTIMATED_DELIVERY_DATE = "estimated_delivery_date"
    REVIEW = "review"
    
    # 추가 silver 토픽들
    PREPROCESSED_REVIEW = "preprocessed_review"
    INFERENCED_REVIEW = "inferenced_review"

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

def get_kafak_consumer(bootstrp_servers: Iterable[str], topic_name: Iterable[str]) -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrp_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=None,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    consumer.subscribe(topic_name)
    wait_for_partition_assignment(consumer)
    return consumer

def wait_for_partition_assignment(consumer):
    max_attempts = 10
    for _ in range(max_attempts):
        if consumer.assignment():
            print('Consumer partition assignment loaded!')
            return consumer
        consumer.poll(1)
        time.sleep(5)
    raise TimeoutError("Consumer 파티션 할당 실패")