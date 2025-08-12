from typing import Iterator, Iterable
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka import KafkaProducer
import json
import time

from dotenv import load_dotenv
import os
from pathlib import Path
from enum import Enum

load_dotenv('./configs/kafka/.env')
BOOTSTRAP_SERVERS_EXTERNAL = os.getenv("BOOTSTRAP_SERVERS_EXTERNAL", "localhost:19092,localhost:19094,localhost:19096").split(",")
BOOTSTRAP_SERVERS_INTERNAL = os.getenv("BOOTSTRAP_SERVERS_INTERNAL", "kafka1:9092,kafka2:19092,kafka3:9092")
DATASET_DIR = Path(os.getenv("DATASET_DIR", "./downloads/olist_redefined"))

class IngestionType(Enum):
    CDC = 'cdc'
    STREAM = 'stream'

class Topic:
    # CDC
    ORDER_ITEM = 'order_item'
    PRODUCT = 'product'
    CUSTOMER = 'customer_cleaned'
    SELLER = 'seller'
    GEOLOCATION = 'geolocation'

    # stream
    ORDER_STATUS = 'order_status'
    PAYMENT = 'payment'
    ESTIMATED_DELIVERY_DATE = 'estimated_delivery_date'
    REVIEW = 'review'

    REVIEW_INFERENCE = 'review_inference'

    @classmethod
    def __iter__(cls) -> Iterator[str]:
        for attr_name, attr_value in vars(cls).items():
            if not attr_name.startswith('__'):
                yield attr_value

def get_producer(bootstrp_servers: Iterable[str]) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrp_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )

def get_client(bootstrp_servers: Iterable[str]) -> KafkaAdminClient:
    return KafkaAdminClient(
        bootstrap_servers=bootstrp_servers
    )

def get_consumer(bootstrp_servers: Iterable[str], topic_name: Iterable[str]) -> KafkaConsumer:
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