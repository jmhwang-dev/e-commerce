from typing import Iterable
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka import KafkaProducer

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer

from service.init.confluent import *
from config.kafka import *

import json
import time

SCHEMA_REGISTRY_CLIENT = SchemaRegistryClient({'url': SCHEMA_REGISTRY_EXTERNAL_URL})
SCHEMA_REGISTRY_INTERNAL = SchemaRegistryClient({'url': SCHEMA_REGISTRY_INTERNAL_URL})

def get_confluent_producer(topic_name, bootstrap_servers=BOOTSTRAP_SERVERS_INTERNAL) -> SerializingProducer:

    # 등록된 모든 subject 확인
    subjects = SCHEMA_REGISTRY_CLIENT.get_subjects()
    print("Available subjects:", subjects)

    schema_obj = SCHEMA_REGISTRY_CLIENT.get_latest_version(topic_name).schema

    avro_serializer = AvroSerializer(
        SCHEMA_REGISTRY_CLIENT,
        schema_obj,  # None으로 두면 subject 기반으로 fetch
        to_dict=lambda obj, ctx: obj,
        conf={
            'auto.register.schemas': False,
            # 'subject.name.strategy': record_subject_name_strategy
            'subject.name.strategy': lambda ctx, record_name: topic_name
            }
    )

    producer_conf = {
        'bootstrap.servers': bootstrap_servers[0],
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'acks': 'all',
        'retries': 3
    }
    return SerializingProducer(producer_conf)

def get_kafka_producer(bootstrp_servers: Iterable[str]) -> KafkaProducer:
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