from typing import Iterable
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import NewTopic, AdminClient
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

from service.common.topic import *
from service.common.schema import *
from config.kafka import *

def get_kafka_admin_client(bootstrp_servers: str) -> AdminClient:
    bootstrp_server_list = bootstrp_servers.split(",")
    admin_config = {
        'bootstrap.servers': bootstrp_server_list[0]
    }
    
    return AdminClient(admin_config)

def get_confluent_kafka_consumer(group_id: str, topic_names:list, use_internal=False) -> DeserializingConsumer:
    if not use_internal:
        bootstrap_server_list = BOOTSTRAP_SERVERS_EXTERNAL.split(',')
    else:
        bootstrap_server_list = BOOTSTRAP_SERVERS_INTERNAL.split(',')

    client = SchemaRegistryManager._get_client(use_internal)
    avro_deserializer = AvroDeserializer(client)

    # 'group.id': 'olist-avro-consumer-group',
    # TODO: dev 모드에서 자동 오프셋 커밋을 토글할 수 있어야함
    consumer_conf = {
        'bootstrap.servers': bootstrap_server_list[0],  # Kafka 브로커
        'auto.offset.reset': 'earliest',
        'group.id': group_id,
        'key.deserializer': StringDeserializer('utf-8'),  # 키는 문자열 가정
        'value.deserializer': avro_deserializer
        # 'enable.auto.commit': False # in dev mode:
    }
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(topic_names)
    return consumer


def get_confluent_kafka_producer(topic:str, use_internal=False) -> SerializingProducer:
    if not use_internal:
        bootstrap_server_list = BOOTSTRAP_SERVERS_EXTERNAL.split(',')
    else:
        bootstrap_server_list = BOOTSTRAP_SERVERS_INTERNAL.split(',')

    client = SchemaRegistryManager._get_client(use_internal)
    schema_obj = client.get_latest_version(topic).schema
    avro_serializer = AvroSerializer(
        client,
        schema_obj,  # None으로 두면 subject 기반으로 fetch
        to_dict=lambda obj, ctx: obj,
        conf={
            'auto.register.schemas': False,
            'subject.name.strategy': lambda ctx, record_name: topic
            }
    )

    producer_conf = {
        'bootstrap.servers': bootstrap_server_list[0],
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'acks': 'all',
        'retries': 3
    }
    return SerializingProducer(producer_conf)


def delete_topics(admin_client: AdminClient, topic_names_to_delete: Iterable[str]):
    for topic_name in topic_names_to_delete:
        try:
            admin_client.delete_topics(topics=[topic_name])
            print(f"Deleted topic: {topic_name}")
        except UnknownTopicOrPartitionError:
            print(f"Topic {topic_name} does not exist, skipping deletion.")
        except Exception as e:  # 기타 예외 (e.g., 지연/연결 문제)
            print(f"Error deleting topic {topic_name}: {e}")

def create_topics(admin_client: AdminClient, topics_names_to_create: Iterable[str]):
    for topic_name in topics_names_to_create:
        topic = NewTopic(
            topic_name,
            num_partitions=1,
            replication_factor=2
        )
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Created topic: {topic_name}")
        except TopicAlreadyExistsError:
            print(f"Topic {topic_name} already exists, skipping creation.")
