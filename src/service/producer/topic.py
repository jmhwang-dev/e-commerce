from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from typing import Iterable
from service.init.kafka import *

ADMIN_CLIENT = get_external_client()

def delete_topics(topic_names_to_delete: Iterable[str]):
    for topic_name in topic_names_to_delete:
        try:
            ADMIN_CLIENT.delete_topics(topics=[topic_name])
            print(f"Deleted topic: {topic_name}")
        except UnknownTopicOrPartitionError:
            print(f"Topic {topic_name} does not exist, skipping deletion.")
        except Exception as e:  # 기타 예외 (e.g., 지연/연결 문제)
            print(f"Error deleting topic {topic_name}: {e}")

def create_topics(topics_names_to_create: Iterable[str]):
    for topic_name in topics_names_to_create:
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=2
        )
        try:
            ADMIN_CLIENT.create_topics(new_topics=[topic], validate_only=False)
            print(f"Created topic: {topic_name}")
        except TopicAlreadyExistsError:
            print(f"Topic {topic_name} already exists, skipping creation.")