from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from typing import Iterable
from service.init.kafka import *

def delete_topics(admin_client: KafkaAdminClient, topic_names_to_delete: Iterable[str]):
    for topic_name in topic_names_to_delete:
        try:
            admin_client.delete_topics(topics=[topic_name])
            print(f"Deleted topic: {topic_name}")
        except UnknownTopicOrPartitionError:
            print(f"Topic {topic_name} does not exist, skipping deletion.")
        except Exception as e:  # 기타 예외 (e.g., 지연/연결 문제)
            print(f"Error deleting topic {topic_name}: {e}")

def create_topics(admin_client: KafkaAdminClient, topics_names_to_create: Iterable[str]):
    for topic_name in topics_names_to_create:
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=2
        )
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Created topic: {topic_name}")
        except TopicAlreadyExistsError:
            print(f"Topic {topic_name} already exists, skipping creation.")