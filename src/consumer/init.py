
from kafka import KafkaConsumer
import json
import time

BOOTSTRAP_SERVERS=['localhost:19092', 'localhost:19094', 'localhost:19096']

def get_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=None,  # 또는 고유한 group_id 설정
        # 수정: 역직렬화(deserialize)로 변경
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )
    wait_for_partition_assignment(consumer)
    return consumer

def wait_for_partition_assignment(consumer):
    max_attempts = 10
    for _ in range(max_attempts):  # 문법 수정
        if consumer.assignment():
            print('Consumer partition assignment loaded!')
            return consumer
        consumer.poll(1)
        time.sleep(5)
    raise TimeoutError("Consumer 파티션 할당 실패")