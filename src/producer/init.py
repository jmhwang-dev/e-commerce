
from kafka.admin import KafkaAdminClient
from kafka import KafkaProducer

from pathlib import Path
import json

class PROCESS_TYPE:
    STREAM = 'stream'
    BATCH = 'batch'

BOOTSTRAP_SERVERS = 'localhost:19092'
DATASET_DIR = Path("./downloads/olist_redefined")

def get_client():
    return KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # api_version=(3, 9, 0),
        # client_id='topic-deleter'
    )

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # 직렬화(serialize) 설정
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        acks='all',  # 모든 복제본에서 확인
        retries=3,
        max_in_flight_requests_per_connection=1
    )