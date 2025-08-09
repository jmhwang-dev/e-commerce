
from kafka.admin import KafkaAdminClient
from kafka import KafkaProducer
import json

BOOTSTRAP_SERVERS='localhost:19092'

def get_client():
    return KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # api_version=(3, 9, 0),
        # client_id='topic-deleter'
    )

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: json.dumps(k).encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        compression_type=None
    )