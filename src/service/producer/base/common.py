from typing import Iterable
from confluent_kafka import SerializingProducer

class BaseProducer:
    dst_topic: str = ''
    pk_column: Iterable[str] = []
    producer: SerializingProducer = None
    message_key_col: str = 'message_key'