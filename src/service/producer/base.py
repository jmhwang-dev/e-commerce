from typing import Iterable
from confluent_kafka import SerializingProducer

class BaseProducer:
    topic: str = ''
    pk_column: Iterable[str] = []
    producer: SerializingProducer = None