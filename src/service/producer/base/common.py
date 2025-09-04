from typing import Iterable
from confluent_kafka import SerializingProducer

class BaseProducer:
    topic: str = ''
    topic_dlq: str = ''
    pk_column: Iterable[str] = []
    dlq_producer: SerializingProducer = None
    main_producer: SerializingProducer = None
    message_key_col: str = 'message_key'