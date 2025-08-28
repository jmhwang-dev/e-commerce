from typing import Iterable
from confluent_kafka import SerializingProducer
from service.utils.kafka import get_confluent_kafka_producer

class BaseProducer:
    topic: str = ''
    topic_dlq: str = ''
    pk_column: Iterable[str] = []
    dlq_producer: SerializingProducer = None
    main_producer: SerializingProducer = None
    message_key_col: str = 'message_key'

    @classmethod
    def init_producer(cls, use_internal: bool = True):
        """
        - DLQ : json
        - 그 외 : avro
        """
        if cls.main_producer is None:
            cls.main_producer = get_confluent_kafka_producer(cls.topic, use_internal=use_internal)

        if cls.dlq_producer is None:
            cls.dlq_producer = get_confluent_kafka_producer(None, use_internal=use_internal)