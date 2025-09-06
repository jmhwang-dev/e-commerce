
import pandas as pd
from confluent_kafka.serialization import SerializationError

from service.producer.base.common import *
from service.utils.kafka import *

class PandasProducer(BaseProducer):
    message: pd.DataFrame = None

    @classmethod
    def generate_message(cls, data: pd.DataFrame):
        """
        CAUTION:
            - Avro 스키마에서 null 타입은 Python의 None만 인식
            - pandas의 `np.nan`은 Python의 `None`이 아닌 `float` 타입이므로 변환 필요: `np.nan` -> `None`
        """
        cls.message = data.where(pd.notnull(data), None)
        cls.message[cls.message_key_col] = cls.message[cls.pk_column].astype(str).agg('-'.join, axis=1)

    @classmethod
    def init_producer(cls, use_internal: bool = True):
        """
        - DLQ : json
        - 그 외 : avro
        """
        if cls.producer is None:
            serializer, bootstrap_server_list = get_confluent_serializer_conf(cls.topic, use_internal)
            cls.producer = get_confluent_kafka_producer(bootstrap_server_list, serializer)

    @classmethod
    def publish(cls, event: pd.DataFrame, use_internal=False) -> None:
        if event.empty:
            print(f'\nEmpty message: {cls.topic}')
            return
        
        cls.init_producer(use_internal)
        cls.generate_message(event)

        for _, message in cls.message.iterrows():
            message_key = message[cls.message_key_col]
            message_value = message.drop(cls.message_key_col).to_dict()
            
            try:
                cls.producer.produce(cls.topic, key=message_key, value=message_value)
                cls.producer.flush()
            except SerializationError:
                print('schema 검증 실패')

        print(f'Published message to {cls.topic}')