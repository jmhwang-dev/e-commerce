
import pandas as pd
from confluent_kafka.serialization import SerializationError

from service.producer.base.common import *
from service.utils.kafka import *

class PandasProducer(BaseProducer):

    @classmethod
    def get_record(cls, dataum: pd.Series) -> dict[str, dict]:
        message = dataum.where(pd.notnull(dataum), None)

        return {
            'key': '-'.join(message[cls.pk_column].astype(str)),
            'value': message.to_dict()
        }

    @classmethod
    def generate_message(cls, event: Union[pd.DataFrame, pd.Series]) -> List[dict[str, dict]]:
        """
        CAUTION:
            - Avro 스키마에서 null 타입은 Python의 None만 인식
            - pandas의 `np.nan`은 Python의 `None`이 아닌 `float` 타입이므로 변환 필요: `np.nan` -> `None`
        """
        if isinstance(event, pd.Series):
            record = cls.get_record(event)
            return [record]
        
        producer_record_list = []
        for _, ev in event.iterrows():
            record = cls.get_record(ev)
            producer_record_list.append(record)
        return producer_record_list

    @classmethod
    def init_producer(cls, use_internal: bool = True):
        """
        - DLQ : json
        - 그 외 : avro
        """
        if cls.producer is None:
            serializer, bootstrap_server_list = get_confluent_serializer_conf(cls.dst_topic, use_internal)
            cls.producer = get_confluent_kafka_producer(bootstrap_server_list, serializer)

    @classmethod
    def publish(cls, event: Union[pd.Series, pd.DataFrame, None], use_internal=False) -> None:
        if event is None or event.empty:
            print(f"[{cls.producer_class_name}]: Empty event")
            return
        
        cls.init_producer(use_internal)

        producer_record_list = cls.generate_message(event)

        for producer_record in producer_record_list:
            try:
                cls.producer.produce(cls.dst_topic, key=producer_record['key'], value=producer_record['value'])
                cls.producer.flush()
                print(f"Published to {cls.dst_topic} - {cls.pk_column}: {producer_record['key']}")

            except SerializationError:
                print(f'[{cls.producer_class_name}]: schema 검증 실패')