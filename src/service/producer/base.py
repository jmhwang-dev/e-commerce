from typing import Iterable
from confluent_kafka import SerializingProducer
from pprint import pformat
from copy import deepcopy
import pandas as pd
from service.utils.kafka import get_confluent_kafka_producer

class BaseProducer:
    topic: str = ''
    pk_column: Iterable[str] = []
    main_producer: SerializingProducer = None

    @classmethod
    def publish(cls, _event: pd.DataFrame | pd.Series, use_internal=False) -> None:
        if _event.empty:
            print(f'\nEmpty message: {cls.topic}')
            return
        
        if cls.main_producer is None:
            cls.main_producer = get_confluent_kafka_producer(cls.topic, use_internal)

        event_list = []
        if isinstance(_event, pd.Series):
            event_list += [deepcopy(_event).to_dict()]
        elif isinstance(_event, pd.DataFrame):
            # pandas의 np.nan: Python의 None이 아닌 float 타입
            # Avro 스키마: null 타입은 Python의 None만 인식
            # 변환 필요: np.nan → None으로 변환해야 Avro 직렬화 성공
            event_list += _event.where(pd.notnull(_event), None).to_dict(orient='records')
        
        for event in event_list:
            key_str_list = []
            for pk_col in cls.pk_column:
                key_str_list.append(str(event[pk_col]))

            key = '|'.join(key_str_list)
            cls.main_producer.produce(cls.topic, key=key, value=event)
            cls.main_producer.flush()

            print(f'\nPublished message to {cls.topic} - key: {key}\n{pformat(event)}')