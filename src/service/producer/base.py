from pathlib import Path
from typing import Iterable
import pandas as pd
from pprint import pformat
from functools import lru_cache
from service.common.topic import *
from copy import deepcopy

from confluent_kafka import SerializingProducer

from service.common.schema import *
from service.utils.kafka import *

class BaseProducer:
    topic: str = ''
    pk_column: Iterable[str] = []
    file_path: Path = Path()
    current_index: int = 0
    producer: SerializingProducer = None

    @classmethod
    def init_file_path(cls, ) -> None:
        cls.file_path = DATASET_DIR / f"{cls.topic}.tsv"
    
    @classmethod
    @lru_cache(maxsize=1)  # 자동 캐싱, maxsize=1로 한 번 로드 후 재사용
    def get_df(cls) -> pd.DataFrame:
        """TSV 파일 로드"""
        try:
            cls.init_file_path()
            df = pd.read_csv(cls.file_path, sep='\t')
            for pk_col in cls.pk_column:
                if pk_col not in df.columns:
                    raise ValueError(f"Column {pk_col} not found in {cls.file_path}")
            return df
        except FileNotFoundError:
            raise ValueError(f"File {cls.file_path} not found")
    
    @classmethod
    @lru_cache(maxsize=1)
    def _get_producer(cls, use_internal=False):
        return get_confluent_kafka_producer(cls.topic, use_internal)

    @classmethod
    def select(cls, col, value: str) -> pd.DataFrame:
        df = cls.get_df()
        return df[df[col] == value]
        
    @classmethod
    def publish(cls, _event: pd.DataFrame | pd.Series) -> None:
        if _event.empty:
            print(f'\nEmpty message: {cls.topic}')
            return
        producer = cls._get_producer()
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
            producer.produce(cls.topic, key=key, value=event)
            producer.flush()

            print(f'\nPublished message to {cls.topic} - key: {key}\n{pformat(event)}')
            cls.current_index += 1