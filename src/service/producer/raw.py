from pathlib import Path
from typing import Iterable
import pandas as pd
from pprint import pformat
from functools import lru_cache
from service.init.kafka import *
from copy import deepcopy

class DataMessage:
    topic: str = ''
    pk_column: Iterable[str] = []
    ingestion_type: IngestionType
    file_path: Path = Path()
    current_index: int = 0
    producer: SerializingProducer = None

    @classmethod
    def init_file_path(cls, ) -> None:
        if cls.ingestion_type == IngestionType.CDC:
            cls.file_path = DATASET_DIR / cls.ingestion_type.value / f"{cls.topic.split('.')[-1]}.tsv"
        elif cls.ingestion_type == IngestionType.STREAM:
            cls.file_path = DATASET_DIR / cls.ingestion_type.value / f"{cls.topic.split('.')[-1]}.tsv"
        else:
            raise ValueError(f"Unknown ingestion type: {cls.ingestion_type}")
    
    @classmethod
    @lru_cache(maxsize=1)
    def get_producer(cls) -> SerializingProducer:
        return get_confluent_producer(cls.topic, BOOTSTRAP_SERVERS_EXTERNAL)
    
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
    def select(cls, col, value: str) -> pd.DataFrame:
        df = cls.get_df()
        return df[df[col] == value]
        
    @classmethod
    def publish(cls, _event: pd.DataFrame | pd.Series) -> None:
        if _event.empty:
            print(f'\nEmpty message: {cls.topic}')
            return
        producer = cls.get_producer()
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

# CDC
class GeolocationMessage(DataMessage):
    topic = RawToBronzeTopic.GEOLOCATION
    pk_column = ['zip_code']
    ingestion_type = IngestionType.CDC

class CustomerMessage(DataMessage):
    topic = RawToBronzeTopic.CUSTOMER
    pk_column = ['customer_id']
    ingestion_type = IngestionType.CDC

class SellerMessage(DataMessage):
    topic = RawToBronzeTopic.SELLER
    pk_column = ['seller_id']
    ingestion_type = IngestionType.CDC

class ProductMessage(DataMessage):
    topic = RawToBronzeTopic.PRODUCT
    pk_column = ['product_id']
    ingestion_type = IngestionType.CDC

# STREAM
class OrderStatusMessage(DataMessage):
    topic = RawToBronzeTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']
    ingestion_type = IngestionType.STREAM

    @classmethod
    def get_current_event(cls,) -> pd.Series:
        return cls.get_df().iloc[cls.current_index]
    
    @classmethod
    def is_end(cls):
        return cls.current_index == len(cls.get_df())

class PaymentMessage(DataMessage):
    topic = RawToBronzeTopic.PAYMENT
    pk_column = ['order_id', 'payment_sequential']
    ingestion_type = IngestionType.STREAM

class OrderItemMessage(DataMessage):
    topic = RawToBronzeTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']
    ingestion_type = IngestionType.STREAM

class EstimatedDeliberyDateMessage(DataMessage):
    topic = RawToBronzeTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']
    ingestion_type = IngestionType.STREAM

class ReviewMessage(DataMessage):
    topic = RawToBronzeTopic.REVIEW
    pk_column = ['review_id']
    ingestion_type = IngestionType.STREAM

if __name__ == "__main__":
    pass