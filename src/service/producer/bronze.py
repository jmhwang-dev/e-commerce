import pandas as pd

from pathlib import Path
from typing import Iterable
from pprint import pformat
from functools import lru_cache
from service.common.topic import *
from copy import deepcopy

from confluent_kafka import SerializingProducer

from service.common.schema import *
from service.utils.kafka import *
from service.common.topic import *

class BronzeProducer:
    topic: str = ''
    pk_column: Iterable[str] = []
    file_path: Path = Path()
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
        # TODO: 불필요하게 `use_internal` 파라미터를` 계속 받아야 하는 문제 해결 필요
        return get_confluent_kafka_producer(cls.topic, use_internal)

    @classmethod
    def select(cls, col, value: str) -> pd.DataFrame:
        df = cls.get_df()
        return df[df[col] == value]
        
    @classmethod
    def publish(cls, _event: pd.DataFrame | pd.Series, use_internal=False) -> None:
        if _event.empty:
            print(f'\nEmpty message: {cls.topic}')
            return
        producer = cls._get_producer(use_internal)
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

# CDC
class GeolocationBronzeProducer(BronzeProducer):
    topic = BronzeTopic.GEOLOCATION
    pk_column = ['zip_code']
    ingestion_type = IngestionType.CDC

class CustomerBronzeProducer(BronzeProducer):
    topic = BronzeTopic.CUSTOMER
    pk_column = ['customer_id']
    ingestion_type = IngestionType.CDC

class SellerBronzeProducer(BronzeProducer):
    topic = BronzeTopic.SELLER
    pk_column = ['seller_id']
    ingestion_type = IngestionType.CDC

class ProductBronzeProducer(BronzeProducer):
    topic = BronzeTopic.PRODUCT
    pk_column = ['product_id']
    ingestion_type = IngestionType.CDC

# STREAM
class OrderStatusBronzeProducer(BronzeProducer):
    topic = BronzeTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']
    ingestion_type = IngestionType.STREAM

class PaymentBronzeProducer(BronzeProducer):
    topic = BronzeTopic.PAYMENT
    pk_column = ['order_id', 'payment_sequential']
    ingestion_type = IngestionType.STREAM

class OrderItemBronzeProducer(BronzeProducer):
    topic = BronzeTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']
    ingestion_type = IngestionType.STREAM

class EstimatedDeliberyDateBronzeProducer(BronzeProducer):
    topic = BronzeTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']
    ingestion_type = IngestionType.STREAM

class ReviewBronzeProducer(BronzeProducer):
    topic = BronzeTopic.REVIEW
    pk_column = ['review_id']
    ingestion_type = IngestionType.STREAM