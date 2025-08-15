from pathlib import Path
from typing import Iterable
import pandas as pd
from pprint import pformat
from functools import lru_cache
from service.init.kafka import *
from copy import deepcopy

class RawMessage:
    topic: str = ''
    pk_column: Iterable[str] = []
    file_path: Path = Path()
    current_index: int = 0
    producer: SerializingProducer = None

    @classmethod
    def init_file_path(cls, ) -> None:
        cls.file_path = DATASET_DIR / f"{cls.topic}.tsv"

    
    @classmethod
    @lru_cache(maxsize=1)
    def get_confluent_producer(cls, bootstrap_servers:str=BOOTSTRAP_SERVERS_EXTERNAL) -> SerializingProducer:
        client = SchemaRegistryManager._get_client()
        schema_obj = client.get_latest_version(cls.topic).schema

        bootstrap_server_list = bootstrap_servers.split(',')

        avro_serializer = AvroSerializer(
            client,
            schema_obj,  # None으로 두면 subject 기반으로 fetch
            to_dict=lambda obj, ctx: obj,
            conf={
                'auto.register.schemas': False,
                'subject.name.strategy': lambda ctx, record_name: cls.topic
                }
        )

        producer_conf = {
            'bootstrap.servers': bootstrap_server_list[0],
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': avro_serializer,
            'acks': 'all',
            'retries': 3
        }
        return SerializingProducer(producer_conf)
    
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
        producer = cls.get_confluent_producer()
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
class GeolocationMessage(RawMessage):
    topic = RawToBronzeTopic.GEOLOCATION
    pk_column = ['zip_code']
    ingestion_type = IngestionType.CDC

class CustomerMessage(RawMessage):
    topic = RawToBronzeTopic.CUSTOMER
    pk_column = ['customer_id']
    ingestion_type = IngestionType.CDC

class SellerMessage(RawMessage):
    topic = RawToBronzeTopic.SELLER
    pk_column = ['seller_id']
    ingestion_type = IngestionType.CDC

class ProductMessage(RawMessage):
    topic = RawToBronzeTopic.PRODUCT
    pk_column = ['product_id']
    ingestion_type = IngestionType.CDC

# STREAM
class OrderStatusMessage(RawMessage):
    topic = RawToBronzeTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']
    ingestion_type = IngestionType.STREAM

    @classmethod
    def get_current_event(cls,) -> pd.Series:
        return cls.get_df().iloc[cls.current_index]
    
    @classmethod
    def is_end(cls):
        return cls.current_index == len(cls.get_df())

class PaymentMessage(RawMessage):
    topic = RawToBronzeTopic.PAYMENT
    pk_column = ['order_id', 'payment_sequential']
    ingestion_type = IngestionType.STREAM

class OrderItemMessage(RawMessage):
    topic = RawToBronzeTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']
    ingestion_type = IngestionType.STREAM

class EstimatedDeliberyDateMessage(RawMessage):
    topic = RawToBronzeTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']
    ingestion_type = IngestionType.STREAM

class ReviewMessage(RawMessage):
    topic = RawToBronzeTopic.REVIEW
    pk_column = ['review_id']
    ingestion_type = IngestionType.STREAM

if __name__ == "__main__":
    pass