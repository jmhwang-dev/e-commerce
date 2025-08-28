import pandas as pd
from pathlib import Path
from functools import lru_cache

from service.producer.base.pandas import PandasProducer
from service.common.topic import *

class BronzeProducer(PandasProducer):
    file_path: Path = Path()

    @classmethod
    @lru_cache(maxsize=1)  # 자동 캐싱, maxsize=1로 한 번 로드 후 재사용
    def get_df(cls) -> pd.DataFrame:
        """TSV 파일 로드"""
        try:
            cls.file_path = DATASET_DIR / f"{cls.topic}.tsv"
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

    @staticmethod
    def mock_order_status_log(order_status_series: pd.Series) -> tuple[pd.DataFrame, str, str]:
        """
        Convert a pandas Series to a single-row DataFrame and extract status and order_id.
        """
        status = order_status_series['status']
        order_id = order_status_series['order_id']
        order_status_log = pd.DataFrame([order_status_series], index=[0])
        return order_status_log, status, order_id

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