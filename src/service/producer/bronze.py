from typing import Union
import pandas as pd
from pathlib import Path
from functools import lru_cache

from service.producer.base.pandas import PandasProducer
from service.stream.topic import *
from config.kafka import DATASET_DIR

class BronzeProducer(PandasProducer):
    file_path: Path = Path()

    @classmethod
    @lru_cache(maxsize=1)  # 자동 캐싱, maxsize=1로 한 번 로드 후 재사용
    def get_df(cls) -> pd.DataFrame:
        """TSV 파일 로드"""
        try:
            cls.file_path = DATASET_DIR / f"{cls.dst_topic}.tsv"
            timestamp_cols = ['estimated_delivery_date', 'shipping_limit_date', 'timestamp', 'review_creation_date', 'review_answer_timestamp']
            df = pd.read_csv(cls.file_path, sep='\t')

            for col in df.columns:
                if col not in timestamp_cols:
                    continue
                df[col] = pd.to_datetime(df[col], errors='coerce')

            for pk_col in cls.pk_column:
                if pk_col not in df.columns:
                    raise ValueError(f"Column {pk_col} not found in {cls.file_path}")
            return df
        except FileNotFoundError:
            raise ValueError(f"File {cls.file_path} not found")

    @classmethod
    def select(cls, log: Union[pd.Series, pd.DataFrame], fk_col: str) -> Optional[pd.DataFrame]:
        if log.empty:
            return pd.DataFrame()
        
        if isinstance(log, pd.Series):
            value = log[fk_col]
        else:
            value = log[fk_col].iloc[0]
        
        try:
            df = cls.get_df()
            return df[df[fk_col] == value]
        except:
            return None
            
class GeolocationBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.GEOLOCATION
    pk_column = ['zip_code']
    
class CustomerBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.CUSTOMER
    pk_column = ['customer_id']
    
class SellerBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.SELLER
    pk_column = ['seller_id']

class ProductBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.PRODUCT
    pk_column = ['product_id']
    
class OrderStatusBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']

    # @staticmethod
    # def mock_order_status_log(order_status_series: pd.Series) -> tuple[pd.DataFrame, str, str]:
    #     """
    #     Convert a pandas Series to a single-row DataFrame and extract status and order_id.
    #     """
    #     status = order_status_series['status']
    #     order_id = order_status_series['order_id']
    #     order_status_log = pd.DataFrame([order_status_series], index=[0])
    #     return order_status_log, status, order_id

class PaymentBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.PAYMENT
    pk_column = ['order_id', 'payment_sequential']
    
class OrderItemBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']

class EstimatedDeliberyDateBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']
    
class ReviewBronzeProducer(BronzeProducer):
    dst_topic = BronzeTopic.REVIEW
    pk_column = ['review_id']
    end_timestamp: Optional[pd.Timestamp] = None

    @classmethod
    def select(cls, log: Union[pd.Series, pd.DataFrame], new_timestamp) -> Optional[pd.DataFrame]:
        if log.empty:
            return None
        
        df = cls.get_df()
        if cls.end_timestamp is None:
            review_in_scope = df[df['review_creation_date'] < new_timestamp]
        else:
            condition = (cls.end_timestamp <= df['review_creation_date']) & ( df['review_creation_date'] < new_timestamp)
            review_in_scope = df[condition]

        if not review_in_scope.empty:
            cls.end_timestamp = new_timestamp
            
        return review_in_scope