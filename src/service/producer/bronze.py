from typing import Union, Optional
import pandas as pd
from pathlib import Path
from functools import lru_cache, reduce

from service.producer.base.pandas import PandasProducer
from service.utils.schema.avsc import *
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

            if not cls.key_column in df.columns:
                raise ValueError(f"Column {cls.key_column} not found in {cls.file_path}")
            return df
        except FileNotFoundError:
            raise ValueError(f"File {cls.file_path} not found")

    @classmethod
    def select(cls, log: Union[pd.Series, pd.DataFrame], fk_col: List[str]) -> Optional[pd.DataFrame]:
        if log.empty:
            return pd.DataFrame()
        
        df = cls.get_df()

        if isinstance(log, pd.Series):
            values = log[fk_col]
            conditions = [df[col] == val for col, val in zip(fk_col, values)]
            final_condition = reduce(lambda x, y: x & y, conditions)
        else:
            values = log[fk_col].drop_duplicates()  # DataFrame
            conditions = [df[col].isin(values[col].tolist()) for col in fk_col]
            final_condition = reduce(lambda x, y: x & y, conditions)

        return df[final_condition]
            
class GeolocationBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.GEOLOCATION
    key_column = 'zip_code'
    
class CustomerBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.CUSTOMER
    key_column = 'customer_id'
    
class SellerBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.SELLER
    key_column = 'seller_id'

###
class ProductBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.PRODUCT
    key_column = 'product_id'

class OrderStatusBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.ORDER_STATUS
    key_column = 'order_id'

class PaymentBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.PAYMENT
    key_column = 'order_id'
    
class OrderItemBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.ORDER_ITEM
    key_column = 'order_id'
###

class EstimatedDeliberyDateBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.ESTIMATED_DELIVERY_DATE
    key_column = 'order_id'
    
class ReviewBronzeProducer(BronzeProducer):
    dst_topic = BronzeAvroSchema.REVIEW
    key_column = 'review_id'
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