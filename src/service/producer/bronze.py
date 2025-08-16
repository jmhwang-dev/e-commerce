import pandas as pd

from service.init.kafka import *
from service.producer.base import *

# CDC
class GeolocationBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.GEOLOCATION
    pk_column = ['zip_code']
    ingestion_type = IngestionType.CDC

class CustomerBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.CUSTOMER
    pk_column = ['customer_id']
    ingestion_type = IngestionType.CDC

class SellerBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.SELLER
    pk_column = ['seller_id']
    ingestion_type = IngestionType.CDC

class ProductBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.PRODUCT
    pk_column = ['product_id']
    ingestion_type = IngestionType.CDC

# STREAM
class OrderStatusBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']
    ingestion_type = IngestionType.STREAM

    @classmethod
    def get_current_event(cls,) -> pd.Series:
        return cls.get_df().iloc[cls.current_index]
    
    @classmethod
    def is_end(cls):
        return cls.current_index == len(cls.get_df())

class PaymentBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.PAYMENT
    pk_column = ['order_id', 'payment_sequential']
    ingestion_type = IngestionType.STREAM

class OrderItemBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']
    ingestion_type = IngestionType.STREAM

class EstimatedDeliberyDateBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']
    ingestion_type = IngestionType.STREAM

class ReviewBronzeProducer(BaseProducer):
    topic = RawToBronzeTopic.REVIEW
    pk_column = ['review_id']
    ingestion_type = IngestionType.STREAM