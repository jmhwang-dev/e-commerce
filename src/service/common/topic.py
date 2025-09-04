from typing import List
from enum import Enum

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic

from service.common.topic import *
from service.utils.schema.registry_manager import *

class IngestionType(Enum):
    CDC = 'cdc'
    STREAM = 'stream'

class BaseTopic:
    """Base class for Kafka topic definitions with common methods."""
    TOPIC_PREFIX: str = ""

    @classmethod
    def get_all_topics(cls) -> List[str]:
        """모든 토픽 이름을 prefix와 함께 반환"""
        topics = []
        for attr_name in dir(cls):
            # 메타클래스에서 이미 값을 변환했으므로, 값 자체를 추가하기만 하면 됨
            attr_value = getattr(cls, attr_name)
            if (isinstance(attr_value, str) and
                    attr_name.isupper() and
                    not attr_name.startswith('_') and
                    not attr_name.startswith('TOPIC_')):
                topics.append(attr_value)
        return topics
    
    @classmethod
    def get_cdc_topics(cls,) -> List[str]:
        return [cls.PRODUCT, cls.CUSTOMER, cls.SELLER, cls.GEOLOCATION, cls.ESTIMATED_DELIVERY_DATE, cls.ORDER_ITEM]
    
    @classmethod
    def get_stream_topics(cls,) -> List[str]:
        return [cls.ORDER_STATUS, cls.PAYMENT, cls.REVIEW]

class BronzeTopic(BaseTopic):
    """Topics for raw data ingestion (raw to bronze)."""
    TOPIC_PREFIX = "bronze"
    
    # CDC Topics
    PRODUCT = "cdc_product" 
    CUSTOMER = "cdc_customer"
    SELLER = "cdc_seller"
    GEOLOCATION = "cdc_geolocation"
    ESTIMATED_DELIVERY_DATE = "cdc_estimated_delivery_date"
    ORDER_ITEM = "cdc_order_item"
    
    # Stream Topics
    ORDER_STATUS = "stream_order_status"
    PAYMENT = "stream_payment"
    REVIEW = "stream_review"

class SilverTopic(BaseTopic):
    """Topics for bronze to silver processing."""
    TOPIC_PREFIX = "silver"

    PRODUCT = "product"
    CUSTOMER = "customer" 
    SELLER = "seller"
    GEOLOCATION = "geolocation"
    PAYMENT = "payment"
    ORDER_ITEM = "order_item"
    ORDER_STATUS = "order_status"
    ESTIMATED_DELIVERY_DATE = "estimated_delivery_date"
    
    REVIEW_METADATA = "review_metadata"
    REVIEW_CLEAN_COMMENT = "review_clean_comment"
    REVIEW_CONSISTENT_SENTIMENT = "review_consistent_sentiment"
    REVIEW_CONFLICT_SENTIMENT = "review_conflict_sentiment"
    
    # DLQ topic
    PAYMENT_DLQ = "payment_dlq"