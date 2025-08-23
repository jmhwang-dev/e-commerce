from enum import Enum

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic

from service.common.topic import *
from service.common.schema import *
from config.kafka import *

class IngestionType(Enum):
    CDC = 'cdc'
    STREAM = 'stream'

class BaseTopic:
    """Base class for Kafka topic definitions with common methods."""
    TOPIC_PREFIX: str = ""

    @classmethod
    def get_all_topics(cls) -> list[str]:
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

class BronzeTopic(BaseTopic):
    """Topics for raw data ingestion (raw to bronze)."""
    TOPIC_PREFIX = "bronze"
    
    # CDC Topics
    PRODUCT = "cdc_product" 
    CUSTOMER = "cdc_customer"
    SELLER = "cdc_seller"
    GEOLOCATION = "cdc_geolocation"
    
    # Stream Topics
    ORDER_ITEM = "stream_order_item"
    ORDER_STATUS = "stream_order_status"
    PAYMENT = "stream_payment"
    ESTIMATED_DELIVERY_DATE = "stream_estimated_delivery_date"
    REVIEW = "stream_review"

class SilverTopic(BaseTopic):
    """Topics for bronze to silver processing."""
    TOPIC_PREFIX = "silver"
    
    ORDER_ITEM = "order_item"
    PRODUCT = "product"
    CUSTOMER = "customer" 
    SELLER = "seller"
    GEOLOCATION = "geolocation"
    ORDER_STATUS = "order_status"
    PAYMENT = "payment"
    ESTIMATED_DELIVERY_DATE = "estimated_delivery_date"
    
    REVIEW_METADATA = "review_metadata"
    REVIEW_CLEAN_COMMENT = "review_clean_comment"
    REVIEW_INFERED = "review_infered"

    PAYMENT_DLQ = "payment_dlq"

class SilverToGoldTopic(BaseTopic):
    """Topics for silver to gold processing."""
    TOPIC_PREFIX = "gold"
    
    AGGREGATED_ORDER = "aggregated_order"