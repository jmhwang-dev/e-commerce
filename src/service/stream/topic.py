from typing import List
from enum import Enum

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic

from service.stream.topic import *
from service.utils.schema.registry_manager import *

class BaseTopic:
    """Base class for Kafka topic definitions with common methods."""
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

class BronzeTopic(BaseTopic):
    PRODUCT = "product" 
    CUSTOMER = "customer"
    SELLER = "seller"
    GEOLOCATION = "geolocation"
    ESTIMATED_DELIVERY_DATE = "estimated_delivery_date"
    ORDER_ITEM = "order_item"    
    ORDER_STATUS = "order_status"
    PAYMENT = "payment"
    REVIEW = "review"

class SilverTopic(BaseTopic):
    REVIEW_CLEAN_COMMENT = "review_clean_comment" 
    REVIEW_METADATA = "review_metadata"
    ORDER_EVENT = "order_event"
    ORDER_DETAIL = "order_detail"