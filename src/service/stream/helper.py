from service.stream.topic import BronzeTopic, SilverTopic
from service.producer.silver import *
from service.stream.base import *

def get_silver2gold_job(src_topic_name: str) -> BronzeToSilverJob:
    job: BronzeToSilverJob
    if src_topic_name == SilverTopic.PAYMENT:
        job = PaymentBronzeToSilverJob()

    elif src_topic_name == SilverTopic.ORDER_STATUS:
        job = OrderStatusBronzeToSilverJob()
        
    else:
        raise ValueError("There is no job for bronze")
    
    return job

def get_bronze2silver_job(src_topic_name: str) -> BronzeToSilverJob:
    job: BronzeToSilverJob
    if src_topic_name == BronzeTopic.REVIEW:
        job = ReviewBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.PAYMENT:
        job = PaymentBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.ORDER_STATUS:
        job = OrderStatusBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.PRODUCT:
        job = ProductBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.CUSTOMER:
        job = CustomerBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.SELLER:
        job = SellerBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.GEOLOCATION:
        job = GeolocationBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.ESTIMATED_DELIVERY_DATE:
        job = EstimatedDeliveryDateBronzeToSilverJob()

    elif src_topic_name == BronzeTopic.ORDER_ITEM:
        job = OrderItemBronzeToSilverJob()

    else:
        raise ValueError("There is no job for bronze")
    
    return job

def get_bronze2silver_producer(dst_topic_name: str) -> SparkProducer:
    if dst_topic_name == SilverTopic.REVIEW_CLEAN_COMMENT:
        producer = ReviewCleanCommentSilverProducer

    elif dst_topic_name == SilverTopic.REVIEW_METADATA:
        producer = ReviewMetadataSilverProducer

    elif dst_topic_name == SilverTopic.PAYMENT:
        producer = PaymentSilverProducer

    elif dst_topic_name == SilverTopic.ORDER_STATUS:
        producer = OrderStatusSilverProducer

    elif dst_topic_name == SilverTopic.PRODUCT:
        producer = ProductSilverProducer

    elif dst_topic_name == SilverTopic.CUSTOMER:
        producer = CustomerSilverProducer

    elif dst_topic_name == SilverTopic.SELLER:
        producer = SellerSilverProducer

    elif dst_topic_name == SilverTopic.GEOLOCATION:
        producer = GeolocationSilverProducer

    elif dst_topic_name == SilverTopic.ESTIMATED_DELIVERY_DATE:
        producer = EstimatedDeliveryDateSilverProducer

    elif dst_topic_name == SilverTopic.ORDER_ITEM:
        producer = OrderItemSilverProducer

    # DLQ
    # TODO: add condition after complete `TODO-producer`
    elif dst_topic_name == DeadLetterQueuerTopic.PRODUCT_DLQ:
        producer = ProductDeadLetterQueueSilverProducer

    elif dst_topic_name == DeadLetterQueuerTopic.PAYMENT_DLQ:
        producer = PaymentDeadLetterQueueSilverProducer

    else:
        raise ValueError("This topic is not destination")

    return producer