from service.producer.base.spark import SparkProducer
from service.stream.topic import SilverTopic, DeadLetterQueuerTopic

class PaymentSilverProducer(SparkProducer):
    topic = SilverTopic.PAYMENT
    pk_column = ['order_id', 'payment_sequential']

class ReviewCleanCommentSilverProducer(SparkProducer):
    topic = SilverTopic.REVIEW_CLEAN_COMMENT
    pk_column = ['review_id']

class ReviewMetadataSilverProducer(SparkProducer):
    topic = SilverTopic.REVIEW_METADATA
    pk_column = ['review_id']

class GeolocationSilverProducer(SparkProducer):
    topic = SilverTopic.GEOLOCATION
    pk_column = ['zip_code']

class CustomerSilverProducer(SparkProducer):
    topic = SilverTopic.CUSTOMER
    pk_column = ['customer_id']

class SellerSilverProducer(SparkProducer):
    topic = SilverTopic.SELLER
    pk_column = ['seller_id']

class ProductSilverProducer(SparkProducer):
    topic = SilverTopic.PRODUCT
    pk_column = ['product_id']

class OrderStatusSilverProducer(SparkProducer):
    topic = SilverTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']

class OrderItemSilverProducer(SparkProducer):
    topic = SilverTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']

class EstimatedDeliveryDateSilverProducer(SparkProducer):
    topic = SilverTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']

# DLQ
class ProductDeadLetterQueueSilverProducer(SparkProducer):
    topic = DeadLetterQueuerTopic.PRODUCT_DLQ
    pk_column = ['product_id']

class PaymentDeadLetterQueueSilverProducer(SparkProducer):
    topic = DeadLetterQueuerTopic.PAYMENT_DLQ
    pk_column = ['order_id', 'payment_sequential']