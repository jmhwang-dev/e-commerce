from service.producer.base.spark import SparkProducer
from service.stream.topic import SilverTopic

class PaymentSilverProducer(SparkProducer):
    dst_topic = SilverTopic.STREAM_PAYMENT
    key_column = ['order_id', 'payment_sequential']

class OrderStatusSilverProducer(SparkProducer):
    dst_topic = SilverTopic.STREAM_ORDER_STATUS
    key_column = ['order_id', 'status']

class PaymentDeadLetterQueueSilverProducer(SparkProducer):
    dst_topic = SilverTopic.STREAM_PAYMENT_DLQ
    key_column = ['order_id', 'payment_sequential']

# class ReviewCleanCommentSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.REVIEW_CLEAN_COMMENT
#     key_column = ['review_id']

# class ReviewMetadataSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.REVIEW_METADATA
#     key_column = ['review_id']

# class GeolocationSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.GEOLOCATION
#     key_column = ['zip_code']

# class CustomerSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.CUSTOMER
#     key_column = ['customer_id']

# class SellerSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.SELLER
#     key_column = ['seller_id']

# class ProductSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.PRODUCT
#     key_column = ['product_id']

# class OrderItemSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.ORDER_ITEM
#     key_column = ['order_id', 'order_item_id']

# class EstimatedDeliveryDateSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.ESTIMATED_DELIVERY_DATE
#     key_column = ['order_id']

# # DLQ
# # TODO-producer: add schema(.avsc) and producer class for other topics,
# class ProductDeadLetterQueueSilverProducer(SparkProducer):
#     dst_topic = DeadLetterQueuerTopic.PRODUCT_DLQ
#     key_column = ['product_id']