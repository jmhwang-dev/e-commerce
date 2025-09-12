from service.producer.base.spark import SparkProducer
from service.stream.topic import SilverTopic

class PaymentSilverProducer(SparkProducer):
    dst_topic = SilverTopic.STREAM_PAYMENT
    pk_column = ['order_id', 'payment_sequential']

class OrderStatusSilverProducer(SparkProducer):
    dst_topic = SilverTopic.STREAM_ORDER_STATUS
    pk_column = ['order_id', 'status']

class PaymentDeadLetterQueueSilverProducer(SparkProducer):
    dst_topic = SilverTopic.STREAM_PAYMENT_DLQ
    pk_column = ['order_id', 'payment_sequential']

# class ReviewCleanCommentSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.REVIEW_CLEAN_COMMENT
#     pk_column = ['review_id']

# class ReviewMetadataSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.REVIEW_METADATA
#     pk_column = ['review_id']

# class GeolocationSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.GEOLOCATION
#     pk_column = ['zip_code']

# class CustomerSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.CUSTOMER
#     pk_column = ['customer_id']

# class SellerSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.SELLER
#     pk_column = ['seller_id']

# class ProductSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.PRODUCT
#     pk_column = ['product_id']

# class OrderItemSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.ORDER_ITEM
#     pk_column = ['order_id', 'order_item_id']

# class EstimatedDeliveryDateSilverProducer(SparkProducer):
#     dst_topic = SilverTopic.ESTIMATED_DELIVERY_DATE
#     pk_column = ['order_id']

# # DLQ
# # TODO-producer: add schema(.avsc) and producer class for other topics,
# class ProductDeadLetterQueueSilverProducer(SparkProducer):
#     dst_topic = DeadLetterQueuerTopic.PRODUCT_DLQ
#     pk_column = ['product_id']