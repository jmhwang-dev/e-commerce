from pyspark.sql.dataframe import DataFrame

from service.init.kafka import *
from service.producer.base import *
from service.io.utils import *

class GeolocationSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.GEOLOCATION
    pk_column = ['zip_code']

class CustomerSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.CUSTOMER
    pk_column = ['customer_id']

class SellerSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.SELLER
    pk_column = ['seller_id']

class ProductSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.PRODUCT
    pk_column = ['product_id']

class OrderStatusSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']

class PaymentSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.PAYMENT
    pk_column = ['order_id', 'payment_sequential']

class OrderItemSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']

class EstimatedDeliberyDateSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']

class ReviewCleanCommentSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.REVIEW_CLEAN_COMMENT
    pk_column = ['review_id']

    @classmethod
    def publish(cls, publish_df: DataFrame, schema_str:str):
        s3_uri, _, table_name = get_iceberg_destination(schema_str)
        query = publish_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
            .option("topic", cls.topic) \
            .option("checkpointLocation", f"s3://{s3_uri}/checkpoints/{table_name}") \
            .trigger(processingTime="10 seconds") \
            .start()