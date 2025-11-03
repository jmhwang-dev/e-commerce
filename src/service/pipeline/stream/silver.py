from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from .base import BaseStream
from ..batch.silver import *
from ..common.silver import *

from service.producer.bronze import BronzeAvroSchema
from service.producer.silver import SilverAvroSchema
from service.utils.spark import get_kafka_stream_df, start_console_stream
from service.utils.schema.reader import AvscReader

class SilverStream(BaseStream):

    dst_layer: Optional[str] = None
    dst_name: Optional[str] = None

    def __init__(self, is_dev:bool, process_time:str, spark_session: Optional[SparkSession] = None):
        self.process_time = process_time
        self.dst_layer = 'silver'
        self.dst_env = 'prod' if not is_dev else 'dev'
        self.spark_session = spark_session

class GeoCoordStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.GEO_COORD
        self.avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        self.geo_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeAvroSchema.GEOLOCATION),
            BronzeAvroSchema.GEOLOCATION
        ) \
        .select('zip_code', 'lng', 'lat', 'ingest_time') \
        .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
        .withWatermark('ingest_time', '1 days') \
        .dropDuplicates(['zip_code'])

    def transform(self):
        self.output_df = GeoCoordBase.transform(self.geo_stream)        
        self.set_byte_stream('zip_code', ['lng', 'lat'])
    
class OlistUserStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.OLIST_USER
        self.avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        # customer
        self.customer_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeAvroSchema.CUSTOMER),
            BronzeAvroSchema.CUSTOMER
        ).withWatermark('ingest_time', '1 days')

        # seller
        self.seller_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeAvroSchema.SELLER),
            BronzeAvroSchema.SELLER
        ).withWatermark('ingest_time', '1 days')

    def transform(self):
        self.output_df = OlistUserBase.transform(self.customer_stream, self.seller_stream)

    def load(self, micro_batch: DataFrame, batch_id: int):
        if micro_batch.isEmpty():
            return
        OlistUserBatch(micro_batch.sparkSession).load(micro_batch, batch_id)
        
    def get_query(self, process_time='5 seconds'):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checpoint_path) \
            .start()

class OrderEventStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        
        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.ORDER_EVENT
        self.avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        self.estimated_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeAvroSchema.ESTIMATED_DELIVERY_DATE), BronzeAvroSchema.ESTIMATED_DELIVERY_DATE)
        self.shippimt_limit_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeAvroSchema.ORDER_ITEM), BronzeAvroSchema.ORDER_ITEM)
        self.order_status_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeAvroSchema.ORDER_STATUS), BronzeAvroSchema.ORDER_STATUS)
            
    def transform(self):
        self.output_df = OrderEventBase.transform(self.estimated_df, self.shippimt_limit_df, self.order_status_df)
        self.set_byte_stream('order_id', ["data_type", "timestamp", "ingest_time"])


class ProductMetadataStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        
        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.PRODUCT_METADATA
        self.avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
        
    def extract(self):        
        self.product_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeAvroSchema.PRODUCT), BronzeAvroSchema.PRODUCT)
        self.product_df = self.product_df.withWatermark('ingest_time', '30 days')

        self.order_item_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeAvroSchema.ORDER_ITEM), BronzeAvroSchema.ORDER_ITEM)
        self.order_item_df = self.order_item_df.withWatermark('ingest_time', '30 days')

    def transform(self, ):
        self.output_df = ProductMetadataBase.transform(self.product_df, self.order_item_df)


    def load(self, micro_batch: DataFrame, batch_id: int):
        if micro_batch.isEmpty():
            return
        ProductMetadataBatch(micro_batch.sparkSession).load(micro_batch, batch_id)

    def get_query(self, process_time='5 seconds'):
        self.extract()
        self.transform()
        
        return self.output_df.writeStream \
            .trigger(processingTime=process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checpoint_path) \
            .start()
    
class OrderDetailStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        
        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.ORDER_DETAIL
        self.avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        self.order_item_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeAvroSchema.ORDER_ITEM), BronzeAvroSchema.ORDER_ITEM) \
            .withWatermark('ingest_time', '3 days')
        self.payment_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeAvroSchema.PAYMENT), BronzeAvroSchema.PAYMENT) \
            .withWatermark('ingest_time', '3 days')
        
    def transform(self):
        self.output_df = OrderDetailBase.transform(self.order_item_stream, self.payment_stream)
        self.set_byte_stream('order_id', ["product_id", "unit_price", "customer_id", "quantity"])

class ReviewMetadataStream(SilverStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        
        self.query_name = self.__class__.__name__
        self.dst_name = SilverAvroSchema.REVIEW_METADATA
        self.avsc_reader = AvscReader(self.dst_name)

        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        self.review_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeAvroSchema.REVIEW),
            BronzeAvroSchema.REVIEW)
    
    def transform(self,):
        self.output_df = ReviewMetadataBase.transform(self.review_stream)
        # TODO: check key column
        self.set_byte_stream('order_id', ["review_id", "review_creation_date", "review_answer_timestamp", "review_score"])