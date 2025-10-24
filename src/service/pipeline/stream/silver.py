from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession

from pyspark.sql.types import StructType
from typing import Union, Optional

from ..base import BaseJob
from service.utils.iceberg import write_iceberg
from schema.silver import *
from service.producer.bronze import BronzeTopic
from service.utils.spark import get_spark_session
from service.utils.schema.reader import AvscReader
from service.utils.helper import get_producer

from service.utils.spark import get_deserialized_avro_stream_df, get_kafka_stream_df, stop_streams, start_console_stream

class StreamSilverJob(BaseJob):
    src_namespace: str = 'bronze'
    dst_namesapce: str = "silver"

    output_df: Optional[DataFrame] = None
    schema: Optional[StructType] = None

    def __init__(self, spark_session: Optional[SparkSession] = None):
        self._dev = True
        self.spark_session = get_spark_session(f"{self.job_name}", dev=self._dev) if spark_session is None else spark_session

 

class Account(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.CUSTOMER, BronzeTopic.SELLER]
        
        self.schema = ACCOUNT
        self.dst_table_name = 'account'
        self.initialize_table()

    def extract(self,):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)

    def transform(self, micro_batch:DataFrame, batch_id: int):
        self.output_df = micro_batch.sparkSession.createDataFrame([], schema=self.schema)
        new_column = 'user_id'

        for topic_name in self.src_topic_names:
            if topic_name == BronzeTopic.CUSTOMER:
                user_type = F.lit('customer')
                existing_column = 'customer_id'
            else:
                user_type = F.lit('seller')
                existing_column = 'seller_id'

            deser_df = self.get_topic_df(micro_batch, topic_name)
            deser_df = deser_df.drop('ingest_time') \
                .withColumnRenamed(existing_column, new_column) \
                .withColumn('user_type', user_type)
            
            self.output_df = self.output_df.unionByName(deser_df)
        
        self.output_df = self.output_df.dropDuplicates()
        self.load()

    def load(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.output_df.sparkSession.sql(
            f"""
            merge into {self.dst_table_identifier} t
            using {self.dst_table_name} s
            on t.user_id = s.user_id
            when not matched then
                insert (zip_code, user_type, user_id)
                values (s. zip_code, s.user_type, s.user_id)
            """)
        self.get_current_dst_count()

class GeoCoordinate(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.GEOLOCATION]
        
        self.schema = GEO_COORDINATE
        self.dst_table_name = 'geo_coordinate'
        self.initialize_table()

    def extract(self,):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)

        # When `readStream` is `iceberg`
        # self.src_df = self.spark_session.readStream.format('iceberg').load(f'{self.src_namespace}.{BronzeTopic.GEOLOCATION}')
        # end

    def transform(self, micro_batch:DataFrame, batch_id: int):
        # TODO: Consider key type conversion for message publishing
        
        # When `readStream` is `iceberg`
        # self.output_df = micro_batch \
        #     .select('zip_code', 'lng', 'lat') \
        #     .dropDuplicates() \
        #     .withColumn('zip_code', F.col('zip_code').cast(IntegerType()))
        # end

        topic_name = self.src_topic_names[0]
        deser_df = self.get_topic_df(micro_batch, topic_name)

        self.output_df = deser_df \
            .select('zip_code', 'lng', 'lat') \
            .dropDuplicates() \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType()))
            
        self.load()
        self.get_current_dst_count()
        self.output_df.show()

    def load(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.output_df.sparkSession.sql(
            f"""
            merge into {self.dst_table_identifier} t
            using {self.dst_table_name} s
            on t.zip_code = s.zip_code
            when not matched then
                insert (zip_code, lng, lat)
                values (s.zip_code, s.lng, s.lat)
            """)

class OrderStatusTimeline(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.ESTIMATED_DELIVERY_DATE, BronzeTopic.ORDER_ITEM, BronzeTopic.ORDER_STATUS]
        
        self.schema = ORDER_STATUS_TIMELINE
        self.dst_table_name = 'order_status_timeline'
        self.initialize_table()

    def extract(self):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)

    def transform(self, micro_batch:DataFrame, batch_id: int):
        estimated_df = self.get_topic_df(micro_batch, BronzeTopic.ESTIMATED_DELIVERY_DATE) \
            .withColumnRenamed('estimated_delivery_date', 'timestamp') \
            .withColumn('status', F.lit('estimated_delivery')) \
            .drop('ingest_time')
        
        shippimt_limit_df = self.get_topic_df(micro_batch, BronzeTopic.ORDER_ITEM) \
            .select('order_id', 'shipping_limit_date') \
            .withColumnRenamed('shipping_limit_date', 'timestamp') \
            .withColumn('status', F.lit('shipping_limit')) \
            .dropDuplicates()
        
        order_status_df = self.get_topic_df(micro_batch, BronzeTopic.ORDER_STATUS) \
            .drop('ingest_time')

        order_status_timeline_df = order_status_df.unionByName(estimated_df).unionByName(shippimt_limit_df)

        self.output_df = order_status_timeline_df \
            .groupBy('order_id') \
            .agg(
                F.max(F.when(F.col('status') == 'purchase', F.col('timestamp'))).alias('purchase'),
                F.max(F.when(F.col('status') == 'approved', F.col('timestamp'))).alias('approve'),
                F.max(F.when(F.col('status') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier'),
                F.max(F.when(F.col('status') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer'),
                F.max(F.when(F.col('status') == 'shipping_limit', F.col('timestamp'))).alias('shipping_limit'),
                F.max(F.when(F.col('status') == 'estimated_delivery', F.col('timestamp'))).alias('estimated_delivery'),
            )
        
        self.load()

    def load(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        
        self.output_df.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_table_identifier} t
            USING {self.dst_table_name} s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.purchase = COALESCE(s.purchase, t.purchase),
                    t.approve = COALESCE(s.approve, t.approve),
                    t.delivered_carrier = COALESCE(s.delivered_carrier, t.delivered_carrier),
                    t.delivered_customer = COALESCE(s.delivered_customer, t.delivered_customer),
                    t.shipping_limit = COALESCE(s.shipping_limit, t.shipping_limit),
                    t.estimated_delivery = COALESCE(s.estimated_delivery, t.estimated_delivery)
            WHEN NOT MATCHED THEN
                INSERT (order_id, purchase, approve, delivered_carrier, delivered_customer, shipping_limit, estimated_delivery)
                VALUES (s.order_id, s.purchase, s.approve, s.delivered_carrier, s.delivered_customer, s.shipping_limit, s.estimated_delivery)
            """)
        
class OrderItemTransformer(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.job_name = self.__class__.__name__
        super().__init__(spark_session, BronzeTopic.ORDER_ITEM)
        self.set_dst_table('delivery_limit', DELIVERY_LIMIT)
        # self.set_dst_table('order_product_stakeholder', DELIVERY_LIMIT)
        # self.set_dst_table('order_transaction', DELIVERY_LIMIT)

    def transform(self, micro_batch:DataFrame, batch_id: int):
        self.output_df = micro_batch.select('order_id', 'shipping_limit_date')
        self.load('delivery_limit')
        self.get_current_dst_count()
        self.dst_df.show(n=10)

    def load(self, dst_table_name):
        self.output_df.createOrReplaceTempView(dst_table_name)
        self.output_df.sparkSession.sql(
            f"""
            merge into {self.dst_table_identifier} t
            using {dst_table_name} s
            on t.order_id = s.order_id
            when matched then
                update set t.shipping_limit = s.shipping_limit_date
            when not matched then
                insert (order_id, shipping_limit)
                values (s.order_id, s.shipping_limit_date)
            """)