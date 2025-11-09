from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from .base import BaseStream
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema
from service.pipeline.common.gold import *
from service.pipeline.batch.gold import DimUserLocationBatch, FactOrderLeadDaysBatch, OrderDetailBatch

from service.utils.spark import get_kafka_stream_df, start_console_stream
from service.utils.schema.reader import AvscReader


class GoldStream(BaseStream):
    dst_layer: Optional[str] = None
    dst_name: Optional[str] = None

    def __init__(self, is_dev:bool, process_time:str, spark_session: Optional[SparkSession] = None):
        self.process_time = process_time
        self.dst_layer = 'gold'
        self.dst_env = 'prod' if not is_dev else 'dev'
        self.spark_session = spark_session

class DimUserLocationStream(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.DIM_USER_LOCATION
        self.dst_avsc_reader = AvscReader(self.dst_name)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
    
    def extract(self):
        geo_coord_avsc_reader = AvscReader(SilverAvroSchema.GEO_COORD)
        self.geo_coord_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, geo_coord_avsc_reader.table_name),
            geo_coord_avsc_reader
        )

        olist_user_avsc_reader = AvscReader(SilverAvroSchema.OLIST_USER)
        self.olist_user_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, olist_user_avsc_reader.table_name),
            olist_user_avsc_reader
        )

    def transform(self,):
        self.geo_coord_stream = self.geo_coord_stream.withWatermark('ingest_time', '30 days')
        self.olist_user_stream = self.olist_user_stream.withWatermark('ingest_time', '30 days')

        joined_df = self.geo_coord_stream \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())).alias('geo_coord') \
            .join(self.olist_user_stream.alias('olist_user'), 
                F.expr("""
                geo_coord.zip_code = olist_user.zip_code AND
                geo_coord.ingest_time >= olist_user.ingest_time - INTERVAL 30 DAYS AND
                geo_coord.ingest_time <= olist_user.ingest_time + INTERVAL 30 DAYS
                """), "fullouter")

        self.output_df = joined_df.select(
            F.coalesce(F.col("geo_coord.zip_code"), F.col("olist_user.zip_code")).alias("zip_code"),
            F.col("geo_coord.lat").alias("lat"),
            F.col("geo_coord.lng").alias("lng"),
            F.col("olist_user.user_id").alias("user_id"),
            F.col("olist_user.user_type").alias("user_type")
        ).dropDuplicates()
        
    def load(self, micro_batch:DataFrame, batch_id: int):
        DimUserLocationBatch(micro_batch.sparkSession).load(micro_batch, batch_id)
        
    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()

class FactOrderLeadDaysStream(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.FACT_ORDER_LEAD_DAYS
        self.dst_avsc_reader = AvscReader(self.dst_name)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        order_event_avsc_reader = AvscReader(SilverAvroSchema.ORDER_EVENT)
        self.order_event_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, order_event_avsc_reader.table_name),
            order_event_avsc_reader
        )

    def transform(self,):
        self.order_event_stream = self.order_event_stream.withWatermark('ingest_time', '30 days')
        self.output_df = FactOrderLeadDaysBase.transform(self.order_event_stream)

        # start_console_stream(self.output_df.filter(F.col('order_id') == '1add1aa4c6e709fd74a4eb691a9fc6bc'), output_mode='complete')
        
    def load(self, micro_batch:DataFrame, batch_id: int):
        FactOrderLeadDaysBatch(micro_batch.sparkSession).load(micro_batch, batch_id)

    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .outputMode('update') \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()

class OrderDetailStream(GoldStream):
    def __init__(self, is_dev: bool, process_time: str, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.ORDER_DETAIL
        self.dst_avsc_reader = AvscReader(self.dst_name)
        
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
        
    def extract(self):
        customer_avsc_reader = AvscReader(SilverAvroSchema.CUSTOMER_ORDER)
        self.customer_order_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, customer_avsc_reader.table_name),
            customer_avsc_reader
        )

        product_avsc_reader = AvscReader(SilverAvroSchema.PRODUCT_METADATA)
        self.product_metadata_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, product_avsc_reader.table_name),
            product_avsc_reader
        )

    def transform(self):
        self.customer_order_stream = self.customer_order_stream.withWatermark('ingest_time', '30 days')
        self.product_metadata_stream = self.product_metadata_stream.withWatermark('ingest_time', '30 days')
        
        # join 후 바로 중복 컬럼 병합 (product_id는 co 쪽 선택, ingest_time은 greatest)
        joined_df = self.customer_order_stream.alias('co').join(
            self.product_metadata_stream.alias('pm'),
            on=F.expr("""
                co.product_id = pm.product_id AND
                co.ingest_time >= pm.ingest_time - INTERVAL 30 DAYS AND
                co.ingest_time <= pm.ingest_time + INTERVAL 30 DAYS
            """),
            how='fullouter'
        ).select(
            F.col('co.order_id').alias('order_id'),
            F.col('co.customer_id').alias('customer_id'),
            F.col('co.product_id').alias('product_id'),  # co.product_id 선택 (pm drop)
            F.col('pm.category').alias('category'),
            F.col('co.quantity').alias('quantity'),
            F.col('co.unit_price').alias('unit_price'),
            F.col('pm.seller_id').alias('seller_id'),
            F.greatest(F.col('co.ingest_time'), F.col('pm.ingest_time')).alias('ingest_time')
        ).dropna()
            
        common_columns = ["order_id", "product_id", "category", "quantity", "unit_price"]
        order_seller_df = joined_df \
            .select(*(common_columns + ['seller_id'])) \
            .withColumnRenamed('seller_id', 'user_id')

        order_customer_df = joined_df \
            .select(*(common_columns + ['customer_id'])) \
            .withColumnRenamed('customer_id', 'user_id')
        
        self.output_df = order_seller_df.unionByName(order_customer_df).dropDuplicates()
    
    def load(self, micro_batch:DataFrame, batch_id: int):
        OrderDetailBatch(micro_batch.sparkSession).load(micro_batch, batch_id)

    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()