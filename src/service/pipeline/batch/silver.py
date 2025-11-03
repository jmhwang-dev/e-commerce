from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from .base import BaseBatch
from ..common.silver import *

from service.producer.silver import SilverAvroSchema
from service.utils.iceberg import write_iceberg
from service.utils.spark import get_spark_session

class SilverBatch(BaseBatch):
    src_namespace: str = 'bronze'
    dst_namespace: str = "silver"
    
class GeoCoordBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.GEO_COORD)

    def extract(self):
        self.geo_df = self.spark_session.read.table("bronze.geolocation") \
            .select('zip_code', 'lng', 'lat', 'ingest_time') \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
            .dropDuplicates(['zip_code'])

    def transform(self):
        self.output_df = GeoCoordBase.transform(self.geo_df)

    def load(self,):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='w')
    
class OlistUserBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.OLIST_USER)

    def extract(self):
        self.customer_df = self.spark_session.read.table("bronze.customer")
        self.seller_df = self.spark_session.read.table("bronze.seller")

    def transform(self):
        self.output_df = OlistUserBase.transform(self.customer_df, self.seller_df)

    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        # watermark 기간에서 제외된 누락된 데이터는 배치로 처리
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_table_identifier} AS target
            USING updates AS source
            ON target.user_type = source.user_type AND target.user_id = source.user_id
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.get_current_dst_count(output_df.sparkSession, batch_id)
        
class OrderEventBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.ORDER_EVENT)

    def extract(self):
        self.estimated_df = self.spark_session.read.table("bronze.estimated_delivery_date")
        self.shippimt_limit_df = self.spark_session.read.table("bronze.order_item")
        self.order_status_df = self.spark_session.read.table("bronze.order_status")
            
    def transform(self):
        self.output_df = OrderEventBase.transform(self.estimated_df, self.shippimt_limit_df, self.order_status_df)

    def load(self):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='w')
        self.get_current_dst_count(self.output_df.sparkSession)


class ProductMetadataBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.PRODUCT_METADATA)

    def extract(self):        
        self.product_df = self.spark_session.read.table("bronze.product")
        self.order_item_df = self.spark_session.read.table("bronze.order_item")

    def transform(self, ):
        self.output_df = ProductMetadataBase.transform(self.product_df, self.order_item_df)

    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        # watermark 기간에서 제외된 누락된 데이터는 배치로 처리
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING updates s
            ON t.product_id = s.product_id AND t.seller_id = s.seller_id
            WHEN MATCHED AND t.category IS NULL AND s.category IS NOT NULL THEN
                UPDATE SET t.category = s.category
            WHEN NOT MATCHED AND s.seller_id IS NOT NULL THEN
                INSERT (product_id, category, seller_id)
                VALUES (s.product_id, s.category, s.seller_id)
        """)

        self.get_current_dst_count(output_df.sparkSession, batch_id)
    
class OrderDetailBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.ORDER_DETAIL)

    def extract(self):
        self.order_item_df = self.spark_session.read.table("bronze.order_item")
        self.payment_df = self.spark_session.read.table("bronze.payment")
        
    def transform(self):
        self.output_df = OrderDetailBase.transform(self.order_item_df, self.payment_df)

    def load(self):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='w')
        self.get_current_dst_count(self.output_df.sparkSession)
    
class ReviewMetadataBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.REVIEW_METADATA)

    def extract(self):
        self.review_df = self.spark_session.read.table("bronze.review")
    
    def transform(self,):
        self.output_df = ReviewMetadataBase.transform(self.review_df)

    def load(self):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='w')
        self.get_current_dst_count(self.output_df.sparkSession)