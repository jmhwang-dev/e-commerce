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
        self.output_df = self.output_df.dropDuplicates()

    def load(self,):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
    
class OlistUserBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.OLIST_USER)

    def extract(self):
        self.customer_df = self.spark_session.read.table("bronze.customer")
        self.seller_df = self.spark_session.read.table("bronze.seller")

    def transform(self):
        self.output_df = OlistUserBase.transform(self.customer_df, self.seller_df)
        self.output_df = self.output_df.drop('ingest_time')
        self.output_df = self.output_df.dropDuplicates()

    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        # watermark 기간에서 제외된 누락된 데이터는 배치로 처리
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.user_type = source.user_type AND target.user_id = source.user_id
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.get_current_dst_count(output_df.sparkSession, batch_id, False)
        
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
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
        self.get_current_dst_count(self.output_df.sparkSession)


class ProductMetadataBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.PRODUCT_METADATA)

    def extract(self):        
        self.product_df = self.spark_session.read.table("bronze.product")
        self.order_item_df = self.spark_session.read.table("bronze.order_item")

    def transform(self, ):
        product_category = self.product_df \
            .select('product_id', 'category') \
            .dropna() \
            .dropDuplicates()
            
        product_seller = self.order_item_df \
            .select('product_id', 'seller_id') \
            .dropDuplicates()
        
        self.output_df = product_category.join(
            product_seller,
            on='product_id',
            how='inner'
            ).dropna()

    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        # watermark 기간에서 제외된 누락된 데이터는 배치로 처리
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.product_id = s.product_id AND t.seller_id = s.seller_id
            WHEN NOT MATCHED THEN
                INSERT (product_id, category, seller_id)
                VALUES (s.product_id, s.category, s.seller_id)
        """)

        self.get_current_dst_count(output_df.sparkSession, batch_id, False)
    
class CustomerOrderBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.spark_session = spark_session if spark_session is not None else get_spark_session(app_name=self.__class__.__name__)
        self.initialize_dst_table(SilverAvroSchema.CUSTOMER_ORDER)

    def extract(self):
        self.order_item_df = self.spark_session.read.table("bronze.order_item")
        self.payment_df = self.spark_session.read.table("bronze.payment")
        
    def transform(self):
        order_item_price = self.order_item_df.select('order_id', 'order_item_id', 'product_id', 'price').dropna()
        aggregated_df = order_item_price \
            .groupBy("order_id", "product_id", "price") \
            .agg(F.count("order_item_id").alias("quantity")) \
            .withColumnRenamed("price", "unit_price") \

        order_customer = self.payment_df \
            .select('order_id', 'customer_id') \
            .dropDuplicates()
            
        self.output_df = aggregated_df.join(
                order_customer,
                'order_id'
                "inner"
            )

    def load(self):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
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
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
        self.get_current_dst_count(self.output_df.sparkSession)