from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType

from .base import BaseBatch
from ..common.silver import *

from service.utils.schema.avsc import BronzeAvroSchema, SilverAvroSchema
from service.utils.iceberg import write_iceberg, get_last_processed_snapshot_id, get_snapshot_details, get_snapshot_df, TimeBoundary
from service.utils.schema.reader import AvscReader

class SilverBatch(BaseBatch):
    watermark_avsc_reader: AvscReader = AvscReader(SilverAvroSchema.WATERMARK)
    watermark_schema: Optional[StructType] = None

    def __init__(self, app_name: str, dst_avsc_filename: str, spark_session: Optional[SparkSession] = None,):
        super().__init__(app_name, dst_avsc_filename, spark_session)
        self.watermark_schema = BaseBatch.get_schema(self.spark_session, self.watermark_avsc_reader)

    def get_incremental_df(self, src_avsc_filename) -> DataFrame:
        """
        배치에서 증분처리
        """

        # TODO: refactoring - too many logic

        src_avsc_reader = AvscReader(src_avsc_filename)
        src_schema = BaseBatch.get_schema(self.spark_session, src_avsc_reader)
        BaseBatch.check_table(self.spark_session, src_avsc_reader.dst_table_identifier)

        self.src_table_identifier = src_avsc_reader.dst_table_identifier
        empty_df = self.spark_session.createDataFrame([], src_schema)

        print(f"[ {self.app_name} | {self.src_table_identifier} ] Setting incremental dataframe...")

        last_id = get_last_processed_snapshot_id(self.spark_session, self.watermark_avsc_reader.dst_table_identifier, self.app_name, self.src_table_identifier)
        if not self.spark_session.catalog.tableExists(self.src_table_identifier): return None

        snapshot_df = get_snapshot_df(self.spark_session, self.src_table_identifier)
        if snapshot_df.isEmpty(): return empty_df

        if last_id is None:
            earliest = get_snapshot_details(snapshot_df, TimeBoundary.EARLIEST)
            if not earliest: return empty_df
            self.end_snapshot_id = earliest["snapshot_id"]
            print(f"[ {self.app_name} | {self.src_table_identifier} ] Initial load on earliest snapshot: {self.end_snapshot_id}")
            return self.spark_session.read.format("iceberg").option("snapshot-id", self.end_snapshot_id).load(self.src_table_identifier)
        else:
            latest = get_snapshot_details(snapshot_df, TimeBoundary.LATEST)
            if not latest: return
            self.end_snapshot_id = latest["snapshot_id"]
            
            # Correctly compare using commit timestamps
            last_details = snapshot_df.filter(F.col("snapshot_id") == last_id).select("committed_at").first()
            if not last_details or latest["committed_at"] <= last_details["committed_at"]:
                print(f"[ {self.app_name} | {self.src_table_identifier} ] No new data.")
                return empty_df

            print(f"[ {self.app_name} | {self.src_table_identifier} ] Incremental load from {last_id} before {self.end_snapshot_id}")
            return self.spark_session.read.format("iceberg").option("start-snapshot-id", last_id).option("end-snapshot-id", self.end_snapshot_id).load(self.src_table_identifier)
        
    def update_watermark(self, ):
        df: DataFrame = self.spark_session.createDataFrame([[self.app_name, self.src_table_identifier, self.end_snapshot_id]], self.watermark_schema)
        df.createOrReplaceTempView('updates')
        self.spark_session.sql(
            f"""
            MERGE INTO {self.watermark_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.app_name = s.app_name and t.src = s.src
            WHEN MATCHED and t.last_processed_snapshot_id != s.last_processed_snapshot_id THEN
                UPDATE SET t.last_processed_snapshot_id = s.last_processed_snapshot_id
            WHEN NOT MATCHED THEN INSERT *
            """)

class GeoCoordBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, SilverAvroSchema.GEO_COORD, spark_session)

    def extract(self) -> bool:
        self.geo_df = self.get_incremental_df(BronzeAvroSchema.GEOLOCATION)

        if not self.geo_df.isEmpty():
            self.update_watermark()

            # print(self.geo_df.count())
            # self.geo_df.orderBy('zip_code').show(n=100)

    def transform(self):
        geo_df = self.geo_df \
            .select('zip_code', 'lng', 'lat') \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
            .dropDuplicates()
        
        self.output_df = GeoCoordBase.transform(geo_df)

    def load(self,):
        dst_df = self.output_df.sparkSession.read.table(self.dst_avsc_reader.dst_table_identifier)

        if dst_df.isEmpty():
            write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
            return

        self.output_df.createOrReplaceTempView("updates")
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.zip_code = s.zip_code
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        
        self.get_current_dst_table(self.output_df.sparkSession, -1, self.is_debug, line_number=5)

class OlistUserBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, SilverAvroSchema.OLIST_USER, spark_session)

    def extract(self):

        self.customer_df = self.get_incremental_df(BronzeAvroSchema.CUSTOMER)

        if not self.customer_df.isEmpty():
            self.update_watermark()

        self.seller_df = self.get_incremental_df(BronzeAvroSchema.SELLER)

        if not self.seller_df.isEmpty():    
            self.update_watermark()

    def transform(self):
        customer_df = self.customer_df.dropDuplicates()
        seller_df = self.seller_df.dropDuplicates()

        self.output_df = OlistUserBase.transform(customer_df, seller_df)
        self.output_df = self.output_df.drop('ingest_time')

    def load(self,):
        dst_df = self.output_df.sparkSession.read.table(self.dst_avsc_reader.dst_table_identifier)

        if dst_df.isEmpty():
            write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
            return
        
        self.output_df.createOrReplaceTempView("updates")
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.user_type = source.user_type AND target.user_id = source.user_id
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.get_current_dst_table(self.output_df.sparkSession, -1, self.is_debug, line_number=5)

class OrderEventBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, SilverAvroSchema.ORDER_EVENT, spark_session)

    def extract(self):
        self.estimated_df = self.get_incremental_df(BronzeAvroSchema.ESTIMATED_DELIVERY_DATE)

        if not self.estimated_df.isEmpty():
            self.update_watermark()

        self.order_item_df = self.get_incremental_df(BronzeAvroSchema.ORDER_ITEM)

        if not self.order_item_df.isEmpty():
            self.update_watermark()

        self.order_status_df = self.get_incremental_df(BronzeAvroSchema.ORDER_STATUS)

        if not self.order_status_df.isEmpty():
            self.update_watermark()
            
    def transform(self):
        # except `ingest_time`
        estimated_df = self.estimated_df.drop('ingest_time')
        shippimt_limit_df = self.order_item_df.select('order_id', 'shipping_limit_date')
        order_status_df = self.order_status_df.drop('ingest_time')

        self.output_df = OrderEventBase.transform(estimated_df, shippimt_limit_df, order_status_df)
        self.output_df = self.output_df.dropDuplicates()

    def load(self):
        dst_df = self.output_df.sparkSession.read.table(self.dst_avsc_reader.dst_table_identifier)

        if dst_df.isEmpty():
            write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
            return
        
        self.output_df.createOrReplaceTempView("updates")
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.order_id = source.order_id AND target.data_type = source.data_type AND target.timestamp = source.timestamp
            WHEN NOT MATCHED THEN INSERT *
        """)
    
        self.get_current_dst_table(self.output_df.sparkSession, -1, self.is_debug, line_number=5)


class ProductMetadataBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, SilverAvroSchema.PRODUCT_METADATA, spark_session)

    def extract(self):
        product_avsc_reader = AvscReader(BronzeAvroSchema.PRODUCT)
        self.product_df = self.spark_session.read.table(product_avsc_reader.dst_table_identifier)

        order_item_avsc_reader = AvscReader(BronzeAvroSchema.ORDER_ITEM)
        self.order_item_df = self.spark_session.read.table(order_item_avsc_reader.dst_table_identifier)

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
            )
        
        self.output_df = self.output_df.drop('ingest_time')

    def load(self):
        dst_df = self.output_df.sparkSession.read.table(self.dst_avsc_reader.dst_table_identifier)

        if dst_df.isEmpty():
            write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
            return
        
        self.output_df.createOrReplaceTempView("updates")
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.product_id = s.product_id AND t.seller_id = s.seller_id
            WHEN NOT MATCHED THEN
                INSERT (product_id, category, seller_id)
                VALUES (s.product_id, s.category, s.seller_id)
        """)

        self.get_current_dst_table(self.output_df.sparkSession, -1, self.is_debug, line_number=5)
    
class CustomerOrderBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, SilverAvroSchema.CUSTOMER_ORDER, spark_session)

    def extract(self):
        order_item_avsc_reader = AvscReader(BronzeAvroSchema.ORDER_ITEM)
        self.order_item_df = self.spark_session.read.table(order_item_avsc_reader.dst_table_identifier)
        
        payment_avsc_reader = AvscReader(BronzeAvroSchema.PAYMENT)
        self.payment_df = self.spark_session.read.table(payment_avsc_reader.dst_table_identifier)
        
    def transform(self):
        # `seller_id`` 포함 이유: 동일 `product_id`를 다수의 `seller_id`가 판매할 수 있음
        order_item_price = self.order_item_df.select('order_id', 'order_item_id', 'product_id', 'price', 'seller_id').dropna()
        aggregated_df = order_item_price \
            .groupBy("order_id", "product_id", "price", 'seller_id') \
            .agg(F.count("order_item_id").alias("quantity")) \
            .withColumnRenamed("price", "unit_price") \

        order_customer = self.payment_df \
            .select('order_id', 'customer_id') \
            .dropDuplicates()
            
        self.output_df = aggregated_df.join(
            order_customer,
            'order_id',
            "inner"
        )
        self.output_df = self.output_df.drop('ingest_time')

    def load(self):
        dst_df = self.output_df.sparkSession.read.table(self.dst_avsc_reader.dst_table_identifier)

        if dst_df.isEmpty():
            write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
            return
        
        self.output_df.createOrReplaceTempView("updates")
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.order_id = s.order_id AND t.product_id = s.product_id AND t.customer_id = s.customer_id AND t.seller_id = s.seller_id
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.get_current_dst_table(self.output_df.sparkSession, -1, self.is_debug, line_number=5)
    
class ReviewMetadataBatch(SilverBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, SilverAvroSchema.REVIEW_METADATA, spark_session)

    def extract(self):
        self.review_df = self.get_incremental_df(BronzeAvroSchema.REVIEW)
        if not self.review_df.isEmpty():
            self.update_watermark()

    def transform(self,):
        self.output_df = ReviewMetadataBase.transform(self.review_df)
        self.output_df = self.output_df.drop('ingest_time')

    def load(self):
        dst_df = self.output_df.sparkSession.read.table(self.dst_avsc_reader.dst_table_identifier)

        if dst_df.isEmpty():
            write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
            return
        
        self.output_df.createOrReplaceTempView("updates")
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.review_id = s.review_id
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.get_current_dst_table(self.output_df.sparkSession, -1, self.is_debug, line_number=5)