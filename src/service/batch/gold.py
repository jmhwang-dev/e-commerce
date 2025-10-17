from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Union

from .base import BatchJob
from service.utils.iceberg import write_iceberg
from schema.gold import *
from service.utils.iceberg import initialize_namespace
from service.utils.spark import get_spark_session

class GoldBatchJob(BatchJob):
    src_namespace: str = 'silver'
    dst_namesapce: str = "gold"
    watermark_namespace: str = "gold.watermarks"
    schema: Union[StructType, None] = None

    def __init__(self,):
        self._dev = True
        self.spark_session = get_spark_session(f"{self.job_name}", dev=self._dev)
        
        # initialize_namespace(self.spark_session, self.watermark_namespace, is_drop=self._dev)
        # self.wartermark_table_identifier = f"{self.watermark_namespace}.{self.dst_table_name}"
        # self.watermark_df = self.spark_session.createDataFrame([], WATERMARK_SCHEMA)
        # write_iceberg(self.spark_session, self.watermark_df, self.wartermark_table_identifier, mode='a')
        # self.watermark_df = self.spark_session.read.table(self.wartermark_table_identifier)
        
        self.dst_table_identifier: str = f"{self.dst_namesapce}.{self.dst_table_name}"
        self.dst_df = self.spark_session.createDataFrame([], schema=self.schema)
        write_iceberg(self.spark_session, self.dst_df, self.dst_table_identifier, mode='a')

class SalesAggregator(GoldBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'sales'
        self.schema = SALES
        super().__init__()

    def generate(self,):        
        delivered_order_df = self.spark_session.read.table(f"{self.src_namespace}.delivered_order")

        product_metadata_df = self.spark_session.read.table(f"{self.src_namespace}.product_metadata")
        product_category_df = product_metadata_df.select('product_id', 'category')

        order_transaction_df = self.spark_session.read.table(f"{self.src_namespace}.order_transaction")

        fact_sales_df = delivered_order_df \
            .join(order_transaction_df, on='order_id', how='inner') \
            .join(product_category_df, on='product_id', how='inner')
        
        self.output_df = fact_sales_df.groupBy('product_id', 'category').agg(
            F.count(F.col('order_id')).alias('sold_count'),
            F.round(F.sum(F.col('price')), 5).alias('total_sales')
            )
        
    def update_table(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING {self.dst_table_name} s
            ON t.product_id = s.product_id
            WHEN MATCHED AND t.sold_count != s.sold_count THEN
                UPDATE SET sold_count = s.sold_count

            WHEN MATCHED AND t.total_sales != s.total_sales THEN
                UPDATE SET total_sales = s.total_sales
            
            WHEN NOT MATCHED THEN
                INSERT (product_id, category, sold_count, total_sales)
                VALUES (s.product_id, s.category, s.sold_count, s.total_sales)
        """)

class DeliveredOrderLocation(GoldBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'delivered_order_location'
        self.schema = DELIVERED_ORDER_LOCATION
        super().__init__()

    def generate(self,):
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)
        delivered_order_df = self.spark_session.read.table(f"{self.src_namespace}.delivered_order").select('order_id')
        delivered_order_df = delivered_order_df.join(self.dst_df, on='order_id', how='left_anti')

        geolocation_df = self.spark_session.read.table(f"{self.src_namespace}.geolocation")
        coord = geolocation_df.select('zip_code', 'lng', 'lat')
        
        # seller location
        product_metadata_df = self.spark_session.read.table(f"{self.src_namespace}.product_metadata")
        seller_df = self.spark_session.read.table(f"{self.src_namespace}.seller")

        order_transaction_df = self.spark_session.read.table(f"{self.src_namespace}.order_transaction")
        order_product = order_transaction_df.select('order_id', 'product_id').dropDuplicates()
        
        seller_location_df = delivered_order_df \
            .join(order_product, on='order_id', how='left') \
            .join(product_metadata_df, on='product_id', how='inner') \
            .join(seller_df, on='seller_id', how='inner') \
            .join(coord, on='zip_code', how='inner') \
            .withColumnRenamed('seller_id', 'user_id') \
            .withColumn('user_type', F.lit('seller'))

        # customer location
        order_customer_df = self.spark_session.read.table(f"{self.src_namespace}.order_customer")
        customer_df = self.spark_session.read.table(f"{self.src_namespace}.customer")
        
        customer_location_df = delivered_order_df \
            .join(order_customer_df, on='order_id', how='inner') \
            .join(customer_df, on='customer_id', how='inner') \
            .join(coord, on='zip_code', how='inner') \
            .withColumnRenamed('customer_id', 'user_id') \
            .withColumn('user_type', F.lit('customer'))

        print("delivered_order_df", delivered_order_df.count())
        print("delivered_order_df.dropDuplicates()", delivered_order_df.dropDuplicates().count())

        print("order_customer_df", order_customer_df.count())
        print("order_customer_df.dropDuplicates()", order_customer_df.dropDuplicates().count())

        print("customer_location_df", customer_location_df.count())
        print("customer_location_df.dropDuplicates()", customer_location_df.dropDuplicates().count())
        exit()
        
        # concat product_id and category
        # 동일한 제품에 여러 판매자가 있을 수 있으므로, seller_location_df.dropDuplicates() 수행
        # 동일 제품의 여러 구매자가 있을 수 있음
        customer_location_df = seller_location_df.select('order_id', 'product_id', 'category').dropDuplicates() \
            .join(customer_location_df, on='order_id', how='inner')
        
        self.output_df = customer_location_df.union(seller_location_df).drop('zip_code')

    def update_table(self,):
        write_iceberg(self.spark_session, self.output_df, self.dst_table_identifier, mode='a')