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
        
        initialize_namespace(self.spark_session, self.watermark_namespace, is_drop=self._dev)

        self.dst_table_identifier: str = f"{self.dst_namesapce}.{self.dst_table_name}"
        self.wartermark_table_identifier = f"{self.watermark_namespace}.{self.dst_table_name}"

        self.watermark_df = self.spark_session.createDataFrame([], WATERMARK_SCHEMA)
        write_iceberg(self.spark_session, self.watermark_df, self.wartermark_table_identifier, mode='a')
        self.watermark_df = self.spark_session.read.table(self.wartermark_table_identifier)
        
        self.dst_df = self.spark_session.createDataFrame([], schema=self.schema)
        write_iceberg(self.spark_session, self.dst_df, self.dst_table_identifier, mode='a')

class SalesAggregation(GoldBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'sales'
        self.schema = SALES
        super().__init__()

    def generate(self,):        
        order_timeline_df = self.spark_session.read.table(f"{self.src_namespace}.order_timeline")
        delivered_order_df = order_timeline_df.filter(F.col('delivered_customer_timestamp').isNotNull()).select('order_id')

        product_metadata_df = self.spark_session.read.table(f"{self.src_namespace}.product_metadata")
        product_category_df = product_metadata_df.select('product_id', 'category', 'seller_id')

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