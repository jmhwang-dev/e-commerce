from pyspark.sql import SparkSession

from .base import BatchJob
from service.utils.iceberg import append_or_create_table
from schema.silver import WATERMARK_SCHEMA

class SilverBatchJob(BatchJob):
    dst_namesapce: str = "warehousedev.silver.batch"
    watermark_namespace: str = "warehousedev.silver.watermarks"

class OrderProduct(SilverBatchJob):
    def __init__(self, spark: SparkSession):
        self.spark_session: SparkSession = spark
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'order_product'
        self.dst_table_identifier: str = f"{self.dst_namesapce}.{self.dst_table_name}"
        self.wartermark_table_identifier = f"{self.watermark_namespace}.{self.dst_table_name}"
        append_or_create_table(spark, self.spark_session.createDataFrame([], WATERMARK_SCHEMA), self.wartermark_table_identifier)

    def generate(self,):
        # TODO: compaction using airflow
        order_item_df = self.get_incremental_df('warehousedev.silver.order_item')
        product_df = self.spark_session.read.table('warehousedev.silver.product').drop_duplicates() # always read all

        select_exprs = [
            "o.shipping_limit_date",
            "o.order_id",
            "o.order_item_id",
            "o.seller_id",
            "p.category",
            "o.price as unit_price",
            "o.freight_value as unit_freight",
            "p.weight_g",
            "p.length_cm",
            "p.height_cm",
            "p.width_cm"
        ]

        
        # gold 집계시 전체 중복제거 필요
        self.output_df = \
            order_item_df.alias('o').join(product_df.alias('p'), on='product_id', how='left').selectExpr(select_exprs) \
                .na.drop(how='any') \
                .dropDuplicates()