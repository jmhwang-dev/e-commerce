from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .base import BatchJob
from service.utils.iceberg import write_iceberg
from schema.silver import WATERMARK_SCHEMA, ORDER_TIMELINE
from service.producer.bronze import BronzeTopic
from service.utils.iceberg import initialize_namespace


class SilverBatchJob(BatchJob):
    src_namespace: str = 'bronze'
    dst_namesapce: str = "silver"
    watermark_namespace: str = "silver.watermarks"

class OrderTimeline(SilverBatchJob):
    def __init__(self, spark: SparkSession):
        self.spark_session: SparkSession = spark
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'order_timeline'
        self.dst_table_identifier: str = f"{self.dst_namesapce}.{self.dst_table_name}"
        self.wartermark_table_identifier = f"{self.watermark_namespace}.{self.dst_table_name}"

        initialize_namespace(self.spark_session, self.dst_namesapce, is_drop=False)
        initialize_namespace(self.spark_session, self.watermark_namespace, is_drop=False)
        
        self.watermark_df = self.spark_session.createDataFrame([], WATERMARK_SCHEMA)
        write_iceberg(spark, self.watermark_df, self.wartermark_table_identifier, mode='a')
        
        self.dst_df = self.spark_session.createDataFrame([], schema=ORDER_TIMELINE)
        write_iceberg(spark, self.dst_df, self.dst_table_identifier, mode='a')

    def generate(self,):
        estimated_delivery_date_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ESTIMATED_DELIVERY_DATE}')
        
        order_item_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ORDER_ITEM}')
        shipping_limit_date_df = order_item_df.select('order_id', 'shipping_limit_date')

        order_status_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ORDER_STATUS}')
        
        # TODO: Another values should be processed: ['unavailable', 'shipped', 'canceled', 'invoiced', 'processing']
        pivot_values = ['purchase', "approved", "delivered_carrier", "delivered_customer"]
        
        order_status_pivot_df = \
            order_status_df.groupBy('order_id') \
                .pivot('status', pivot_values) \
                .agg(F.first('timestamp'))
        
        order_status_pivot_df = order_status_pivot_df.withColumnsRenamed(
            {'purchase': 'purchase_timestamp',
             "approved": "approve_timestamp",
             "delivered_carrier": "delivered_carrier_timestamp",
             "delivered_customer": "delivered_customer_timestamp"}
             )
        
        order_timeline_df = order_status_pivot_df \
            .join(shipping_limit_date_df, on='order_id', how='inner') \
            .join(estimated_delivery_date_df, on='order_id', how='inner')
        
        return


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