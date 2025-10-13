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

        initialize_namespace(self.spark_session, self.dst_namesapce, is_drop=True)
        initialize_namespace(self.spark_session, self.watermark_namespace, is_drop=True)
        
        self.watermark_df = self.spark_session.createDataFrame([], WATERMARK_SCHEMA)
        write_iceberg(spark, self.watermark_df, self.wartermark_table_identifier, mode='a')
        self.watermark_df = self.spark_session.read.table(self.wartermark_table_identifier)
        
        self.dst_df = self.spark_session.createDataFrame([], schema=ORDER_TIMELINE)
        write_iceberg(spark, self.dst_df, self.dst_table_identifier, mode='a')
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)

    def generate(self,):
        estimated_delivery_date_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ESTIMATED_DELIVERY_DATE}')
        estimated_delivery_timestamp_df = estimated_delivery_date_df.withColumnRenamed('estimated_delivery_date', 'estimated_delivery_timestamp')
        
        order_item_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ORDER_ITEM}')
        shipping_limit_date_df = order_item_df.select('order_id', 'shipping_limit_date')
        shipping_limit_timestamp_df = shipping_limit_date_df.withColumnRenamed('shipping_limit_date', 'shipping_limit_timestamp')

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
        
        self.output_df = order_status_pivot_df \
            .join(shipping_limit_timestamp_df, on='order_id', how='inner') \
            .join(estimated_delivery_timestamp_df, on='order_id', how='inner')
        
        # self.output_df.select('order_id').
        # nunique = self.output_df.select(F.countDistinct('order_id').alias('unique_count')).collect()[0]['unique_count']
        # print(f"Unique order_id count: {nunique}")
        # duplicates = self.output_df.groupBy("order_id").agg(F.count("*").alias("count")) \
        #     .filter(F.col("count") > 1)

        # # 결과 출력
        # duplicates.show()
    
    def update_table(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING {self.dst_table_name} s
            ON t.order_id = s.order_id
            WHEN MATCHED AND (
                t.purchase_timestamp != s.purchase_timestamp OR
                t.approve_timestamp != s.approve_timestamp OR
                t.shipping_limit_timestamp != s.shipping_limit_timestamp OR
                t.delivered_carrier_timestamp != s.delivered_carrier_timestamp OR
                t.delivered_customer_timestamp != s.delivered_customer_timestamp OR
                t.estimated_delivery_timestamp != s.estimated_delivery_timestamp
            ) THEN
                UPDATE SET
                    purchase_timestamp = s.purchase_timestamp,
                    approve_timestamp = s.approve_timestamp,
                    shipping_limit_timestamp = s.shipping_limit_timestamp,
                    delivered_carrier_timestamp = s.delivered_carrier_timestamp,
                    delivered_customer_timestamp = s.delivered_customer_timestamp,
                    estimated_delivery_timestamp = s.estimated_delivery_timestamp
            WHEN NOT MATCHED THEN
                INSERT (
                    order_id,
                    purchase_timestamp,
                    approve_timestamp,
                    shipping_limit_timestamp,
                    delivered_carrier_timestamp,
                    delivered_customer_timestamp,
                    estimated_delivery_timestamp
                ) VALUES (
                    s.order_id,
                    s.purchase_timestamp,
                    s.approve_timestamp,
                    s.shipping_limit_timestamp,
                    s.delivered_carrier_timestamp,
                    s.delivered_customer_timestamp,
                    s.estimated_delivery_timestamp
                )
        """)
        tmp_df = self.spark_session.read.table(self.dst_table_identifier)
        null_counts = {col_name: tmp_df.filter(F.col(col_name).isNull()).count()
               for col_name in tmp_df.columns}
        for col_name, count in null_counts.items():
            print(f"{col_name}: {count}")
        # print(tmp_df.count())
        return