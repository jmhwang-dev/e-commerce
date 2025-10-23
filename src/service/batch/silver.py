from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Union

from .base import BatchJob
from service.utils.iceberg import write_iceberg
from schema.silver import *
from service.producer.bronze import BronzeTopic
from service.utils.iceberg import initialize_namespace
from service.utils.spark import get_spark_session

class SilverBatchJob(BatchJob):
    src_namespace: str = 'bronze'
    dst_namesapce: str = "silver"
    watermark_namespace: str = "silver.watermarks"
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

class DeliveredOrder(SilverBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'delivered_order'
        self.schema = DELIVERED_ORDER
        super().__init__()

    def generate(self,):
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)

        order_status_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ORDER_STATUS}')
        
        self.output_df = order_status_df \
            .join(self.dst_df, on='order_id', how='left_anti').dropDuplicates() \
                .filter(F.col('status') == 'delivered_customer') \
                    .select('order_id', 'timestamp') \
                        .withColumnRenamed("timestamp", "delivered_customer_timestamp")

    def update_table(self,):
        write_iceberg(self.spark_session, self.output_df, self.dst_table_identifier, mode='a')

class OrderTimeline(SilverBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'order_timeline'
        self.schema = ORDER_TIMELINE
        super().__init__()

    def generate(self,):
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)
        complete_timeline_order_id_df = self.dst_df.select('order_id').dropna()

        estimated_delivery_date_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ESTIMATED_DELIVERY_DATE}')
        estimated_delivery_date_df = estimated_delivery_date_df.join(complete_timeline_order_id_df, on='order_id', how='left_anti')
        estimated_delivery_timestamp_df = estimated_delivery_date_df.withColumnRenamed('estimated_delivery_date', 'estimated_delivery_timestamp')
        
        order_item_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ORDER_ITEM}')
        order_item_df = order_item_df.join(complete_timeline_order_id_df, on='order_id', how='left_anti')
        shipping_limit_date_df = order_item_df.select('order_id', 'shipping_limit_date').dropDuplicates()
        shipping_limit_timestamp_df = shipping_limit_date_df.withColumnRenamed('shipping_limit_date', 'shipping_limit_timestamp')

        order_status_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ORDER_STATUS}')
        order_status_df = order_status_df.join(complete_timeline_order_id_df, on='order_id', how='left_anti')

        # TODO:
        # - order_status.tsv의 status 컬럼에 5개 상태값 비즈니스 로직 추론 후 임의 시간을 부여하여 데이터 추가 후, ETL 파이프라인 업데이트
        #   (unavailable, shipped, canceled, invoiced, processing)
        # - 참고 스크립트: order_status_legacy.ipynb
        # - Note: 소스 데이터에 타임스탬프 레코드 없어 상태값 제외됨
        pivot_values = ['purchase', "approved", "delivered_carrier"]
        
        order_status_pivot_df = \
            order_status_df.groupBy('order_id') \
                .pivot('status', pivot_values) \
                    .agg(F.first('timestamp'))
        
        order_status_pivot_df = order_status_pivot_df.withColumnsRenamed(
            {'purchase': 'purchase_timestamp',
            "approved": "approve_timestamp",
            "delivered_carrier": "delivered_carrier_timestamp"}
            )
        
        self.output_df = order_status_pivot_df \
            .join(shipping_limit_timestamp_df, on='order_id', how='inner') \
            .join(estimated_delivery_timestamp_df, on='order_id', how='inner')
        
    def update_table(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING {self.dst_table_name} s
            ON t.order_id = s.order_id
            WHEN MATCHED AND t.purchase_timestamp != s.purchase_timestamp THEN
                UPDATE SET purchase_timestamp = s.purchase_timestamp

            WHEN MATCHED AND t.approve_timestamp != s.approve_timestamp THEN
                UPDATE SET approve_timestamp = s.approve_timestamp

            WHEN MATCHED AND t.shipping_limit_timestamp != s.shipping_limit_timestamp THEN
                UPDATE SET shipping_limit_timestamp = s.shipping_limit_timestamp

            WHEN MATCHED AND t.delivered_carrier_timestamp != s.delivered_carrier_timestamp THEN
                UPDATE SET delivered_carrier_timestamp = s.delivered_carrier_timestamp

            WHEN MATCHED AND t.estimated_delivery_timestamp != s.estimated_delivery_timestamp THEN
                UPDATE SET estimated_delivery_timestamp = s.estimated_delivery_timestamp
            
            WHEN NOT MATCHED THEN
                INSERT (order_id, purchase_timestamp, approve_timestamp, shipping_limit_timestamp,
                        delivered_carrier_timestamp, estimated_delivery_timestamp)
                VALUES (s.order_id, s.purchase_timestamp, s.approve_timestamp, s.shipping_limit_timestamp,
                        s.delivered_carrier_timestamp, s.estimated_delivery_timestamp)
        """)

class OrderCustomer(SilverBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'order_customer'
        self.schema = ORDER_CUSTOMER
        super().__init__()

    def generate(self,):
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)
        payment_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.PAYMENT}')
        self.output_df = payment_df \
            .join(self.dst_df, on='order_id', how='left_anti') \
                .select('order_id', 'customer_id').dropDuplicates()
    
    def update_table(self,):
        write_iceberg(self.spark_session, self.output_df, self.dst_table_identifier, mode='a')

class ProductMetadata(SilverBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'product_metadata'
        self.schema = PRODUCT_METADATA
        super().__init__()

    def generate(self,):
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)

        # TODO: Select additional columns as needed: ['weight_g', 'length_cm', 'height_cm', 'width_cm']
        product_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.PRODUCT}')
        product_category_df = product_df.select('product_id', 'category')
        product_category_df = product_category_df.join(self.dst_df, on='product_id', how='left_anti').dropDuplicates()

        order_item_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ORDER_ITEM}')
        product_seller_df = order_item_df.select('product_id', 'seller_id')
        product_seller_df = product_seller_df.join(self.dst_df, on='product_id', how='left_anti').dropDuplicates()

        # Drop unknown category
        self.output_df = product_category_df.join(product_seller_df, on='product_id', how='inner').dropna()
    
    def update_table(self,):
        write_iceberg(self.spark_session, self.output_df, self.dst_table_identifier, mode='a')

class OrderTransaction(SilverBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'order_transaction'
        self.schema = ORDER_TRANSACTION
        super().__init__()

    def generate(self,):
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)

        order_item_df = self.spark_session.read.table(f'{self.src_namespace}.{BronzeTopic.ORDER_ITEM}')
        self.output_df = order_item_df \
            .join(self.dst_df, on='order_id', how='left_anti') \
                .select("order_id", "order_item_id", "product_id", "price", "freight_value")
    
    def update_table(self,):
        write_iceberg(self.spark_session, self.output_df, self.dst_table_identifier, mode='a')