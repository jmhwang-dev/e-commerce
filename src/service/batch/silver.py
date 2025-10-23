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