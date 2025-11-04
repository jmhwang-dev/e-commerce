from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType


from abc import ABC, abstractmethod

class CommonGoldTask(ABC):
    @abstractmethod
    def transform(self, ):
        pass

class FactOrderTimelineBase(CommonGoldTask):
    
    @classmethod
    def transform(cls, order_event_df: DataFrame):

        return order_event_df \
            .groupBy('order_id') \
            .agg(
                F.max(F.when(F.col('data_type') == 'purchase', F.col('timestamp'))).alias('purchase'),
                F.max(F.when(F.col('data_type') == 'approved', F.col('timestamp'))).alias('approve'),
                F.max(F.when(F.col('data_type') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier'),
                F.max(F.when(F.col('data_type') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer'),
                F.max(F.when(F.col('data_type') == 'shipping_limit', F.col('timestamp'))).alias('shipping_limit'),
                F.max(F.when(F.col('data_type') == 'estimated_delivery', F.col('timestamp'))).alias('estimated_delivery'),
            )
    

class DimUserLocationBase(CommonGoldTask):
    
    @classmethod
    def transform(cls, olist_user: DataFrame, geo_coord_df: DataFrame):

        unique_olist_user_df = olist_user.dropDuplicates()
        unique_geo_coord_df = geo_coord_df.dropDuplicates()

        return unique_geo_coord_df \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
            .join(unique_olist_user_df, on='zip_code', how='left')
    

class FactOrderLocationBase(CommonGoldTask):
    
    @classmethod
    def transform(cls, order_detail_df: DataFrame, product_metadata_df: DataFrame, dim_user_location_df: DataFrame):

        order_customer_df = order_detail_df.select('order_id', 'customer_id').withColumnRenamed('customer_id', 'user_id')
        order_seller_df = order_detail_df.select('order_id', 'product_id') \
            .join(product_metadata_df.select('product_id', 'seller_id'), on='product_id', how='left') \
            .select('order_id', 'seller_id') \
            .withColumnRenamed('seller_id', 'user_id')
        
        order_user_id = order_customer_df.unionByName(order_seller_df)
        return order_user_id.join(dim_user_location_df, on='user_id', how='left')