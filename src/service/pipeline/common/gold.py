from abc import ABC, abstractmethod

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from service.utils.spark import  start_console_stream


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