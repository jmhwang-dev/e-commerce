from typing import Union, Dict
from abc import ABC, abastrctmethoud

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnull, isnan, lower, lit, when
from functools import reduce
from operator import or_

from schema.silver import *
from service.stream.topic import SilverTopic

class SilverToGoldJob(ABC):
    """
    Strema Jobs for Silver to Gold
    """
    dst_namespace = 'warehousedev.gold'
    dst_table_name: str = ''

    def __init__(self,):
        self.job_name = self.__class__.__name__

    @abastrctmethoud
    def aggregate(self, ) -> dict[str, DataFrame]:
        pass
    
# --- Concrete Job Implementation ---

class Sales(SilverToGoldJob):
    dst_table_name = 'sales'

    def aggregate(self, micro_batch_df: DataFrame):
        payment_df = micro_batch_df.filter(col("topic") == SilverTopic.PAYMENT)
        ordert_status_df = micro_batch_df.filter(col("topic") == SilverTopic.ORDER_STATUS)
        order_item_df = 
        
        complete_order_id = ordert_status_df.filter(col('status') == 'delivered').select('order_id')
        complete_payment_df = payment_df.filter(col('order_id').isin(complete_order_id.select('order_id'))).drop_duplicates()

