from typing import Union, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnull, isnan, lower, lit, when
from functools import reduce
from operator import or_

from schema.silver import *
from service.stream.topic import SilverTopic
from service.stream.review import PortuguessPreprocessor, get_review_metadata

class BronzeToSilverJob:
    """
    Strema Jobs for Bronze to Silver
    """
    dst_topic_name: str = ''
    dst_topic_name_dlq: str = ''
    dst_schema: Union[Dict[str, StructType], StructType]

    def __init__(self,):
        self.job_name = self.__class__.__name__

    def transform(self, _df: DataFrame) -> dict[str, DataFrame]:
        print(f"[{self.job_name}] Starting ETL...")
        df = _df.dropDuplicates()

        expressions = []
        for field in self.dst_schema.fields:
            field_name = field.name
            target_type = field.dataType
            
            if field_name not in df.columns:
                expressions.append(lit(None).cast(target_type).alias(field_name))
                continue

            current_col = col(field_name)
            expression = current_col
            source_type = df.schema[field_name].dataType

            # 실수 타입 처리: NaN -> null
            if isinstance(source_type, (FloatType, DoubleType)):
                expression = when(isnan(current_col), None).otherwise(current_col)
            # 문자열 타입 처리: 'null', 'nan', '' -> null
            elif isinstance(source_type, StringType):
                expression = when(lower(current_col).isin('null', 'nan', ''), None).otherwise(current_col)

            # 최종적으로 대상 스키마의 데이터 타입으로 변환
            expressions.append(expression.cast(target_type).alias(field_name))

        cleaned_df = df.select(*expressions)

        # Null 포함 여부에 따라 데이터프레임 분리
        null_conditions = [isnull(c) for c in cleaned_df.columns]
        final_null_condition = reduce(or_, null_conditions)

        df_null: DataFrame = cleaned_df.filter(final_null_condition)
        df_not_null: DataFrame = cleaned_df.filter(~final_null_condition)

        destination_dfs = {}
        if not df_null.isEmpty():
            destination_dfs[self.dst_topic_name_dlq] = df_null
        if not df_not_null.isEmpty():
            destination_dfs[self.dst_topic_name] = df_not_null
        
        print(f"[{self.job_name}] Finish ETL...")
        return destination_dfs
    
# --- Concrete Job Implementation ---
class PaymentBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self):
        super().__init__()
        self.dst_topic_name = SilverTopic.STREAM_PAYMENT
        self.dst_topic_name_dlq = SilverTopic.STREAM_PAYMENT_DLQ
        self.dst_schema = PAYMENT_SCHEMA

class OrderStatusBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self):
        super().__init__()
        self.dst_topic_name = SilverTopic.STREAM_ORDER_STATUS
        ## `self.dst_topic_name_dlq` is not required, as the bronze topic is derived from the order_status topic initially.
        # self.dst_topic_name_dlq = SilverTopic.STREAM_ORDER_STATUS 
        self.dst_schema = ORDER_STATUS_SCHEMA

# class CustomerBronzeToSilverJob(BronzeToSilverJob):
#     def __init__(self):
#         super().__init__()
#         self.dst_topic_name = SilverTopic.CUSTOMER
#         self.dst_topic_name_dlq = DeadLetterQueuerTopic.CUSTOMER_DLQ
#         self.dst_schema = CUSTOMER_SCHEMA

# class EstimatedDeliveryDateBronzeToSilverJob(BronzeToSilverJob):
#     def __init__(self):
#         super().__init__()
#         self.dst_topic_name = SilverTopic.ESTIMATED_DELIVERY_DATE
#         self.dst_topic_name_dlq = DeadLetterQueuerTopic.GEOLOCATION_DLQ
#         self.dst_schema = ESTIMATED_DELIVERY_DATE_SCHEMA

# class GeolocationBronzeToSilverJob(BronzeToSilverJob):
#     def __init__(self):
#         super().__init__()
#         self.dst_topic_name = SilverTopic.GEOLOCATION
#         self.dst_topic_name_dlq = DeadLetterQueuerTopic.GEOLOCATION_DLQ
#         self.dst_schema = GEOLOCATION_SCHEMA

# class OrderItemBronzeToSilverJob(BronzeToSilverJob):
#     def __init__(self):
#         super().__init__()
#         self.dst_topic_name = SilverTopic.ORDER_ITEM
#         self.dst_topic_name_dlq = DeadLetterQueuerTopic.ORDER_ITEM_DLQ
#         self.dst_schema = ORDER_ITEM_SCHEMA

# class ProductBronzeToSilverJob(BronzeToSilverJob):
#     def __init__(self):
#         super().__init__()
#         self.dst_topic_name = SilverTopic.PRODUCT
#         self.dst_topic_name_dlq = DeadLetterQueuerTopic.PRODUCT_DLQ
#         self.dst_schema = PRODUCT_SCHEMA

# class SellerBronzeToSilverJob(BronzeToSilverJob):
#     def __init__(self):
#         super().__init__()
#         self.dst_topic_name = SilverTopic.SELLER
#         self.dst_topic_name_dlq = DeadLetterQueuerTopic.SELLER_DLQ
#         self.dst_schema = SELLER_SCHEMA

# class ReviewBronzeToSilverJob(BronzeToSilverJob):
#     def __init__(self):
#         super().__init__()

#     def transform(self, df) -> dict[str, DataFrame]:
#         """Custom ETL logic for review data."""
#         print(f"[{self.job_name}] Starting custom ETL pipeline...")

#         melted_df = PortuguessPreprocessor.melt_reviews(df)
#         clean_comment_df = PortuguessPreprocessor.clean_review_comment(melted_df)
#         metadata_df = get_review_metadata(df)

#         # A dictionary to hold the final DataFrames for each destination table
#         destination_dfs = {
#             SilverTopic.REVIEW_CLEAN_COMMENT: clean_comment_df,
#             SilverTopic.REVIEW_METADATA: metadata_df
#         }
#         print(f"[{self.job_name}] Finish ETL...")
#         return destination_dfs