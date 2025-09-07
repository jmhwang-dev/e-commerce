from typing import Optional, Union, Dict
from pyspark.sql.functions import col, isnull, isnan, lower, lit, when
from functools import reduce
from operator import or_

from schema.silver import *
from service.stream.topic import SilverTopic, DeadLetterQueuerTopic
from service.utils.iceberg import *
from service.stream.review import PortuguessPreprocessor, get_review_metadata

class BronzeToSilverJob:
    """
    Jobs for Bronze to Silver
    Input: DataFrame
    Output: Table
    """
    # ... (BronzeToSilverJob base class remains mostly the same as before) ...
    dst_namesapce = "warehousedev.silver"
    watermark_table = "warehousedev.silver.watermarks"

    def __init__(self, spark: SparkSession = None):
        self.spark = spark
        self.job_name = self.__class__.__name__
        self.incremental_df: Optional[DataFrame] = None
        self.end_snapshot_id: Optional[int] = None
        self.src_table_identifier: str = ''
        self.dst_table_name: str = ''
        self.dst_table_name_dlq: str = ''
        self.dst_schema: Union[Dict[str, StructType], StructType]

    def set_incremental_df(self):
        print(f"[{self.job_name}] Setting incremental dataframe...")
        last_id = get_last_processed_snapshot_id(self.spark, self.watermark_table, self.job_name)
        if not self.spark.catalog.tableExists(self.src_table_identifier): return

        snapshot_df = get_snapshot_df(self.spark, self.src_table_identifier)
        if snapshot_df.isEmpty(): return

        if last_id is None:
            earliest = get_snapshot_details(snapshot_df, TimeBoundary.EARLIEST)
            if not earliest: return
            self.end_snapshot_id = earliest["snapshot_id"]
            print(f"[{self.job_name}] Initial load on earliest snapshot: {self.end_snapshot_id}")
            self.incremental_df = self.spark.read.format("iceberg").option("snapshot-id", self.end_snapshot_id).load(self.src_table_identifier)
        else:
            latest = get_snapshot_details(snapshot_df, TimeBoundary.LATEST)
            if not latest: return
            self.end_snapshot_id = latest["snapshot_id"]
            
            # 2. Correctly compare using commit timestamps
            last_details = snapshot_df.filter(col("snapshot_id") == last_id).select("committed_at").first()
            if not last_details or latest["committed_at"] <= last_details["committed_at"]:
                print(f"[{self.job_name}] No new data.")
                return

            print(f"[{self.job_name}] Incremental load from after {last_id} to {self.end_snapshot_id}")
            self.incremental_df = self.spark.read.format("iceberg").option("start-snapshot-id", last_id).option("end-snapshot-id", self.end_snapshot_id).load(self.src_table_identifier)

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

        destination_dfs = {
            self.dst_table_name: df_not_null,
            self.dst_table_name_dlq: df_null
        }
        
        print(f"[{self.job_name}] Finish ETL...")
        return destination_dfs
# --- Concrete Job Implementation ---

class CustomerBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_customer'
        self.dst_table_name = SilverTopic.CUSTOMER
        self.dst_schema = CUSTOMER_SCHEMA

class EstimatedDeliveryDateBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_estimated_delivery_date'
        self.dst_table_name = SilverTopic.ESTIMATED_DELIVERY_DATE
        self.dst_schema = ESTIMATED_DELIVERY_DATE_SCHEMA

class GeolocationBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_geolocation'
        self.dst_table_name = SilverTopic.GEOLOCATION
        self.dst_schema = GEOLOCATION_SCHEMA

class OrderItemBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_order_item'
        self.dst_table_name = SilverTopic.ORDER_ITEM
        self.dst_schema = ORDER_ITEM_SCHEMA

class ProductBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_product'
        self.dst_table_name = SilverTopic.PRODUCT
        self.dst_table_name_dlq = DeadLetterQueuerTopic.PRODUCT_DLQ
        self.dst_schema = PRODUCT_SCHEMA

class SellerBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_seller'
        self.dst_table_name = SilverTopic.SELLER
        self.dst_schema = SELLER_SCHEMA

class OrderStatusBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.stream_order_status'
        self.dst_table_name = SilverTopic.ORDER_STATUS
        self.dst_schema = ORDER_STATUS_SCHEMA

class PaymentBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.stream_payment'
        self.dst_table_name = SilverTopic.PAYMENT
        self.dst_table_name_dlq = DeadLetterQueuerTopic.PAYMENT_DLQ
        self.dst_schema = PAYMENT_SCHEMA

class ReviewBronzeToSilverJob(BronzeToSilverJob):
    def __init__(self, spark: SparkSession = None):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.stream_review'

    def transform(self, df) -> dict[str, DataFrame]:
        """Custom ETL logic for review data."""
        print(f"[{self.job_name}] Starting custom ETL pipeline...")

        melted_df = PortuguessPreprocessor.melt_reviews(df)
        clean_comment_df = PortuguessPreprocessor.clean_review_comment(melted_df)
        metadata_df = get_review_metadata(df)

        # A dictionary to hold the final DataFrames for each destination table
        destination_dfs = {
            SilverTopic.REVIEW_CLEAN_COMMENT: clean_comment_df,
            SilverTopic.REVIEW_METADATA: metadata_df
        }
        print(f"[{self.job_name}] Finish ETL...")
        return destination_dfs