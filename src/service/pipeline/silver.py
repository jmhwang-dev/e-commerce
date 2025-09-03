from typing import Optional, Union, Dict
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, isnull, isnan, trim, lower, lit
from functools import reduce
from operator import or_

from schema.silver import *
from service.utils.iceberg import *
from service.pipeline.review import PortuguessPreprocessor, get_review_metadata

class SilverJob:
    # ... (SilverJob base class remains mostly the same as before) ...
    clean_namespace = "warehousedev.silver"
    error_namespace = "warehousedev.silver.error"
    watermark_table = "warehousedev.silver.watermarks"

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.job_name = self.__class__.__name__
        self.incremental_df: Optional[DataFrame] = None
        self.end_snapshot_id: Optional[int] = None
        self.src_table_identifier: str = ''
        self.dst_table_name: str = ''
        self.clean_schema: Union[Dict[str, StructType], StructType] = {}

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

    def etl_pipeline(self):
        print(f"[{self.job_name}] Starting ETL...")
        # 3. Efficient and robust null/NaN/whitespace handling
        df_trimmed = self.incremental_df
        for c, dtype in self.incremental_df.dtypes:
            if dtype == 'string':
                df_trimmed = df_trimmed.withColumn(c, trim(col(c)))
        
        df = df_trimmed.dropDuplicates()
        
        conditions = []
        for c, dtype in df.dtypes:
            if dtype in ('float', 'double'): conditions.append(isnull(c) | isnan(c))
            elif dtype == 'string': conditions.append(isnull(c) | (lower(col(c)).isin('null', 'nan', '')))
            else: conditions.append(isnull(c))
        
        null_condition = reduce(or_, conditions) if conditions else lit(False)
        df_null = df.filter(null_condition)
        df_not_null = df.filter(~null_condition)

        if not df_not_null.isEmpty():
            clean_table_identifier = f"{self.clean_namespace}.{self.dst_table_name}"
            print(f"Writing {df_not_null.count()} clean records to {clean_table_identifier}...")
            applied_df = df_not_null.select([col(f.name).cast(f.dataType) for f in self.clean_schema.fields])
            append_or_create_table(self.spark, applied_df, clean_table_identifier)

        if not df_null.isEmpty():
            error_table_identifier = f"{self.error_namespace}.{self.dst_table_name}"
            print(f"Writing {df_null.count()} bad records to {error_table_identifier}...")
            append_or_create_table(self.spark, df_null, error_table_identifier)

    def run(self):
        print(f"============== [{self.job_name}] Job Started ==============")
        self.set_incremental_df()

        if self.incremental_df and not self.incremental_df.isEmpty():
            self.etl_pipeline()
            if self.end_snapshot_id:
                update_watermark(self.spark, self.watermark_table, self.job_name, self.end_snapshot_id)
        else:
            print(f"[{self.job_name}] No new data found. Skipping ETL.")
        print(f"============== [{self.job_name}] Job Finished ==============")

# --- Concrete Job Implementation ---

class CustomerSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_customer'
        self.dst_table_name = 'customer'
        self.clean_schema = CUSTOMER_SCHEMA

class EstimatedDeliveryDateSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_estimated_delivery_date'
        self.dst_table_name = 'estimated_delivery_date'
        self.clean_schema = ESTIMATED_DELIVERY_DATE_SCHEMA

class GeolocationSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_geolocation'
        self.dst_table_name = 'geolocation'
        self.clean_schema = GEOLOCATION_SCHEMA

class OrderItemSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_order_item'
        self.dst_table_name = 'order_item'
        self.clean_schema = ORDER_ITEM_SCHEMA

class ProductSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_product'
        self.dst_table_name = 'product'
        self.clean_schema = PRODUCT_SCHEMA

class SellerSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.cdc_seller'
        self.dst_table_name = 'seller'
        self.clean_schema = SELLER_SCHEMA

class OrderStatusSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.stream_order_status'
        self.dst_table_name = 'order_status'
        self.clean_schema = ORDER_STATUS_SCHEMA

class PaymentSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.stream_payment'
        self.dst_table_name = 'payment'
        self.clean_schema = PAYMENT_SCHEMA

class ReviewSilverJob(SilverJob):
    class TableName(Enum):
        REVIEW_CLEAN_COMMENT = "review_clean_comment"
        REVIEW_METADATA = "review_metadata"

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.stream_review'
        self.clean_schema = {
            self.TableName.REVIEW_CLEAN_COMMENT.value: REVIEW_CLEAN_COMMENT_SCHEMA,
            self.TableName.REVIEW_METADATA.value: REVIEW_METADATA_SCHEMA
        }

    def etl_pipeline(self):
        """Custom ETL logic for review data."""
        print(f"[{self.job_name}] Starting custom ETL pipeline...")
        
        # Pre-processing can be done here before complex transformations
        # df = self.incremental_df.dropDuplicates(["review_id"]) # Example: deduplicate by a key
        df = self.incremental_df

        melted_df = PortuguessPreprocessor.melt_reviews(df)
        clean_comment_df = PortuguessPreprocessor.clean_review_comment(melted_df)
        metadata_df = get_review_metadata(df)

        # A dictionary to hold the final DataFrames for each destination table
        destination_dfs = {
            self.TableName.REVIEW_CLEAN_COMMENT.value: clean_comment_df,
            self.TableName.REVIEW_METADATA.value: metadata_df
        }
        
        for table_name_enum in self.TableName:
            table_name = table_name_enum.value
            _df = destination_dfs.get(table_name)
            if _df and not _df.isEmpty():
                table_identifier = f"{self.clean_namespace}.{table_name}"
                schema = self.clean_schema[table_name]
                
                print(f"Writing {_df.count()} records to {table_identifier}...")
                # Null/error handling can be applied here before writing
                applied_df = _df.select([col(f.name).cast(f.dataType) for f in schema.fields])
                append_or_create_table(self.spark, applied_df, table_identifier)