from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, isnull, isnan, trim, lower, lit
from functools import reduce
from operator import or_

from schema.silver import *
from service.utils.iceberg.spark import *

class SilverJob:
    clean_namespace = "warehousedev.silver"
    error_namespace = "warehousedev.silver.error"
    watermark_table = "warehousedev.silver.watermarks"

    def __init__(self, spark: SparkSession):
        # 1. Using Dependency Injection for SparkSession
        self.spark = spark
        self.job_name = self.__class__.__name__
        self.incremental_df: Optional[DataFrame] = None
        self.end_snapshot_id: Optional[int] = None
        self.src_table_identifier = ''
        self.dst_table_name = ''
        self.clean_schema: Optional[StructType] = None

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

    def common_etl(self):
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
            clean_table = f"{self.clean_namespace}.{self.dst_table_name}"
            print(f"Writing {df_not_null.count()} clean records to {clean_table}...")
            applied_df = df_not_null.select([col(f.name).cast(f.dataType) for f in self.clean_schema.fields])
            append_or_create_table(self.spark, applied_df, clean_table)

        if not df_null.isEmpty():
            error_table = f"{self.error_namespace}.{self.dst_table_name}"
            print(f"Writing {df_null.count()} bad records to {error_table}...")
            append_or_create_table(self.spark, df_null, error_table)

    def run(self):
        print(f"============== [{self.job_name}] Job Started ==============")
        self.set_incremental_df()

        if self.incremental_df and not self.incremental_df.isEmpty():
            self.common_etl()
            if self.end_snapshot_id:
                update_watermark(self.spark, self.watermark_table, self.job_name, self.end_snapshot_id)
        else:
            print(f"[{self.job_name}] No new data found. Skipping ETL.")
        print(f"============== [{self.job_name}] Job Finished ==============")

# --- Concrete Job Implementation ---

class PaymentSilverJob(SilverJob):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.src_table_identifier = 'warehousedev.bronze.stream_payment'
        self.dst_table_name = 'payment'
        self.clean_schema = PAYMENT_SCHEMA