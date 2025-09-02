from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

from schema.silver import *
from service.utils.iceberg.spark import *

class SilverJob:
    """
    SilverJob with Dependency Injection and Watermarking.
    """
    clean_namespace: str = "warehousedev.silver"
    error_namespace: str = "warehousedev.silver.error"
    watermark_table: str = "warehousedev.silver.watermarks"

    src_table_identifier: str = ''
    dst_table_name: str = ''
    clean_schema: StructType = None

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.incremental_df: DataFrame = self.spark.createDataFrame([], StructType([]))
        self.end_snapshot_id: Optional[str] = None
        self.job_name = self.__class__.__name__

    def set_incremental_df(self):
        """
        워터마크를 기반으로 증분 데이터를 설정합니다.
        """
        print(f"[{self.job_name}] Setting incremental dataframe from {self.src_table_identifier}...")

        # 1. 마지막으로 처리한 스냅샷 ID를 워터마크 테이블에서 조회
        last_id = get_last_processed_snapshot_id(self.spark, self.watermark_table, self.job_name)

        if not self.spark.catalog.tableExists(self.src_table_identifier):
             print(f"Source table {self.src_table_identifier} does not exist. Skipping.")
             return

        snapshot_df = get_snapshot_df(self.spark, self.src_table_identifier)

        # 2. 시작 지점 결정
        if last_id is None:
            # 최초 실행: 가장 오래된 스냅샷부터 시작
            start_snapshot_id = get_snapshot_id_by_time_boundary(snapshot_df, TimeBoundary.EARLIEST)
            print(f"[{self.job_name}] Initial load. Starting from earliest snapshot: {start_snapshot_id}")
        else:
            # 증분 실행: 마지막 지점의 다음 스냅샷부터 시작
            start_snapshot_id = get_next_start_id(snapshot_df, last_id)
            print(f"[{self.job_name}] Incremental load. Starting from snapshot after: {last_id}")

        # 3. 종료 지점 결정 (항상 최신)
        self.end_snapshot_id = get_snapshot_id_by_time_boundary(snapshot_df, TimeBoundary.LATEST)

        if start_snapshot_id is None or start_snapshot_id > self.end_snapshot_id:
            print(f"[{self.job_name}] No new data to process.")
            return

        # 4. 증분 데이터 로드
        print(f"[{self.job_name}] Reading data from snapshot {start_snapshot_id} to {self.end_snapshot_id}")
        self.incremental_df = self.spark.read \
            .format("iceberg") \
            .option("start-snapshot-id", start_snapshot_id) \
            .option("end-snapshot-id", self.end_snapshot_id) \
            .load(self.src_table_identifier)

    def common_etl(self):
        if self.incremental_df.isEmpty():
            print(f"[{self.job_name}] No new data to process.")
            return

        print(f"[{self.job_name}] Starting common ETL process...")
        df = self.incremental_df.dropDuplicates()
        df_not_null = df.dropna(how='any')
        df_null = df.subtract(df_not_null)

        if not df_not_null.isEmpty():
            clean_table_identifier: str = f"{self.clean_namespace}.{self.dst_table_name}"
            print(f"Writing {df_not_null.count()} clean records to {clean_table_identifier}...")
            ordered_cols = [col(field.name).cast(field.dataType) for field in self.clean_schema.fields]
            applied_df = df_not_null.select(*ordered_cols)
            append_or_create_table(self.spark, applied_df, clean_table_identifier)

        if not df_null.isEmpty():
            error_table_identifier: str = f"{self.error_namespace}.{self.dst_table_name}"
            print(f"Writing {df_null.count()} bad records to {error_table_identifier}...")
            append_or_create_table(self.spark, df_null, error_table_identifier)

    def update_watermark(self):
        """ETL이 성공적으로 끝나면 워터마크를 업데이트합니다."""
        if self.end_snapshot_id:
            update_last_processed_snapshot_id(self.spark, self.watermark_table, self.job_name, self.end_snapshot_id)
        else:
            print(f"[{self.job_name}] No new data was processed, watermark will not be updated.")

    def run(self):
        """잡의 전체 파이프라인을 실행합니다."""
        self.set_incremental_df()

        # ETL 및 워터마크 업데이트는 새로운 데이터가 있을 때만 실행합니다.
        if not self.incremental_df.isEmpty():
            self.common_etl()
            self.update_watermark()
        else:
            print(f"[{self.job_name}] No new data found. Skipping ETL and watermark update.")
            
        print(f"[{self.job_name}] Job finished.")

class PaymentSilverJob(SilverJob):
    src_table_identifier = 'warehousedev.bronze.stream_payment'
    dst_table_name = 'payment'
    clean_schema = PAYMENT_SCHEMA