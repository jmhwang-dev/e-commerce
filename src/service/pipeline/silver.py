from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

from schema.silver import *
from service.utils.iceberg.spark import *
from service.utils.spark import get_spark_session

class SilverJob:
    """
    SilverJob with Dependency Injection and Watermarking.
    """
    spark_session: SparkSession = get_spark_session("SilverJob")
    clean_namespace: str = "warehousedev.silver"
    error_namespace: str = "warehousedev.silver.error"
    watermark_table_identifier: str = "warehousedev.silver.watermarks"

    def __init__(self,):
        self.job_name = self.__class__.__name__
        self.incremental_df: DataFrame = self.spark_session.createDataFrame([], StructType([]))
        self.end_snapshot_id: Optional[str] = None

        # vars to init by job
        self.src_table_identifier: str = ''
        self.dst_table_name: str = ''
        self.clean_schema: StructType = None

    @classmethod
    def initialize(cls):
        cls.spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cls.clean_namespace}")
        cls.spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cls.error_namespace}")

        print(f"Ensuring watermark table '{cls.watermark_table_identifier}' exists...")
        empty_df = cls.spark_session.createDataFrame([], WATERMARK_SCHEMA)
        append_or_create_table(cls.spark_session, empty_df, cls.watermark_table_identifier)

    def set_incremental_df(self):
        """
        워터마크를 기반으로 증분 데이터를 설정합니다.
        """
        print(f"[{self.job_name}] Setting incremental dataframe from {self.src_table_identifier}...")

        # 1. 마지막으로 처리한 스냅샷 ID를 워터마크 테이블에서 조회
        last_processed_id = get_last_processed_snapshot_id(self.spark_session, self.watermark_table_identifier, self.job_name)

        if not self.spark_session.catalog.tableExists(self.src_table_identifier):
            print(f"Source table {self.src_table_identifier} does not exist. Skipping.")
            return

        snapshot_df = get_snapshot_df(self.spark_session, self.src_table_identifier)

        # 2. 시작 지점 결정
        if last_processed_id is None:
            # Initial Load: Process the very first snapshot to ensure idempotency.
            # This is a fixed target, not a moving one.
            self.end_snapshot_id = get_snapshot_id_by_time_boundary(snapshot_df, TimeBoundary.EARLIEST)
            
            print(f"[{self.job_name}] Initial load. Reading table state as of the earliest snapshot: {self.end_snapshot_id}")
            self.incremental_df = self.spark_session.read \
                .format("iceberg") \
                .option("snapshot-id", self.end_snapshot_id) \
                .load(self.src_table_identifier)
            return 
        
        # Incremental Load: Read changes since the last processed snapshot.
        self.end_snapshot_id = get_snapshot_id_by_time_boundary(snapshot_df, TimeBoundary.LATEST)

        if self.end_snapshot_id == last_processed_id:
            self.incremental_df = self.spark_session.createDataFrame([], StructType([]))
            return 
        
        print(f"[{self.job_name}] Incremental load. Reading from snapshot after {last_processed_id} up to {self.end_snapshot_id}")
        self.incremental_df = self.spark_session.read \
            .format("iceberg") \
            .option("start-snapshot-id", last_processed_id) \
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
            append_or_create_table(self.spark_session, applied_df, clean_table_identifier)

        if not df_null.isEmpty():
            error_table_identifier: str = f"{self.error_namespace}.{self.dst_table_name}"
            print(f"Writing {df_null.count()} bad records to {error_table_identifier}...")
            append_or_create_table(self.spark_session, df_null, error_table_identifier)

    def update_watermark(self):
        """ETL이 성공적으로 끝나면 워터마크를 업데이트합니다."""
        if self.end_snapshot_id:
            update_last_processed_snapshot_id(self.spark_session, self.watermark_table_identifier, self.job_name, self.end_snapshot_id)
        else:
            print(f"[{self.job_name}] No new data was processed, watermark will not be updated.")

    def run(self):
        """잡의 전체 파이프라인을 실행합니다."""
        self.set_incremental_df()
        self.common_etl()
        self.update_watermark()            
        print(f"[{self.job_name}] Job finished.")

class PaymentSilverJob(SilverJob):
    def __init__(self):
        super().__init__()
        self.src_table_identifier = 'warehousedev.bronze.stream_payment'
        self.dst_table_name = 'payment'
        self.clean_schema = PAYMENT_SCHEMA