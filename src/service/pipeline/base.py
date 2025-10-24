from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from service.utils.iceberg import *
from schema.silver import WATERMARK_SCHEMA

class BaseJob(ABC):
    """
    Batch Jobs for Silver
    Input: DataFrame
    Output: Table
    """
    spark_session: Optional[SparkSession] = None
    job_name: str = ''

    dst_namesapce: str = ''
    dst_table_name: str = ''
    dst_table_identifier: str = ''
    watermark_namespace: str = ''
    wartermark_table_identifier: str = ''

    src_df: Optional[DataFrame] = None
    dst_df: Optional[DataFrame] = None
    output_df: Optional[DataFrame] = None

    # # @classmethod
    # def get_incremental_df(self, src_table_identifier: str) -> Optional[DataFrame]:
    #     print(f"[{self.job_name}] Setting incremental dataframe...")
    #     last_id = get_last_processed_snapshot_id(self.spark_session, self.wartermark_table_identifier, self.job_name)
    #     if not self.spark_session.catalog.tableExists(src_table_identifier): return None

    #     snapshot_df = get_snapshot_df(self.spark_session, src_table_identifier)
    #     if snapshot_df.isEmpty(): return None

    #     if last_id is None:
    #         earliest = get_snapshot_details(snapshot_df, TimeBoundary.EARLIEST)
    #         if not earliest: return None
    #         self.end_snapshot_id = earliest["snapshot_id"]
    #         print(f"[{self.job_name}] Initial load on earliest snapshot: {self.end_snapshot_id}")
    #         return self.spark_session.read.format("iceberg").option("snapshot-id", self.end_snapshot_id).load(src_table_identifier)
    #     else:
    #         latest = get_snapshot_details(snapshot_df, TimeBoundary.LATEST)
    #         if not latest: return
    #         self.end_snapshot_id = latest["snapshot_id"]
            
    #         # Correctly compare using commit timestamps
    #         last_details = snapshot_df.filter(col("snapshot_id") == last_id).select("committed_at").first()
    #         if not last_details or latest["committed_at"] <= last_details["committed_at"]:
    #             print(f"[{self.job_name}] No new data.")
    #             return None

    #         print(f"[{self.job_name}] Incremental load from {last_id} before {self.end_snapshot_id}")
    #         return self.spark_session.read.format("iceberg").option("start-snapshot-id", last_id).option("end-snapshot-id", self.end_snapshot_id).load(src_table_identifier)

    @abstractmethod
    def extract(self,):
        pass

    @abstractmethod
    def transform(self,):
        pass

    @abstractmethod
    def load(self,):
        pass

    @abstractmethod
    def get_query(self, process_time='5 seconds'):
        pass
        # self.src_df.writeStream \
        #     .format('iceberg') \
        #     .foreachBatch(self.transform) \
        #     .queryName(self.job_name) \
        #     .option('checkpointLocation', f's3a://warehousedev/{self.dst_namesapce}/{self.dst_table_name}/checkpoint') \
        #     .trigger(processingTime=process_time) \
        #     .start()

        # query = input_df.writeStream \
        #     .foreachBatch(process_batch) \
        #     .trigger(processingTime="10 seconds") \
        #     .start()
        
    def update_watermark(self,):
        df = self.spark_session.createDataFrame([(self.job_name, self.end_snapshot_id)], WATERMARK_SCHEMA)
        df.createOrReplaceTempView("new_watermark")
        self.spark_session.sql(f"MERGE INTO {self.wartermark_table_identifier} t USING new_watermark s ON t.job_name = s.job_name "
                f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")