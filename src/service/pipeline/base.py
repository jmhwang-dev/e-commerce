from abc import ABC, abstractmethod
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from service.utils.iceberg import *
from pyspark.sql import functions as F

from service.utils.schema.reader import AvscReader
from service.utils.spark import get_deserialized_avro_stream_df
from service.utils.helper import get_producer

class BaseJob(ABC):
    """
    Batch Jobs for Silver
    Input: DataFrame
    Output: Table
    """
    spark_session: Optional[SparkSession] = None
    job_name: str = ''

    dst_namespace: str = ''
    dst_table_name: str = ''
    dst_table_identifier: str = ''
    watermark_namespace: str = ''
    wartermark_table_identifier: str = ''

    src_df: Optional[DataFrame] = None
    dst_df: Optional[DataFrame] = None
    output_df: Optional[DataFrame] = None
    schema: Optional[StructType] = None

    @abstractmethod
    def extract(self,):
        pass

    @abstractmethod
    def transform(self,):
        pass

    @abstractmethod
    def load(self,):
        pass
        
    def initialize_dst_table(self, ):
        self.dst_table_identifier: str = f"{self.dst_namespace}.{self.dst_table_name}"
        self.dst_df = self.spark_session.createDataFrame([], schema=self.schema)
        write_iceberg(self.spark_session, self.dst_df, self.dst_table_identifier, mode='a')
    
    def initialize_qurantine_table(self, qurantine_table_identifier, qurantine_schema):
        qurantine_df = self.spark_session.createDataFrame([], schema=qurantine_schema)
        write_iceberg(self.spark_session, qurantine_df, qurantine_table_identifier, mode='a')

    def get_current_dst_count(self, micro_batch:DataFrame, batch_id, debug=False, line_number=200):
        self.dst_df = micro_batch.sparkSession.read.table(f"{self.dst_table_identifier}")
        print(f"batch_id: {batch_id}, Current # of {self.dst_table_identifier }: ", self.dst_df.count())

        if debug:
            conditions = [F.col(c).isNull() for c in self.dst_df.columns]
            final_condition = reduce(lambda x, y: x | y, conditions)
            null_rows = self.dst_df.filter(final_condition)
            null_rows.show(n=100, truncate=False)
            self.dst_df.show(n=line_number, truncate=False)

    def get_query(self, process_time='5 seconds'):
        self.extract()

        # TODO: 표준 checkpointLocation 으로 변경: s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        return self.src_df.writeStream \
            .foreachBatch(self.transform) \
            .queryName(self.job_name) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .trigger(processingTime=process_time) \
            .start()
    
    def get_topic_df(self, micro_batch:DataFrame, topic_name: str) -> DataFrame:
        ser_df = micro_batch.filter(F.col("topic") == topic_name)
        avsc_reader = AvscReader(topic_name)
        producer_class = get_producer(topic_name)
        return get_deserialized_avro_stream_df(ser_df, producer_class.key_column, avsc_reader.schema_str)
    
    def get_incremental_df(self, src_table_identifier: str) -> Optional[DataFrame]:
        """
        증분처리 직접 구현
        """
        print(f"[{self.job_name}] Setting incremental dataframe...")
        last_id = get_last_processed_snapshot_id(self.spark_session, self.wartermark_table_identifier, self.job_name)
        if not self.spark_session.catalog.tableExists(src_table_identifier): return None

        snapshot_df = get_snapshot_df(self.spark_session, src_table_identifier)
        if snapshot_df.isEmpty(): return None

        if last_id is None:
            earliest = get_snapshot_details(snapshot_df, TimeBoundary.EARLIEST)
            if not earliest: return None
            self.end_snapshot_id = earliest["snapshot_id"]
            print(f"[{self.job_name}] Initial load on earliest snapshot: {self.end_snapshot_id}")
            return self.spark_session.read.format("iceberg").option("snapshot-id", self.end_snapshot_id).load(src_table_identifier)
        else:
            latest = get_snapshot_details(snapshot_df, TimeBoundary.LATEST)
            if not latest: return
            self.end_snapshot_id = latest["snapshot_id"]
            
            # Correctly compare using commit timestamps
            last_details = snapshot_df.filter(col("snapshot_id") == last_id).select("committed_at").first()
            if not last_details or latest["committed_at"] <= last_details["committed_at"]:
                print(f"[{self.job_name}] No new data.")
                return None

            print(f"[{self.job_name}] Incremental load from {last_id} before {self.end_snapshot_id}")
            return self.spark_session.read.format("iceberg").option("start-snapshot-id", last_id).option("end-snapshot-id", self.end_snapshot_id).load(src_table_identifier)