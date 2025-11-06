from pprint import pprint
from typing import Optional
from abc import ABC, abstractmethod
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from functools import reduce

from service.utils.schema.reader import AvscReader
from service.utils.iceberg import *


class BaseBatch(ABC):
    """
    Base Batch Job for Silver
    Input: Table
    Output: Table
    """

    spark_session: Optional[SparkSession] = None
    output_df: Optional[DataFrame] = None

    dst_namespace: str = ''
    dst_table_name: str = ''
    dst_table_identifier: str = ''
    dst_table_schema: Optional[StructType] = None
    dst_avsc_reader: Optional[AvscReader] = None

    @abstractmethod
    def extract(self,):
        pass

    @abstractmethod
    def transform(self,):
        pass

    @abstractmethod
    def load(self,):
        """
        upsert table
        """
        pass

    def initialize_dst_table(self, avsc_filename):
        self.dst_avsc_reader = AvscReader(avsc_filename)

        if self.spark_session.catalog.tableExists(self.dst_avsc_reader.dst_table_identifier):
            return

        jvm = self.spark_session._jvm
        avro_schema_java = jvm.org.apache.avro.Schema.Parser().parse(self.dst_avsc_reader.schema_str)

        # Spark StructType으로 변환
        SchemaConverters = jvm.org.apache.spark.sql.avro.SchemaConverters
        converted = SchemaConverters.toSqlType(avro_schema_java)
        json_str = converted.dataType().json()  # JSON 문자열

        json_dict = json.loads(json_str)  # dict로 변환

        for filed_dict in json_dict['fields'][:]:
            if filed_dict['name'] == 'ingest_time':
                json_dict['fields'].remove(filed_dict)
        
        self.dst_table_schema = StructType.fromJson(json_dict)

        dst_df = self.spark_session.createDataFrame([], self.dst_table_schema)
        write_iceberg(self.spark_session, dst_df, self.dst_avsc_reader.dst_table_identifier, mode='a')

    def get_current_dst_table(self, spark_session:SparkSession, batch_id:int=-1, debug=False, line_number=200):
        dst_df = spark_session.read.table(self.dst_avsc_reader.dst_table_identifier)

        if debug:
            conditions = [F.col(c).isNull() for c in dst_df.columns]
            final_condition = reduce(lambda x, y: x | y, conditions)
            null_rows = dst_df.filter(final_condition)
            null_rows.show(n=100, truncate=False)
            dst_df.show(n=line_number, truncate=False)
        
        if batch_id == -1:
            info_message = ''
        else:
            info_message = f'batch_id: {batch_id} '

        print(f"{info_message}Current # of {self.dst_avsc_reader.dst_table_identifier }: ", dst_df.count())

    
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