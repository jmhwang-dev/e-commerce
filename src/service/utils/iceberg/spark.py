from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import  StreamingQuery
from pyspark.sql.functions import col, max, to_timestamp
from pyspark.sql.readwriter import DataFrameWriterV2

from service.utils.schema.registry_manager import *
from schema.silver import WATERMARK_SCHEMA

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, min, max
from enum import Enum

class TimeBoundary(Enum):
    EARLIEST = "Earlies"
    LATEST = "Latest"

def append_or_create_table(spark: SparkSession, df: DataFrame, table_identifier: str):
    """
    데이터프레임을 테이블에 추가합니다. 테이블이 존재하지 않으면 새로 생성합니다.
    """
    if not spark.catalog.tableExists(table_identifier):
        print(f"Table '{table_identifier}' not found. Creating it with the provided data.")
        df.writeTo(table_identifier).create()
    else:
        # 비어있는 데이터프레임을 append하는 것은 아무 작업도 수행하지 않으므로 안전합니다.
        print(f"Appending data to table: {table_identifier}")
        df.writeTo(table_identifier).append()

def get_last_processed_snapshot_id(spark: SparkSession, watermark_table: str, job_name: str) -> Optional[str]:
    print(f"Getting last processed snapshot ID for job '{job_name}' from '{watermark_table}'...")
    if not spark.catalog.tableExists(watermark_table):
        return None # 테이블이 없으면 최초 실행으로 간주
    try:
        row = spark.read.table(watermark_table).filter(f"job_name = '{job_name}'").first()
        return row["last_processed_snapshot_id"] if row else None
    except Exception:
        return None

def update_last_processed_snapshot_id(spark: SparkSession, watermark_table: str, job_name: str, snapshot_id: str):
    """
    MERGE INTO를 사용하여 워터마크를 안전하게 업데이트(Upsert)합니다.
    """
    print(f"Updating watermark for job '{job_name}' to snapshot ID '{snapshot_id}' in '{watermark_table}'.")

    new_watermark_df = spark.createDataFrame([(job_name, snapshot_id)], WATERMARK_SCHEMA)
    new_watermark_df.createOrReplaceTempView("new_watermark")

    spark.sql(f"""
        MERGE INTO {watermark_table} t
        USING new_watermark s
        ON t.job_name = s.job_name
        WHEN MATCHED THEN
            UPDATE SET t.last_processed_snapshot_id = s.last_processed_snapshot_id
        WHEN NOT MATCHED THEN
            INSERT (job_name, last_processed_snapshot_id)
            VALUES (s.job_name, s.last_processed_snapshot_id)
    """)
    
    ## 쿼리를 DataFrame API로 한다면,
    # target_table = spark.table(watermark_table)
    # target_table.alias("t").merge(
    #     source=new_watermark_df.alias("s"),
    #     condition="t.job_name = s.job_name"
    # ).whenMatchedUpdate(
    #     set={"last_processed_snapshot_id": col("s.last_processed_snapshot_id")}
    # ).whenNotMatchedInsert(
    #     values={
    #         "job_name": col("s.job_name"),
    #         "last_processed_snapshot_id": col("s.last_processed_snapshot_id")
    #     }
    # ).execute()


def get_snapshot_df(spark: SparkSession, table_identifier: str):
    # |committed_at|snapshot_id|parent_id|operation|manifest_list|summary|
    # 동일: snapshots_df = spark_session.sql(f"SELECT * FROM {table_identifier}.snapshots ORDER BY committed_at DESC")
    snapshots_df = spark.read.table(f"{table_identifier}.snapshots").select("committed_at", "snapshot_id")
    return snapshots_df.withColumn("committed_at", to_timestamp(col("committed_at")))


def get_snapshot_id_by_time_boundary(snapshots_df: DataFrame, time_boundary: TimeBoundary):
    try:
        if snapshots_df.count() == 0:
            print("스냅샷이 존재하지 않습니다.")
            return None
        agg_func = min if time_boundary == TimeBoundary.EARLIEST else max
        target_timestamps = snapshots_df.select(
            agg_func(col("committed_at")).alias("target_timestamp")
        ).first()

        target_ts = target_timestamps["target_timestamp"]
        target_snapshot_id = snapshots_df.filter(col("committed_at") == target_ts).select("snapshot_id").first()[0]
        print(f"{time_boundary.value} committed_at: {target_ts} 에 해당하는 snapshot_id: {target_snapshot_id}")
        return target_snapshot_id

    except Exception as e:
        print(f"오류가 발생했습니다: {e}")
        return None, None

def write_stream_iceberg(spark_session: SparkSession, decoded_stream_df: DataFrame, schema_str:str, process_time="10 seconds") -> StreamingQuery:
    """
    options
    # spark.sql.streaming.checkpointLocation
    - df.writeStream.option("checkpointLocation", "s3a://your-bucket/checkpoints")
        : 스트리밍 체크포인트 저장 경로. Kafka 오프셋, 상태 등을 저장해 장애 복구 지원.

    - df.writeStream.trigger(processingTime="X seconds")
        : 마이크로 배치 실행 간격. 데이터 처리 주기를 조절.
    
    -  df.writeStream.option("fanout.enabled", "false")
        : Iceberg 스트리밍 쓰기 시 파티션별 파일 생성 방식 제어. false로 파일 수 감소.

    # ceberg 테이블 파일 compaction 및 오래된 스냅샷 삭제. 스트리밍 후 소규모 파일 문제 해결.
        - spark.sql("CALL iceberg_catalog.system.rewrite_data_files('your_table')") // OPTIMIZE
        - spark.sql("CALL iceberg_catalog.system.expire_snapshots('your_table', TIMESTAMP '2025-08-13 00:00:00')") // expire_snapshots
    """
    # s3_uri, table_identifier, table_name = get_iceberg_destination(schema_str)    
    # return decoded_stream_df.writeStream \
    #     .outputMode("append") \
    #     .format("iceberg") \
    #     .option("checkpointLocation", f"s3a://{s3_uri}/checkpoints/{table_name}") \
    #     .option("fanout.enabled", "false") \
    #     .trigger(processingTime=process_time) \
    #     .toTable(table_identifier)
    pass

    # fanout.enabled=false
    # OPTIMIZE TABLE / expire_snapshots

# def get_catalog(
#         catalog_uri: str,
#         s3_endpoint: str,
#         bucket: str = MedallionLayer.BUCKET
#         ):
#     """
#     ex)
#     option = {
#             "type": "REST",
#             "uri": "http://rest-catalog:8181",
#             "s3.endpoint": "http://minio:9000",
#             "s3.access-key-id": "minioadmin",
#             "s3.secret-access-key": "minioadmin",
#             "s3.use-ssl": "false",
#             "warehouse": f"s3://{MedallionLayer.BUCKET}"
#         }

#     """
#     option = {
#         "type": "REST",
#         "uri": catalog_uri,
#         "s3.endpoint": s3_endpoint,
#         "s3.access-key-id": "minioadmin",
#         "s3.secret-access-key": "minioadmin",
#         "s3.use-ssl": "false",
#         "warehouse": f"s3://{bucket}"
#     }
#     return load_catalog("REST", **option)