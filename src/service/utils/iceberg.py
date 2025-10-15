from enum import Enum

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import  StreamingQuery
from pyspark.sql.functions import col, min, max

from service.utils.schema.registry_manager import *
from service.utils.schema.reader import AvscReader

class TimeBoundary(Enum):
    EARLIEST = "Earlies"
    LATEST = "Latest"

def initialize_namespace(spark:SparkSession, namespace:str, is_drop:bool = False):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    if not is_drop:
        return
    for table in [row.tableName for row in spark.sql(f'show tables in {namespace}').collect()]:
        spark.sql(f'drop table if exists {namespace}.{table} purge')
        print(f'drop done: {namespace}.{table}')
        # spark.sql(f'DESCRIBE FORMATTED {namespace}.{table}').show()

def write_iceberg(spark_session: SparkSession, df: DataFrame, dst_table_identifier: str, mode:str = '') -> None:
    if mode == '':
        raise TypeError(f"write_iceberg() missing 1 required positional argument: 'mode'. mode must be one of 'w' or 'a'")
    
    if not spark_session.catalog.tableExists(dst_table_identifier):
        print(f"{dst_table_identifier} does not exist")
        df.writeTo(dst_table_identifier).create()
        print(f"{dst_table_identifier} has created")
        return

    if mode == 'w':
        df.writeTo(dst_table_identifier).overwrite()
        print(f"{dst_table_identifier} has overwrited")

    elif mode == 'a':
        df.writeTo(dst_table_identifier).append()
        print(f"{dst_table_identifier} has appended")

    return

def get_snapshot_details(df: DataFrame, boundary: str) -> Optional[dict]:
    if df.isEmpty(): return None
    order_col = col("committed_at").asc() if boundary == TimeBoundary.EARLIEST else col("committed_at").desc()
    row = df.orderBy(order_col).select("snapshot_id", "committed_at").first()
    return {"snapshot_id": row["snapshot_id"], "committed_at": row["committed_at"]} if row else None

def get_last_processed_snapshot_id(spark: SparkSession, table: str, job: str) -> Optional[int]:
    if not spark.catalog.tableExists(table): return None
    row = spark.read.table(table).filter(f"job_name = '{job}'").first()
    return row["last_processed_snapshot_id"] if row else None

def get_snapshot_df(spark: SparkSession, table: str) -> DataFrame:
    return spark.sql(f"SELECT * FROM {table}.snapshots")

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

def load_stream_to_iceberg(deserialized_df: DataFrame, dst_table_identifier: str, process_time="10 seconds") -> StreamingQuery:
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

    # # TODO: partition 유무 성능 확인
    # SPARK_SESSION.sql(f"""
    #     CREATE TABLE IF NOT EXISTS {table_identifier}
    #     USING iceberg
    # """)

    s3_uri = dst_table_identifier.replace('.', '/') # ex) bronze/customer
    return deserialized_df.writeStream \
        .queryName(f"Load to {dst_table_identifier}") \
        .outputMode("append") \
        .format("iceberg") \
        .option("checkpointLocation", f"s3a://warehousedev/{s3_uri}/checkpoint") \
        .option("fanout.enabled", "false") \
        .trigger(processingTime=process_time) \
        .toTable(dst_table_identifier)