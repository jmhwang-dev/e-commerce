from typing import Tuple
from pyspark.sql import SparkSession
from service.common.topic import *

from service.common.schema import *

# from pyiceberg.catalog import load_catalog

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import  StreamingQuery
from pyspark.sql import SparkSession

from service.utils.iceberg.spark import *

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
    create_namespace(spark_session, schema_str)
    s3_uri, table_identifier, table_name = get_iceberg_destination(schema_str)
    
    return decoded_stream_df.writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("checkpointLocation", f"s3a://{s3_uri}/checkpoints/{table_name}") \
        .option("fanout.enabled", "false") \
        .trigger(processingTime=process_time) \
        .toTable(table_identifier)

    # fanout.enabled=false
    # OPTIMIZE TABLE / expire_snapshots

# def load_batch(spark_session: SparkSession, df: DataFrame, table_identifier: str, comment_message: str='') -> None:
#     create_namespace()
#     writer = df.writeTo(table_identifier).tableProperty("comment", comment_message)
    
#     if not spark_session.catalog.tableExists(table_identifier):
#         writer.create()
#     else:
#         writer.overwritePartitions()
#     print(f"[INFO] {table_identifier} 테이블 저장 완료")

def create_namespace(spark_session: SparkSession, schema_str: str) -> None:
    namespace, _ = SchemaRegistryManager.get_schem_identifier(schema_str)
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    return

def get_iceberg_destination(schema_str: str) -> Tuple[str, str, str]:
    namespace, table_name = SchemaRegistryManager.get_schem_identifier(schema_str)

    s3_uri = namespace.replace('.', '/')
    table_identifier = f"{namespace}.{table_name}"
    return s3_uri, table_identifier, table_name

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