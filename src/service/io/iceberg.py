from service.init.iceberg import *
from service.io import minio
from pyiceberg.exceptions import NoSuchTableError
from typing import Iterable
import pandas as pd
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery

def get_table():
    try:
        table = catalog.load_table(TABLE_IDENTIFIER)
        print(f"Table {TABLE_IDENTIFIER} loaded.")
        return table
    except NoSuchTableError:
        try:
            table = catalog.create_table(
                identifier=TABLE_IDENTIFIER,
                schema=table_schema,
                location=DST_LOCATION,
                partition_spec=partition_spec
            )
            print(f"Table {TABLE_IDENTIFIER} created at {DST_LOCATION}.")
            return table
        except Exception as e:
            print(f"Failed to create table {TABLE_IDENTIFIER}: {str(e)}")
            raise
    except Exception as e:
        print(f"Failed to load table {TABLE_IDENTIFIER}: {str(e)}")
        raise

def delete_table(tables_to_delete: Iterable[Iterable]):
    """
    삭제할 테이블 목록 예시
    tables_to_delete = [
        ("default", "example_table", "s3://warehouse-dev/silver/test/example_table/"),
        ("gold", "example_table", "s3://warehouse-dev/gold/test/example_table/")
    ]
    """
    # 테이블 삭제 및 S3 객체 정리
    for namespace, table_name, location in tables_to_delete:
        try:
            # Iceberg 테이블 삭제
            catalog.drop_table((namespace, table_name))
            print(f"Dropped table {namespace}.{table_name}")

            # S3에서 메타데이터 및 데이터 파일 삭제
            bucket = BUCKET_NAME
            prefix = location.replace(f"s3://{bucket}/", "")
            minio.delete_s3_objects(bucket, prefix)
            print("All specified tables and their data have been deleted.")
        except Exception as e:
            print(f"Failed to drop table {namespace}.{table_name}: {str(e)}")

def get_table_data(data:dict):
    df = pd.DataFrame(data)
    table_data = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
    return table_data

def insert(data):
    try:
        table = get_table()
        with table.transaction() as txn:
            txn.append(data)
        print(f"Data appended to {TABLE_IDENTIFIER} successfully.")
    except Exception as e:
        print(f"Failed to append data to {TABLE_IDENTIFIER}: {str(e)}")
        raise

def load_stream(decoded_stream_df: DataFrame, full_table_name:str, process_time="10 secondes") -> StreamingQuery:
    return decoded_stream_df.writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("checkpointLocation", "/tmp/checkpoint/olist_stream") \
        .trigger(processingTime=process_time) \
        .toTable(full_table_name) \
        .start()

        
def load_batch(df: DataFrame) -> None:
    pass
    # DST_QUALIFIED_NAMESPACE = "warehouse_dev.silver.review"
    # DST_TABLE_NAME = "raw"
    # spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
    # full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

    # writer = (
    #     processed_df.writeTo(full_table_name)
    #     .tableProperty(
    #         "comment",
    #         "test"
    #     )
    # )

    # if not spark.catalog.tableExists(full_table_name):
    #     writer.create()
    # else:
    #     writer.overwritePartitions()