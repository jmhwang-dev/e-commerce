from typing import Iterable

from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table
from pyiceberg.catalog import Catalog
import pandas as pd

from service.utils.schema.registry_manager import *
from service.utils.iceberg.spark import *
from service.utils import minio

from config.iceberg import *
from config.minio import *
from domain.schema.iceberg_python import *

NAMESPCE = "silver"
TABLE_NAME = "example_table"
BUCKET_NAME = 'warehousedev'

def create_table(catalog: Catalog, table_schema, partition_spec, s3_uri:str, table_identifier: str):
    """
    ex) s3_uri = s3://warehousedev.silver
    """
    try:
        table = catalog.create_table(
            identifier=table_identifier,
            schema=table_schema,
            location=s3_uri,
            partition_spec=partition_spec
        )
        print(f"Table {table_identifier} created at {s3_uri}.")
        return table
    except Exception as e:
        print(f"Failed to create table {table_identifier}: {str(e)}")
        raise

def get_table(catalog: Catalog, table_identifier: str) -> Table | None:
    try:
        table = catalog.load_table(table_identifier)
        print(f"Table {table_identifier} loaded.")
        return table
    except NoSuchTableError:
        print(f"Failed to load table {table_identifier}: {str(e)}")
        raise

def delete_table(catalog: Catalog, tables_to_delete: Iterable[Table]) -> None:
    """
    삭제할 테이블 목록 예시
    tables_to_delete = [
        ("default", "example_table", "s3://warehousedev/silver/test/example_table/"),
        ("gold", "example_table", "s3://warehousedev/gold/test/example_table/")
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

def insert(table: Table, data: pa.Table):
    try:
        with table.transaction() as txn:
            txn.append(data)
            print(f"Data appended to {table.identifier}")
    except Exception as e:
        print(f"Failed to append data to {table.identifier}: {str(e)}")
        raise

