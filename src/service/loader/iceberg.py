
from pyspark.sql import DataFrame, SparkSession
from service.utils.iceberg import append_or_create_table

def load_to_iceberg(spark: SparkSession, destination_dfs: dict[str, DataFrame]) -> None:
    for table_identifier, df in destination_dfs.items():
        if not df.isEmpty():
            append_or_create_table(spark, df, table_identifier)
            print(f"Writing {df.count()} clean records to {table_identifier}...")