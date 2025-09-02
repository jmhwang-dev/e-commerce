from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from schema.silver import *
from service.utils.iceberg.spark import load_batch

class SilverJob:
    clean_namespace: str = "warehousedev.silver"
    error_namespace: str = "warehousedev.silver.error"
    dst_table_name: str = ''
    clean_schema: StructType = None

    @classmethod
    def common_etl(cls, spark_session: SparkSession, incremental_df: DataFrame):
        """
        1. 중복 제거
        2. null 분리
        3. 스키마 변경
        """
        df = incremental_df.dropDuplicates()
        df_not_null = df.dropna(how='any')
        df_null_df = df.subtract(df_not_null)

        # TODO: comment 작성하도록 수정
        # ex) writer = df.writeTo(table_identifier).tableProperty("comment", comment_message)
        if not df_not_null.isEmpty():
            clean_table_identifier: str = f"{cls.clean_namespace}.{cls.dst_table_name}"
            print(f"Writing {df_not_null.count()} clean records to {clean_table_identifier}...")
            applied_df = df_not_null.select(
                [col(field.name).cast(field.dataType) for field in cls.clean_schema.fields]
            )
            writer = applied_df.writeTo(clean_table_identifier)
            load_batch(spark_session, writer, clean_table_identifier)

        if not df_null_df.isEmpty():
            error_table_identifier: str = f"{cls.error_namespace}.{cls.dst_table_name}"
            print(f"Writing {df_null_df.count()} bad records to {error_table_identifier}...")
            writer = df_null_df.writeTo(error_table_identifier)
            load_batch(spark_session, writer, error_table_identifier)

class PaymentSilverJob(SilverJob):
    src_table_identifier = 'warehousedev.bronze.stream_payment'
    dst_table_name = 'payment'
    clean_schema = PAYMENT_SCHEMA