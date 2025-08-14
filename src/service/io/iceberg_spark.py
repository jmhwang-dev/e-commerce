from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame




def create_table(spark_session:SparkSession, df:DataFrame) -> None:
    DST_QUALIFIED_NAMESPACE = "warehouse_dev.silver.products"
    DST_TABLE_NAME = "products_spec"
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
    full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

    writer = df.writeTo(full_table_name) \
        .tableProperty("comment", "Replace Portuguese to English for `category name`.")

    if not spark_session.catalog.tableExists(full_table_name):
        writer.create()
    else:
        writer.overwritePartitions()

    print(f"[INFO] {full_table_name} 테이블 저장 완료")

