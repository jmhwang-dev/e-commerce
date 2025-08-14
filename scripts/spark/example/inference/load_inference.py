from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName('tmp_inference').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    SOURCE_TSV_PATH = "s3://warehousedev/tmp/eng_reviews_with_senti.tsv"
    df = spark.read.option("header", "true").option("delimiter", "\t").csv(SOURCE_TSV_PATH)

    DST_QUALIFIED_NAMESPACE = "warehousedev.silver.inference"
    DST_TABLE_NAME = "reviews"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
    full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

    writer = (
        df.writeTo(full_table_name)
        .tableProperty(
            "comment",
            "To show inference results of reviews on superset."
        )
    )

    if not spark.catalog.tableExists(full_table_name):
        writer.create()
    else:
        writer.overwritePartitions()

    print(f"[INFO] {full_table_name} 테이블 저장 완료")
    spark.stop()
