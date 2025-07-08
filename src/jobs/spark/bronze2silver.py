from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

BUCKET = "warehouse-dev"    # `-`, `.`만 포함 가능
BRONZE_NAMESPACE = 'bronze'

bronze_path = f"s3a://{BUCKET}/{BRONZE_NAMESPACE}"  # s3a://, s3:// 둘다 가능: spark-defaults.conf 참조
jPath = spark._jvm.org.apache.hadoop.fs.Path(bronze_path)

# FileSystem 얻기
fs = jPath.getFileSystem(spark._jsc.hadoopConfiguration())
status_list = fs.listStatus(jPath)
file_list = [f.getPath().toString() for f in status_list if f.isFile()]

CATALOG = 'warehouse_dev'   # `-` 포함 불가. `_` 가능
SILVER_NAMESPACE = 'silver.dedup'
TARGET_QUALIFIED_NAMESPACE = f"{CATALOG}.{SILVER_NAMESPACE}"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {TARGET_QUALIFIED_NAMESPACE}")

for file_path in file_list:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    dedup_df = df.dropDuplicates()
    print(f"{file_path}, before: {df.count()}, after: {dedup_df.count()}")

    table_name = file_path.split("/")[-1].replace(".csv", "")
    full_table_name = f"{TARGET_QUALIFIED_NAMESPACE}.{table_name}"

    if not spark.catalog.tableExists(full_table_name):
        dedup_df.writeTo(full_table_name).create()
    else:
        dedup_df.writeTo(full_table_name).overwritePartitions()


df = spark.sql(f"SHOW TABLES IN {TARGET_QUALIFIED_NAMESPACE}")
table_names = [row.tableName for row in df.collect()]
# for table_name in table_names:
#     spark.sql(f"DROP TABLE IF EXISTS {TARGET_QUALIFIED_NAMESPACE}.{table_name} PURGE")
spark.stop()
