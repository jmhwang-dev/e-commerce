from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
    TimestampType,
    LongType
)

WATERMARK_SCHEMA = StructType([
    StructField("job_name", StringType(), False),
    StructField("last_processed_snapshot_id", LongType(), False)
])

SALES = StructType([
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("sold_count", IntegerType(), False),
    StructField("total_sales", IntegerType(), False),
])