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

DELIVERED_ORDER_LOCATION = StructType([
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("user_type", StringType(), False),
    StructField("lng", IntegerType(), False),
    StructField("lat", IntegerType(), False),
])

ORDER_LEAD_DAYS = StructType([
    StructField('approve', IntegerType(), False),
    StructField('delivered_carrier', IntegerType(), False),
    StructField('delivered_customer', IntegerType(), False),
    StructField('total_delivery_days', IntegerType(), False),
    StructField('is_late_delivery', StringType(), False),
    StructField('is_late_shipping', StringType(), False),
])