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

ACCOUNT = StructType([
    StructField("zip_code", IntegerType(), False),
    StructField("user_type", StringType(), False),
    StructField("user_id", StringType(), False),
])

GEO_COORDINATE = StructType([
    StructField("zip_code", IntegerType(), False),
    StructField("lat", IntegerType(), False),
    StructField("lng", IntegerType(), False),
])

ORDER_STATUS_TIMELINE = StructType([
    StructField("order_id", StringType(), False),
    StructField("purchase", TimestampType(), True),
    StructField("approve", TimestampType(), True),
    StructField("delivered_carrier", TimestampType(), True),
    StructField("delivered_customer", TimestampType(), True),
    StructField("shipping_limit", TimestampType(), True),
    StructField("estimated_delivery", TimestampType(), True)
])

PRODUCT_METADATA = StructType([
    StructField("category", StringType(), True),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), True),
])

ORDER_DETAIL = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("product_count", IntegerType(), True),
])

QUARANTINE_FACT_ORDER_TRANSACTION_SCHEMA = StructType([
    StructField('order_id', StringType(), False),
    StructField('product_id', StringType(), True),
    StructField('product_count', IntegerType(), True),
    StructField('unit_price', FloatType(), True),
])

QUARANTINE_ORDER_CUSTOMER_SCHEMA = StructType([
    StructField('order_id', StringType(), True),
    StructField('customer_id', StringType(), True)
])

REVIEW_METADATA_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("review_id", StringType(), False),
    StructField("review_creation_date", TimestampType(), False),
    StructField("review_answer_timestamp", TimestampType(), False),
    StructField("review_score", IntegerType(), False),
])

# REVIEW_CLEAN_COMMENT_SCHEMA = StructType([
#     StructField("review_id", StringType(), True),
#     StructField("message_type", StringType(), True),
#     StructField("portuguess", StringType(), True)
# ])

# REVIEW_INFERENCE_SCHEMA = StructType([
#     StructField("review_id", StringType(), True),
#     StructField("message_type", StringType(), True),
#     StructField("eng", StringType(), True),
#     StructField("negative", FloatType(), True),
#     StructField("neutral", FloatType(), True),
#     StructField("positive", FloatType(), True),
#     StructField("main_sentiment", StringType(), True)
# ])