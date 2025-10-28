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

USER_LOCATION = StructType([
    StructField("user_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("zip_code", IntegerType(), True),
    StructField("lat", FloatType(), True),
    StructField("lng", FloatType(), True),
])

ORDER_STATUS_TIMELINE = StructType([
    StructField("order_id", StringType(), False),
    StructField("purchase", TimestampType(), True),
    StructField("approve", TimestampType(), True),
    StructField("delivered_carrier", TimestampType(), True),
    StructField("delivered_customer", TimestampType(), True),
    StructField("shipping_limit", TimestampType(), True),
    StructField("estimated_delivery", TimestampType(), True),
    StructField("process_timestamp", TimestampType(), False),
])

PRODUCT_METADATA = StructType([
    StructField("category", StringType(), True),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), True),
])

PURCHASE_ORDER = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("product_count", IntegerType(), True),
])

QUARANTINE_FACT_ORDER_TRANSACTION = StructType([
    StructField('order_id', StringType(), False),
    StructField('product_id', StringType(), True),
    StructField('product_count', IntegerType(), True),
    StructField('unit_price', FloatType(), True),
])

QUARANTINE_ORDER_CUSTOMER = StructType([
    StructField('order_id', StringType(), True),
    StructField('customer_id', StringType(), True)
])

REVIEW_METADATA = StructType([
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