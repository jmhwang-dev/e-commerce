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

CUSTOMER_ZIP_CODE = StructType([
    StructField("customer_id", StringType(), False),
    StructField("zip_code", IntegerType(), False),
])

SELLER_ZIP_CODE = StructType([
    StructField("seller_id", StringType(), False),
    StructField("zip_code", IntegerType(), False),
])

GEO_COORDINATE = StructType([
    StructField("zip_code", IntegerType(), False),
    StructField("lat", IntegerType(), False),
    StructField("lng", IntegerType(), False),
])

ORDER_STATUS_TIMELINE = StructType([
    StructField("order_id", StringType(), False),
    StructField("purchase_timestamp", TimestampType(), True),
    StructField("approve_timestamp", TimestampType(), True),
    StructField("delivered_carrier_timestamp", TimestampType(), True),
    StructField("delivered_customer_timestamp", TimestampType(), True),
])

DELIVERY_LIMIT = StructType([
    StructField("order_id", StringType(), False),
    StructField("shipping_limit_timestamp", TimestampType(), True),
    StructField("estimated_delivery_timestamp", TimestampType(), True)
])

ORDER_CUSTOMER = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
])

PRODUCT_METADATA = StructType([
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("seller_id", StringType(), False),
])

ORDER_TRANSACTION = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("freight_value", FloatType(), True),
])

# REVIEW_METADATA_SCHEMA = StructType([
#     StructField("review_id", StringType(), True),
#     StructField("review_creation_date", TimestampType(), True),
#     StructField("review_answer_timestamp", TimestampType(), True),
#     StructField("review_score", IntegerType(), True),
#     StructField("order_id", StringType(), True),
# ])

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