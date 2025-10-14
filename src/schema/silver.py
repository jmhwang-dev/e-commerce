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
    StructField("job_name", StringType(), True),
    StructField("last_processed_snapshot_id", LongType(), True)
])

ORDER_TIMELINE = StructType([
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("purchase_timestamp", TimestampType(), True),
    StructField("approve_timestamp", TimestampType(), True),
    StructField("shipping_limit_timestamp", TimestampType(), True),
    StructField("delivered_carrier_timestamp", TimestampType(), True),
    StructField("delivered_customer_timestamp", TimestampType(), True),
    StructField("estimated_delivery_timestamp", TimestampType(), True)
])

ORDER_CUSTOMER = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
])

# CUSTOMER_SCHEMA = StructType([
#     StructField("customer_id", StringType(), True),
#     StructField("zip_code", IntegerType(), True)
# ])

# ESTIMATED_DELIVERY_DATE_SCHEMA = StructType([
#     StructField("order_id", StringType(), True),
#     StructField("estimated_delivery_date", TimestampType(), True)
# ])

# GEOLOCATION_SCHEMA = StructType([
#     StructField("zip_code", IntegerType(), True),
#     StructField("lat", FloatType(), True),
#     StructField("lng", FloatType(), True),
#     StructField("state", StringType(), True),
#     StructField("city", StringType(), True)
# ])

# ORDER_ITEM_SCHEMA = StructType([
#     StructField("order_id", StringType(), True),
#     StructField("order_item_id", IntegerType(), True),
#     StructField("shipping_limit_date", TimestampType(), True),
#     StructField("price", FloatType(), True),
#     StructField("freight_value", FloatType(), True),
#     StructField("product_id", StringType(), True),
#     StructField("seller_id", StringType(), True)
# ])

# ORDER_STATUS_SCHEMA = StructType([
#     StructField("order_id", StringType(), True),
#     StructField("status", StringType(), True),
#     StructField("timestamp", TimestampType(), True)
# ])

# PAYMENT_SCHEMA = StructType([
#     StructField("order_id", StringType(), True),
#     StructField("payment_sequential", IntegerType(), True),
#     StructField("timestamp", TimestampType(), True),
#     StructField("customer_id", StringType(), True),
#     StructField("payment_type", StringType(), True),
#     StructField("payment_installments", IntegerType(), True),
#     StructField("payment_value", DoubleType(), True)
# ])

# PRODUCT_SCHEMA = StructType([
#     StructField("product_id", StringType(), True),
#     StructField("weight_g", FloatType(), True),
#     StructField("length_cm", FloatType(), True),
#     StructField("height_cm", FloatType(), True),
#     StructField("width_cm", FloatType(), True),
#     StructField("category", StringType(), True)
# ])

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

# # seller
# SELLER_SCHEMA = StructType([
#     StructField("seller_id", StringType(), True),
#     StructField("zip_code", IntegerType(), True),
# ])