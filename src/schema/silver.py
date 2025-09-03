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

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("zip_code", IntegerType(), False)
])

ESTIMATED_DELIVERY_DATE_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("estimated_delivery_date", StringType(), False)
])

GEOLOCATION_SCHEMA = StructType([
    StructField("zip_code", IntegerType(), False),
    StructField("lat", FloatType(), False),
    StructField("lng", FloatType(), False),
    StructField("state", StringType(), False),
    StructField("city", StringType(), False)
])

ORDER_ITEM_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("shipping_limit_date", StringType(), False),
    StructField("price", FloatType(), False),
    StructField("freight_value", FloatType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False)
])

ORDER_STATUS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("timestamp", StringType(), False)
])

PAYMENT_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("customer_id", StringType(), False),
    StructField("payment_type", StringType(), False),
    StructField("payment_installments", IntegerType(), False),
    StructField("payment_value", DoubleType(), False)
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("weight_g", FloatType(), False),
    StructField("length_cm", FloatType(), False),
    StructField("height_cm", FloatType(), False),
    StructField("width_cm", FloatType(), False),
    StructField("category", StringType(), False)
])

REVIEW_METADATA_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("review_creation_date", TimestampType(), False),
    StructField("review_answer_timestamp", TimestampType(), False),
    StructField("review_score", IntegerType(), False),
    StructField("order_id", StringType(), False),
])

REVIEW_CLEAN_COMMENT_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("message_type", StringType(), False),
    StructField("portuguess", StringType(), False)
])

REVIEW_INFERENCE_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("message_type", StringType(), False),
    StructField("eng", StringType(), False),
    StructField("negative", FloatType(), False),
    StructField("neutral", FloatType(), False),
    StructField("positive", FloatType(), False),
    StructField("main_sentiment", StringType(), False)
])

# seller
SELLER_SCHEMA = StructType([
    StructField("seller_id", StringType(), False),
    StructField("zip_code", IntegerType(), False),
])