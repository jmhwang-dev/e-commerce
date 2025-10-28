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

DIM_USER_LOCATION = StructType([
    StructField("user_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("zip_code", IntegerType(), True),
    StructField("lat", FloatType(), True),
    StructField("lng", FloatType(), True),
])

FACT_ORDER_TIMELINE = StructType([
    StructField("order_id", StringType(), False),
    StructField("purchase", TimestampType(), True),
    StructField("approve", TimestampType(), True),
    StructField("delivered_carrier", TimestampType(), True),
    StructField("delivered_customer", TimestampType(), True),
    StructField("shipping_limit", TimestampType(), True),
    StructField("estimated_delivery", TimestampType(), True),
    StructField("process_timestamp", TimestampType(), False),
])

DIM_PRODUCT = StructType([
    StructField("category", StringType(), True),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), True),
])

FACT_ORDER_ITEM = StructType([
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("product_count", IntegerType(), True),
])

FACT_ORDER_REVIEW = StructType([
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