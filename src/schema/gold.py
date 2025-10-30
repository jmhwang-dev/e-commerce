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

DELIVERY_DETAIL = StructType([
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", FloatType(), False),
    StructField("user_id", StringType(), False),
    StructField("user_type", StringType(), False),
    StructField("lng", FloatType(), False),
    StructField("lat", FloatType(), False),
])

FACT_ORDER_TIMELINE = StructType([
    StructField("order_id", StringType(), False),
    StructField("purchase", TimestampType(), True),
    StructField("approve", TimestampType(), True),
    StructField("delivered_carrier", TimestampType(), True),
    StructField("delivered_customer", TimestampType(), True),
    StructField("shipping_limit", TimestampType(), True),
    StructField("estimated_delivery", TimestampType(), True),
])

# batch
FACT_ORDER_LEAD_DAYS = StructType([
    StructField("order_id", StringType(), False),
    StructField('approve', IntegerType(), True),
    StructField('delivered_carrier', IntegerType(), True),
    StructField('delivered_customer', IntegerType(), True),
    StructField('total_delivery', IntegerType(), True),
    StructField('is_late_delivery', StringType(), True),
    StructField('is_late_shipping', StringType(), True),
])

# SALE_STATS = StructType([
#     StructField("delivered_customer", TimestampType(), False),
#     StructField("order_id", StringType(), False),
#     StructField("product_id", StringType(), False),
#     StructField("category", StringType(), True),
#     StructField("sold_count", IntegerType(), False),
#     StructField("sales", FloatType(), False),
# ])

PRODUCT_PORTFOLIO_MATRIX = StructType([
    StructField("category", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("group", StringType(), True),
    StructField("total_sold_count", IntegerType(), True),
    StructField("total_sales", FloatType(), True),
    StructField("mean_sales", FloatType(), True),
])
# batch end