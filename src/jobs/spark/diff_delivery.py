from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, col

spark = SparkSession.builder.getOrCreate()

orders = spark.read.table("warehouse_dev.silver.dedup.olist_orders_dataset")

diff_delivery = orders.select(
    "order_id", "customer_id",
    datediff(col("order_estimated_delivery_date"), col("order_delivered_customer_date")).alias("diff_delivery")
).filter(col("diff_delivery").isNotNull()) \
 .withColumn("is_late", col("diff_delivery") < 0)

full_table_name = "warehouse_dev.silver.features.diff_delivery"

diff_delivery.writeTo(full_table_name) \
    .using("iceberg") \
    .tableProperty("comment", "Difference between estimated and delivered dates. Positive means early delivery. `is_late` means delivery is late.") \
    .tableProperty("layer", "silver") \
    .createOrReplace()