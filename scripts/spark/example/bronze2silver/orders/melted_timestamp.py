from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Delivered Orders Processing") \
        .getOrCreate()
    
    orders = spark.read.table("silver.dedup.olist_orders_dataset")
    spark.sparkContext.setLogLevel("WARN")

    melted_df = orders.select(
        col("order_id"),
        col("customer_id"),
        col("order_status").alias("final_status"),
        expr("""
            stack(4, 
                'purchase', order_purchase_timestamp,
                'approved', order_approved_at,
                'delivered_carrier', order_delivered_carrier_date,
                'delivered_customer', order_delivered_customer_date
            ) as (detail_status, timestamp)
        """)
    )
    melted_df = melted_df.dropna(subset=["timestamp"]).dropDuplicates()

    DST_QUALIFIED_NAMESPACE = "warehouse_dev.silver.orders"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")

    DST_TABLE_NAME = "melted_timestamp"
    full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"
    writer = (
        melted_df.writeTo(full_table_name)
        .tableProperty(
            "comment",
            "Order timestamp data melted from `silver.dedup.olist_orders_dataset`, transforming `order_purchase_timestamp`, "
            "`order_approved_at`, `order_delivered_carrier_date`, and `order_delivered_customer_date` into long format with "
            "`detail_status` and `timestamp` columns. Null timestamps are dropped."
        )
    )

    if not spark.catalog.tableExists(full_table_name):
        writer.create()
    else:
        writer.overwritePartitions()

    print(f"[INFO] {full_table_name} 테이블 저장 완료")
    melted_df.show(n=5)
    spark.stop()