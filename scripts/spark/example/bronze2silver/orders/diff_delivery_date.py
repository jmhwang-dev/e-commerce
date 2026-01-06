from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import datediff, col

def get_diff_delivery_date(orders: DataFrame) -> DataFrame:
    """
    주문 데이터로부터 실제/예상 배송일 차이 및 지연여부 컬럼 추가
    """
    return (
        orders
        .select(
            "order_id", "customer_id",
            datediff(
                col("order_estimated_delivery_date"),
                col("order_delivered_customer_date")
            ).alias("diff_delivery_date")
        )
        .filter(col("diff_delivery_date").isNotNull())
        .withColumn("is_late", col("diff_delivery_date") < 0)
    )

if __name__ == "__main__":
    spark = SparkSession.builder.appName('diff_delivery_date').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ORDERS_TABLE_NAME = "warehousedev.silver.dedup.olist_orders_dataset"
    orders = spark.read.table(ORDERS_TABLE_NAME)
    diff_delivery_date = get_diff_delivery_date(orders)

    DST_QUALIFIED_NAMESPACE = "warehousedev.silver.orders"
    DST_TABLE_NAME = "diff_delivery_date"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
    full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

    writer = (
        diff_delivery_date.writeTo(full_table_name)
        .tableProperty(
            "comment",
            "Difference between estimated and delivered dates. "
            "Positive means early delivery. `is_late` means delivery is late."
        )
    )

    if not spark.catalog.tableExists(full_table_name):
        writer.create()
    else:
        writer.overwritePartitions()

    print(f"[INFO] {full_table_name} 테이블 저장 완료")
    diff_delivery_date.show(n=5)

    spark.stop()
