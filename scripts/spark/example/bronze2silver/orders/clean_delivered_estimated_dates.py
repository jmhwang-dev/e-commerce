from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("Estimated Delivery Date Processing") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

    # 로그 레벨 설정 (장황한 INFO 로그 감소)
    spark.sparkContext.setLogLevel("WARN")

    # 테이블 읽기
    orders = spark.read.table("silver.dedup.olist_orders_dataset")

    # order_status가 'delivered'인 행 필터링
    orders_delivered_df = orders.filter(col('order_status') == "delivered")

    # 필요한 컬럼 선택
    estimated_delivery_date_df = orders_delivered_df.select(
        col("order_id"),
        col("customer_id"),
        col("order_estimated_delivery_date")
    )

    # order_estimated_delivery_date에서 null 제거 및 중복 제거
    estimated_delivery_date_df = estimated_delivery_date_df.dropna(subset=["order_estimated_delivery_date"]).dropDuplicates()

    # 전체 행 수 출력
    print(f"Total rows after filtering, null removal, and deduplication: {estimated_delivery_date_df.count()}")

    # 결과 미리보기
    print("First 5 rows of cleaned DataFrame:")
    estimated_delivery_date_df.show(n=5, truncate=False)

    # Iceberg 테이블 저장 설정
    DST_QUALIFIED_NAMESPACE = "warehousedev.silver.orders"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")

    DST_TABLE_NAME = "delivered_estimated_dates"  # 테이블 이름 파일명과 조화
    full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"
    writer = (
        estimated_delivery_date_df.writeTo(full_table_name)
        .tableProperty(
            "comment",
            "Delivered orders from `silver.dedup.olist_orders_dataset`, selecting `order_id`, `customer_id`, and "
            "`order_estimated_delivery_date`. Nulls and duplicates removed from `order_estimated_delivery_date`."
        )
    )

    # 테이블 생성 또는 파티션 덮어쓰기
    if not spark.catalog.tableExists(full_table_name):
        writer.create()
    else:
        writer.overwritePartitions()

    # 저장 완료 메시지 출력
    print(f"[INFO] {full_table_name} 테이블 저장 완료")

    # Spark 세션 종료
    spark.stop()