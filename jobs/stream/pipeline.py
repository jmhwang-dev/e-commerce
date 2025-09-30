from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql import DataFrame
from typing import List
from functools import reduce
from service.utils.spark import get_spark_session  # 기존 모듈 가정
from pyspark.sql.functions import expr  # uuid 사용을 위해
from service.stream.topic import BronzeTopic
from service.utils.schema.reader import AvscReader
from service.utils.spark import *  # 모든 spark 유틸리티 함수 임포트

# 단일 Iceberg 저장 함수
def load_to_iceberg(batch_df: DataFrame, table_name: str) -> None:
    batch_df.writeTo(table_name).createOrReplace()

# Kafka 토픽 로드 함수
def load_kafka_topic(spark: SparkSession, topic_name: str) -> DataFrame:
    topic_stream_df: DataFrame = get_kafka_stream_df(spark, topic_name)
    avsc_reader = AvscReader(topic_name)
    return get_deserialized_stream_df(topic_stream_df, avsc_reader.schema_str)

# 1. order_timestamp 처리 (Silver)
def process_order_timestamp(spark: SparkSession) -> DataFrame:
    payment_df: DataFrame = load_kafka_topic(spark, BronzeTopic.PAYMENT)
    order_customer_id: DataFrame = payment_df.select('order_id', 'customer_id').distinct()

    order_status_df: DataFrame = load_kafka_topic(spark, BronzeTopic.ORDER_STATUS)
    delivered_order_id: DataFrame = order_status_df.filter(F.col('status') == 'delivered_customer').select('order_id')
    delivered_order_status: DataFrame = order_status_df.join(delivered_order_id, on='order_id', how='inner')

    delivered_order_status = delivered_order_status.withColumn('timestamp', F.to_timestamp(F.substring(F.col('timestamp'), 1, 19), 'yyyy-MM-dd HH:mm:ss'))
    distinct_status = delivered_order_status.select('status').distinct().collect()
    pivot_values: List[str] = [row.status for row in distinct_status]
    pivoted_order_status_df: DataFrame = delivered_order_status.groupBy('order_id').pivot('status', pivot_values).agg(F.first('timestamp'))
    pivoted_order_customer_df: DataFrame = pivoted_order_status_df.join(order_customer_id, on='order_id', how='inner')

    # NA 체크 및 격리 (참조 무결성)
    conditions = [F.isnull(F.col(c)) for c in pivoted_order_customer_df.columns]
    final_condition = reduce(lambda a, b: a | b, conditions)
    pivoted_order_customer_df.filter(final_condition).count()  # 격리 예시
    order_timestamp: DataFrame = pivoted_order_customer_df.select('order_id', 'customer_id', 'purchase', 'approved', 'delivered_carrier', 'delivered_customer').dropna()
    return order_timestamp

# 2. order_product 처리 (Silver)
def process_order_product(spark: SparkSession, silver_namespace: str) -> DataFrame:
    product_df: DataFrame = load_kafka_topic(spark, BronzeTopic.PRODUCT)
    essential_product_df: DataFrame = product_df.select('product_id', 'category')

    order_item_df: DataFrame = load_kafka_topic(spark, BronzeTopic.ORDER_ITEM)
    order_product_category: DataFrame = essential_product_df.join(order_item_df, on='product_id', how='inner')
    order_product: DataFrame = order_product_category.withColumn('total_price', F.round(F.col('price') + F.col('freight_value'), 4)).drop('price', 'freight_value')

    delivered_order_id: DataFrame = spark.read.table(f"{silver_namespace}.order_timestamp").select('order_id').distinct()
    order_product: DataFrame = order_product.join(delivered_order_id, on='order_id', how='inner')
    return order_product

# 3. delivered_order_timestamp 처리 (Silver)
def process_delivered_order_timestamp(spark: SparkSession, silver_namespace: str) -> DataFrame:
    estimated_delivery_date_df: DataFrame = load_kafka_topic(spark, BronzeTopic.ESTIMATED_DELIVERY_DATE)
    shipping_limit_date: DataFrame = spark.read.table(f"{silver_namespace}.order_product").select('order_id', 'shipping_limit_date').withColumn('shipping_limit_date', F.to_timestamp('shipping_limit_date', 'yyyy-MM-dd HH:mm:ss'))

    delivered_order_timestamp: DataFrame = shipping_limit_date.join(spark.read.table(f"{silver_namespace}.order_timestamp"), on='order_id', how='outer') \
        .join(estimated_delivery_date_df.withColumn('estimated_delivery_date', F.to_timestamp('estimated_delivery_date', 'yyyy-MM-dd HH:mm:ss')), on='order_id', how='outer') \
        .dropna().drop_duplicates()
    return delivered_order_timestamp

# 4. delivered_order_product 처리 (Silver)
def process_delivered_order_product(spark: SparkSession, silver_namespace: str) -> DataFrame:
    return spark.read.table(f"{silver_namespace}.order_product").drop('shipping_limit_date')

# 5. sale_stats 처리 (Gold)
def process_sale_stats(spark: SparkSession, silver_namespace: str) -> DataFrame:
    sale_stats: DataFrame = spark.read.table(f"{silver_namespace}.delivered_order_product").groupBy('product_id', 'category', 'seller_id').agg(
        F.count(F.col('order_id')).alias('order_count'),
        F.round(F.sum(F.col('total_price')), 4).alias('total_sales')
    ).withColumn('mean_sale', F.round(F.col('total_sales') / F.col('order_count'), 4)).orderBy(F.col('order_count').desc())
    return sale_stats

# 6. health_beauty_sales_stats_bcg 처리 (Gold)
def process_health_beauty_sales_stats_bcg(spark: SparkSession, gold_namespace: str) -> DataFrame:
    target_product_sales_stats: DataFrame = spark.read.table(f"{gold_namespace}.sale_stats").filter(F.col('category') == 'health_beauty')
    order_count_threshold: int = target_product_sales_stats.agg(F.expr("percentile_approx(order_count, 0.75)")).collect()[0][0]
    median_avg_price: float = target_product_sales_stats.agg(F.expr("percentile_approx(mean_sale, 0.5)")).collect()[0][0]

    health_beauty_sales_stats_bcg: DataFrame = target_product_sales_stats.withColumn("segment",
        F.when((F.col("order_count") >= order_count_threshold) & (F.col("mean_sale") >= median_avg_price), "Star Products")
        .when((F.col("order_count") >= order_count_threshold) & (F.col("mean_sale") < median_avg_price), "Volume Drivers")
        .when((F.col("order_count") < order_count_threshold) & (F.col("mean_sale") >= median_avg_price), "Niche Gems")
        .otherwise("Question Marks")
    )
    return health_beauty_sales_stats_bcg

# 7. delivered_order_product_bcg 처리 (Silver)
def process_delivered_order_product_bcg(spark: SparkSession, silver_namespace: str, gold_namespace: str) -> DataFrame:
    return spark.read.table(f"{silver_namespace}.delivered_order_product").join(
        spark.read.table(f"{gold_namespace}.health_beauty_sales_stats_bcg").select('product_id', 'segment'), on='product_id', how='inner'
    )

# 8. timestamp_stats_long 처리 (Gold)
def process_timestamp_stats_long(spark: SparkSession, silver_namespace: str) -> DataFrame:
    timestamp_stats_wide: DataFrame = spark.read.table(f"{silver_namespace}.delivered_order_timestamp").withColumn(
        'lead_time_approve_days', F.datediff(F.col('approved'), F.col('purchase'))
    ).withColumn(
        'lead_time_carrier_days', F.datediff(F.col('delivered_carrier'), F.col('approved'))
    ).withColumn(
        'lead_time_customer_days', F.datediff(F.col('delivered_customer'), F.col('delivered_carrier'))
    ).withColumn(
        'is_late_delivery', F.when(F.col('delivered_customer') > F.col('estimated_delivery_date'), 'late_delivery').otherwise('on_time_delivery')
    ).withColumn(
        'is_late_shipping', F.when(F.col('delivered_carrier') > F.col('shipping_limit_date'), 'late_shipping').otherwise('on_time_ship')
    )

    timestamp_stats_long: DataFrame = timestamp_stats_wide.selectExpr(
        "order_id",
        "is_late_delivery",
        "is_late_shipping",
        "stack(3, 'until_approve', lead_time_approve_days, 'until_carrier', lead_time_carrier_days, 'until_customer', lead_time_customer_days) as (lead_time_type, lead_time_days)"
    )
    return timestamp_stats_long

# 9. user_location 처리 (Silver)
def process_user_location(spark: SparkSession) -> DataFrame:
    geolocation_df: DataFrame = load_kafka_topic(spark, BronzeTopic.GEOLOCATION)
    condition = (F.col("lat") >= -33.7) & (F.col("lat") <= 5.3) & (F.col("lng") >= -74.0) & (F.col("lng") <= -34.8)
    geolocation_df = geolocation_df.filter(condition)

    customer_df: DataFrame = load_kafka_topic(spark, BronzeTopic.CUSTOMER).withColumn('user_type', F.lit('customer')).withColumnRenamed('customer_id', 'user_id')
    seller_df: DataFrame = load_kafka_topic(spark, BronzeTopic.SELLER).withColumn('user_type', F.lit('seller')).withColumnRenamed('seller_id', 'user_id')
    all_user_df: DataFrame = customer_df.union(seller_df).dropDuplicates()

    user_location: DataFrame = all_user_df.join(geolocation_df, on='zip_code', how='inner').orderBy('zip_code')
    return user_location

# 10. order_location 처리 (Gold)
def process_order_location(spark: SparkSession, silver_namespace: str) -> DataFrame:
    delivered_order_customer_id: DataFrame = spark.read.table(f"{silver_namespace}.delivered_order_timestamp").select('order_id', 'customer_id')
    users_order_product: DataFrame = spark.read.table(f"{silver_namespace}.delivered_order_product_bcg").join(delivered_order_customer_id, on='order_id', how='inner')
    customer_order: DataFrame = users_order_product.select('product_id', 'category', 'customer_id').withColumnRenamed('customer_id', 'user_id').withColumn("tmp_id", expr("uuid()"))
    seller_order: DataFrame = users_order_product.select('product_id', 'category', 'seller_id').withColumnRenamed('seller_id', 'user_id').withColumn("tmp_id", expr("uuid()"))
    user_order: DataFrame = customer_order.union(seller_order)

    user_location: DataFrame = spark.read.table(f"{silver_namespace}.user_location").withColumns({'lat': F.col("lat").cast(FloatType()), 'lng': F.col("lng").cast(FloatType())})
    order_location: DataFrame = user_order.join(user_location, on='user_id', how='inner').orderBy('product_id')
    return order_location

# 11. review_metatdata_product 처리 (Gold)
def process_review_metatdata(spark: SparkSession) -> DataFrame:
    review_df: DataFrame = load_kafka_topic(spark, BronzeTopic.REVIEW)
    return review_df.select('review_id', 'review_creation_date', 'review_answer_timestamp', 'review_score', 'order_id')

# 메인 로직
if __name__ == "__main__":
    # SparkSession 생성
    spark: SparkSession = get_spark_session(dev=True)

    # 네임스페이스 생성
    silver_namespace: str = 'warehousedev.silver'
    gold_namespace: str = 'warehousedev.gold'
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {silver_namespace}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gold_namespace}")

    for table in [row.tableName for row in spark.sql('show tables in gold').collect()]:
        spark.sql(f'drop table if exists gold.{table}')

    exit()

    # 각 단계별 스트리밍 쿼리 실행
    order_timestamp: DataFrame = process_order_timestamp(spark)
    order_timestamp.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{silver_namespace}.order_timestamp")) \
        .queryName("load_order_timestamp") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/order_timestamp") \
        .trigger(processingTime="90 seconds") \
        .start()

    order_product: DataFrame = process_order_product(spark, silver_namespace)
    order_product.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{silver_namespace}.order_product")) \
        .queryName("load_order_product") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/order_product") \
        .trigger(processingTime="90 seconds") \
        .start()

    delivered_order_timestamp: DataFrame = process_delivered_order_timestamp(spark, silver_namespace)
    delivered_order_timestamp.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{silver_namespace}.delivered_order_timestamp")) \
        .queryName("load_delivered_order_timestamp") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/delivered_order_timestamp") \
        .trigger(processingTime="90 seconds") \
        .start()

    delivered_order_product: DataFrame = process_delivered_order_product(spark, silver_namespace)
    delivered_order_product.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{silver_namespace}.delivered_order_product")) \
        .queryName("load_delivered_order_product") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/delivered_order_product") \
        .trigger(processingTime="90 seconds") \
        .start()

    sale_stats: DataFrame = process_sale_stats(spark, silver_namespace)
    sale_stats.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{gold_namespace}.sale_stats")) \
        .queryName("load_sale_stats") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/sale_stats") \
        .trigger(processingTime="90 seconds") \
        .start()

    health_beauty_sales_stats_bcg: DataFrame = process_health_beauty_sales_stats_bcg(spark, gold_namespace)
    health_beauty_sales_stats_bcg.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{gold_namespace}.health_beauty_sales_stats_bcg")) \
        .queryName("load_health_beauty_sales_stats_bcg") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/health_beauty_sales_stats_bcg") \
        .trigger(processingTime="90 seconds") \
        .start()

    delivered_order_product_bcg: DataFrame = process_delivered_order_product_bcg(spark, silver_namespace, gold_namespace)
    delivered_order_product_bcg.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{silver_namespace}.delivered_order_product_bcg")) \
        .queryName("load_delivered_order_product_bcg") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/delivered_order_product_bcg") \
        .trigger(processingTime="90 seconds") \
        .start()

    timestamp_stats_long: DataFrame = process_timestamp_stats_long(spark, silver_namespace)
    timestamp_stats_long.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{gold_namespace}.timestamp_stats_long")) \
        .queryName("load_timestamp_stats_long") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/timestamp_stats_long") \
        .trigger(processingTime="90 seconds") \
        .start()

    user_location: DataFrame = process_user_location(spark)
    user_location.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{silver_namespace}.user_location")) \
        .queryName("load_user_location") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/user_location") \
        .trigger(processingTime="90 seconds") \
        .start()

    order_location: DataFrame = process_order_location(spark, silver_namespace)
    order_location.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{gold_namespace}.order_location")) \
        .queryName("load_order_location") \
        .option("checkpointLocation", "s3a://warehousedev/bronze/checkpoints/order_location") \
        .trigger(processingTime="90 seconds") \
        .start()

    # TODO: 로직 수정
    review_metatdata: DataFrame = process_review_metatdata(spark)
    review_metatdata.writeStream \
        .foreachBatch(lambda df, id: load_to_iceberg(df, f"{silver_namespace}.review_metatdata")) \
        .queryName("load_review_metatdata") \
        .option("checkpointLocation", "s3a://warehousedev/silver/checkpoints/review_metatdata") \
        .trigger(processingTime="90 seconds") \
        .start()

    # 모든 스트림 쿼리 대기
    spark.streams.awaitAnyTermination()