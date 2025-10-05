from typing import Optional
from functools import partial, reduce
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, FloatType
from pyspark.sql.functions import expr
from confluent_kafka.serialization import SerializationContext, MessageField

from service.stream.topic import BronzeTopic, SilverTopic
from service.producer.bronze import *
from service.utils.spark import get_spark_session, get_kafka_stream_df, get_serialized_df, get_deserialized_stream_df, start_console_stream
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.utils.schema.reader import AvscReader
from service.utils.kafka import get_confluent_serializer_conf
from service.stream.helper import *

PRODUCER = [
    CustomerBronzeProducer, SellerBronzeProducer, GeolocationBronzeProducer,
    PaymentBronzeProducer, OrderStatusBronzeProducer, ProductBronzeProducer,
    OrderItemBronzeProducer, EstimatedDeliberyDateBronzeProducer, ReviewBronzeProducer
]
TOPIC_NAMES = [producer.dst_topic for producer in PRODUCER]

def load_to_iceberg(batch_df: DataFrame, table_name: str) -> None:
    # 중복 컬럼 방지 위해 ingest_time 드롭
    if 'ingest_time' in batch_df.columns:
        batch_df = batch_df.drop('ingest_time')
    batch_df.writeTo(table_name).createOrReplace()

def process_order_timestamp(topic_df_dict: dict, silver_namespace: str) -> None:
    payment_df = topic_df_dict.get(BronzeTopic.PAYMENT)
    order_status_df = topic_df_dict.get(BronzeTopic.ORDER_STATUS)

    if payment_df and order_status_df:
        order_customer_id = payment_df.select('order_id', 'customer_id').distinct()
        # dev mode: purchase
        delivered_order_id = order_status_df.filter(F.col('status') == 'purchase').select('order_id')
        delivered_order_status = order_status_df.join(delivered_order_id, on='order_id', how='inner')
        delivered_order_status = delivered_order_status.withColumn('timestamp', F.to_timestamp(F.substring(F.col('timestamp'), 1, 19), 'yyyy-MM-dd HH:mm:ss'))

        # 피벗 대신 조건부 컬럼 생성 (status 값은 데이터 확인 후 조정)
        order_timestamp = delivered_order_status.groupBy('order_id').agg(
            F.max(F.when(F.col('status') == 'purchase', F.col('timestamp'))).alias('purchase'),
            F.max(F.when(F.col('status') == 'approved', F.col('timestamp'))).alias('approved'),
            F.max(F.when(F.col('status') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier'),
            F.max(F.when(F.col('status') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer')
        ).join(order_customer_id, on='order_id', how='inner').dropna()

        delivered_order_status.show()
        load_to_iceberg(order_timestamp, f"{silver_namespace}.order_timestamp")

def process_order_product(topic_df_dict: dict, spark: SparkSession, silver_namespace: str) -> None:
    product_df = topic_df_dict.get(BronzeTopic.PRODUCT)
    order_item_df = topic_df_dict.get(BronzeTopic.ORDER_ITEM)

    if product_df and order_item_df:
        essential_product_df = product_df.select('product_id', 'category')
        order_product_category = essential_product_df.join(order_item_df, on='product_id', how='inner')
        order_product = order_product_category.withColumn('total_price', F.round(F.col('price') + F.col('freight_value'), 4)).drop('price', 'freight_value')

        delivered_order_id = spark.read.table(f"{silver_namespace}.order_timestamp").select('order_id').distinct()
        order_product = order_product.join(delivered_order_id, on='order_id', how='inner')

        load_to_iceberg(order_product, f"{silver_namespace}.order_product")

def process_delivered_order_timestamp(topic_df_dict: dict, spark: SparkSession, silver_namespace: str) -> None:
    estimated_delivery_date_df = topic_df_dict.get(BronzeTopic.ESTIMATED_DELIVERY_DATE)
    if estimated_delivery_date_df:
        shipping_limit_date = spark.read.table(f"{silver_namespace}.order_product").select('order_id', 'shipping_limit_date').withColumn('shipping_limit_date', F.to_timestamp('shipping_limit_date', 'yyyy-MM-dd HH:mm:ss'))
        delivered_order_timestamp = shipping_limit_date.join(spark.read.table(f"{silver_namespace}.order_timestamp"), on='order_id', how='outer') \
            .join(estimated_delivery_date_df.withColumn('estimated_delivery_date', F.to_timestamp('estimated_delivery_date', 'yyyy-MM-dd HH:mm:ss')), on='order_id', how='outer') \
            .dropna().drop_duplicates()
        load_to_iceberg(delivered_order_timestamp, f"{silver_namespace}.delivered_order_timestamp")

def process_delivered_order_product(spark: SparkSession, silver_namespace: str) -> None:
    delivered_order_product = spark.read.table(f"{silver_namespace}.order_product").drop('shipping_limit_date')
    load_to_iceberg(delivered_order_product, f"{silver_namespace}.delivered_order_product")

def process_sale_stats(spark: SparkSession, silver_namespace: str, gold_namespace: str) -> None:
    sale_stats = spark.read.table(f"{silver_namespace}.delivered_order_product").groupBy('product_id', 'category', 'seller_id').agg(
        F.count(F.col('order_id')).alias('order_count'),
        F.round(F.sum(F.col('total_price')), 4).alias('total_sales')
    ).withColumn('mean_sale', F.round(F.col('total_sales') / F.col('order_count'), 4)).orderBy(F.col('order_count').desc())
    load_to_iceberg(sale_stats, f"{gold_namespace}.sale_stats")

def process_health_beauty_sales_stats_bcg(spark: SparkSession, gold_namespace: str) -> None:
    target_product_sales_stats = spark.read.table(f"{gold_namespace}.sale_stats").filter(F.col('category') == 'health_beauty')
    order_count_threshold = target_product_sales_stats.agg(F.expr("percentile_approx(order_count, 0.75)")).collect()[0][0]
    median_avg_price = target_product_sales_stats.agg(F.expr("percentile_approx(mean_sale, 0.5)")).collect()[0][0]
    health_beauty_sales_stats_bcg = target_product_sales_stats.withColumn("segment",
        F.when((F.col("order_count") >= order_count_threshold) & (F.col("mean_sale") >= median_avg_price), "Star Products")
        .when((F.col("order_count") >= order_count_threshold) & (F.col("mean_sale") < median_avg_price), "Volume Drivers")
        .when((F.col("order_count") < order_count_threshold) & (F.col("mean_sale") >= median_avg_price), "Niche Gems")
        .otherwise("Question Marks")
    )
    load_to_iceberg(health_beauty_sales_stats_bcg, f"{gold_namespace}.health_beauty_sales_stats_bcg")

def process_delivered_order_product_bcg(spark: SparkSession, silver_namespace: str, gold_namespace: str) -> None:
    delivered_order_product_bcg = spark.read.table(f"{silver_namespace}.delivered_order_product").join(
        spark.read.table(f"{gold_namespace}.health_beauty_sales_stats_bcg").select('product_id', 'segment'), on='product_id', how='inner'
    )
    load_to_iceberg(delivered_order_product_bcg, f"{silver_namespace}.delivered_order_product_bcg")

def process_timestamp_stats_long(spark: SparkSession, silver_namespace: str, gold_namespace: str) -> None:
    timestamp_stats_wide = spark.read.table(f"{silver_namespace}.delivered_order_timestamp").withColumn(
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
    timestamp_stats_long = timestamp_stats_wide.selectExpr(
        "order_id",
        "is_late_delivery",
        "is_late_shipping",
        "stack(3, 'until_approve', lead_time_approve_days, 'until_carrier', lead_time_carrier_days, 'until_customer', lead_time_customer_days) as (lead_time_type, lead_time_days)"
    )

    load_to_iceberg(timestamp_stats_long, f"{gold_namespace}.timestamp_stats_long")

def process_user_location(topic_df_dict: dict, silver_namespace: str) -> None:
    geolocation_df = topic_df_dict.get('geolocation')
    geolocation_df = geolocation_df.select('zip_code', 'lng', 'lat', 'ingest_time')
    condition = (F.col("lat") >= -33.7) & (F.col("lat") <= 5.3) & (F.col("lng") >= -74.0) & (F.col("lng") <= -34.8)
    clean_geolocation_df = geolocation_df.filter(condition)
    # TODO: use an AVSC file for key schema instead of hard-coding in Python class.
    clean_geolocation_df = clean_geolocation_df.withColumn('zip_code', F.col('zip_code').cast(IntegerType()))
    customer_df = topic_df_dict.get('customer')
    customer_df = customer_df.withColumn('user_type', F.lit('customer')).withColumnRenamed('customer_id', 'user_id')
    seller_df = topic_df_dict.get('seller')
    seller_df = seller_df.withColumn('user_type', F.lit('seller')).withColumnRenamed('seller_id', 'user_id')
    all_user_df = customer_df.union(seller_df).dropDuplicates()
    user_location = all_user_df.join(clean_geolocation_df, on='zip_code', how='inner').orderBy('zip_code')
    load_to_iceberg(user_location, f"{silver_namespace}.user_location")

def process_order_location(spark: SparkSession, silver_namespace: str, gold_namespace: str) -> None:
    delivered_order_customer_id = spark.read.table(f"{silver_namespace}.delivered_order_timestamp").select('order_id', 'customer_id')
    users_order_product = spark.read.table(f"{silver_namespace}.delivered_order_product_bcg").join(delivered_order_customer_id, on='order_id', how='inner')
    customer_order = users_order_product.select('product_id', 'category', 'customer_id').withColumnRenamed('customer_id', 'user_id').withColumn("tmp_id", expr("uuid()"))
    seller_order = users_order_product.select('product_id', 'category', 'seller_id').withColumnRenamed('seller_id', 'user_id').withColumn("tmp_id", expr("uuid()"))
    user_order = customer_order.union(seller_order)
    user_location = spark.read.table(f"{silver_namespace}.user_location").withColumns({'lat': F.col("lat").cast(FloatType()), 'lng': F.col("lng").cast(FloatType())})
    order_location = user_order.join(user_location, on='user_id', how='inner').orderBy('product_id')
    load_to_iceberg(order_location, f"{gold_namespace}.order_location")

def process_review_metatdata(topic_df_dict: dict, silver_namespace: str) -> None:
    review_df = topic_df_dict.get(BronzeTopic.REVIEW)
    if review_df:
        review_metatdata = review_df.select('review_id', 'review_creation_date', 'review_answer_timestamp', 'review_score', 'order_id')
        load_to_iceberg(review_metatdata, f"{silver_namespace}.review_metatdata")

def transform_topic_stream(micro_batch_df: DataFrame, batch_id: int):
    micro_batch_df.cache()
    print(f"Processing Batch ID: {batch_id}")

    topic_df_dict = {}
    # TODO: solve falling behind
    for producer in PRODUCER:
        try:
            topic_df = micro_batch_df.filter(F.col("topic") == producer.dst_topic)
            avsc_reader = AvscReader(producer.dst_topic)
            deserialized_df = get_deserialized_stream_df(topic_df, avsc_reader.schema_str, producer.key_column)
            topic_df_dict[producer.dst_topic] = deserialized_df
        except Exception as e:
            print(f"{producer.dst_topic} in batch {batch_id}: {e}")

    silver_namespace = 'warehousedev.silver'
    gold_namespace = 'warehousedev.gold'
    spark = micro_batch_df.sparkSession

    process_order_timestamp(topic_df_dict, silver_namespace)
    process_order_product(topic_df_dict, spark, silver_namespace)
    process_delivered_order_timestamp(topic_df_dict, spark, silver_namespace)
    process_delivered_order_product(spark, silver_namespace)
    process_sale_stats(spark, silver_namespace, gold_namespace)
    process_health_beauty_sales_stats_bcg(spark, gold_namespace)
    process_delivered_order_product_bcg(spark, silver_namespace, gold_namespace)
    process_timestamp_stats_long(spark, silver_namespace, gold_namespace)
    process_user_location(topic_df_dict, silver_namespace)
    process_order_location(spark, silver_namespace, gold_namespace)
    process_review_metatdata(topic_df_dict, silver_namespace)

if __name__ == "__main__":
    spark_session = get_spark_session("Process stream")
    client = SchemaRegistryManager._get_client(use_internal=True)

    silver_namespace = 'warehousedev.silver'
    gold_namespace = 'warehousedev.gold'
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {silver_namespace}")
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {gold_namespace}")

    src_stream_df = get_kafka_stream_df(spark_session, TOPIC_NAMES)
    
    query = src_stream_df.writeStream \
        .foreachBatch(transform_topic_stream) \
        .queryName("process_stream") \
        .option("checkpointLocation", f"s3a://warehousedev/silver/checkpoints/stream") \
        .trigger(processingTime="30 seconds") \
        .start()
        
    query.awaitTermination()