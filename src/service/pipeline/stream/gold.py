from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from ..base import BaseJob
from schema import gold
from service.utils.iceberg import write_iceberg
from service.utils.spark import get_spark_session

class StreamGoldJob(BaseJob):
    src_namespace: str = 'silver'
    dst_namespace: str = "gold"

    output_df: Optional[DataFrame] = None
    schema: Optional[StructType] = None

    def __init__(self, spark_session: Optional[SparkSession] = None):
        self._dev = True
        self.spark_session = get_spark_session(f"{self.job_name}", dev=self._dev) if spark_session is None else spark_session

class FactOrderTimeline(StreamGoldJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.schema = gold.FACT_ORDER_TIMELINE
        self.dst_table_name = 'fact_order_timeline'
        self.initialize_dst_table()

    def extract(self):
        self.src_df = self.spark_session.readStream \
            .format('iceberg') \
            .table(f'{self.src_namespace}.fact_order_status')
            # .withWatermark('process_timestamp', '60 days')

    def transform(self,):
        self.output_df = self.src_df \
            .groupBy('order_id') \
            .agg(
                F.max(F.when(F.col('data_type') == 'purchase', F.col('timestamp'))).alias('purchase'),
                F.max(F.when(F.col('data_type') == 'approved', F.col('timestamp'))).alias('approve'),
                F.max(F.when(F.col('data_type') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier'),
                F.max(F.when(F.col('data_type') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer'),
                F.max(F.when(F.col('data_type') == 'shipping_limit', F.col('timestamp'))).alias('shipping_limit'),
                F.max(F.when(F.col('data_type') == 'estimated_delivery', F.col('timestamp'))).alias('estimated_delivery'),
            )
        
    def load(self, micro_batch:DataFrame, batch_id: int):
        micro_batch.createOrReplaceTempView(self.dst_table_name)
        micro_batch.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_table_identifier} t
            USING {self.dst_table_name} s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.purchase = COALESCE(s.purchase, t.purchase),
                    t.approve = COALESCE(s.approve, t.approve),
                    t.delivered_carrier = COALESCE(s.delivered_carrier, t.delivered_carrier),
                    t.delivered_customer = COALESCE(s.delivered_customer, t.delivered_customer),
                    t.shipping_limit = COALESCE(s.shipping_limit, t.shipping_limit),
                    t.estimated_delivery = COALESCE(s.estimated_delivery, t.estimated_delivery)
            WHEN NOT MATCHED THEN
                INSERT (order_id, purchase, approve, delivered_carrier, delivered_customer, shipping_limit, estimated_delivery)
                VALUES (s.order_id, s.purchase, s.approve, s.delivered_carrier, s.delivered_customer, s.shipping_limit, s.estimated_delivery)
            """)
        self.get_current_dst_count(micro_batch, batch_id, False)

    def get_query(self, process_time='5 seconds'):
        self.extract()
        self.transform()
        return self.output_df.writeStream \
            .outputMode("update") \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()

class DeliveryDetal(StreamGoldJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.schema = gold.DELIVERY_DETAIL
        self.dst_table_name = 'delivery_detail'
        self.dst_namespace = "gold"  # Gold 레이어로 설정
        self.initialize_dst_table()  # Iceberg 테이블 초기화

    def extract(self):
        # Iceberg 테이블 변경 스트림 읽기 (Spark 3.5.6 지원)
        self.src_df = self.spark_session.readStream \
            .format('iceberg') \
            .table(f'{self.src_namespace}.fact_order_item') \
            .withWatermark('process_timestamp', '60 days')

    def transform(self, micro_batch: DataFrame, batch_id: int):
    
        aggregated_df = micro_batch.groupBy("order_id", "product_id", "price", "customer_id") \
            .agg(
                F.count("order_item_id").alias("quantity"),
            ).withColumnRenamed("price", "unit_price")
        
        # # TODO: compare with join after filter
        dim_product = micro_batch.sparkSession.read.table(f'{self.src_namespace}.dim_product')
        dim_user_location = micro_batch.sparkSession.read.table(f'{self.src_namespace}.dim_user_location')
        
        # Customer 쪽: aggregated_df (FACT_ORDER_ITEM 기반) + dim_user_location 조인 + category 추가
        with_customer = aggregated_df \
            .join(dim_user_location.alias("cust_loc"), aggregated_df["customer_id"] == F.col("cust_loc.user_id"), "inner") \
            .join(dim_product, "product_id", "inner") \
            .withColumn("user_type", F.lit("customer")) \
            .select(
                "order_id", "product_id", "category", "quantity", 'unit_price',
                F.col("customer_id").alias("user_id"),
                "user_type",
                F.col("cust_loc.lat").alias("lat"),
                F.col("cust_loc.lng").alias("lng")
            ) \
            .drop("zip_code")  # 불필요 컬럼 제거

        # Seller 쪽: aggregated_df + dim_product (seller_id) + dim_user_location 조인
        with_seller = aggregated_df \
            .join(dim_product, "product_id", "inner") \
            .join(dim_user_location.alias("sell_loc"), F.col("seller_id") == F.col("sell_loc.user_id"), "inner") \
            .withColumn("user_type", F.lit("seller")) \
            .select(
                "order_id", "product_id", "category", "quantity", "unit_price",
                F.col("seller_id").alias("user_id"),
                "user_type",
                F.col("sell_loc.lat").alias("lat"),
                F.col("sell_loc.lng").alias("lng")
            ) \
            .drop("zip_code")

        self.output_df = with_customer.unionByName(with_seller)
        
        self.load()
        self.get_current_dst_count(self.output_df, batch_id, False)

    def load(self, ):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='a')
        
    def get_query(self, process_time='5 seconds'):
        self.extract()

        return self.src_df.writeStream \
            .outputMode("update") \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .foreachBatch(self.transform) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()