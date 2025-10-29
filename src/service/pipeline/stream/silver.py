from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, IntegerType

from ..base import BaseJob
from schema import silver
from service.producer.bronze import BronzeTopic
from service.utils.iceberg import write_iceberg
from service.utils.spark import get_spark_session, get_kafka_stream_df

class StreamSilverJob(BaseJob):
    src_namespace: str = 'bronze'
    dst_namespace: str = "silver"

    output_df: Optional[DataFrame] = None
    schema: Optional[StructType] = None

    def __init__(self, spark_session: Optional[SparkSession] = None):
        self._dev = True
        self.spark_session = get_spark_session(f"{self.job_name}", dev=self._dev) if spark_session is None else spark_session

class DimUserLocation(StreamSilverJob):
    """
    # Full Outer Join 기반 실시간 사용자 위치 보강 (Iceberg 저장)
    
    ## 핵심 로직
    # `users_stream` (customer + seller) + `geo_stream` → `fullouter`
        - 조건: `zip_code` 동일 + `ingest_time ±10분`

    ## 상태 스토어 (RocksDB) 동작
        1. 배치 시작 → 기존 unmatched 로드
        2. 새 데이터와 fullouter join
        3. Matched → 출력 + 상태 제거
        4. Unmatched → 상태 저장
        5. 워터마크 초과 → 자동 삭제
    """
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)
        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.CUSTOMER, BronzeTopic.SELLER, BronzeTopic.GEOLOCATION]
        self.schema = silver.DIM_USER_LOCATION
        self.dst_table_name = 'dim_user_location'
        self.initialize_dst_table()  # Iceberg 테이블 초기화

    def extract(self):
        # customer
        customer_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeTopic.CUSTOMER),
            BronzeTopic.CUSTOMER
        ) \
        .withColumnRenamed('customer_id', 'user_id') \
        .withColumn('user_type', F.lit('customer')) \

        # seller
        seller_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeTopic.SELLER),
            BronzeTopic.SELLER
        ) \
        .withColumnRenamed('seller_id', 'user_id') \
        .withColumn('user_type', F.lit('seller')) \

        # geolocation
        self.geo_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeTopic.GEOLOCATION),
            BronzeTopic.GEOLOCATION
        ) \
        .select('zip_code', 'lng', 'lat', 'ingest_time') \
        .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
        .withWatermark('ingest_time', '30 days') \
        .dropDuplicates(['zip_code'])

        self.users_stream = customer_stream.unionByName(seller_stream).dropDuplicates() \
            .withWatermark('ingest_time', '30 days')

    def transform(self):
        self.output_df = self.users_stream.alias('users').join(
            self.geo_stream.alias('geo'),
            F.expr("""
                users.zip_code = geo.zip_code AND
                users.ingest_time >= geo.ingest_time - interval 30 days AND
                users.ingest_time <= geo.ingest_time + interval 30 days
            """),
            how="fullouter"
        ).select(
            F.col("users.user_type").alias("user_type"),
            F.col("users.user_id").alias("user_id"),
            F.col("users.zip_code").alias("zip_code"),
            F.col("geo.lng").alias("lng"),
            F.col("geo.lat").alias("lat")
        ).dropDuplicates() \
        .dropna()

    def load(self, micro_batch: DataFrame, batch_id: int):
        
        if micro_batch.isEmpty():
            return
        
        # TODO: watermark 기간에서 제외된 누락된 데이터는 배치로 처리
        micro_batch.createOrReplaceTempView("updates")
        micro_batch.sparkSession.sql(f"""
            MERGE INTO {self.dst_table_identifier} AS target
            USING updates AS source
            ON target.user_type = source.user_type AND target.user_id = source.user_id
            WHEN MATCHED and target.lng is null THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        self.get_current_dst_count(micro_batch, batch_id)
        
    def get_query(self, process_time='5 seconds'):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()

class FactOrderStatus(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.ESTIMATED_DELIVERY_DATE, BronzeTopic.ORDER_ITEM, BronzeTopic.ORDER_STATUS]
        
        self.schema = silver.FACT_ORDER_STATUS
        self.dst_table_name = 'fact_order_status'
        self.initialize_dst_table()

    def extract(self):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)

    def transform(self, micro_batch:DataFrame, batch_id: int):
        
        # TODO: chage timezone to UTC. (현재는 KST로 들어감)
        estimated_df = self.get_topic_df(micro_batch, BronzeTopic.ESTIMATED_DELIVERY_DATE) \
            .withColumnRenamed('estimated_delivery_date', 'timestamp') \
            .withColumn('data_type', F.lit('estimated_delivery')) \
        
        shippimt_limit_df = self.get_topic_df(micro_batch, BronzeTopic.ORDER_ITEM) \
            .select('order_id', 'shipping_limit_date', 'ingest_time') \
            .withColumnRenamed('shipping_limit_date', 'timestamp') \
            .withColumn('data_type', F.lit('shipping_limit')) \
        
        order_status_df = self.get_topic_df(micro_batch, BronzeTopic.ORDER_STATUS)
        order_status_df = order_status_df.withColumnRenamed('status', 'data_type')

        # add process_timestamp
        tmp_df = order_status_df.unionByName(estimated_df).unionByName(shippimt_limit_df)
        max_value = tmp_df.agg(F.max("ingest_time").alias("process_timestamp")).collect()[0]["process_timestamp"]
        self.output_df = tmp_df.drop('ingest_time').withColumn('process_timestamp', F.lit(max_value)).dropDuplicates()

        self.load()
        self.get_current_dst_count(micro_batch, batch_id, True)

    def load(self,):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='a')

class DimProduct(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.PRODUCT, BronzeTopic.ORDER_ITEM]
        
        self.schema = silver.DIM_PRODUCT
        self.dst_table_name = 'dim_product'
        self.initialize_dst_table()

    def extract(self):
        self.product_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.PRODUCT), BronzeTopic.PRODUCT)
        self.order_item_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ORDER_ITEM), BronzeTopic.ORDER_ITEM)

    def transform(self, ):

        product_category = self.product_df \
            .select('product_id', 'category', 'ingest_time') \
            .dropna() \
            .dropDuplicates() \
            .withWatermark('ingest_time', '30 days')
            
        product_seller = self.order_item_df \
            .select('product_id', 'seller_id', 'ingest_time') \
            .dropDuplicates() \
            .withWatermark('ingest_time', '30 days')
        
        self.output_df = product_category.alias('p_cat').join(
            product_seller.alias('p_sel'),
            on=F.expr("""
                p_cat.product_id = p_sel.product_id and
                p_cat.ingest_time >= p_sel.ingest_time - interval 30 days AND
                p_cat.ingest_time <= p_sel.ingest_time + interval 30 days
            """),
            how='fullouter'
            ).select(
                F.col('p_cat.category').alias('category'),
                F.col('p_cat.product_id').alias('product_id'),
                F.col('p_sel.seller_id').alias('seller_id'),
            ).dropDuplicates() \
            .dropna()

    def load(self, micro_batch: DataFrame, batch_id: int):
        if micro_batch.isEmpty():
            return
        
        micro_batch.createOrReplaceTempView("updates")
        micro_batch.sparkSession.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING updates s
            ON t.product_id = s.product_id AND t.seller_id = s.seller_id
            WHEN MATCHED AND t.category IS NULL AND s.category IS NOT NULL THEN
                UPDATE SET t.category = s.category
            WHEN NOT MATCHED AND s.seller_id IS NOT NULL THEN
                INSERT (product_id, category, seller_id)
                VALUES (s.product_id, s.category, s.seller_id)
        """)

        self.get_current_dst_count(micro_batch, batch_id, False)

    def get_query(self, process_time='5 seconds'):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()
    
class FactOrderItem(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.schema = silver.FACT_ORDER_ITEM
        self.dst_table_name = 'fact_order_item'
        self.initialize_dst_table()

    def extract(self):
        self.order_item_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ORDER_ITEM), BronzeTopic.ORDER_ITEM) \
            .withWatermark('ingest_time', '30 days')
        
        self.payment_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.PAYMENT), BronzeTopic.PAYMENT) \
            .withWatermark('ingest_time', '30 days')

    def transform(self):
        # Stream-Stream Join (Append 모드 지원)
        order_item = self.order_item_stream.select('order_id', 'order_item_id', 'product_id', 'price', 'ingest_time')
        order_customer = self.payment_stream.select('order_id', 'customer_id', 'ingest_time')

        joined_stream = order_item.alias("oi") \
            .join(
                order_customer.alias("oc"),
                F.expr("""
                    oi.order_id = oc.order_id AND
                    oc.ingest_time >= oi.ingest_time - interval 30 days AND
                    oc.ingest_time <= oi.ingest_time + interval 30 days
                """),
                how="fullouter"
            ).select(
                "oc.customer_id",
                "oi.order_id",
                "oi.order_item_id",
                "oi.product_id",
                "oi.price",
                F.least("oi.ingest_time", "oc.ingest_time").alias('process_timestamp')
            ).dropDuplicates() \
            .dropna()

        # Append 모드에서는 groupBy 직접 불가 → foreachBatch에서 처리
        self.output_df = joined_stream

    def load(self, micro_batch: DataFrame, batch_id: int):
        """
        - 일반적인 경우, payment.ingest_time < order_item.ingest_time
        - 항상 위 조건이 성립하는 것은 아님 (네트워크 지연으로 반대의 경우가 있을 수 있음)
        - 거래 기록은 정확도가 중요하므로 outer join 후 append
        - 이 후, 재집계 (실제 order_id는 존재하나, payment에 기록이 없는 경우가 있음)
        """

        write_iceberg(micro_batch.sparkSession, micro_batch, self.dst_table_identifier, mode='a')
        self.get_current_dst_count(micro_batch, batch_id, False)
        
    def get_query(self, process_time='5 seconds'):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .outputMode("append") \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()

class FactOrderReview(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.REVIEW]
        
        self.schema = silver.FACT_ORDER_REVIEW
        self.dst_table_name = 'fact_order_review'
        self.initialize_dst_table()

    def extract(self):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)
    
    def transform(self, micro_batch:DataFrame, batch_id: int):
        self.output_df = self.get_topic_df(micro_batch, BronzeTopic.REVIEW) \
            .drop('review_comment_title', 'review_comment_message', 'ingest_time')

        self.load()
        self.get_current_dst_count(micro_batch, batch_id)

    def load(self):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='a')