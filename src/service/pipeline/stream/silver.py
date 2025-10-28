from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from ..base import BaseJob
from schema.silver import *
from service.producer.bronze import BronzeTopic
from service.utils.iceberg import write_iceberg
from service.utils.spark import get_spark_session, get_kafka_stream_df

class StreamSilverJob(BaseJob):
    src_namespace: str = 'bronze'
    dst_namesapce: str = "silver"

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
        self.schema = DIM_USER_LOCATION
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
            F.col("users.zip_code").alias("zip_code"),  # users_stream 기준 선택
            F.col("geo.lng").alias("lng"),
            F.col("geo.lat").alias("lat")
        ).dropDuplicates()

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
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namesapce}/{self.dst_table_name}/checkpoint") \
            .start()

class FactOrderTimeline(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.ESTIMATED_DELIVERY_DATE, BronzeTopic.ORDER_ITEM, BronzeTopic.ORDER_STATUS]
        
        self.schema = FACT_ORDER_TIMELINE
        self.dst_table_name = 'fact_order_timeline'
        self.initialize_dst_table()

    def extract(self):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)

    def transform(self, micro_batch:DataFrame, batch_id: int):
        estimated_df = self.get_topic_df(micro_batch, BronzeTopic.ESTIMATED_DELIVERY_DATE) \
            .withColumnRenamed('estimated_delivery_date', 'timestamp') \
            .withColumn('status', F.lit('estimated_delivery')) \
            .drop('ingest_time')
        
        shippimt_limit_df = self.get_topic_df(micro_batch, BronzeTopic.ORDER_ITEM) \
            .select('order_id', 'shipping_limit_date') \
            .withColumnRenamed('shipping_limit_date', 'timestamp') \
            .withColumn('status', F.lit('shipping_limit')) \
            .dropDuplicates()
        
        order_status_src_df = self.get_topic_df(micro_batch, BronzeTopic.ORDER_STATUS)

        order_status_df = order_status_src_df.drop('ingest_time')

        fact_order_timeline_df = order_status_df.unionByName(estimated_df).unionByName(shippimt_limit_df)

        self.output_df = fact_order_timeline_df \
            .groupBy('order_id') \
            .agg(
                F.max(F.when(F.col('status') == 'purchase', F.col('timestamp'))).alias('purchase'),
                F.max(F.when(F.col('status') == 'approved', F.col('timestamp'))).alias('approve'),
                F.max(F.when(F.col('status') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier'),
                F.max(F.when(F.col('status') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer'),
                F.max(F.when(F.col('status') == 'shipping_limit', F.col('timestamp'))).alias('shipping_limit'),
                F.max(F.when(F.col('status') == 'estimated_delivery', F.col('timestamp'))).alias('estimated_delivery'),
            )
        
        # add process_timestamp
        tmp_df = order_status_src_df.select('ingest_time')
        max_value = tmp_df.agg(F.max("ingest_time").alias("process_timestamp")).collect()[0]["process_timestamp"]
        self.output_df = self.output_df.withColumn('process_timestamp', F.lit(max_value))
        self.load()
        self.get_current_dst_count(micro_batch, batch_id)

    def load(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        
        self.output_df.sparkSession.sql(
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
                    t.estimated_delivery = COALESCE(s.estimated_delivery, t.estimated_delivery),
                    t.process_timestamp = s.process_timestamp
            WHEN NOT MATCHED THEN
                INSERT (order_id, purchase, approve, delivered_carrier, delivered_customer, shipping_limit, estimated_delivery, process_timestamp)
                VALUES (s.order_id, s.purchase, s.approve, s.delivered_carrier, s.delivered_customer, s.shipping_limit, s.estimated_delivery, s.process_timestamp)
            """)
        
class DimProduct(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.PRODUCT, BronzeTopic.ORDER_ITEM]
        
        self.schema = DIM_PRODUCT
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
            ).dropDuplicates()
        

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
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namesapce}/{self.dst_table_name}/checkpoint") \
            .start()

class FactOrderItem(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.schema = FACT_ORDER_ITEM
        self.dst_table_name = 'fact_order_item'
        self.initialize_dst_table()

    def extract(self):
        self.order_item_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ORDER_ITEM), BronzeTopic.ORDER_ITEM) \
            .withWatermark('ingest_time', '30 days')
    
    def transform(self):

        order_trasaction = self.order_item_stream \
            .select('order_id', 'order_item_id', 'product_id', 'price', 'ingest_time') \

        self.output_df = order_trasaction.groupBy('order_id', 'product_id', 'price') \
            .agg(
                F.count('order_item_id').alias('product_count'),
                F.max('ingest_time').alias('ingest_time')
            ).withColumnRenamed('price', 'unit_price')
        

    def load(self, micro_batch: DataFrame, batch_id: int):
        """
        - 일반적인 경우, payment.ingest_time < order_item.ingest_time
        - 항상 위 조건이 성립하는 것은 아님 (네트워크 지연으로 반대의 경우가 있을 수 있음)
        - 거래 기록은 정확도가 중요하므로 모두 append
        - 이 후, 재집계
        """
        micro_batch.createOrReplaceTempView('updates')
        micro_batch.sparkSession.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING updates s
            ON t.order_id = s.order_id 
               AND t.product_id = s.product_id 
               AND t.unit_price = s.unit_price
            WHEN MATCHED THEN
                UPDATE SET 
                    t.product_count = t.product_count + s.product_count
            WHEN NOT MATCHED THEN
                INSERT (order_id, product_id, unit_price, product_count)
                VALUES (s.order_id, s.product_id, s.unit_price, s.product_count)
        """)

        self.get_current_dst_count(micro_batch, batch_id, False)
        
    def get_query(self, process_time='5 seconds'):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .outputMode("complete") \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namesapce}/{self.dst_table_name}/checkpoint") \
            .start()

class FactOrderReview(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.REVIEW]
        
        self.schema = FACT_ORDER_REVIEW
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