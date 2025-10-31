from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, IntegerType
from pyspark.sql.avro.functions import to_avro

from ..base import BaseJob
from schema import silver
from service.producer.bronze import BronzeTopic
from service.producer.silver import SilverTopic
from service.utils.iceberg import write_iceberg
from service.utils.spark import get_spark_session, get_kafka_stream_df, start_console_stream
from service.utils.schema.reader import AvscReader
from config.kafka import BOOTSTRAP_SERVERS_INTERNAL

class StreamSilverJob(BaseJob):
    src_namespace: str = 'bronze'
    dst_namespace: str = "silver"

    output_df: Optional[DataFrame] = None
    schema: Optional[StructType] = None

    def __init__(self, spark_session: Optional[SparkSession] = None, is_batch:bool=False):
        self._dev = True
        
        self.is_batch = is_batch
        self.spark_session = get_spark_session(f"{self.job_name}", dev=self._dev) if spark_session is None else spark_session

class GeoCoord(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_batch:bool = False):
        super().__init__(spark_session, is_batch)
        self.job_name = self.__class__.__name__

        self.schema = silver.GEO_COORD
        self.dst_table_name = 'geo_coord'
        if self.is_batch:
            self.initialize_dst_table()  # Iceberg 테이블 초기화

    def extract(self):
        self.geo_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeTopic.GEOLOCATION),
            BronzeTopic.GEOLOCATION
        ) \
        .select('zip_code', 'lng', 'lat', 'ingest_time') \
        .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
        .withWatermark('ingest_time', '1 days') \
        .dropDuplicates(['zip_code'])

    def transform(self):
        # 브라질 위경도 범위 상수
        BRAZIL_BOUNDS = {
            "min_lat": -33.69111,
            "max_lat": 2.81972,
            "min_lon": -72.89583,
            "max_lon": -34.80861
        }

        self.output_df = self.geo_stream \
            .dropDuplicates(['zip_code']) \
            .filter(
                (F.col("lat").between(BRAZIL_BOUNDS["min_lat"], BRAZIL_BOUNDS["max_lat"])) &
                (F.col("lng").between(BRAZIL_BOUNDS["min_lon"], BRAZIL_BOUNDS["max_lon"]))
            )
        
        self.avsc_reader = AvscReader(SilverTopic.GEO_COORD)
        self.output_df = self.output_df.select(
            F.col("zip_code").cast("string").alias("key"),
            to_avro(
                F.struct("lat", "lng"),
                self.avsc_reader.schema_str).alias('value'))

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

        if self.is_batch:
            self.load()
            return

        return self.output_df.writeStream \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .format("kafka") \
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
            .option("topic", self.avsc_reader.table_name) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()
    
class OlistUser(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_batch:bool = False):
        super().__init__(spark_session, is_batch)
        self.job_name = self.__class__.__name__

        self.schema = silver.OLIST_USER
        self.dst_table_name = 'olist_user'
        self.initialize_dst_table()  # Iceberg 테이블 초기화

    def extract(self):
        # customer
        self.customer_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeTopic.CUSTOMER),
            BronzeTopic.CUSTOMER
        ).withWatermark('ingest_time', '1 days')

        # seller
        self.seller_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, BronzeTopic.SELLER),
            BronzeTopic.SELLER
        ).withWatermark('ingest_time', '1 days')

    def transform(self):
        
        customer_df = self.customer_stream \
            .withColumnRenamed('customer_id', 'user_id') \
            .withColumn('user_type', F.lit('customer')) \
            .drop('ingest_time') \
            .dropDuplicates()

        seller_df = self.seller_stream \
            .withColumnRenamed('seller_id', 'user_id') \
            .withColumn('user_type', F.lit('seller')) \
            .drop('ingest_time') \
            .dropDuplicates()
        
        self.output_df = customer_df.unionByName(seller_df)

    def load(self, micro_batch: DataFrame, batch_id: int):
        
        if micro_batch.isEmpty():
            return
        
        # TODO: watermark 기간에서 제외된 누락된 데이터는 배치로 처리
        micro_batch.createOrReplaceTempView("updates")
        micro_batch.sparkSession.sql(f"""
            MERGE INTO {self.dst_table_identifier} AS target
            USING updates AS source
            ON target.user_type = source.user_type AND target.user_id = source.user_id
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

class OrderEvent(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_batch:bool = False):
        super().__init__(spark_session, is_batch)

        self.job_name = self.__class__.__name__
        
        if is_batch:
            self.schema = silver.ORDER_EVENT
            self.dst_table_name = 'order_event'
            self.initialize_dst_table()

    def extract(self):
        # self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)
        self.estimated_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ESTIMATED_DELIVERY_DATE), BronzeTopic.ESTIMATED_DELIVERY_DATE)
        self.shippimt_limit_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ORDER_ITEM), BronzeTopic.ORDER_ITEM)
        self.order_status_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ORDER_STATUS), BronzeTopic.ORDER_STATUS)

        if not self.is_batch:
            # 데이터에 기록된 timestamp 열은 Watermark 로 사용할 수 없음
            #   - data_type이 estimated_delivery 와 shipping_limit_date은 실제 이벤트 시간과 기간이 차이남
            #   - 불필요하게 긴 워터마크 설정으로 인해 상태 스토어가 커질 수 있음
            self.estimated_df = self.estimated_df.withWatermark('ingest_time', '10 minutes')
            self.shippimt_limit_df = self.shippimt_limit_df.withWatermark('ingest_time', '10 minutes')
            self.order_status_df = self.order_status_df.withWatermark('ingest_time', '10 minutes')
            
    def transform(self):
        # TODO: chage timezone to UTC. (현재는 KST로 들어감)
        estimated_df = self.estimated_df \
            .withColumnRenamed('estimated_delivery_date', 'timestamp') \
            .withColumn('data_type', F.lit('estimated_delivery'))
        
        shippimt_limit_df = self.shippimt_limit_df \
            .select('order_id', 'shipping_limit_date', 'ingest_time') \
            .withColumnRenamed('shipping_limit_date', 'timestamp') \
            .withColumn('data_type', F.lit('shipping_limit'))
        
        order_status_df = self.order_status_df.withColumnRenamed('status', 'data_type')

        self.output_df = order_status_df.unionByName(estimated_df).unionByName(shippimt_limit_df)

        if self.is_batch:
            self.load()
            return
        
        self.avsc_reader = AvscReader(SilverTopic.ORDER_EVENT)
        self.output_df = self.output_df.select(
            F.col("order_id").cast("string").alias("key"),
            to_avro(
                F.struct("data_type", "timestamp", "ingest_time"),
                self.avsc_reader.schema_str).alias('value'))

    def load(self,):
        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='a')

    def get_query(self, process_time='5 seconds'):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .format("kafka") \
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
            .option("topic", self.avsc_reader.table_name) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()

class ProductMetadata(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_batch:bool = False):
        super().__init__(spark_session, is_batch)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.PRODUCT, BronzeTopic.ORDER_ITEM]
        
        self.schema = silver.PRODUCT_METADATA
        self.dst_table_name = 'product_metadata'
        self.initialize_dst_table()

    def extract(self):
        if self.is_batch:
            self.product_df = self.spark_session.read.table(f"{self.src_namespace}.product")
            self.order_item_df = self.spark_session.read.table(f"{self.src_namespace}.order_item")
            return
        
        self.product_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.PRODUCT), BronzeTopic.PRODUCT)
        self.product_df = self.product_df.withWatermark('ingest_time', '30 days')

        self.order_item_df = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ORDER_ITEM), BronzeTopic.ORDER_ITEM)
        self.order_item_df = self.order_item_df.withWatermark('ingest_time', '30 days')

    def transform(self, ):

        product_category = self.product_df \
            .select('product_id', 'category', 'ingest_time') \
            .dropna() \
            .dropDuplicates()
            
        product_seller = self.order_item_df \
            .select('product_id', 'seller_id', 'ingest_time') \
            .dropDuplicates()
        
        if self.is_batch:
            self.output_df = product_category.join(product_seller, on='product_id', how='inner')
            return
        
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
        
        if self.is_batch:
            self.load()
            return

        return self.output_df.writeStream \
            .trigger(processingTime=process_time) \
            .queryName(self.job_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()
    
class OrderDetail(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_batch:bool = False):
        super().__init__(spark_session, is_batch)

        self.job_name = self.__class__.__name__
        self.schema = silver.ORDER_DETAIL
        self.dst_table_name = 'order_detail'
        self.initialize_dst_table()

    def extract(self):
        self.order_item_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ORDER_ITEM), BronzeTopic.ORDER_ITEM) \
            .withWatermark('ingest_time', '3 days')
        self.payment_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.PAYMENT), BronzeTopic.PAYMENT) \
            .withWatermark('ingest_time', '3 days')
        
        # self.order_item_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.ORDER_ITEM), BronzeTopic.ORDER_ITEM)
        # self.payment_stream = self.get_topic_df(get_kafka_stream_df(self.spark_session, BronzeTopic.PAYMENT), BronzeTopic.PAYMENT)

    def transform(self):
        """
        Non-windowed groupBy: 전체 스트림 기간 동안 누적 집로, 상태가 무한이 증가한다. (unbounded state)
        - watermark는 배치에서 late data를 처리하는 목적이다.
        - 따라서 watermark가 있어도 상태를 drop할 시간 경계가 없어서 상태가 finalized 되지 않음
        - 결국 OOM으로 이어질 가능성이 매우 높아지기 때문에, 다음과 같은 예외를 발생시킨다.
        (AnalysisException: "Append output mode not supported for streaming aggregations without watermark")

        대안1. window 함수

        대안2. Append 대신 update (변경된 행만 출력) 또는 complete (전체 결과 재출력) mode로 전환
        - 상태 drop: Update mode에서 watermark 설정 시 오래된 상태 정리 가능 (complete는 모든 상태 유지, drop 안 함).
        - 한계: Append가 필요한 싱크 (e.g., Kafka append-only)에서 불가. Complete는 대규모 데이터에서 비효율적 (전체 재출력).

        대안 3: mapGroupsWithState 또는 flatMapGroupsWithState로 커스텀 상태 관리
        """

        # order_customer state
        order_item = self.order_item_stream.select('order_id', 'order_item_id', 'product_id', 'price', 'ingest_time').dropna()
        aggregated_df = order_item.groupBy(
            F.window("ingest_time", "5 minutes"),  # 필수: non-windowed → windowed
            "order_id", "product_id", "price") \
            .agg(
                F.count("order_item_id").alias("quantity"),
                F.max("ingest_time").alias('agg_ingest_time')
            ).withColumnRenamed("price", "unit_price") \
            .drop('window')                    

    
        order_customer = self.payment_stream.groupBy(
            F.window("ingest_time", "5 minutes"),  # 필수: non-windowed → windowed
            'order_id', 'customer_id').agg(F.max('ingest_time').alias('oc_ingest_time')) \
            .drop('window')
        
        self.output_df = aggregated_df.alias('agg').join(
            order_customer.alias('oc'),
            F.expr("""
                agg.order_id = oc.order_id AND
            """),
            "inner"
        ).select(
            F.col('agg.order_id').alias('order_id'), 'customer_id', 'product_id', 'unit_price', 'quantity'
        )

        self.avsc_reader = AvscReader(SilverTopic.ORDER_DETAIL)
        self.output_df = self.output_df.select(
            F.col("order_id").cast("string").alias("key"),
            to_avro(
                F.struct("product_id", "unit_price", "customer_id", "quantity"),
                self.avsc_reader.schema_str).alias('value'))

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
            .trigger(processingTime=process_time) \
            .outputMode('append') \
            .queryName(self.job_name) \
            .format("kafka") \
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
            .option("topic", self.avsc_reader.table_name) \
            .option("checkpointLocation", f"s3a://warehousedev/{self.dst_namespace}/{self.dst_table_name}/checkpoint") \
            .start()

class ReviewMetadata(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None, is_batch:bool = False):
        super().__init__(spark_session, is_batch)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.REVIEW]
        
        self.schema = silver.REVIEW_METADATA
        self.dst_table_name = 'review_metadata'
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