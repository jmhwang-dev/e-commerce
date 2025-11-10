import pandas as pd

from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pyspark.sql.streaming.state import GroupStateTimeout

from .base import BaseStream
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema
from service.pipeline.batch.gold import DimUserLocationBatch, OrderDetailBatch
from service.pipeline.batch.base import BaseBatch

from service.utils.spark import get_kafka_stream_df, start_console_stream
from service.utils.schema.reader import AvscReader

class GoldStream(BaseStream):
    dst_layer: Optional[str] = None
    dst_name: Optional[str] = None

    def __init__(self, is_dev:bool, process_time:str, spark_session: Optional[SparkSession] = None):
        self.process_time = process_time
        self.dst_layer = 'gold'
        self.dst_env = 'prod' if not is_dev else 'dev'
        self.spark_session = spark_session

class DeliverStatus(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.DELIVERY_STATUS
        self.dst_avsc_reader = AvscReader(self.dst_name)
        self.output_schema = BaseBatch.get_schema(self.spark_session, self.dst_avsc_reader)
        BaseBatch.initialize_dst_table(self.spark_session, self.output_schema, self.dst_avsc_reader.dst_table_identifier)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
        
        self.state_schema = StructType([
            StructField("estimated_delivery_date", TimestampType(), True),
            StructField("delivered_customer_date", TimestampType(), True),
            StructField("delay_days", IntegerType(), True),
            StructField("current_ts", TimestampType(), True),
        ])

    def extract(self):
        order_event_avsc_reader = AvscReader(SilverAvroSchema.ORDER_EVENT)
        self.order_event_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, order_event_avsc_reader.table_name),
            order_event_avsc_reader
        )

        # TODO: join gold.order_detail, and gold.dim_user_location
        # order_detail_avsc_reader = AvscReader(GoldAvroSchema.ORDER_DETAIL)
        # self.order_detail = self.spark_session.read.table(order_detail_avsc_reader.dst_table_identifier)

        # dim_user_location_avsc_reader = AvscReader(GoldAvroSchema.DIM_USER_LOCATION)
        # self.dim_user_location = self.spark_session.read.table(dim_user_location_avsc_reader.dst_table_identifier)

    def transform(self):
        
        def stateful_func(key, pdf_iter, state):
            order_id = key[0]
            pdf_list = [pdf for pdf in pdf_iter]
            has_data = len(pdf_list) > 0
            
            # 새로운 데이터 처리
            if has_data:
                all_pdf = pd.concat(pdf_list)
                
                # estimated_delivery와 delivered_customer 이벤트 추출
                estim_date_series = all_pdf[all_pdf['data_type'] == 'estimated_delivery']['timestamp']
                deliv_date_series = all_pdf[all_pdf['data_type'] == 'delivered_customer']['timestamp']
                estim_date = estim_date_series.max() if not estim_date_series.empty else None
                deliv_date = deliv_date_series.max() if not deliv_date_series.empty else None
                
                if estim_date is not None:
                    estim_date = estim_date.tz_localize(None)
                if deliv_date is not None:
                    deliv_date = deliv_date.tz_localize(None)
                
                ingest_time_max = all_pdf['ingest_time'].max()
                current_ts = ingest_time_max.tz_localize(None) if isinstance(ingest_time_max, pd.Timestamp) else ingest_time_max

            else:
                estim_date = None
                deliv_date = None
                current_ts = None
            
            # 기존 상태 가져오기
            if state.exists:
                estim_date_state, deliv_date_state, delay_days_state, current_ts_state = state.get
            else:
                estim_date_state = None
                deliv_date_state = None
                delay_days_state = None
                current_ts_state = None

            # 타임아웃 처리 (시뮬레이션: 타임아웃 시 +1일)
            if state.hasTimedOut and estim_date_state is not None and deliv_date_state is None:
                if current_ts_state is not None:
                    current_ts_state += pd.Timedelta(hours=1)  # 시뮬레이션 시간 증가
                
            
            # 상태 업데이트
            if estim_date is not None:
                estim_date_state = estim_date
            if deliv_date is not None:
                deliv_date_state = deliv_date
            if isinstance(current_ts, pd.Timestamp):
                if current_ts_state is None:
                    current_ts_state = current_ts
                else:
                    current_ts_state = max(current_ts, current_ts_state)
            
            # delay_days 재계산
            if estim_date_state is None:
                delay_days_state = None
            else:
                if deliv_date_state is None:
                    delay_days_state = (current_ts_state - estim_date_state).days

                else:
                    delay_days_state = (deliv_date_state - estim_date_state).days

            # 상태 저장
            state.update((
                estim_date_state,
                deliv_date_state,
                delay_days_state,
                current_ts_state
            ))
            
            # 타임아웃 설정
            if estim_date_state is not None and deliv_date_state is None and current_ts_state is not None:
                state.setTimeoutDuration(3000)  # 1일 (밀리초)

            # 결과 출력
            yield pd.DataFrame({
                "order_id": [order_id],
                "estimated_delivery_date": [estim_date_state],
                "delivered_customer_date": [deliv_date_state],
                "delay_days": [delay_days_state],
                # "current_ts": [current_ts_state]
            })
        
        # Watermark 설정
        delivery_status = self.order_event_stream.withWatermark("ingest_time", "1 day")
        
        # Stateful 처리
        self.output_df = delivery_status.groupBy("order_id") \
            .applyInPandasWithState(
                func=stateful_func,
                outputStructType=self.output_schema,
                stateStructType=self.state_schema,
                outputMode="update",
                timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
            )
        
        # self.output_df = self.output_df \
        #     .join(self.order_detail, on='order_id', how='left') \
        #     .join(self.dim_user_location, on='user_id', how='left')

    def load(self, micro_batch: DataFrame, batch_id):
        micro_batch.createOrReplaceTempView("updates")
        micro_batch.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN
                UPDATE set
                    t.estimated_delivery_date = s.estimated_delivery_date,
                    t.delivered_customer_date = s.delivered_customer_date,
                    t.delay_days = s.delay_days
            WHEN NOT MATCHED THEN
                INSERT *
            """)
        
        # self.get_current_dst_table(output_df.sparkSession, batch_id, False)
        
    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .outputMode('update') \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()

class DimUserLocationStream(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.DIM_USER_LOCATION
        self.dst_avsc_reader = AvscReader(self.dst_name)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
    
    def extract(self):
        geo_coord_avsc_reader = AvscReader(SilverAvroSchema.GEO_COORD)
        self.geo_coord_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, geo_coord_avsc_reader.table_name),
            geo_coord_avsc_reader
        )

        olist_user_avsc_reader = AvscReader(SilverAvroSchema.OLIST_USER)
        self.olist_user_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, olist_user_avsc_reader.table_name),
            olist_user_avsc_reader
        )

    def transform(self,):
        self.geo_coord_stream = self.geo_coord_stream.withWatermark('ingest_time', '30 days')
        self.olist_user_stream = self.olist_user_stream.withWatermark('ingest_time', '30 days')

        joined_df = self.geo_coord_stream \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())).alias('geo_coord') \
            .join(self.olist_user_stream.alias('olist_user'), 
                F.expr("""
                geo_coord.zip_code = olist_user.zip_code AND
                geo_coord.ingest_time >= olist_user.ingest_time - INTERVAL 30 DAYS AND
                geo_coord.ingest_time <= olist_user.ingest_time + INTERVAL 30 DAYS
                """), "fullouter")

        self.output_df = joined_df.select(
            F.coalesce(F.col("geo_coord.zip_code"), F.col("olist_user.zip_code")).alias("zip_code"),
            F.col("geo_coord.lat").alias("lat"),
            F.col("geo_coord.lng").alias("lng"),
            F.col("olist_user.user_id").alias("user_id"),
            F.col("olist_user.user_type").alias("user_type")
        ).dropDuplicates()
        
    def load(self, micro_batch:DataFrame, batch_id: int):
        DimUserLocationBatch(micro_batch.sparkSession).load(micro_batch, batch_id)
        
    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()

class OrderDetailStream(GoldStream):
    def __init__(self, is_dev: bool, process_time: str, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)
        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.ORDER_DETAIL
        self.dst_avsc_reader = AvscReader(self.dst_name)
        
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checkpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
        
    def extract(self):
        customer_avsc_reader = AvscReader(SilverAvroSchema.CUSTOMER_ORDER)
        self.customer_order_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, customer_avsc_reader.table_name),
            customer_avsc_reader
        )

        product_avsc_reader = AvscReader(SilverAvroSchema.PRODUCT_METADATA)
        self.product_metadata_stream = BaseStream.get_topic_df(
            get_kafka_stream_df(self.spark_session, product_avsc_reader.table_name),
            product_avsc_reader
        )

    def transform(self):
        self.customer_order_stream = self.customer_order_stream.withWatermark('ingest_time', '30 days')
        self.product_metadata_stream = self.product_metadata_stream.withWatermark('ingest_time', '30 days')
        
        # join 후 바로 중복 컬럼 병합 (product_id는 co 쪽 선택, ingest_time은 greatest)
        joined_df = self.customer_order_stream.alias('co').join(
            self.product_metadata_stream.alias('pm'),
            on=F.expr("""
                co.product_id = pm.product_id AND
                co.ingest_time >= pm.ingest_time - INTERVAL 30 DAYS AND
                co.ingest_time <= pm.ingest_time + INTERVAL 30 DAYS
            """),
            how='fullouter'
        ).select(
            F.col('co.order_id').alias('order_id'),
            F.col('co.customer_id').alias('customer_id'),
            F.col('co.product_id').alias('product_id'),  # co.product_id 선택 (pm drop)
            F.col('pm.category').alias('category'),
            F.col('co.quantity').alias('quantity'),
            F.col('co.unit_price').alias('unit_price'),
            F.col('pm.seller_id').alias('seller_id'),
            F.greatest(F.col('co.ingest_time'), F.col('pm.ingest_time')).alias('ingest_time')
        ).dropna()
            
        common_columns = ["order_id", "product_id", "category", "quantity", "unit_price"]
        order_seller_df = joined_df \
            .select(*(common_columns + ['seller_id'])) \
            .withColumnRenamed('seller_id', 'user_id')

        order_customer_df = joined_df \
            .select(*(common_columns + ['customer_id'])) \
            .withColumnRenamed('customer_id', 'user_id')
        
        self.output_df = order_seller_df.unionByName(order_customer_df).dropDuplicates()
    
    def load(self, micro_batch:DataFrame, batch_id: int):
        OrderDetailBatch(micro_batch.sparkSession).load(micro_batch, batch_id)

    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checkpoint_path) \
            .start()