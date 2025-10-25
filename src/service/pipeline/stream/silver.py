from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from functools import reduce

from pyspark.sql.types import StructType
from typing import Union, Optional

from ..base import BaseJob
from service.utils.iceberg import write_iceberg
from schema.silver import *
from service.producer.bronze import BronzeTopic
from service.utils.spark import get_spark_session
from service.utils.schema.reader import AvscReader
from service.utils.helper import get_producer

from service.utils.spark import get_deserialized_avro_stream_df, get_kafka_stream_df, stop_streams, start_console_stream

class StreamSilverJob(BaseJob):
    src_namespace: str = 'bronze'
    dst_namesapce: str = "silver"

    output_df: Optional[DataFrame] = None
    schema: Optional[StructType] = None

    def __init__(self, spark_session: Optional[SparkSession] = None):
        self._dev = True
        self.spark_session = get_spark_session(f"{self.job_name}", dev=self._dev) if spark_session is None else spark_session

class Account(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.CUSTOMER, BronzeTopic.SELLER]
        
        self.schema = ACCOUNT
        self.dst_table_name = 'account'
        self.initialize_dst_table()

    def extract(self,):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)

    def transform(self, micro_batch:DataFrame, batch_id: int):
        self.output_df = micro_batch.sparkSession.createDataFrame([], schema=self.schema)
        new_column = 'user_id'

        for topic_name in self.src_topic_names:
            if topic_name == BronzeTopic.CUSTOMER:
                user_type = F.lit('customer')
                existing_column = 'customer_id'
            else:
                user_type = F.lit('seller')
                existing_column = 'seller_id'

            deser_df = self.get_topic_df(micro_batch, topic_name)
            deser_df = deser_df.drop('ingest_time') \
                .withColumnRenamed(existing_column, new_column) \
                .withColumn('user_type', user_type)
            
            self.output_df = self.output_df.unionByName(deser_df)
        
        self.output_df = self.output_df.dropDuplicates()
        self.load()

    def load(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.output_df.sparkSession.sql(
            f"""
            merge into {self.dst_table_identifier} t
            using {self.dst_table_name} s
            on t.user_id = s.user_id
            when not matched then
                insert (zip_code, user_type, user_id)
                values (s. zip_code, s.user_type, s.user_id)
            """)
        self.get_current_dst_count()

class GeoCoordinate(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.GEOLOCATION]
        
        self.schema = GEO_COORDINATE
        self.dst_table_name = 'geo_coordinate'
        self.initialize_dst_table()

    def extract(self,):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)

        # When `readStream` is `iceberg`
        # self.src_df = self.spark_session.readStream.format('iceberg').load(f'{self.src_namespace}.{BronzeTopic.GEOLOCATION}')
        # end

    def transform(self, micro_batch:DataFrame, batch_id: int):
        # TODO: Consider key type conversion for message publishing
        
        # When `readStream` is `iceberg`
        # self.output_df = micro_batch \
        #     .select('zip_code', 'lng', 'lat') \
        #     .dropDuplicates() \
        #     .withColumn('zip_code', F.col('zip_code').cast(IntegerType()))
        # end

        topic_name = self.src_topic_names[0]
        deser_df = self.get_topic_df(micro_batch, topic_name)

        self.output_df = deser_df \
            .select('zip_code', 'lng', 'lat') \
            .dropDuplicates() \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType()))
            
        self.load()
        self.get_current_dst_count()
        self.output_df.show()

    def load(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.output_df.sparkSession.sql(
            f"""
            merge into {self.dst_table_identifier} t
            using {self.dst_table_name} s
            on t.zip_code = s.zip_code
            when not matched then
                insert (zip_code, lng, lat)
                values (s.zip_code, s.lng, s.lat)
            """)

class OrderStatusTimeline(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.ESTIMATED_DELIVERY_DATE, BronzeTopic.ORDER_ITEM, BronzeTopic.ORDER_STATUS]
        
        self.schema = ORDER_STATUS_TIMELINE
        self.dst_table_name = 'order_status_timeline'
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
        
        order_status_df = self.get_topic_df(micro_batch, BronzeTopic.ORDER_STATUS) \
            .drop('ingest_time')

        order_status_timeline_df = order_status_df.unionByName(estimated_df).unionByName(shippimt_limit_df)

        self.output_df = order_status_timeline_df \
            .groupBy('order_id') \
            .agg(
                F.max(F.when(F.col('status') == 'purchase', F.col('timestamp'))).alias('purchase'),
                F.max(F.when(F.col('status') == 'approved', F.col('timestamp'))).alias('approve'),
                F.max(F.when(F.col('status') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier'),
                F.max(F.when(F.col('status') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer'),
                F.max(F.when(F.col('status') == 'shipping_limit', F.col('timestamp'))).alias('shipping_limit'),
                F.max(F.when(F.col('status') == 'estimated_delivery', F.col('timestamp'))).alias('estimated_delivery'),
            )
        
        self.load()

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
                    t.estimated_delivery = COALESCE(s.estimated_delivery, t.estimated_delivery)
            WHEN NOT MATCHED THEN
                INSERT (order_id, purchase, approve, delivered_carrier, delivered_customer, shipping_limit, estimated_delivery)
                VALUES (s.order_id, s.purchase, s.approve, s.delivered_carrier, s.delivered_customer, s.shipping_limit, s.estimated_delivery)
            """)
        
class ProductMetadata(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.PRODUCT, BronzeTopic.ORDER_ITEM]
        
        self.schema = PRODUCT_METADATA
        self.dst_table_name = 'product_metdata'
        self.initialize_dst_table()

    def extract(self):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)

    def transform(self, micro_batch:DataFrame, batch_id: int):

        # Null category is dropped.
        product_category = self.get_topic_df(micro_batch, BronzeTopic.PRODUCT) \
            .select('product_id', 'category') \
            .dropna() \
            .dropDuplicates()
            
        product_seller = self.get_topic_df(micro_batch, BronzeTopic.ORDER_ITEM) \
            .select('product_id', 'seller_id') \
            .dropDuplicates()
        
        self.output_df = product_category.join(product_seller, on='product_id', how='outer')
        
        self.load()
        self.get_current_dst_count()

        conditions = [F.col(c).isNull() for c in self.dst_df.columns]
        final_condition = reduce(lambda x, y: x | y, conditions)
        null_rows = self.dst_df.filter(final_condition)
        null_rows.orderBy('order_id',).show(truncate=False)

    def load(self):
        # TODO: optimize. processing time: 14 ~ 20 senconds (publish interval: 0)
        self.output_df.createOrReplaceTempView(self.dst_table_name)

        # 1. product_id 기반으로 category 업데이트 (이전 행의 NULL category 채우기)
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING (
                SELECT product_id, category
                FROM {self.dst_table_name}
                WHERE category IS NOT NULL
            ) s
            ON t.product_id = s.product_id
            WHEN MATCHED AND t.category IS NULL THEN
                UPDATE SET t.category = s.category
        """)

        # 2. product_id와 seller_id 기반으로 새 행 삽입 (seller_id NULL 제외)
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING {self.dst_table_name} s
            ON t.product_id = s.product_id AND t.seller_id = s.seller_id
            WHEN NOT MATCHED AND s.seller_id IS NOT NULL THEN
                INSERT (product_id, category, seller_id)
                VALUES (s.product_id, s.category, s.seller_id)
        """)

        ## Sol 2.
        # self.output_df.createOrReplaceTempView(self.dst_table_name)
        # # 하나의 MERGE INTO로 결합 (성능 향상)
        # self.output_df.sparkSession.sql(f"""
        #     MERGE INTO {self.dst_table_identifier} t
        #     USING {self.dst_table_name} s
        #     ON t.product_id = s.product_id AND t.seller_id = s.seller_id
        #     WHEN MATCHED AND t.category IS NULL AND s.category IS NOT NULL THEN
        #         UPDATE SET t.category = s.category
        #     WHEN NOT MATCHED AND s.seller_id IS NOT NULL THEN
        #         INSERT (product_id, category, seller_id)
        #         VALUES (s.product_id, s.category, s.seller_id)
        # """)


class OrderDetail(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(spark_session)

        self.job_name = self.__class__.__name__
        self.src_topic_names = [BronzeTopic.PAYMENT, BronzeTopic.ORDER_ITEM]
        
        self.schema = ORDER_DETAIL
        self.dst_table_name = 'order_detail'
        self.initialize_dst_table()

        self.quarantine_fact_order_transaction_identifier = 'silver.qurantine.fact_order_transaction'
        self.initialize_qurantine_table(self.quarantine_fact_order_transaction_identifier, QUARANTINE_FACT_ORDER_TRANSACTION_SCHEMA)

        self.quarantine_order_customer_identifier = 'silver.qurantine.order_customer'
        self.initialize_qurantine_table(self.quarantine_order_customer_identifier, QUARANTINE_ORDER_CUSTOMER_SCHEMA)

    def extract(self):
        self.src_df = get_kafka_stream_df(self.spark_session, self.src_topic_names)
    
    def transform(self, micro_batch:DataFrame, batch_id: int):
        """
        - 토픽을 소비할 때, 배치에 포함이 안돼서 결제 내역과 주문 내역이 매칭이 안될 수 있음
        - quratine.* 테이블에 저장하고 후처리하여 데이터의 완전성을 최대한 보장할 수 있도록 구현
        """

        ### start to process qurantine
        self.quarantine_fact_order_transaction_df = micro_batch.sparkSession.read.table(self.quarantine_fact_order_transaction_identifier)
        self.quarantine_order_customer_df = micro_batch.sparkSession.read.table(self.quarantine_order_customer_identifier)

        matched_df = self.quarantine_fact_order_transaction_df.join(self.quarantine_order_customer_df, on='order_id', how='outer')
        self.quarantine_fact_order_transaction_df = matched_df.filter(F.col('customer_id').isNull()).drop('customer_id')
        self.quarantine_order_customer_df = matched_df.filter(F.col('product_id').isNull()).select('order_id', 'customer_id')
        matched_df = matched_df.dropna()
        ### end

        order_customer = self.get_topic_df(micro_batch, BronzeTopic.PAYMENT) \
            .select('order_id', 'customer_id') \
            .dropDuplicates()

        order_trasaction = self.get_topic_df(micro_batch, BronzeTopic.ORDER_ITEM) \
            .select('order_id', 'order_item_id', 'product_id', 'price') \

        fact_order_trasaction = order_trasaction.groupBy('order_id', 'product_id', 'price') \
            .agg(
                F.count('order_item_id').alias('product_count')
            ).withColumnRenamed('price', 'unit_price')
        

        self.output_df = fact_order_trasaction.join(order_customer, on='order_id', how='outer')

        # TODO: check duplicate (재현이 안됨. 중복이 있을 떄가 있고 없을 떄가 있음)
        self.quarantine_fact_order_transaction_df = self.output_df.filter(F.col('customer_id').isNull()).drop('customer_id').unionByName(self.quarantine_fact_order_transaction_df)
        self.quarantine_order_customer_df = self.output_df.filter(F.col('product_id').isNull()).select('order_id', 'customer_id').unionByName(self.quarantine_order_customer_df)
        self.output_df = self.output_df.dropna().unionByName(matched_df)

        self.load()
        self.get_current_dst_count(batch_id)

    def load(self):
        """
        - 일반적인 경우, payment.ingest_time < order_item.ingest_time
        - 항상 위 조건이 성립하는 것은 아님 (네트워크 지연으로 반대의 경우가 있을 수 있음)
        - 거래 기록은 정확도가 중요하므로 watermark로 레코드의 하한을 정하면 안됨
        - (준)실시간으로 데이터를 처리해야하므로, 임시 뷰를 활용하여 데이터를 upsert
        """

        write_iceberg(self.output_df.sparkSession, self.output_df, self.dst_table_identifier, mode='a')

        tmp_view_name = 'tmp_view'
        self.quarantine_fact_order_transaction_df.createOrReplaceTempView(tmp_view_name)
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.quarantine_fact_order_transaction_identifier} t
            USING {tmp_view_name} s
            ON t.order_id = s.order_id
            WHEN NOT MATCHED THEN
                INSERT (order_id, product_id, unit_price, product_count)
                VALUES (s.order_id, s.product_id, s.unit_price, s.product_count)
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE
            """)
        
        self.quarantine_order_customer_df.createOrReplaceTempView(tmp_view_name)
        self.output_df.sparkSession.sql(f"""
            MERGE INTO {self.quarantine_order_customer_identifier} t
            USING {tmp_view_name} s
            ON t.order_id = s.order_id
            WHEN NOT MATCHED THEN
                INSERT (order_id, customer_id)
                VALUES (s.order_id, s.customer_id)
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE
            """)