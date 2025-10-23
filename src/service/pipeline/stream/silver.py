from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession

from pyspark.sql.types import StructType
from typing import Union, Optional

from ..base import BaseJob
from service.utils.iceberg import write_iceberg
from schema.silver import *
from service.producer.bronze import BronzeTopic
from service.utils.spark import get_spark_session

class StreamSilverJob(BaseJob):
    src_namespace: str = 'bronze'
    dst_namesapce: str = "silver"
    schema: Union[StructType, None] = None

    def __init__(self, spark_session: Optional[SparkSession] = None, src_table_name: str = ''):
        self._dev = True
        self.spark_session = get_spark_session(f"{self.job_name}", dev=self._dev) if spark_session is None else spark_session
        
        self.src_table_identifier = f"{self.src_namespace}.{src_table_name}"
        self.src_df = self.spark_session.readStream.format('iceberg').load(self.src_table_identifier)

        self.dst_table_identifier: str = f"{self.dst_namesapce}.{self.dst_table_name}"
        self.dst_df = self.spark_session.createDataFrame([], schema=self.schema)
        write_iceberg(self.spark_session, self.dst_df, self.dst_table_identifier, mode='a')

    def get_query(self, process_time='5 seconds'):
        self.src_df.writeStream \
            .format('iceberg') \
            .foreachBatch(self.generate) \
            .queryName(self.job_name) \
            .option('checkpointLocation', f's3a://warehousedev/{self.dst_namesapce}/{self.dst_table_name}/checkpoint') \
            .trigger(processingTime=process_time) \
            .start()
        
    def get_current_dst_count(self,):
        self.dst_df = self.output_df.sparkSession.read.table(f"{self.dst_table_identifier}")
        print(f"Current # of {self.dst_table_identifier }: ", self.dst_df.count())

class GeolocationTransformer(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'geo_coordinate'
        self.schema = GEO_COORDINATE
        super().__init__(spark_session, BronzeTopic.GEOLOCATION)

    def generate(self, micro_batch:DataFrame, batch_id: int):
        # TODO: Consider key type conversion for message publishing
        self.output_df = micro_batch \
            .select('zip_code', 'lng', 'lat') \
            .dropDuplicates() \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType()))
        
        self.update_table()
        self.get_current_dst_count()

    def update_table(self,):
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
class CustomerTransformer(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'customer_zip_code'
        self.schema = CUSTOMER_ZIP_CODE
        super().__init__(spark_session, BronzeTopic.CUSTOMER)

    def generate(self, micro_batch:DataFrame, batch_id: int):
        self.output_df = micro_batch.dropDuplicates()
        self.update_table()
        self.get_current_dst_count()

    def update_table(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.output_df.sparkSession.sql(
            f"""
            merge into {self.dst_table_identifier} t
            using {self.dst_table_name} s
            on t.customer_id = s.customer_id
            when not matched then
                insert (customer_id, zip_code)
                values (s.customer_id, s.zip_code)
            """)
class SellerTransformer(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'seller_zip_code'
        self.schema = SELLER_ZIP_CODE
        super().__init__(spark_session, BronzeTopic.SELLER)

    def generate(self, micro_batch:DataFrame, batch_id: int):
        self.output_df = micro_batch.dropDuplicates()
        self.update_table()
        self.get_current_dst_count()

    def update_table(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.output_df.sparkSession.sql(
            f"""
            merge into {self.dst_table_identifier} t
            using {self.dst_table_name} s
            on t.seller_id = s.seller_id
            when not matched then
                insert (seller_id, zip_code)
                values (s.seller_id, s.zip_code)
            """)
        
class OrderStatusTransformer(StreamSilverJob):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'order_status_timeline'
        self.schema = ORDER_STATUS_TIMELINE
        super().__init__(spark_session, BronzeTopic.ORDER_STATUS)

    def generate(self, micro_batch:DataFrame, batch_id: int):
        self.output_df = micro_batch \
        .groupBy('order_id') \
        .agg(
            F.max(F.when(F.col('status') == 'purchase', F.col('timestamp'))).alias('purchase_timestamp'),
            F.max(F.when(F.col('status') == 'approved', F.col('timestamp'))).alias('approve_timestamp'),
            F.max(F.when(F.col('status') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier_timestamp'),
            F.max(F.when(F.col('status') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer_timestamp')
        )
        self.update_table()
        self.get_current_dst_count()
        self.dst_df.show(n=100)

    def update_table(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.output_df.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_table_identifier} t
            USING {self.dst_table_name} s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.purchase_timestamp = COALESCE(s.purchase_timestamp, t.purchase_timestamp),
                    t.approve_timestamp = COALESCE(s.approve_timestamp, t.approve_timestamp),
                    t.delivered_carrier_timestamp = COALESCE(s.delivered_carrier_timestamp, t.delivered_carrier_timestamp),
                    t.delivered_customer_timestamp = COALESCE(s.delivered_customer_timestamp, t.delivered_customer_timestamp)
            WHEN NOT MATCHED THEN
                INSERT (order_id, purchase_timestamp, approve_timestamp, delivered_carrier_timestamp, delivered_customer_timestamp)
                VALUES (s.order_id, s.purchase_timestamp, s.approve_timestamp, s.delivered_carrier_timestamp, s.delivered_customer_timestamp)
            """)