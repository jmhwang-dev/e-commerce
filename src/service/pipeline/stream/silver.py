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