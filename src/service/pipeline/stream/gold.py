from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.utils import AnalysisException

from .base import BaseStream
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema
from service.pipeline.common.gold import *
from service.pipeline.batch.silver import OlistUserBatch, ProductMetadataBatch
from service.pipeline.batch.gold import DimUserLocationBatch, FactOrderTimelineBatch, FactOrderLocationBatch


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

class DimUserLocationStream(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.DIM_USER_LOCATION
        self.dst_avsc_reader = AvscReader(self.dst_name)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"
    
    def extract(self):
        self.geo_coord_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, SilverAvroSchema.GEO_COORD),
            SilverAvroSchema.GEO_COORD
        )
        olist_user_avc_reader = AvscReader(SilverAvroSchema.OLIST_USER)
        self.olist_user = self.spark_session.read.table(olist_user_avc_reader.dst_table_identifier)

    def transform(self,):
        # query = start_console_stream(self.geo_coord_stream, output_mode='append')
        # query.awaitTermination()
        # return
        self.output_df = DimUserLocationBase.transform(olist_user=self.olist_user, geo_coord_df=self.geo_coord_stream)
    
    def load(self, micro_batch:DataFrame, batch_id: int):
        DimUserLocationBatch(micro_batch.sparkSession).load(micro_batch, batch_id)
        
    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checpoint_path) \
            .start()

class FactOrderTimelineStream(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.FACT_ORDER_TIMELINE
        self.dst_avsc_reader = AvscReader(self.dst_name)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        self.order_event_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, SilverAvroSchema.ORDER_EVENT),
            SilverAvroSchema.ORDER_EVENT
        ).withWatermark('ingest_time', '30 days')

    def transform(self,):
        self.output_df = FactOrderTimelineBase.transform(self.order_event_stream)
        
    def load(self, micro_batch:DataFrame, batch_id: int):
        FactOrderTimelineBatch(micro_batch.sparkSession).load(micro_batch, batch_id)

    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .outputMode('update') \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checpoint_path) \
            .start()
    
class FactOrderLocationStream(GoldStream):
    def __init__(self, is_dev:bool, process_time, query_version: str, spark_session: Optional[SparkSession] = None):
        super().__init__(is_dev, process_time, spark_session)

        self.query_name = self.__class__.__name__
        self.dst_name = GoldAvroSchema.FACT_ORDER_LOCATION
        self.dst_avsc_reader = AvscReader(self.dst_name)
    
        # s3a://bucket/app/{env}/{layer}/{table}/checkpoint/{version}
        self.checpoint_path = \
            f"s3a://warehousedev/{self.spark_session.sparkContext.appName}/{self.dst_env}/{self.dst_layer}/{self.dst_name}/checkpoint/{query_version}"

    def extract(self):
        self.order_detail_stream = self.get_topic_df(
            get_kafka_stream_df(self.spark_session, SilverAvroSchema.ORDER_DETAIL),
            SilverAvroSchema.ORDER_DETAIL
        )

        dim_user_location_avsc_reader = AvscReader(GoldAvroSchema.DIM_USER_LOCATION)
        self.dim_user_location_df = self.spark_session.read.table(dim_user_location_avsc_reader.dst_table_identifier)

        product_metadata_avsc_reader = AvscReader(SilverAvroSchema.PRODUCT_METADATA)
        self.product_metadata_df = self.spark_session.read.table(product_metadata_avsc_reader.dst_table_identifier)
        
    def transform(self,):
        self.output_df = FactOrderLocationBase.transform(
            order_detail_df=self.order_detail_stream,
            product_metadata_df=self.product_metadata_df,
            dim_user_location_df=self.dim_user_location_df
        )
        
    def load(self, micro_batch:DataFrame, batch_id: int):
        FactOrderLocationBatch(micro_batch.sparkSession).load(micro_batch, batch_id)

    def get_query(self):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .foreachBatch(self.load) \
            .option("checkpointLocation", self.checpoint_path) \
            .start()
