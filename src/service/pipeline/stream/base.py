from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from service.utils.iceberg import *
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro

from service.utils.schema.reader import AvscReader
from service.utils.spark import get_deserialized_avro_stream_df

from service.utils.helper import get_avro_key_column
from config.kafka import BOOTSTRAP_SERVERS_INTERNAL


class BaseStream(ABC):
    """
    Base Stream Job for Silver
    Input: Message
    Output: Table or Message
    """
    spark_session: Optional[SparkSession] = None
    dst_env: Optional[str] = None
    query_name: Optional[str] = None
    query_version: Optional[str] = None

    src_df: Optional[DataFrame] = None
    output_df: Optional[DataFrame] = None
    avsc_reader: Optional[AvscReader] = None
    checpoint_path: Optional[str] = None
    process_time: str = '5 seconds'

    @abstractmethod
    def extract(self,):
        pass

    @abstractmethod
    def transform(self,):
        pass

    def set_byte_stream(self, key_column:str, value_columns: List[str]):
        self.output_df = self.output_df.select(
            F.col(key_column).cast("string").alias("key"),
            to_avro(
                F.struct(*value_columns),
                self.avsc_reader.schema_str).alias('value'))
        
        schema_id_hex = f"{self.avsc_reader.schema_id:08x}"  # AvscReader에 schema_id 추가 필요
        self.output_df = self.output_df.withColumn(
            "value",
            F.concat(
                F.unhex(F.lit("00")),  # Magic byte
                F.unhex(F.lit(schema_id_hex)),  # Schema ID
                F.col("value")  # Avro binary data
            )
        )

    def get_query(self,):
        self.extract()
        self.transform()

        return self.output_df.writeStream \
            .trigger(processingTime=self.process_time) \
            .queryName(self.query_name) \
            .format("kafka") \
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
            .option("topic", self.avsc_reader.table_name) \
            .option("checkpointLocation", self.checpoint_path) \
            .start()
    
    def get_topic_df(self, micro_batch:DataFrame, topic_name: str) -> DataFrame:
        ser_df = micro_batch.filter(F.col("topic") == topic_name)
        avsc_reader = AvscReader(topic_name)
        key_column = get_avro_key_column(topic_name)
        return get_deserialized_avro_stream_df(ser_df, key_column, avsc_reader.schema_str)