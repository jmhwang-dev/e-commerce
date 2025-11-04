from typing import List

from pyspark.sql import SparkSession
from confluent_kafka.schema_registry.error import SchemaRegistryError
from pyspark.sql.streaming.query import StreamingQuery

from service.utils.schema.reader import AvscReader
from service.utils.spark import get_deserialized_avro_stream_df, get_kafka_stream_df, stop_streams
from service.utils.iceberg import load_stream_to_iceberg
from service.utils.helper import get_producer, get_avro_key_column

def get_load_cdc_query_list(src_topic_names: List[str], spark_session: SparkSession) -> List[StreamingQuery]:
    try:
        query_list: List[StreamingQuery] = []

        for topic_name in src_topic_names:
            dst_avsc_reader = AvscReader(topic_name)
            key_column = get_avro_key_column(topic_name)    # before: get_producer().key_column

            kafka_stream_df = get_kafka_stream_df(spark_session, topic_name)
            deser_stream_df = get_deserialized_avro_stream_df(kafka_stream_df, key_column, dst_avsc_reader.schema_str)

            query = load_stream_to_iceberg(deser_stream_df, dst_avsc_reader.dst_table_identifier, process_time='5 seconds')
            query_list.append(query)
        
        return query_list

    except SchemaRegistryError as e:
        # From `AvscReader()`
        print(e)
        stop_streams(spark_session, query_list)
        exit()