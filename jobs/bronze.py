
import argparse

from service.utils.schema.reader import AvscReader
from service.utils.spark import get_deserialized_stream_df
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.utils.spark import get_spark_session, get_kafka_stream_df, start_console_stream
from service.utils.iceberg import write_stream_iceberg

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic-name", required=True, help="Source Kafka topic name")
    args = parser.parse_args()

    spark_session = get_spark_session(f"Load {args.topic_name} Topic To Bronze layer")
    src_stream_df = get_kafka_stream_df(spark_session, [args.topic_name])
    
    client = SchemaRegistryManager._get_client(use_internal=True)
    avsc_reader = AvscReader(args.topic_name)
    deserialized_df = get_deserialized_stream_df(src_stream_df, avsc_reader.schema_str)

    query = write_stream_iceberg(deserialized_df, avsc_reader)
    query.awaitTermination()