import traceback

from typing import List
from pyspark.sql.utils import StreamingQueryException, AnalysisException
from pyspark.sql.streaming.query import StreamingQuery
from service.producer.bronze import BronzeTopic
from service.utils.spark import get_spark_session, get_deserialized_avro_stream_df, get_kafka_stream_df, stop_streams, start_console_stream
from service.utils.iceberg import load_stream_to_iceberg, initialize_namespace
from service.utils.helper import get_producer
from service.utils.schema.reader import AvscReader
from service.utils.logger import *

LOGGER = get_logger(__name__, '/opt/spark/logs/cdc.log')

SRC_TOPIC_NAMES:List[str] = BronzeTopic.get_all_topics()
BRONZE_NAMESPACE = 'bronze'

SPARK_SESSION = get_spark_session("Load CDC to bronze layer")
QUERY_LIST: List[StreamingQuery] = []

def setup_bronze_streams():
    try:
        for topic_name in SRC_TOPIC_NAMES:
            avsc_reader = AvscReader(topic_name)
            producer_class = get_producer(topic_name)

            kafka_stream_df = get_kafka_stream_df(SPARK_SESSION, topic_name)
            deser_stream_df = get_deserialized_avro_stream_df(kafka_stream_df, producer_class.key_column, avsc_reader.schema_str)

            query = load_stream_to_iceberg(deser_stream_df, avsc_reader, process_time='5 seconds')
            QUERY_LIST.append(query)

    except ValueError as e:
        # Topic does not exist
        print(e)
        stop_streams(SPARK_SESSION, QUERY_LIST)
        exit()

if __name__ == "__main__":
    initialize_namespace(SPARK_SESSION, BRONZE_NAMESPACE, is_drop=True)
    setup_bronze_streams()
        
    try:
        SPARK_SESSION.streams.awaitAnyTermination()

    except StreamingQueryException as e:
        write_log(LOGGER, f"StreamingQueryException occurred: {str(e)}")
        for q in QUERY_LIST[:]:
            if not q.isActive():
                LOGGER.error(f"Query for {q.name} failed. Last progress: {q.lastProgress}")
                QUERY_LIST.remove(q)
        write_log(LOGGER, f"Stack trace: {traceback.format_exc()}")

    except AnalysisException as e:
        write_log(LOGGER, f"AnalysisException occurred: {str(e)}")
        for q in QUERY_LIST[:]:
            if not q.isActive():
                write_log(LOGGER, f"Query for {q.name} failed. Last progress: {q.lastProgress}")
                QUERY_LIST.remove(q)
        write_log(LOGGER, f"Stack trace: {traceback.format_exc()}")

    except KeyboardInterrupt:
        print("Stopping all streaming QUERY_LIST and sessions...")
        stop_streams(SPARK_SESSION, QUERY_LIST)