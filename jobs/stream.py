from typing import List

from pyspark.sql.streaming.query import StreamingQuery
from service.utils.spark import get_spark_session, run_stream_queries
from service.utils.iceberg import init_catalog
from service.utils.logger import *
from service.pipeline.stream.silver import *
from service.pipeline.stream.gold import *
from service.utils.kafka import delete_topics, create_topics, get_confluent_kafka_admin_client
from config.kafka import BOOTSTRAP_SERVERS_INTERNAL

LOGGER = get_logger(__name__, '/opt/spark/logs/stream.log')

def run_stream(job_class_list):
    QUERY_LIST: List[StreamingQuery] = []

    is_dev = True
    process_time='10 seconds'
    query_version = 'v1.0'
    for job_class in job_class_list:
        job_instance:BaseStream = job_class(is_dev, process_time, query_version, spark_session)
        QUERY_LIST += [job_instance.get_query()]

    run_stream_queries(spark_session, QUERY_LIST, LOGGER)

if __name__ == "__main__":
    spark_session = get_spark_session("stream")
    admin_client = get_confluent_kafka_admin_client(BOOTSTRAP_SERVERS_INTERNAL)
    silver_avsc_filenames = SilverAvroSchema.get_all_filenames()
    delete_topics(admin_client, silver_avsc_filenames)
    create_topics(admin_client, silver_avsc_filenames)
    init_catalog(spark_session, 'gold', is_drop=True)

    # TODO: inference
    # review_comment = review_stream_df.select('review_id', 'review_comment_title', 'review_comment_message').dropna()
    silver_job = [GeoCoordStream, OrderEventStream, CustomerOrderStream, ReviewMetadataStream, OlistUserStream, ProductMetadataStream]
    gold_job = [DimUserLocationStream, DeliverStatus, OrderDetailStream]
    
    job_class_list:List[BaseStream] = silver_job + gold_job
    run_stream(job_class_list)