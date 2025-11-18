from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from service.utils.logger import *
from service.utils.iceberg import init_catalog
from service.utils.helper import get_stream_pipeline
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema
from service.utils.spark import get_spark_session, run_stream_queries
from service.utils.kafka import delete_topics, create_topics, get_confluent_kafka_admin_client
from service.pipeline.stream import base
from config.kafka import BOOTSTRAP_SERVERS_INTERNAL

LOGGER = get_logger(__name__, '/opt/spark/logs/stream.log')

def run_stream(spark_session: SparkSession, job_class_list: List[base.BaseStream]):
    QUERY_LIST: List[StreamingQuery] = []

    is_dev = True
    process_time='10 seconds'
    query_version = 'v1.0'
    for job_class in job_class_list:
        job_instance:base.BaseStream = job_class(is_dev, process_time, query_version, spark_session)
        QUERY_LIST += [job_instance.get_query()]

    run_stream_queries(spark_session, QUERY_LIST, LOGGER)

if __name__ == "__main__":
    spark_session = get_spark_session("stream")
    admin_client = get_confluent_kafka_admin_client(BOOTSTRAP_SERVERS_INTERNAL)
    silver_avsc_filenames = SilverAvroSchema.get_all_filenames()
    delete_topics(admin_client, silver_avsc_filenames)
    create_topics(admin_client, silver_avsc_filenames)
    init_catalog(spark_session, 'gold', is_drop=True)

    target_job = get_stream_pipeline(GoldAvroSchema.ORDER_DETAIL)
    
    job_class_list:List[base.BaseStream] = target_job
    run_stream(spark_session, job_class_list)