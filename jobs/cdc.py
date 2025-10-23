from service.producer.bronze import BronzeTopic
from service.utils.spark import get_spark_session, run_stream_queries
from service.utils.iceberg import initialize_namespace
from service.utils.logger import *
from service.pipeline.stream.bronze import get_load_cdc_query_list

if __name__ == "__main__":
    src_topic_names = BronzeTopic.get_all_topics()
    spark_session = get_spark_session("Load CDC to bronze layer")
    dst_namespace = 'bronze'
    logger = get_logger(__name__, '/opt/spark/logs/cdc.log')

    initialize_namespace(spark_session, dst_namespace, is_drop=True)
    query_list = get_load_cdc_query_list(src_topic_names, spark_session)
    run_stream_queries(spark_session, query_list, logger)