from service.producer.bronze import BronzeAvroSchema
from service.utils.spark import get_spark_session, run_stream_queries
from service.utils.iceberg import init_catalog
from service.utils.logger import *
from service.pipeline.stream.bronze import get_load_cdc_query_list

if __name__ == "__main__":
    src_avsc_filenames = BronzeAvroSchema.get_all_filenames()
    spark_session = get_spark_session("Load CDC to bronze layer")
    dst_namespace = 'bronze'
    logger = get_logger(__name__, '/opt/spark/logs/cdc.log')

    init_catalog(spark_session, dst_namespace, is_drop=True)
    query_list = get_load_cdc_query_list(src_avsc_filenames, spark_session)
    run_stream_queries(spark_session, query_list, logger)