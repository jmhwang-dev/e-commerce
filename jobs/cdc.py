from service.utils.spark import get_spark_session
from service.utils.iceberg import init_catalog
from service.utils.logger import *
from service.pipeline.stream.bronze import load_cdc_stream, load_cdc_batch

if __name__ == "__main__":
    spark_session = get_spark_session("cdc")
    init_catalog(spark_session, 'bronze', is_drop=True)

    logger = get_logger(__name__, '/opt/spark/logs/cdc.log')
    
    CDC_OPTIONS = {
        'app_name': spark_session.sparkContext.appName,
        'dst_env': 'dev',
        'query_version': 'v1.0',
        'process_time': '5 seconds'
    }

    # load_cdc_stream(spark_session, CDC_OPTIONS, logger)
    load_cdc_batch(spark_session, CDC_OPTIONS)