from service.producer.bronze import BronzeAvroSchema
from service.utils.spark import get_spark_session
from service.utils.iceberg import init_catalog
from service.utils.logger import *
from service.pipeline.stream.bronze import load_cdc

if __name__ == "__main__":
    src_avsc_filenames = BronzeAvroSchema.get_all_filenames()
    spark_session = get_spark_session("Load CDC to bronze layer")
    dst_namespace = 'bronze'
    logger = get_logger(__name__, '/opt/spark/logs/cdc.log')

    init_catalog(spark_session, dst_namespace, is_drop=True)
    load_cdc(src_avsc_filenames, spark_session, logger)
    