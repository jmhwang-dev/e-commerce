import time
import argparse

from service.utils.iceberg import init_catalog
from service.utils.spark import get_spark_session
from service.pipeline.batch import base, silver, gold
from service.utils.schema.reader import AvscReader
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Spark DAG Runner')
    parser.add_argument('--is_drop', type=bool, required=True, help='Initialize catalog or not')
    args = parser.parse_args()

    _is_drop = args.is_drop
    
    spark_session = get_spark_session(app_name='initilize catalog', dev=False)

    init_catalog(spark_session, 'silver', is_drop=_is_drop)
    init_catalog(spark_session, 'gold', is_drop=_is_drop)

    watermark_avsc_reader = AvscReader(SilverAvroSchema.WATERMARK)
    watermark_scheam = base.BaseBatch.get_schema(spark_session, watermark_avsc_reader)
    base.BaseBatch.initialize_dst_table(spark_session, watermark_scheam, watermark_avsc_reader.dst_table_identifier)

    spark_session.stop()