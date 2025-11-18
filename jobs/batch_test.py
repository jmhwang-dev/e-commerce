import time


from typing import List
from service.utils.iceberg import init_catalog
from service.utils.spark import get_spark_session
from service.utils.helper import get_batch_pipeline
from service.pipeline.batch import base, silver, gold
from service.utils.schema.reader import AvscReader
from service.utils.schema.avsc import SilverAvroSchema

if __name__ == "__main__":
    spark_session = get_spark_session("Batch", dev=True)
    
    init_catalog(spark_session, 'silver', is_drop=False)
    init_catalog(spark_session, 'gold', is_drop=False)

    watermark_avsc_reader = AvscReader(SilverAvroSchema.WATERMARK)
    watermark_scheam = base.BaseBatch.get_schema(spark_session, watermark_avsc_reader)
    base.BaseBatch.initialize_dst_table(spark_session, watermark_scheam, watermark_avsc_reader.dst_table_identifier)
    SilverAvroSchema.CUSTOMER_ORDER

    spark_session.stop()
    
    app_class_list: List[base.BaseBatch] = get_batch_pipeline('all')

    for app_class in app_class_list:
        app: base.BaseBatch = app_class()
        app.extract()
        app.transform()
        app.load()
        app.spark_session.stop()