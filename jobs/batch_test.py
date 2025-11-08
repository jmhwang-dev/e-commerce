import time

from typing import List
from service.utils.iceberg import init_catalog
from service.utils.spark import get_spark_session
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

    job_list: List[base.BaseBatch] = [
        # silver.GeoCoordBatch(spark_session),
        # silver.OlistUserBatch(spark_session),
        # silver.ReviewMetadataBatch(spark_session),
        silver.OrderEventBatch(spark_session),
        silver.CustomerOrderBatch(spark_session),
        silver.ProductMetadataBatch(spark_session),

        # gold.DimUserLocationBatch(spark_session),
        gold.FactOrderTimelineBatch(spark_session),
        gold.OrderDetailBatch(spark_session),
        # gold.FactReviewStatsBatch(spark_session),
        # gold.FactOrderLeadDaysBatch(spark_session),
        gold.FactProductPeriodSalesBatch(spark_session),
        # gold.FactProductPeriodPortfolioBatch(spark_session)
    ]

    schedule_interval = 5
    try:
        while True:
            for job_instance in job_list:
                job_instance.extract()
                job_instance.transform()
                job_instance.load()
            time.sleep(schedule_interval)
            print('='*80)
    except KeyboardInterrupt:
        for job_instance in job_list:
            job_instance.spark_session.stop()