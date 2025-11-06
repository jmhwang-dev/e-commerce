from typing import List
from service.utils.iceberg import init_catalog
from service.utils.spark import get_spark_session
from service.pipeline.batch import base, silver, gold

if __name__ == "__main__":
    spark_session = get_spark_session("Batch test", dev=True)

    init_catalog(spark_session, 'silver', is_drop=True)
    init_catalog(spark_session, 'gold', is_drop=True)

    job_list: List[base.BaseBatch] = [
        silver.GeoCoordBatch(),
        silver.ReviewMetadataBatch(),
        silver.OlistUserBatch(),
        silver.OrderEventBatch(),
        silver.CustomerOrderBatch(),
        silver.ProductMetadataBatch(),

        gold.DimUserLocationBatch(),
        gold.FactOrderTimelineBatch(),
        gold.OrderDetailBatch(),
        gold.FactReviewStatsBatch(),
        gold.FactOrderLeadDaysBatch(),
        gold.FactProductPeriodSalesBatch(),
        gold.FactProductPeriodPortfolioBatch()
    ]

    i = 0
    end = 1

    while i < end:
        for job_instance in job_list:
            job_instance.extract()
            job_instance.transform()
            job_instance.load()
        i += 1

    for job_instance in job_list:
        job_instance.spark_session.stop()