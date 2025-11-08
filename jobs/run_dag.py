import time
import argparse

from typing import List
from service.utils.iceberg import init_catalog
from service.utils.spark import get_spark_session
from service.pipeline.batch import base, silver, gold
from service.utils.schema.reader import AvscReader
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Spark DAG Runner')
    parser.add_argument('--app_name', type=str, required=True, help='The name of the application')
    args = parser.parse_args()

    app_name = args.app_name

    app_class_dict = {
        SilverAvroSchema.GEO_COORD: silver.GeoCoordBatch,
        SilverAvroSchema.OLIST_USER: silver.OlistUserBatch,
        SilverAvroSchema.REVIEW_METADATA: silver.ReviewMetadataBatch,
        SilverAvroSchema.PRODUCT_METADATA: silver.ProductMetadataBatch,
        SilverAvroSchema.CUSTOMER_ORDER: silver.CustomerOrderBatch,
        SilverAvroSchema.ORDER_EVENT: silver.OrderEventBatch,

        GoldAvroSchema.DIM_USER_LOCATION: gold.DimUserLocationBatch,
        GoldAvroSchema.ORDER_DETAIL: gold.OrderDetailBatch,
        GoldAvroSchema.FACT_ORDER_TIMELINE: gold.FactOrderTimelineBatch,
        GoldAvroSchema.FACT_ORDER_LEAD_DAYS: gold.FactOrderLeadDaysBatch,
        GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT: gold.FactMonthlySalesByProductBatch,
        GoldAvroSchema.FACT_REVIEW_STATS: gold.FactReviewStatsBatch,
        GoldAvroSchema.MONTHLY_CATEGORY_PORTFOLIO_MATRIX: gold.FactMonthlySalesByProductBatch,
    }

    spark_session = get_spark_session("Batch", dev=True)
    
    init_catalog(spark_session, 'silver', is_drop=False)
    init_catalog(spark_session, 'gold', is_drop=False)

    watermark_avsc_reader = AvscReader(SilverAvroSchema.WATERMARK)
    watermark_scheam = base.BaseBatch.get_schema(spark_session, watermark_avsc_reader)
    base.BaseBatch.initialize_dst_table(spark_session, watermark_scheam, watermark_avsc_reader.dst_table_identifier)

    job_instance: base.BaseBatch = app_class_dict[app_name](spark_session)
    job_instance.extract()
    job_instance.transform()
    job_instance.load()

    job_instance.spark_session.stop()