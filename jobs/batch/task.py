import time
import argparse

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
        GoldAvroSchema.FACT_ORDER_LEAD_DAYS: gold.FactOrderLeadDaysBatch,
        GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT: gold.FactMonthlySalesByProductBatch,
        GoldAvroSchema.FACT_REVIEW_ANSWER_LEAD_DAYS: gold.FactReviewAnswerLeadDaysBatch,
    }

    app_class = app_class_dict[app_name]
    app: base.BaseBatch = app_class()
    app.extract()
    app.transform()
    app.load()
    app.spark_session.stop()