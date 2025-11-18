from typing import Optional
from service.producer.bronze import *
from service.pipeline.batch import base, silver, gold
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema


def get_producer(topic_name):
    if topic_name == OrderStatusBronzeProducer.dst_topic:
        return OrderStatusBronzeProducer
    
    elif topic_name == ProductBronzeProducer.dst_topic:
        return ProductBronzeProducer
    
    elif topic_name == CustomerBronzeProducer.dst_topic:
        return CustomerBronzeProducer
    
    elif topic_name == SellerBronzeProducer.dst_topic:
        return SellerBronzeProducer
    
    elif topic_name == GeolocationBronzeProducer.dst_topic:
        return GeolocationBronzeProducer
    
    elif topic_name == EstimatedDeliberyDateBronzeProducer.dst_topic:
        return EstimatedDeliberyDateBronzeProducer
    
    elif topic_name == OrderItemBronzeProducer.dst_topic:
        return OrderItemBronzeProducer
    
    elif topic_name == PaymentBronzeProducer.dst_topic:
        return PaymentBronzeProducer
    
    elif topic_name == ReviewBronzeProducer.dst_topic:
        return ReviewBronzeProducer

def get_avro_key_column(topic_name):
    if topic_name == OrderStatusBronzeProducer.dst_topic:
        return OrderStatusBronzeProducer.key_column

    elif topic_name == ProductBronzeProducer.dst_topic:
        return ProductBronzeProducer.key_column
    
    elif topic_name == CustomerBronzeProducer.dst_topic:
        return CustomerBronzeProducer.key_column
    
    elif topic_name == SellerBronzeProducer.dst_topic:
        return SellerBronzeProducer.key_column
    
    elif topic_name == GeolocationBronzeProducer.dst_topic:
        return GeolocationBronzeProducer.key_column
    
    elif topic_name == EstimatedDeliberyDateBronzeProducer.dst_topic:
        return EstimatedDeliberyDateBronzeProducer.key_column
    
    elif topic_name == OrderItemBronzeProducer.dst_topic:
        return OrderItemBronzeProducer.key_column
    
    elif topic_name == PaymentBronzeProducer.dst_topic:
        return PaymentBronzeProducer.key_column
    
    elif topic_name == ReviewBronzeProducer.dst_topic:
        return ReviewBronzeProducer.key_column
    
    elif topic_name == ReviewBronzeProducer.dst_topic:
        return ReviewBronzeProducer.key_column
    
    # silver
    # TODO: resolve hard coding
    elif topic_name == 'geo_coord':
        return 'zip_code'
    
    elif topic_name in ['order_event', 'customer_order']:
        return 'order_id'
    
    elif topic_name == 'olist_user':
        return 'user_id'
    
    elif topic_name == 'product_metadata':
        return 'product_id'
    
def get_batch_pipeline(target_pipeline: str) -> List[Optional[base.BaseBatch]]:
    """
    Get the list of batch job classes for the specified pipeline.

    Parameters
    ----------
    target_pipeline : str
        - `*AvroSchema` name (ex: SilverAvroSchema.CUSTOMER_ORDER)
        - Use 'all' for the full pipeline.

    Returns
    -------
    list
        `[]` or list for batch job classes to run for the given pipeline
    """
    
    silver_jobs = [
        silver.GeoCoordBatch,
        silver.OlistUserBatch,
        silver.ReviewMetadataBatch,
        silver.ProductMetadataBatch,
        silver.CustomerOrderBatch,
        silver.OrderEventBatch,
    ]

    gold_jobs = [
        gold.DimUserLocationBatch,
        gold.OrderDetailBatch,
        gold.FactOrderLeadDaysBatch,
        gold.FactMonthlySalesByProductBatch,
        gold.FactReviewAnswerLeadDaysBatch,
    ]

    # 전체 pipeline 구성
    app_dict = {
        'all': silver_jobs + gold_jobs,

        GoldAvroSchema.FACT_REVIEW_ANSWER_LEAD_DAYS: [
            silver.ReviewMetadataBatch,
            gold.FactReviewAnswerLeadDaysBatch
        ],

        GoldAvroSchema.FACT_ORDER_LEAD_DAYS: [
            silver.OrderEventBatch,
            gold.FactOrderLeadDaysBatch
        ],

        GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT: [
            silver.OrderEventBatch,
            silver.CustomerOrderBatch,
            silver.ProductMetadataBatch,
            gold.FactOrderLeadDaysBatch,
            gold.OrderDetailBatch,
            gold.FactMonthlySalesByProductBatch
        ],

        GoldAvroSchema.ORDER_DETAIL: [
            silver.ProductMetadataBatch,
            silver.CustomerOrderBatch,
            gold.OrderDetailBatch
        ],

        GoldAvroSchema.DIM_USER_LOCATION: [
            silver.GeoCoordBatch,
            silver.OlistUserBatch,
            gold.DimUserLocationBatch
        ],
    }

    return app_dict.get(target_pipeline, [])
