from pyiceberg.catalog import load_catalog
from service.init.kafka import *

class MedallionLayer():
    BUCKET = "warehouse_dev"

class BronzeLayer(MedallionLayer):
    NAMESPACE = 'bronze'
    
    ORDER_ITEM_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.ORDER_ITEM}'
    PRODUCT_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.PRODUCT}'
    CUSTOMER_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.CUSTOMER}'
    SELLER_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.SELLER}'
    GEOLOCATION_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.GEOLOCATION}'
    ORDER_STATUS_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.ORDER_STATUS}'
    PAYMENT_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.PAYMENT}'
    ESTIMATED_DELIVERY_DATE_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.ESTIMATED_DELIVERY_DATE}'
    REVIEW_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.REVIEW}'

class SilverLayer(MedallionLayer):
    NAMESPACE = 'silver'

    ORDER_ITEM_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.ORDER_ITEM}'
    PRODUCT_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.PRODUCT}'
    CUSTOMER_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.CUSTOMER}'
    SELLER_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.SELLER}'
    GEOLOCATION_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.GEOLOCATION}'
    ORDER_STATUS_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.ORDER_STATUS}'
    PAYMENT_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.PAYMENT}'
    ESTIMATED_DELIVERY_DATE_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.ESTIMATED_DELIVERY_DATE}'
    REVIEW_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.REVIEW}'

    INFERENCE_REVIEW_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.INFERENCED_REVIEW}'
    PREPROCESSED_REVIEW_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{NAMESPACE}.{Topic.PREPROCESSED_REVIEW}'


def get_catalog(
        catalog_uri: str,
        s3_endpoint: str,
        bucket: str
        ):
    """
    ex)
    option = {
            "type": "REST",
            "uri": "http://rest-catalog:8181",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.use-ssl": "false",
            "warehouse": f"s3://{MedallionLayer.BUCKET}"
        }

    """
    option = {
        "type": "REST",
        "uri": catalog_uri,
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.use-ssl": "false",
        "warehouse": f"s3://{MedallionLayer.BUCKET}"
    }
    return load_catalog("REST", **option)