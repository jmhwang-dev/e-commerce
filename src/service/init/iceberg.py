from typing import Tuple
from pyiceberg.catalog import load_catalog
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from service.init.kafka import *

class MedallionLayer():
    BUCKET = "warehousedev"
    NAMESPACE_BRONZE = 'bronze'
    NAMESPACE_SILVER = 'silver'
    NAMESPACE_SILVER = 'gold'

class BronzeLayer(MedallionLayer):
    ORDER_ITEM_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.ORDER_ITEM}'
    PRODUCT_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.PRODUCT}'
    CUSTOMER_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.CUSTOMER}'
    SELLER_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.SELLER}'
    GEOLOCATION_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.GEOLOCATION}'
    ORDER_STATUS_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.ORDER_STATUS}'
    PAYMENT_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.PAYMENT}'
    ESTIMATED_DELIVERY_DATE_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.ESTIMATED_DELIVERY_DATE}'
    REVIEW_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_BRONZE}.{Topic.REVIEW}'

    @classmethod
    def __iter__(cls) -> Iterator[Tuple[str, str]]:
        for attr_name, attr_value in vars(cls).items():
            if not attr_name.startswith('__'):
                topic = attr_name.split('_TABLE_IDENTIFIER')[0].lower()
                yield topic, attr_value  # (topic, table_identifier) 반환

class SilverLayer(MedallionLayer):
    ORDER_ITEM_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.ORDER_ITEM}'
    PRODUCT_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.PRODUCT}'
    CUSTOMER_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.CUSTOMER}'
    SELLER_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.SELLER}'
    GEOLOCATION_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.GEOLOCATION}'
    ORDER_STATUS_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.ORDER_STATUS}'
    PAYMENT_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.PAYMENT}'
    ESTIMATED_DELIVERY_DATE_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.ESTIMATED_DELIVERY_DATE}'
    REVIEW_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.REVIEW}'
    
    INFERENCE_REVIEW_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.INFERENCED_REVIEW}'
    PREPROCESSED_REVIEW_TABLE_IDENTIFIER = f'{MedallionLayer.BUCKET}.{MedallionLayer.NAMESPACE_SILVER}.{Topic.PREPROCESSED_REVIEW}'

def create_namespace(spark_session: SparkSession, table_identifier: str) -> Tuple[str, str]:
    components = table_identifier.split('.')
    table_name = components[-1]
    if len(components) < 2:
        raise ValueError(f"Invalid table_identifier: {table_identifier}. Expected format: catalog.namespace.table")
    qualified_namespace = '.'.join(components[:-1])
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {qualified_namespace}")
    return qualified_namespace, table_name

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