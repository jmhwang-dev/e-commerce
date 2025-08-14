from typing import Tuple
from pyspark.sql import SparkSession

from service.init.kafka import *

from typing import Tuple, Iterator
# from pyiceberg.catalog import load_catalog
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from service.init.kafka import *

class MedallionLayer:
    BUCKET: str = "warehousedev"
    TOPIC_CLASS: BaseTopic = None  # 자식 클래스에서 정의 (예: RawToBronzeTopic, BronzeToSilverTopic)

    @classmethod
    def __iter__(cls) -> Iterator[Tuple[str, str]]:
        if not cls.TOPIC_CLASS:
            raise ValueError("TOPIC_CLASS must be defined in subclass")
        topic_names = cls.TOPIC_CLASS.get_all_topics()
        for base_topic_name in topic_names:
            table_identifier = f"{cls.BUCKET}.{base_topic_name}"
            yield base_topic_name, table_identifier

class BronzeLayer(MedallionLayer):
    TOPIC_CLASS = RawToBronzeTopic

class SilverLayer(MedallionLayer):
    TOPIC_CLASS = BronzeToSilverTopic

def create_namespace(spark_session: SparkSession, table_identifier: str) -> Tuple[str, str]:
    components = table_identifier.split('.')
    table_name = components[-1]
    if len(components) < 2:
        raise ValueError(f"Invalid table_identifier: {table_identifier}. Expected format: catalog.namespace.table")
    qualified_namespace = '.'.join(components[:-1])
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {qualified_namespace}")
    return qualified_namespace, table_name

# def get_catalog(
#         catalog_uri: str,
#         s3_endpoint: str,
#         bucket: str = MedallionLayer.BUCKET
#         ):
#     """
#     ex)
#     option = {
#             "type": "REST",
#             "uri": "http://rest-catalog:8181",
#             "s3.endpoint": "http://minio:9000",
#             "s3.access-key-id": "minioadmin",
#             "s3.secret-access-key": "minioadmin",
#             "s3.use-ssl": "false",
#             "warehouse": f"s3://{MedallionLayer.BUCKET}"
#         }

#     """
#     option = {
#         "type": "REST",
#         "uri": catalog_uri,
#         "s3.endpoint": s3_endpoint,
#         "s3.access-key-id": "minioadmin",
#         "s3.secret-access-key": "minioadmin",
#         "s3.use-ssl": "false",
#         "warehouse": f"s3://{bucket}"
#     }
#     return load_catalog("REST", **option)