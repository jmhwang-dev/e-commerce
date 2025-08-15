from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from service.init.kafka import *

from service.init.confluent import *

# from pyiceberg.catalog import load_catalog

def create_namespace(spark_session: SparkSession, schema_str: str) -> None:
    namespace, _ = SchemaRegistryManager.get_schem_identifier(schema_str)
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    return 
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