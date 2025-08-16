from typing import Tuple
from service.init.confluent import *

def get_iceberg_destination(schema_str: str) -> Tuple[str, str, str]:
    namespace, table_name = SchemaRegistryManager.get_schem_identifier(schema_str)

    s3_uri = namespace.replace('.', '/')
    table_identifier = f"{namespace}.{table_name}"
    return s3_uri, table_identifier, table_name