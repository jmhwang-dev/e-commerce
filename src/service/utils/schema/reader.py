import json
from service.utils.schema.registry_manager import SchemaRegistryManager

class AvscReader:
    client = SchemaRegistryManager._get_client(use_internal=True)

    def __init__(self, schema_name):
        self.schema_str = self.client.get_latest_version(schema_name).schema.schema_str
        if not self.schema_str:
            raise f"{schema_name} does not exist."
        
        self.set_metadata(self.schema_str)

    def set_metadata(self, schema_str):
        """
        schema template
        {
            "type":"record",
            "name":"customer",
            "namespace":"warehousedev.silver",
            "fields":[
                {"name":"customer_id","type":"string"},
                {"name":"zip_code","type":"int"}
            ]
        }
        """
        self.json_schema = json.loads(schema_str)
        self.namespace = self.json_schema.get('namespace')
        self.table_name = self.json_schema.get('name')
        self.dst_table_identifier = f"{self.namespace}.{self.table_name}"
        self.s3_uri = self.namespace.replace('.', '/')