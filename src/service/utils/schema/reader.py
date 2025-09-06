import json
from service.utils.schema.registry_manager import SchemaRegistryManager
from confluent_kafka.schema_registry.error import SchemaRegistryError

class AvscReader:
    client = SchemaRegistryManager._get_client(use_internal=True)

    def __init__(self, schema_name):
        try:
            self.schema_name = schema_name
            self.schema_str = self.client.get_latest_version(schema_name).schema.schema_str
        except SchemaRegistryError:
            self.schema_str = None

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
        # TODO: fix hard coding for `dlq` topic
        if schema_str is None:
            self.namespace = "warehousedev.silver"
            self.dst_table_identifier = f"warehousedev.silver.{self.schema_name}"
        else:
            self.json_schema = json.loads(schema_str)
            self.namespace = self.json_schema.get('namespace')
            self.table_name = self.json_schema.get('name')
            self.dst_table_identifier = f"{self.namespace}.{self.table_name}"

        self.s3_uri = self.namespace.replace('.', '/')