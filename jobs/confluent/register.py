from service.init.confluent import *
from pathlib import Path
import os

if __name__ == "__main__":
    # Load schemas (in prod: read from files or Git)
    SCHEMA_DIR = Path('infra/confluent/schemas')
    for file_name in os.listdir(SCHEMA_DIR):
        schema_path = SCHEMA_DIR / file_name
        schema_name = schema_path.stem
        
        with open(schema_path, "r") as f:
            schema = f.read()
        
        # Set compatibility first
        set_compatibility(schema_name)
        
        # Register
        register_schema(schema_name, schema)