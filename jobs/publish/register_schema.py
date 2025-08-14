from service.init.confluent import *
from pathlib import Path
import os
import json

if __name__ == "__main__":
    # Load schemas (in prod: read from files or Git)
    SCHEMA_DIR = Path('infra/confluent/schemas/')
    
    for dir_name in os.listdir(SCHEMA_DIR):
        dir_path = SCHEMA_DIR / dir_name
        
        for avsc_path in os.listdir(dir_path):
            schema_path = dir_path / avsc_path
            
            with open(schema_path, "r") as f:
                schema_str = f.read()
            
            # 스키마 JSON에서 name 필드를 추출해서 subject로 사용
            schema_json = json.loads(schema_str)
            schema_name = schema_json.get("name")  # bronze.payment, silver.payment
            
            if not schema_name:
                print(f"No 'name' field found in schema: {schema_path}")
                continue
            
            # Set compatibility first
            set_compatibility(schema_name)
            
            # Register
            register_schema(schema_name, schema_str)