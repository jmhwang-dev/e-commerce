import requests
import json
import sys

from config.confluent import *

def set_compatibility(subject, level="BACKWARD"):
    """Set subject-level compatibility (e.g., BACKWARD for safe evolution)."""
    url = f"{SCHEMA_REGISTRY_INTERNAL_URL}/config/{subject}"
    response = requests.put(url, headers=HEADERS, data=json.dumps({"compatibility": level}))

    if response.status_code == 200:
        print(f"Compatibility set to {level} for {subject}")
    else:
        print(f"Error setting compatibility: {response.status_code}, {response.text}")

def check_schema_exists(subject, schema_str):
    """Check if schema already exists; returns ID if found."""
    url = f"{SCHEMA_REGISTRY_INTERNAL_URL}/subjects/{subject}"
    response = requests.post(url, headers=HEADERS, data=json.dumps({"schema": schema_str}))
    if response.status_code == 200:
        data = response.json()
        return data.get("id"), data.get("version")
    return None, None

def register_schema(subject, schema_str, schema_type="AVRO"):
    """Register new schema version."""
    existing_id, existing_version = check_schema_exists(subject, schema_str)
    if existing_id:
        print(f"Schema already exists for {subject}: ID {existing_id}, Version {existing_version}")
        return existing_id
    
    url = f"{SCHEMA_REGISTRY_INTERNAL_URL}/subjects/{subject}/versions"
    payload = {
        "schema": schema_str,
        "schemaType": schema_type,
        "normalize": True  # Deduplicate via normalization
    }
    response = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    if response.status_code == 200:
        schema_id = response.json()["id"]
        print(f"Registered schema for {subject} with ID: {schema_id}")
        return schema_id
    else:
        print(f"Error registering: {response.status_code}, {response.text}")
        sys.exit(1)