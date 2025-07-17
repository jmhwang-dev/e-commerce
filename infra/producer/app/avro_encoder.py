# producer/app/avro_encoder.py

from fastavro import parse_schema, schemaless_writer
import json
import io
import os

SCHEMA_PATH = "/mnt/schemas/review_raw.avsc"

with open(SCHEMA_PATH, "r") as f:
    schema = parse_schema(json.load(f))

def encode_avro(record: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()
