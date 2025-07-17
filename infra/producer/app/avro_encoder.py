from fastavro import parse_schema, schemaless_writer
import io
import json
import os

# 경로 설정
SCHEMA_PATH = os.path.join("schemas", "review_raw.avsc")

# 1. 스키마 로드 및 파싱
with open(SCHEMA_PATH, "r") as f:
    schema = parse_schema(json.load(f))

def encode_avro(record: dict) -> bytes:
    """
    주어진 dict를 Avro 바이너리 포맷으로 직렬화
    """
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, record)
    return buffer.getvalue()
