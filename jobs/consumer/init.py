from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    NestedField
)

import boto3
from botocore.client import Config
import pyarrow as pa

NAMESPCE = "silver"
TABLE_NAME = "example_table"
BUCKET_NAME = 'warehouse-dev'

TABLE_IDENTIFIER = (NAMESPCE, TABLE_NAME)
S3_URI = f"s3://{BUCKET_NAME}"
DST_LOCATION = f"{S3_URI}/{NAMESPCE}"


catalog = load_catalog(
    "REST",
    **{
        "type": "REST",
        "uri": "http://rest-catalog:8181",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.use-ssl": "false",
        "warehouse": S3_URI
    }
)

s3_client = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4")
)

# 테이블 스키마 정의 (field_id 중복 해결)
table_schema = Schema(
    NestedField(field_id=1, name="review_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="comment_type", field_type=StringType(), required=True),
    NestedField(field_id=3, name="por2eng", field_type=StringType(), required=True),
    NestedField(field_id=4, name="sentimentality", field_type=StringType(), required=True)
)

# PyArrow 스키마 정의
arrow_schema = pa.schema([
    pa.field("review_id", pa.string(), nullable=False),
    pa.field("comment_type", pa.string(), nullable=False),
    pa.field("por2eng", pa.string(), nullable=False),
    pa.field("sentimentality", pa.string(), nullable=False)
])

# 파티션 스펙 정의 (sentimentality에 IdentityTransform 사용)
partition_spec = PartitionSpec(
    PartitionField(
        source_id=4,  # sentimentality 필드 (field_id=4)
        field_id=1000,
        transform=IdentityTransform(),  # 문자열 값을 그대로 파티션 키로 사용
        name="sentimentality"
    )
)
