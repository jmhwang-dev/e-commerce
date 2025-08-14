from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    NestedField
)
import pyarrow as pa

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

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
