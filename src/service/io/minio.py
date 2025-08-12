from service.init.iceberg import *
from pyiceberg.exceptions import NoSuchTableError
from typing import Iterable
import pandas as pd

def delete_s3_objects(bucket, prefix):
    """S3 버킷에서 지정된 prefix의 모든 객체를 삭제"""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
        print(f"Deleted objects in s3://{bucket}/{prefix}")