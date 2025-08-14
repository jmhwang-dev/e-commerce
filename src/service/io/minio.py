from service.init.iceberg import *
from service.init.minio import *

def delete_s3_objects(s3_endpoint_url, bucket, prefix):
    """S3 버킷에서 지정된 prefix의 모든 객체를 삭제"""
    s3_client = get_s3_client(s3_endpoint_url)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
        print(f"Deleted objects in s3://{bucket}/{prefix}")