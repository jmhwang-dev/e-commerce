from service.utils.iceberg.spark import *
import boto3
from botocore.client import Config

def get_s3_client(s3_endpoint_url: str):
    """
    ex) 
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4")
    """
    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint_url,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4")
    )

def delete_s3_objects(s3_endpoint_url, bucket, prefix):
    """S3 버킷에서 지정된 prefix의 모든 객체를 삭제"""
    s3_client = get_s3_client(s3_endpoint_url)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
        print(f"Deleted objects in s3://{bucket}/{prefix}")