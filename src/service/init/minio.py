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