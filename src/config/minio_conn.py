import os
from dotenv import load_dotenv, find_dotenv

import boto3

load_dotenv(find_dotenv())

def get_s3_client():
    s3_client = boto3.client(
        's3',
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    return s3_client

MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "us")

