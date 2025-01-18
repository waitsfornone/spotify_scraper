import os
import boto3
from botocore.config import Config

def get_b2_resource():
    """Initialize B2 resource"""
    b2_key_id = os.getenv("B2_SPOTIFY_KEY_ID")
    b2_app_key = os.getenv("B2_APP_KEY")
    if not b2_key_id or not b2_app_key:
        raise ValueError("B2_SPOTIFY_KEY_ID and B2_APP_KEY environment variables must be set")
        
    b2_endpoint = 'https://s3.us-west-004.backblazeb2.com'
    
    b2 = boto3.resource(service_name='s3',
                    endpoint_url=b2_endpoint,
                    aws_access_key_id=b2_key_id,
                    aws_secret_access_key=b2_app_key,
                    config=Config(signature_version='s3v4'))
    return b2 