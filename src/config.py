import os
from dotenv import load_dotenv

load_dotenv()

# Storage Mode Configuration
STORAGE_MODE = os.environ.get("STORAGE_MODE", "LOCAL").upper()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
S3_BUCKET = os.environ.get("S3_BUCKET")
S3_PREFIX = os.environ.get("S3_PREFIX", "parquet/")
SF_S3_ROLE = os.environ.get("SF_S3_ROLE")

def validate_aws_config():
    """Validate AWS S3 credentials are configured."""
    # checks that all three required aws values are actually set, if any is missing it raises
    # an error so we dont silently fail later when trying to upload something
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET]):
        raise ValueError(
            "AWS credentials not configured. "
            "Please set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET in .env"
        )

def get_s3_client():
    """Create and return boto3 S3 client."""
    import boto3
    # run validation first so we dont get a confusing boto3 error later
    validate_aws_config()
    # builds the client using our env credentials and the configured region
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

def upload_to_s3(file_path, s3_key):
    """Upload file to S3 and return S3 URI."""
    # get a fresh s3 client then push the local file up to the bucket
    s3 = get_s3_client()
    s3.upload_file(file_path, S3_BUCKET, s3_key)
    # return the full s3 uri so the caller knows exactly where the file landed
    return f"s3://{S3_BUCKET}/{s3_key}"

def get_s3_uri(filename):
    """Get full S3 URI for a file."""
    # just builds the s3 path by combining bucket, prefix and filename together
    # handy so callers dont have to know about the prefix themselves
    return f"s3://{S3_BUCKET}/{S3_PREFIX}{filename}"
