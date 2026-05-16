import os
from dotenv import load_dotenv

load_dotenv()

# Storage Configuration
STORAGE_MODE = os.environ.get("STORAGE_MODE", "LOCAL").upper()

# AWS S3 Configuration (optional, only if STORAGE_MODE=S3)
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET")
S3_PREFIX = os.environ.get("S3_PREFIX", "parquet/")

# Snowflake S3 Role (for external stage)
SF_S3_ROLE = os.environ.get("SF_S3_ROLE")


def validate_aws_config():
    """Validate AWS configuration if using S3 storage."""
    if STORAGE_MODE == "S3":
        missing = []
        if not S3_BUCKET:
            missing.append("S3_BUCKET")
        if not AWS_ACCESS_KEY_ID:
            missing.append("AWS_ACCESS_KEY_ID")
        if not AWS_SECRET_ACCESS_KEY:
            missing.append("AWS_SECRET_ACCESS_KEY")

        if missing:
            raise ValueError(
                f"S3 mode requires: {', '.join(missing)}. "
                f"Set these in .env or environment variables."
            )


def upload_to_s3(local_path, s3_key):
    """Upload a file to S3 and return the S3 URI.

    Args:
        local_path: Path to local file
        s3_key: S3 key (path within bucket, e.g., "parquet/file.parquet")

    Returns:
        S3 URI (s3://bucket/key)

    Raises:
        ValueError: If S3 upload fails
    """
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 is required for S3 storage. Install with: pip install boto3")

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )

        s3_client.upload_file(local_path, S3_BUCKET, s3_key)
        s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
        return s3_uri
    except Exception as e:
        raise ValueError(f"Failed to upload {local_path} to S3: {e}")


def get_s3_path(filename):
    """Get the S3 path for a filename."""
    return f"s3://{S3_BUCKET}/{S3_PREFIX}{filename}"
