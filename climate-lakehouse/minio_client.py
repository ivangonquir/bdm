import boto3
import io
from datetime import datetime

# MinIO Connection Settings
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "climate-lakehouse"

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def ensure_bucket_exists(s3_client):
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Created new MinIO bucket: {BUCKET_NAME}")

def upload_to_bronze(source_name, data_type, format_extension, data_content):
    """
    Uploads raw data to the Bronze layer in MinIO following the project naming convention.
    data_type must be: 'structured', 'semi-structured', or 'unstructured'
    """
    s3_client = get_minio_client()
    ensure_bucket_exists(s3_client)
    
    # Enforce naming convention: source_YYYYMMDD_timestamp.format 
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{source_name}_{timestamp}.{format_extension}"
    
    # Enforce folder structure
    object_name = f"bronze/{data_type}/{file_name}"
    
    try:
        # Handle string data (JSON/CSV) vs binary data (Images)
        if isinstance(data_content, str):
            data_content = data_content.encode('utf-8')
            
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=object_name,
            Body=data_content
        )
        print(f"Successfully uploaded to Landing Zone: {object_name}")
    except Exception as e:
        print(f"Failed to upload to MinIO: {e}")