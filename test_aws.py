"""Quick AWS S3 Connection Test"""

import boto3
import os
from dotenv import load_dotenv

load_dotenv('.env')

print("=" * 50)
print("   AWS S3 CONNECTION TEST")
print("=" * 50)

try:
    # S3 client banao
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'us-east-1')
    )

    # List all buckets
    response = s3.list_buckets()
    print("\nYour S3 Buckets:")
    for bucket in response['Buckets']:
        print(f"   - {bucket['Name']}")

    # Create our bucket
    bucket_name = os.getenv('S3_BUCKET_NAME', 'ecommerce-pipeline-raw-data')
    print(f"\nCreating bucket: {bucket_name}")

    existing = [b['Name'] for b in response['Buckets']]

    if bucket_name in existing:
        print(f"   Bucket already exists: {bucket_name}")
    else:
        region = os.getenv('AWS_REGION', 'us-east-1')
        if region == 'us-east-1':
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f"   Bucket created: {bucket_name}")

    print("\n" + "=" * 50)
    print("   AWS CONNECTION SUCCESSFUL!")
    print("=" * 50)

except Exception as e:
    print(f"\n   ERROR: {e}")
    print("\n   Check your AWS credentials in .env file")
    print("=" * 50)