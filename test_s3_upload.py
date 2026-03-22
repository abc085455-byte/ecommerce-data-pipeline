"""Upload raw data CSV files to AWS S3"""

import boto3
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

load_dotenv('.env')

print("=" * 55)
print("   UPLOADING DATA TO AWS S3")
print("=" * 55)

start_time = datetime.now()

try:
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'us-east-1')
    )

    bucket = os.getenv('S3_BUCKET_NAME', 'ecommerce-pipeline-raw-data')
    raw_data = Path('raw_data')

    uploaded = 0

    # Upload all CSV files
    folders = ['customers', 'products', 'orders']

    for folder in folders:
        folder_path = raw_data / folder
        if folder_path.exists():
            for file in folder_path.iterdir():
                if file.suffix == '.csv':
                    s3_key = f"raw_data/{folder}/{file.name}"
                    s3.upload_file(str(file), bucket, s3_key)
                    size_kb = round(file.stat().st_size / 1024, 2)
                    print(f"   Uploaded: {s3_key} ({size_kb} KB)")
                    uploaded += 1

    # Verify - list files in S3
    print(f"\n   Total files uploaded: {uploaded}")
    print("\n   Files in S3 bucket:")

    response = s3.list_objects_v2(Bucket=bucket, Prefix='raw_data/')
    if 'Contents' in response:
        for obj in response['Contents']:
            size_kb = round(obj['Size'] / 1024, 2)
            print(f"   s3://{bucket}/{obj['Key']} ({size_kb} KB)")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 55)
    print(f"   S3 UPLOAD COMPLETE!")
    print(f"   Time Taken: {duration:.2f} seconds")
    print("=" * 55)

except Exception as e:
    print(f"\n   ERROR: {e}")
    import traceback
    traceback.print_exc()