"""
==========================================
AWS S3 Utility Functions
Real-Time E-Commerce Data Pipeline
==========================================
Yeh file S3 ke saare operations handle karti hai:
- Bucket banana
- Files upload karna
- Files download karna
- Files list karna
- Files delete karna
==========================================
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pathlib import Path

# -------------------------------------------
# Project root ko Python path mein add karo
# Taake config import ho sake
# -------------------------------------------
sys.path.append(str(Path(__file__).parent.parent))
from config.config import AWSConfig, PathConfig
from loguru import logger

# -------------------------------------------
# Logger setup
# -------------------------------------------
log_file = PathConfig.LOG_DIR / "s3_operations.log"
logger.add(str(log_file), rotation="10 MB", level="INFO")


class S3Manager:
    """
    S3 ke saare operations yeh class handle karti hai.
    Ek jagah se S3 interact karna easy ho jata hai.
    """

    def __init__(self):
        """
        S3 client initialize karo AWS credentials se.
        Credentials .env file se AWSConfig class uthati hai.
        """
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
                aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY,
                region_name=AWSConfig.REGION
            )
            self.s3_resource = boto3.resource(
                's3',
                aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
                aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY,
                region_name=AWSConfig.REGION
            )
            self.bucket_name = AWSConfig.S3_BUCKET_NAME
            logger.info("S3 client initialized successfully")

        except NoCredentialsError:
            logger.error("AWS credentials not found! .env file check karo")
            raise

    # ==========================================
    # BUCKET OPERATIONS
    # ==========================================

    def create_bucket(self):
        """
        S3 bucket create karo agar exist nahi karta.
        Region ke hisaab se LocationConstraint set hota hai.
        """
        try:
            # Check if bucket already exists
            existing = self.s3_client.list_buckets()
            bucket_names = [b['Name'] for b in existing['Buckets']]

            if self.bucket_name in bucket_names:
                logger.info(f"Bucket already exists: {self.bucket_name}")
                return True

            # Create bucket with region config
            if AWSConfig.REGION == 'us-east-1':
                # us-east-1 ke liye LocationConstraint nahi lagta
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name
                )
            else:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': AWSConfig.REGION
                    }
                )

            logger.info(f"Bucket created successfully: {self.bucket_name}")
            return True

        except ClientError as e:
            logger.error(f"Error creating bucket: {e}")
            return False

    def create_folder_structure(self):
        """
        S3 bucket mein folder structure banao:
        - raw_data/orders/
        - raw_data/customers/
        - raw_data/products/
        - bronze/
        - silver/
        - gold/
        """
        folders = [
            f"{AWSConfig.S3_RAW_PREFIX}/orders/",
            f"{AWSConfig.S3_RAW_PREFIX}/customers/",
            f"{AWSConfig.S3_RAW_PREFIX}/products/",
            f"{AWSConfig.S3_BRONZE_PREFIX}/",
            f"{AWSConfig.S3_SILVER_PREFIX}/",
            f"{AWSConfig.S3_GOLD_PREFIX}/",
        ]

        for folder in folders:
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=folder
                )
                logger.info(f"Folder created: s3://{self.bucket_name}/{folder}")
            except ClientError as e:
                logger.error(f"Error creating folder {folder}: {e}")

        logger.info("S3 folder structure created successfully!")

    # ==========================================
    # UPLOAD OPERATIONS
    # ==========================================

    def upload_file(self, local_file_path, s3_key):
        """
        Ek file local system se S3 pe upload karo.

        Args:
            local_file_path: Local file ka full path
            s3_key: S3 mein file ka path (e.g., raw_data/orders/orders.csv)
        """
        try:
            self.s3_client.upload_file(
                str(local_file_path),
                self.bucket_name,
                s3_key
            )
            logger.info(f"Uploaded: {local_file_path} -> s3://{self.bucket_name}/{s3_key}")
            return True

        except FileNotFoundError:
            logger.error(f"File not found: {local_file_path}")
            return False
        except ClientError as e:
            logger.error(f"Upload error: {e}")
            return False

    def upload_directory(self, local_dir, s3_prefix):
        """
        Poora folder upload karo S3 pe.
        Folder ke andar ki saari files upload ho jayengi.

        Args:
            local_dir: Local folder path
            s3_prefix: S3 mein prefix (e.g., raw_data/orders)
        """
        local_dir = Path(local_dir)
        uploaded_count = 0

        if not local_dir.exists():
            logger.error(f"Directory not found: {local_dir}")
            return 0

        for file_path in local_dir.iterdir():
            if file_path.is_file():
                s3_key = f"{s3_prefix}/{file_path.name}"
                if self.upload_file(str(file_path), s3_key):
                    uploaded_count += 1

        logger.info(f"Total {uploaded_count} files uploaded from {local_dir}")
        return uploaded_count

    def upload_raw_data(self):
        """
        Saara raw data (orders, customers, products)
        ek saath S3 pe upload karo.
        Yeh function data generation ke baad call hoga.
        """
        logger.info("Starting raw data upload to S3...")

        # Orders upload
        self.upload_directory(
            PathConfig.ORDERS_DIR,
            f"{AWSConfig.S3_RAW_PREFIX}/orders"
        )

        # Customers upload
        self.upload_directory(
            PathConfig.CUSTOMERS_DIR,
            f"{AWSConfig.S3_RAW_PREFIX}/customers"
        )

        # Products upload
        self.upload_directory(
            PathConfig.PRODUCTS_DIR,
            f"{AWSConfig.S3_RAW_PREFIX}/products"
        )

        logger.info("All raw data uploaded to S3 successfully!")

    # ==========================================
    # DOWNLOAD OPERATIONS
    # ==========================================

    def download_file(self, s3_key, local_file_path):
        """
        S3 se ek file download karo local system pe.

        Args:
            s3_key: S3 mein file ka path
            local_file_path: Local mein kahan save karna hai
        """
        try:
            # Local directory banao agar nahi hai
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                str(local_file_path)
            )
            logger.info(f"Downloaded: s3://{self.bucket_name}/{s3_key} -> {local_file_path}")
            return True

        except ClientError as e:
            logger.error(f"Download error: {e}")
            return False

    # ==========================================
    # LIST OPERATIONS
    # ==========================================

    def list_files(self, prefix=""):
        """
        S3 bucket mein files list karo.

        Args:
            prefix: Folder path (e.g., raw_data/orders/)
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': str(obj['LastModified'])
                    })
                    logger.info(
                        f"  {obj['Key']} "
                        f"(Size: {obj['Size']} bytes)"
                    )

            logger.info(f"Total files found: {len(files)}")
            return files

        except ClientError as e:
            logger.error(f"List error: {e}")
            return []

    # ==========================================
    # DELETE OPERATIONS
    # ==========================================

    def delete_file(self, s3_key):
        """S3 se ek file delete karo."""
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            logger.info(f"Deleted: s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Delete error: {e}")
            return False

    def check_file_exists(self, s3_key):
        """Check karo file S3 pe exist karti hai ya nahi."""
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            return True
        except ClientError:
            return False


# ==========================================
# QUICK SETUP FUNCTION
# ==========================================
def setup_s3_infrastructure():
    """
    Ek function se poora S3 setup ho jaye:
    1. Bucket banao
    2. Folder structure banao
    3. Verify karo
    """
    print("=" * 55)
    print("   SETTING UP AWS S3 INFRASTRUCTURE")
    print("=" * 55)

    s3 = S3Manager()

    # Step 1: Bucket banao
    print("\n[1/3] Creating S3 Bucket...")
    s3.create_bucket()

    # Step 2: Folder structure banao
    print("\n[2/3] Creating folder structure...")
    s3.create_folder_structure()

    # Step 3: Verify
    print("\n[3/3] Verifying structure...")
    s3.list_files()

    print("\n" + "=" * 55)
    print("   S3 SETUP COMPLETE!")
    print("=" * 55)

    return s3


# -------------------------------------------
# Direct run karo to S3 setup ho jayega
# -------------------------------------------
if __name__ == "__main__":
    setup_s3_infrastructure()