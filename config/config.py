"""
==========================================
Configuration Module
Real-Time E-Commerce Data Pipeline
==========================================
Yeh file project ki saari configurations
ek jagah centrally manage karti hai.
.env file se values load hoti hain.

Includes: AWS, Snowflake, Databricks,
          Paths, Pipeline settings
==========================================
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# -------------------------------------------
# .env file ka path set karo aur load karo
# -------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(dotenv_path=PROJECT_ROOT / '.env')


class AWSConfig:
    """
    AWS Configuration.
    S3 bucket mein raw data store hoga.
    Databricks aur Snowflake dono S3 se data read karenge.
    """
    ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
    SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    REGION            = os.getenv('AWS_REGION', 'us-east-1')

    # S3 Bucket Settings
    S3_BUCKET_NAME    = os.getenv('S3_BUCKET_NAME', 'ecommerce-pipeline-raw-data')
    S3_RAW_PREFIX     = os.getenv('S3_RAW_PREFIX', 'raw_data')
    S3_BRONZE_PREFIX  = os.getenv('S3_BRONZE_PREFIX', 'bronze')
    S3_SILVER_PREFIX  = os.getenv('S3_SILVER_PREFIX', 'silver')
    S3_GOLD_PREFIX    = os.getenv('S3_GOLD_PREFIX', 'gold')

    # Full S3 paths (easy access ke liye)
    @classmethod
    def get_s3_path(cls, prefix, filename=""):
        """S3 full path generate karo"""
        return f"s3://{cls.S3_BUCKET_NAME}/{prefix}/{filename}"

    @classmethod
    def get_raw_path(cls, folder=""):
        """Raw data ka S3 path"""
        return f"s3://{cls.S3_BUCKET_NAME}/{cls.S3_RAW_PREFIX}/{folder}"

    @classmethod
    def get_bronze_path(cls, folder=""):
        """Bronze layer ka S3 path"""
        return f"s3://{cls.S3_BUCKET_NAME}/{cls.S3_BRONZE_PREFIX}/{folder}"

    @classmethod
    def get_silver_path(cls, folder=""):
        """Silver layer ka S3 path"""
        return f"s3://{cls.S3_BUCKET_NAME}/{cls.S3_SILVER_PREFIX}/{folder}"

    @classmethod
    def get_gold_path(cls, folder=""):
        """Gold layer ka S3 path"""
        return f"s3://{cls.S3_BUCKET_NAME}/{cls.S3_GOLD_PREFIX}/{folder}"


class SnowflakeConfig:
    """
    Snowflake Data Warehouse ki connection settings.
    Saari values .env file se aati hain.
    """
    ACCOUNT    = os.getenv('SNOWFLAKE_ACCOUNT')
    USER       = os.getenv('SNOWFLAKE_USER')
    PASSWORD   = os.getenv('SNOWFLAKE_PASSWORD')
    DATABASE   = os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DB')
    SCHEMA     = os.getenv('SNOWFLAKE_SCHEMA', 'RAW')
    WAREHOUSE  = os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH')
    ROLE       = os.getenv('SNOWFLAKE_ROLE', 'ECOMMERCE_ROLE')


class DatabricksConfig:
    """
    Databricks connection ki settings.
    PySpark notebooks trigger karne ke liye use hoga.
    """
    HOST       = os.getenv('DATABRICKS_HOST')
    TOKEN      = os.getenv('DATABRICKS_TOKEN')
    CLUSTER_ID = os.getenv('DATABRICKS_CLUSTER_ID')


class PathConfig:
    """
    Project ke saare important paths.
    Local + S3 dono paths yahan hain.
    """
    # Project root directory
    ROOT          = PROJECT_ROOT

    # Local raw data directories
    RAW_DATA_DIR  = PROJECT_ROOT / 'raw_data'
    ORDERS_DIR    = RAW_DATA_DIR / 'orders'
    CUSTOMERS_DIR = RAW_DATA_DIR / 'customers'
    PRODUCTS_DIR  = RAW_DATA_DIR / 'products'

    # Logs directory
    LOG_DIR       = PROJECT_ROOT / 'logs'

    # S3 paths (from AWSConfig)
    S3_RAW_ORDERS    = AWSConfig.get_raw_path('orders')
    S3_RAW_CUSTOMERS = AWSConfig.get_raw_path('customers')
    S3_RAW_PRODUCTS  = AWSConfig.get_raw_path('products')


class PipelineConfig:
    """
    Pipeline ki general settings.
    Kitna data generate karna hai, date range, etc.
    """
    # Data generation counts
    NUM_CUSTOMERS = int(os.getenv('NUM_CUSTOMERS', 500))
    NUM_PRODUCTS  = int(os.getenv('NUM_PRODUCTS', 100))
    NUM_ORDERS    = int(os.getenv('NUM_ORDERS', 5000))

    # Order date range
    ORDER_START_DATE = '2024-01-01'
    ORDER_END_DATE   = '2024-12-31'

    # Snowflake loading batch size
    BATCH_SIZE = 1000

    # Upload to S3 after generation?
    UPLOAD_TO_S3 = True


# -------------------------------------------
# Quick Test - Yeh file directly run karo
# to sab settings dikh jayengi
# -------------------------------------------
if __name__ == "__main__":
    print("=" * 55)
    print("   CONFIGURATION LOADED SUCCESSFULLY!")
    print("=" * 55)

    print("\n--- PROJECT PATHS ---")
    print(f"  Project Root    : {PathConfig.ROOT}")
    print(f"  Raw Data Dir    : {PathConfig.RAW_DATA_DIR}")
    print(f"  Log Dir         : {PathConfig.LOG_DIR}")

    print("\n--- AWS S3 SETTINGS ---")
    print(f"  Region          : {AWSConfig.REGION}")
    print(f"  Bucket          : {AWSConfig.S3_BUCKET_NAME}")
    print(f"  S3 Raw Orders   : {PathConfig.S3_RAW_ORDERS}")
    print(f"  S3 Raw Customers: {PathConfig.S3_RAW_CUSTOMERS}")
    print(f"  S3 Raw Products : {PathConfig.S3_RAW_PRODUCTS}")
    print(f"  S3 Bronze Path  : {AWSConfig.get_bronze_path()}")
    print(f"  S3 Silver Path  : {AWSConfig.get_silver_path()}")
    print(f"  S3 Gold Path    : {AWSConfig.get_gold_path()}")

    print("\n--- SNOWFLAKE SETTINGS ---")
    print(f"  Database        : {SnowflakeConfig.DATABASE}")
    print(f"  Warehouse       : {SnowflakeConfig.WAREHOUSE}")
    print(f"  Schema          : {SnowflakeConfig.SCHEMA}")

    print("\n--- PIPELINE SETTINGS ---")
    print(f"  Num Customers   : {PipelineConfig.NUM_CUSTOMERS}")
    print(f"  Num Products    : {PipelineConfig.NUM_PRODUCTS}")
    print(f"  Num Orders      : {PipelineConfig.NUM_ORDERS}")
    print(f"  Upload to S3    : {PipelineConfig.UPLOAD_TO_S3}")

    print("\n" + "=" * 55)