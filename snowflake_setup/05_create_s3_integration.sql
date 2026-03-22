-- ==========================================
-- SNOWFLAKE SETUP - Step 5
-- AWS S3 Integration
-- S3 se directly data load karne ke liye
-- ==========================================

USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

-- 1. Create Storage Integration (S3 connection)
-- NOTE: Yeh ACCOUNTADMIN role se run karna padega
CREATE OR REPLACE STORAGE INTEGRATION S3_ECOMMERCE_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://ecommerce-pipeline-raw-data/');

-- 2. Describe integration to get AWS IAM values
-- Yeh values AWS IAM Trust Policy mein dalni hain
DESC INTEGRATION S3_ECOMMERCE_INT;

-- 3. Create File Format for CSV
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = 'NONE';

-- 4. Create External Stages (S3 paths)
CREATE OR REPLACE STAGE STG_CUSTOMERS
    STORAGE_INTEGRATION = S3_ECOMMERCE_INT
    URL = 's3://ecommerce-pipeline-raw-data/raw_data/customers/'
    FILE_FORMAT = CSV_FORMAT;

CREATE OR REPLACE STAGE STG_PRODUCTS
    STORAGE_INTEGRATION = S3_ECOMMERCE_INT
    URL = 's3://ecommerce-pipeline-raw-data/raw_data/products/'
    FILE_FORMAT = CSV_FORMAT;

CREATE OR REPLACE STAGE STG_ORDERS
    STORAGE_INTEGRATION = S3_ECOMMERCE_INT
    URL = 's3://ecommerce-pipeline-raw-data/raw_data/orders/'
    FILE_FORMAT = CSV_FORMAT;

-- 5. Verify stages
SHOW STAGES;

-- 6. List files in stage (S3 se)
LIST @STG_CUSTOMERS;
LIST @STG_PRODUCTS;
LIST @STG_ORDERS;

-- 7. Load data from S3 to tables
COPY INTO RAW.CUSTOMERS
    FROM @STG_CUSTOMERS
    PATTERN = '.*customers_latest.*'
    ON_ERROR = 'CONTINUE';

COPY INTO RAW.PRODUCTS
    FROM @STG_PRODUCTS
    PATTERN = '.*products_latest.*'
    ON_ERROR = 'CONTINUE';

COPY INTO RAW.ORDERS
    FROM @STG_ORDERS
    PATTERN = '.*orders_latest.*'
    ON_ERROR = 'CONTINUE';

-- 8. Verify data loaded
SELECT COUNT(*) AS customer_count FROM RAW.CUSTOMERS;
SELECT COUNT(*) AS product_count FROM RAW.PRODUCTS;
SELECT COUNT(*) AS order_count FROM RAW.ORDERS;

-- 9. Quick data preview
SELECT * FROM RAW.CUSTOMERS LIMIT 5;
SELECT * FROM RAW.PRODUCTS LIMIT 5;
SELECT * FROM RAW.ORDERS LIMIT 5;