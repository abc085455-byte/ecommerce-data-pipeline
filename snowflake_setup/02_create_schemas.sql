-- ==========================================
-- SNOWFLAKE SETUP - Step 2
-- Create Schemas (Medallion Architecture)
-- RAW -> STAGING -> ANALYTICS
-- ==========================================

USE DATABASE ECOMMERCE_DB;

-- 1. RAW Schema - Raw data as-is from source
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data from source systems - no transformations';

-- 2. STAGING Schema - Cleaned & standardized data
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Staging area - cleaned and standardized data';

-- 3. ANALYTICS Schema - Business-ready aggregated data
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Analytics layer - business-ready data for reporting';

-- 4. Verify
SHOW SCHEMAS IN DATABASE ECOMMERCE_DB;