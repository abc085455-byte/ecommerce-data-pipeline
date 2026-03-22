-- ==========================================
-- SNOWFLAKE SETUP - Step 1
-- Create Database & Warehouse
-- Real-Time E-Commerce Data Pipeline
-- ==========================================

-- 1. Create Warehouse (Compute Resource)
CREATE WAREHOUSE IF NOT EXISTS ECOMMERCE_WH
    WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for E-Commerce Data Pipeline';

-- 2. Create Database
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB
    COMMENT = 'E-Commerce Analytics Database';

-- 3. Use the database
USE DATABASE ECOMMERCE_DB;

-- 4. Verify
SHOW DATABASES LIKE 'ECOMMERCE_DB';
SHOW WAREHOUSES LIKE 'ECOMMERCE_WH';