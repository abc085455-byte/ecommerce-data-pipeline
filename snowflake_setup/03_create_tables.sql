-- ==========================================
-- SNOWFLAKE SETUP - Step 3
-- Create Tables in RAW Schema
-- These tables will receive data from CSV/S3
-- ==========================================

USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;
USE WAREHOUSE ECOMMERCE_WH;

-- ==========================================
-- 1. RAW CUSTOMERS TABLE
-- ==========================================
CREATE OR REPLACE TABLE RAW.CUSTOMERS (
    customer_id        VARCHAR(20)     NOT NULL,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    email              VARCHAR(255),
    phone              VARCHAR(50),
    address            VARCHAR(500),
    city               VARCHAR(100),
    state              VARCHAR(10),
    zip_code           VARCHAR(20),
    country            VARCHAR(10),
    date_of_birth      DATE,
    gender             VARCHAR(20),
    registration_date  DATE,
    is_active          BOOLEAN,
    -- Metadata columns
    loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file        VARCHAR(500),
    PRIMARY KEY (customer_id)
);

-- ==========================================
-- 2. RAW PRODUCTS TABLE
-- ==========================================
CREATE OR REPLACE TABLE RAW.PRODUCTS (
    product_id         VARCHAR(20)     NOT NULL,
    product_name       VARCHAR(500),
    category           VARCHAR(100),
    sub_category       VARCHAR(100),
    brand              VARCHAR(100),
    price              DECIMAL(10,2),
    cost_price         DECIMAL(10,2),
    weight_kg          DECIMAL(8,2),
    is_available       BOOLEAN,
    created_date       DATE,
    -- Metadata columns
    loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file        VARCHAR(500),
    PRIMARY KEY (product_id)
);

-- ==========================================
-- 3. RAW ORDERS TABLE
-- ==========================================
CREATE OR REPLACE TABLE RAW.ORDERS (
    order_id           VARCHAR(20)     NOT NULL,
    customer_id        VARCHAR(20),
    product_id         VARCHAR(20),
    order_date         DATE,
    quantity           INTEGER,
    unit_price         DECIMAL(10,2),
    total_amount       DECIMAL(12,2),
    discount_percent   DECIMAL(5,2),
    discount_amount    DECIMAL(12,2),
    final_amount       DECIMAL(12,2),
    payment_method     VARCHAR(50),
    order_status       VARCHAR(50),
    shipping_address   VARCHAR(500),
    shipping_city      VARCHAR(100),
    shipping_state     VARCHAR(10),
    delivery_date      DATE,
    -- Metadata columns
    loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file        VARCHAR(500),
    PRIMARY KEY (order_id)
);

-- ==========================================
-- VERIFY TABLES
-- ==========================================
SHOW TABLES IN SCHEMA RAW;

-- Check table structures
DESCRIBE TABLE RAW.CUSTOMERS;
DESCRIBE TABLE RAW.PRODUCTS;
DESCRIBE TABLE RAW.ORDERS;