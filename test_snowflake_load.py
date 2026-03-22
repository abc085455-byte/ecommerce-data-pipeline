"""
Fast Bulk Load CSV data into Snowflake
Using write_pandas (bulk upload - FAST!)
"""

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

load_dotenv('.env')

print("=" * 55)
print("   SNOWFLAKE FAST DATA LOADING")
print("=" * 55)

start_time = datetime.now()

try:
    # ---- CONNECT ----
    print("\n[1/7] Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        database='ECOMMERCE_DB',
        schema='RAW',
        warehouse='ECOMMERCE_WH'
    )
    cursor = conn.cursor()
    print("   Connected!")

    # ---- CREATE TABLES ----
    print("\n[2/7] Creating tables...")

    cursor.execute("""
        CREATE OR REPLACE TABLE RAW.CUSTOMERS (
            customer_id VARCHAR(20),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            phone VARCHAR(50),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(10),
            zip_code VARCHAR(20),
            country VARCHAR(10),
            date_of_birth VARCHAR(20),
            gender VARCHAR(20),
            registration_date VARCHAR(20),
            is_active VARCHAR(10),
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    print("   CUSTOMERS table created!")

    cursor.execute("""
        CREATE OR REPLACE TABLE RAW.PRODUCTS (
            product_id VARCHAR(20),
            product_name VARCHAR(500),
            category VARCHAR(100),
            sub_category VARCHAR(100),
            brand VARCHAR(100),
            price FLOAT,
            cost_price FLOAT,
            weight_kg FLOAT,
            is_available VARCHAR(10),
            created_date VARCHAR(20),
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    print("   PRODUCTS table created!")

    cursor.execute("""
        CREATE OR REPLACE TABLE RAW.ORDERS (
            order_id VARCHAR(20),
            customer_id VARCHAR(20),
            product_id VARCHAR(20),
            order_date VARCHAR(20),
            quantity INTEGER,
            unit_price FLOAT,
            total_amount FLOAT,
            discount_percent FLOAT,
            discount_amount FLOAT,
            final_amount FLOAT,
            payment_method VARCHAR(50),
            order_status VARCHAR(50),
            shipping_address VARCHAR(500),
            shipping_city VARCHAR(100),
            shipping_state VARCHAR(10),
            delivery_date VARCHAR(20),
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    print("   ORDERS table created!")

    # ---- READ CSV FILES ----
    print("\n[3/7] Reading CSV files...")
    raw_data = Path('raw_data')

    df_cust = pd.read_csv(raw_data / 'customers' / 'customers_latest.csv')
    print(f"   Customers CSV: {len(df_cust)} rows")

    df_prod = pd.read_csv(raw_data / 'products' / 'products_latest.csv')
    print(f"   Products CSV: {len(df_prod)} rows")

    df_ord = pd.read_csv(raw_data / 'orders' / 'orders_latest.csv')
    print(f"   Orders CSV: {len(df_ord)} rows")

    # ---- CONVERT COLUMN NAMES TO UPPERCASE ----
    # Snowflake uppercase columns expect karta hai
    df_cust.columns = [c.upper() for c in df_cust.columns]
    df_prod.columns = [c.upper() for c in df_prod.columns]
    df_ord.columns = [c.upper() for c in df_ord.columns]

    # Convert all to string to avoid type issues
    df_cust = df_cust.astype(str)
    df_prod = df_prod.astype(str)
    df_ord = df_ord.astype(str)

    # ---- BULK LOAD CUSTOMERS ----
    print("\n[4/7] Loading Customers (bulk)...")
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df_cust,
        table_name='CUSTOMERS',
        schema='RAW',
        database='ECOMMERCE_DB',
        auto_create_table=False,
        overwrite=False
    )
    print(f"   Loaded {nrows} customers in {nchunks} chunks!")

    # ---- BULK LOAD PRODUCTS ----
    print("\n[5/7] Loading Products (bulk)...")
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df_prod,
        table_name='PRODUCTS',
        schema='RAW',
        database='ECOMMERCE_DB',
        auto_create_table=False,
        overwrite=False
    )
    print(f"   Loaded {nrows} products in {nchunks} chunks!")

    # ---- BULK LOAD ORDERS ----
    print("\n[6/7] Loading Orders (bulk)...")
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df_ord,
        table_name='ORDERS',
        schema='RAW',
        database='ECOMMERCE_DB',
        auto_create_table=False,
        overwrite=False
    )
    print(f"   Loaded {nrows} orders in {nchunks} chunks!")

    # ---- VERIFY ----
    print("\n[7/7] Verifying loaded data...")

    cursor.execute("SELECT COUNT(*) FROM RAW.CUSTOMERS")
    cust_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM RAW.PRODUCTS")
    prod_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM RAW.ORDERS")
    ord_count = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(FINAL_AMOUNT::FLOAT) FROM RAW.ORDERS")
    total_rev = cursor.fetchone()[0]

    print(f"   Customers in Snowflake : {cust_count}")
    print(f"   Products in Snowflake  : {prod_count}")
    print(f"   Orders in Snowflake    : {ord_count}")
    print(f"   Total Revenue          : ${total_rev:,.2f}")

    # Sample data
    print("\n   Sample Orders:")
    cursor.execute("""
        SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE, 
               FINAL_AMOUNT, ORDER_STATUS 
        FROM RAW.ORDERS LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]} | {row[1]} | {row[2]} | ${row[3]} | {row[4]}")

    # Close connection
    cursor.close()
    conn.close()

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 55)
    print(f"   DATA LOADED SUCCESSFULLY!")
    print(f"   Time Taken: {duration:.2f} seconds")
    print("=" * 55)

except Exception as e:
    print(f"\n   ERROR: {e}")
    import traceback
    traceback.print_exc()
    print("=" * 55)