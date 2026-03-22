# Databricks notebook source
# ==========================================
# BRONZE LAYER - Raw Data Ingestion
# ==========================================
# Bronze layer mein raw data as-is store hota hai
# Koi transformation nahi - sirf read aur save
# Source: AWS S3 CSV files
# Target: Bronze Delta Tables
# ==========================================

# COMMAND ----------

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit
)
from datetime import datetime

print("=" * 60)
print("    BRONZE LAYER - Raw Data Ingestion")
print("=" * 60)

# COMMAND ----------

# ==========================================
# CONFIGURATION
# ==========================================

# S3 paths (yahan se data aayega)
S3_BUCKET = "s3://ecommerce-pipeline-raw-data"
S3_RAW_CUSTOMERS = f"{S3_BUCKET}/raw_data/customers/"
S3_RAW_PRODUCTS = f"{S3_BUCKET}/raw_data/products/"
S3_RAW_ORDERS = f"{S3_BUCKET}/raw_data/orders/"

# Bronze layer paths (yahan data jayega)
BRONZE_PATH = "dbfs:/mnt/ecommerce/bronze"
BRONZE_CUSTOMERS = f"{BRONZE_PATH}/customers"
BRONZE_PRODUCTS = f"{BRONZE_PATH}/products"
BRONZE_ORDERS = f"{BRONZE_PATH}/orders"

# Processing timestamp
PROCESSING_TIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print(f"Processing Time: {PROCESSING_TIME}")
print(f"Source: {S3_BUCKET}")
print(f"Target: {BRONZE_PATH}")

# COMMAND ----------

# ==========================================
# 1. READ RAW CUSTOMERS FROM S3
# ==========================================
print("\n[1/6] Reading Customers from S3...")

df_customers_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(S3_RAW_CUSTOMERS)
)

# Add metadata columns
df_customers_bronze = (
    df_customers_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_id", lit(PROCESSING_TIME))
    .withColumn("_layer", lit("bronze"))
)

print(f"   Customers loaded: {df_customers_bronze.count()} rows")
print(f"   Columns: {len(df_customers_bronze.columns)}")
df_customers_bronze.printSchema()

# COMMAND ----------

# ==========================================
# 2. READ RAW PRODUCTS FROM S3
# ==========================================
print("\n[2/6] Reading Products from S3...")

df_products_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(S3_RAW_PRODUCTS)
)

# Add metadata columns
df_products_bronze = (
    df_products_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_id", lit(PROCESSING_TIME))
    .withColumn("_layer", lit("bronze"))
)

print(f"   Products loaded: {df_products_bronze.count()} rows")
print(f"   Columns: {len(df_products_bronze.columns)}")
df_products_bronze.printSchema()

# COMMAND ----------

# ==========================================
# 3. READ RAW ORDERS FROM S3
# ==========================================
print("\n[3/6] Reading Orders from S3...")

df_orders_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(S3_RAW_ORDERS)
)

# Add metadata columns
df_orders_bronze = (
    df_orders_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_id", lit(PROCESSING_TIME))
    .withColumn("_layer", lit("bronze"))
)

print(f"   Orders loaded: {df_orders_bronze.count()} rows")
print(f"   Columns: {len(df_orders_bronze.columns)}")
df_orders_bronze.printSchema()

# COMMAND ----------

# ==========================================
# 4. SAVE CUSTOMERS TO BRONZE (Delta Format)
# ==========================================
print("\n[4/6] Saving Customers to Bronze...")

(
    df_customers_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(BRONZE_CUSTOMERS)
)

print(f"   Saved to: {BRONZE_CUSTOMERS}")

# COMMAND ----------

# ==========================================
# 5. SAVE PRODUCTS TO BRONZE (Delta Format)
# ==========================================
print("\n[5/6] Saving Products to Bronze...")

(
    df_products_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(BRONZE_PRODUCTS)
)

print(f"   Saved to: {BRONZE_PRODUCTS}")

# COMMAND ----------

# ==========================================
# 6. SAVE ORDERS TO BRONZE (Delta Format)
# ==========================================
print("\n[6/6] Saving Orders to Bronze...")

(
    df_orders_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(BRONZE_ORDERS)
)

print(f"   Saved to: {BRONZE_ORDERS}")

# COMMAND ----------

# ==========================================
# VERIFICATION
# ==========================================
print("\n" + "=" * 60)
print("    BRONZE LAYER SUMMARY")
print("=" * 60)

customers_count = spark.read.format("delta").load(BRONZE_CUSTOMERS).count()
products_count = spark.read.format("delta").load(BRONZE_PRODUCTS).count()
orders_count = spark.read.format("delta").load(BRONZE_ORDERS).count()

print(f"   Customers : {customers_count} rows")
print(f"   Products  : {products_count} rows")
print(f"   Orders    : {orders_count} rows")
print(f"   Timestamp : {PROCESSING_TIME}")
print("=" * 60)
print("    BRONZE LAYER COMPLETE!")
print("=" * 60)