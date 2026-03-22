# Databricks notebook source
# ==========================================
# SILVER LAYER - Data Cleaning & Standardization
# ==========================================
# Silver layer mein data clean hota hai:
# - Null values handle
# - Duplicates remove
# - Data types fix
# - Column names standardize
# - Invalid data filter out
# Source: Bronze Delta Tables
# Target: Silver Delta Tables
# ==========================================

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, lower, upper, initcap,
    to_date, to_timestamp, current_timestamp,
    regexp_replace, lit, count, isnan, isnull,
    round as spark_round, coalesce
)
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType,
    DateType, BooleanType, DecimalType
)
from datetime import datetime

print("=" * 60)
print("    SILVER LAYER - Data Cleaning")
print("=" * 60)

# COMMAND ----------

# ==========================================
# CONFIGURATION
# ==========================================
BRONZE_PATH = "dbfs:/mnt/ecommerce/bronze"
SILVER_PATH = "dbfs:/mnt/ecommerce/silver"

BRONZE_CUSTOMERS = f"{BRONZE_PATH}/customers"
BRONZE_PRODUCTS = f"{BRONZE_PATH}/products"
BRONZE_ORDERS = f"{BRONZE_PATH}/orders"

SILVER_CUSTOMERS = f"{SILVER_PATH}/customers"
SILVER_PRODUCTS = f"{SILVER_PATH}/products"
SILVER_ORDERS = f"{SILVER_PATH}/orders"

PROCESSING_TIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# ==========================================
# HELPER FUNCTION - Data Quality Report
# ==========================================
def data_quality_report(df, table_name):
    """Har table ka data quality report print karo."""
    print(f"\n--- Data Quality Report: {table_name} ---")
    total_rows = df.count()
    print(f"   Total Rows: {total_rows}")

    for col_name in df.columns:
        null_count = df.filter(
            col(col_name).isNull() | (col(col_name) == "")
        ).count()
        null_pct = round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0
        if null_count > 0:
            print(f"   {col_name}: {null_count} nulls ({null_pct}%)")

    dup_count = total_rows - df.dropDuplicates().count()
    print(f"   Duplicate Rows: {dup_count}")
    print(f"   Clean Rows: {total_rows - dup_count}")
    return total_rows, dup_count

# COMMAND ----------

# ==========================================
# 1. CLEAN CUSTOMERS
# ==========================================
print("\n[1/3] Cleaning Customers...")

# Read from Bronze
df_cust_bronze = spark.read.format("delta").load(BRONZE_CUSTOMERS)
print(f"   Bronze rows: {df_cust_bronze.count()}")

# Quality report BEFORE cleaning
data_quality_report(df_cust_bronze, "Customers (Before)")

# Apply cleaning transformations
df_cust_silver = (
    df_cust_bronze
    # Remove duplicates based on customer_id
    .dropDuplicates(["customer_id"])

    # Clean string columns - trim spaces, proper case
    .withColumn("first_name", initcap(trim(col("first_name"))))
    .withColumn("last_name", initcap(trim(col("last_name"))))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", initcap(trim(col("city"))))
    .withColumn("state", upper(trim(col("state"))))

    # Fix data types
    .withColumn("date_of_birth", to_date(col("date_of_birth"), "yyyy-MM-dd"))
    .withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))
    .withColumn("is_active", col("is_active").cast(BooleanType()))

    # Handle nulls
    .withColumn("country", coalesce(col("country"), lit("US")))
    .withColumn("gender", coalesce(col("gender"), lit("Unknown")))
    .withColumn("is_active", coalesce(col("is_active"), lit(True)))

    # Clean phone number - remove special chars
    .withColumn("phone", regexp_replace(col("phone"), "[^0-9+]", ""))

    # Filter out invalid records
    .filter(col("customer_id").isNotNull())
    .filter(col("email").isNotNull())
    .filter(col("email").contains("@"))

    # Add metadata
    .withColumn("_cleaned_timestamp", current_timestamp())
    .withColumn("_layer", lit("silver"))

    # Drop bronze metadata columns
    .drop("_ingestion_timestamp", "_source_file", "_batch_id")
)

print(f"   Silver rows: {df_cust_silver.count()}")
data_quality_report(df_cust_silver, "Customers (After)")

# COMMAND ----------

# ==========================================
# 2. CLEAN PRODUCTS
# ==========================================
print("\n[2/3] Cleaning Products...")

df_prod_bronze = spark.read.format("delta").load(BRONZE_PRODUCTS)
print(f"   Bronze rows: {df_prod_bronze.count()}")

data_quality_report(df_prod_bronze, "Products (Before)")

df_prod_silver = (
    df_prod_bronze
    # Remove duplicates
    .dropDuplicates(["product_id"])

    # Clean strings
    .withColumn("product_name", initcap(trim(col("product_name"))))
    .withColumn("category", initcap(trim(col("category"))))
    .withColumn("sub_category", initcap(trim(col("sub_category"))))
    .withColumn("brand", initcap(trim(col("brand"))))

    # Fix data types
    .withColumn("price", col("price").cast(DecimalType(10, 2)))
    .withColumn("cost_price", col("cost_price").cast(DecimalType(10, 2)))
    .withColumn("weight_kg", col("weight_kg").cast(DecimalType(8, 2)))
    .withColumn("created_date", to_date(col("created_date"), "yyyy-MM-dd"))
    .withColumn("is_available", col("is_available").cast(BooleanType()))

    # Handle nulls
    .withColumn("is_available", coalesce(col("is_available"), lit(True)))
    .withColumn("weight_kg", coalesce(col("weight_kg"), lit(0.0)))

    # Calculate profit margin
    .withColumn("profit_margin",
        spark_round(
            (col("price") - col("cost_price")) / col("price") * 100, 2
        )
    )

    # Filter invalid records
    .filter(col("product_id").isNotNull())
    .filter(col("price") > 0)
    .filter(col("cost_price") > 0)

    # Add metadata
    .withColumn("_cleaned_timestamp", current_timestamp())
    .withColumn("_layer", lit("silver"))
    .drop("_ingestion_timestamp", "_source_file", "_batch_id")
)

print(f"   Silver rows: {df_prod_silver.count()}")
data_quality_report(df_prod_silver, "Products (After)")

# COMMAND ----------

# ==========================================
# 3. CLEAN ORDERS
# ==========================================
print("\n[3/3] Cleaning Orders...")

df_ord_bronze = spark.read.format("delta").load(BRONZE_ORDERS)
print(f"   Bronze rows: {df_ord_bronze.count()}")

data_quality_report(df_ord_bronze, "Orders (Before)")

df_ord_silver = (
    df_ord_bronze
    # Remove duplicates
    .dropDuplicates(["order_id"])

    # Fix data types
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("delivery_date", to_date(col("delivery_date"), "yyyy-MM-dd"))
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    .withColumn("total_amount", col("total_amount").cast(DecimalType(12, 2)))
    .withColumn("discount_percent", col("discount_percent").cast(DecimalType(5, 2)))
    .withColumn("discount_amount", col("discount_amount").cast(DecimalType(12, 2)))
    .withColumn("final_amount", col("final_amount").cast(DecimalType(12, 2)))

    # Clean strings
    .withColumn("payment_method", initcap(trim(col("payment_method"))))
    .withColumn("order_status", initcap(trim(col("order_status"))))
    .withColumn("shipping_city", initcap(trim(col("shipping_city"))))
    .withColumn("shipping_state", upper(trim(col("shipping_state"))))

    # Handle nulls
    .withColumn("discount_percent", coalesce(col("discount_percent"), lit(0)))
    .withColumn("discount_amount", coalesce(col("discount_amount"), lit(0)))

    # Recalculate final_amount for data integrity
    .withColumn("calculated_total", spark_round(col("unit_price") * col("quantity"), 2))
    .withColumn("calculated_discount", spark_round(col("calculated_total") * col("discount_percent") / 100, 2))
    .withColumn("calculated_final", spark_round(col("calculated_total") - col("calculated_discount"), 2))

    # Filter invalid records
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("final_amount") >= 0)

    # Add metadata
    .withColumn("_cleaned_timestamp", current_timestamp())
    .withColumn("_layer", lit("silver"))
    .drop("_ingestion_timestamp", "_source_file", "_batch_id")
)

print(f"   Silver rows: {df_ord_silver.count()}")
data_quality_report(df_ord_silver, "Orders (After)")

# COMMAND ----------

# ==========================================
# SAVE TO SILVER LAYER
# ==========================================
print("\nSaving to Silver layer...")

df_cust_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_CUSTOMERS)
print(f"   Customers saved to: {SILVER_CUSTOMERS}")

df_prod_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_PRODUCTS)
print(f"   Products saved to: {SILVER_PRODUCTS}")

df_ord_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_ORDERS)
print(f"   Orders saved to: {SILVER_ORDERS}")

# COMMAND ----------

# ==========================================
# SUMMARY
# ==========================================
print("\n" + "=" * 60)
print("    SILVER LAYER SUMMARY")
print("=" * 60)
print(f"   Customers : {df_cust_silver.count()} rows")
print(f"   Products  : {df_prod_silver.count()} rows")
print(f"   Orders    : {df_ord_silver.count()} rows")
print(f"   Timestamp : {PROCESSING_TIME}")
print("=" * 60)
print("    SILVER LAYER COMPLETE!")
print("=" * 60)