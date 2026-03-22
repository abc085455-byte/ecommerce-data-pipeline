# Databricks notebook source
# ==========================================
# GOLD LAYER - Business Aggregations
# ==========================================
# Gold layer mein business-ready data hota hai:
# - Daily sales summary
# - Monthly revenue trends
# - Customer analytics
# - Product performance
# - Top customers & products
# Source: Silver Delta Tables
# Target: Gold Delta Tables + Snowflake
# ==========================================

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct,
    avg, min as spark_min, max as spark_max,
    month, year, dayofweek, date_format,
    dense_rank, row_number, current_timestamp,
    lit, when, round as spark_round
)
from pyspark.sql.window import Window
from datetime import datetime

print("=" * 60)
print("    GOLD LAYER - Business Aggregations")
print("=" * 60)

# COMMAND ----------

# ==========================================
# CONFIGURATION
# ==========================================
SILVER_PATH = "dbfs:/mnt/ecommerce/silver"
GOLD_PATH = "dbfs:/mnt/ecommerce/gold"

SILVER_CUSTOMERS = f"{SILVER_PATH}/customers"
SILVER_PRODUCTS = f"{SILVER_PATH}/products"
SILVER_ORDERS = f"{SILVER_PATH}/orders"

PROCESSING_TIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# ==========================================
# READ SILVER DATA
# ==========================================
print("\nReading Silver layer data...")

df_customers = spark.read.format("delta").load(SILVER_CUSTOMERS)
df_products = spark.read.format("delta").load(SILVER_PRODUCTS)
df_orders = spark.read.format("delta").load(SILVER_ORDERS)

print(f"   Customers: {df_customers.count()} rows")
print(f"   Products: {df_products.count()} rows")
print(f"   Orders: {df_orders.count()} rows")

# COMMAND ----------

# ==========================================
# GOLD TABLE 1: DAILY SALES SUMMARY
# ==========================================
print("\n[1/6] Creating Daily Sales Summary...")

df_daily_sales = (
    df_orders
    .filter(col("order_status") != "Cancelled")
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_sum("final_amount").alias("total_revenue"),
        spark_sum("discount_amount").alias("total_discount"),
        avg("final_amount").alias("avg_order_value"),
        spark_sum("quantity").alias("total_items_sold"),
        spark_sum(when(col("order_status") == "Delivered", 1).otherwise(0)).alias("delivered_orders"),
        spark_sum(when(col("order_status") == "Returned", 1).otherwise(0)).alias("returned_orders")
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .withColumn("total_discount", spark_round(col("total_discount"), 2))
    .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
    .withColumn("day_of_week", dayofweek(col("order_date")))
    .withColumn("day_name", date_format(col("order_date"), "EEEE"))
    .withColumn("_created_timestamp", current_timestamp())
    .orderBy("order_date")
)

print(f"   Daily Sales rows: {df_daily_sales.count()}")
df_daily_sales.show(5, truncate=False)

# COMMAND ----------

# ==========================================
# GOLD TABLE 2: MONTHLY REVENUE
# ==========================================
print("\n[2/6] Creating Monthly Revenue Summary...")

df_monthly_revenue = (
    df_orders
    .filter(col("order_status") != "Cancelled")
    .withColumn("order_month", month(col("order_date")))
    .withColumn("order_year", year(col("order_date")))
    .withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))
    .groupBy("order_year", "order_month", "year_month")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_sum("final_amount").alias("total_revenue"),
        spark_sum("discount_amount").alias("total_discount"),
        avg("final_amount").alias("avg_order_value"),
        spark_sum("quantity").alias("total_items_sold")
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
    .withColumn("_created_timestamp", current_timestamp())
    .orderBy("order_year", "order_month")
)

print(f"   Monthly Revenue rows: {df_monthly_revenue.count()}")
df_monthly_revenue.show(5, truncate=False)

# COMMAND ----------

# ==========================================
# GOLD TABLE 3: CUSTOMER ANALYTICS
# ==========================================
print("\n[3/6] Creating Customer Analytics...")

df_customer_analytics = (
    df_orders
    .filter(col("order_status") != "Cancelled")
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders"),
        spark_sum("final_amount").alias("total_spent"),
        avg("final_amount").alias("avg_order_value"),
        spark_min("order_date").alias("first_order_date"),
        spark_max("order_date").alias("last_order_date"),
        spark_sum("quantity").alias("total_items_bought"),
        countDistinct("product_id").alias("unique_products_bought"),
        spark_sum(when(col("order_status") == "Returned", 1).otherwise(0)).alias("return_count")
    )
    .withColumn("total_spent", spark_round(col("total_spent"), 2))
    .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
    # Customer segments based on spending
    .withColumn("customer_segment",
        when(col("total_spent") >= 5000, "Premium")
        .when(col("total_spent") >= 2000, "Gold")
        .when(col("total_spent") >= 500, "Silver")
        .otherwise("Bronze")
    )
    .withColumn("return_rate",
        spark_round(col("return_count") / col("total_orders") * 100, 2)
    )
    .withColumn("_created_timestamp", current_timestamp())
)

# Join with customer details
df_customer_gold = (
    df_customer_analytics
    .join(
        df_customers.select(
            "customer_id", "first_name", "last_name",
            "email", "city", "state", "gender",
            "registration_date", "is_active"
        ),
        on="customer_id",
        how="left"
    )
)

print(f"   Customer Analytics rows: {df_customer_gold.count()}")
df_customer_gold.show(5, truncate=False)

# COMMAND ----------

# ==========================================
# GOLD TABLE 4: PRODUCT PERFORMANCE
# ==========================================
print("\n[4/6] Creating Product Performance...")

df_product_performance = (
    df_orders
    .filter(col("order_status") != "Cancelled")
    .groupBy("product_id")
    .agg(
        count("order_id").alias("total_orders"),
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("final_amount").alias("total_revenue"),
        avg("final_amount").alias("avg_sale_value"),
        countDistinct("customer_id").alias("unique_buyers"),
        spark_sum(when(col("order_status") == "Returned", 1).otherwise(0)).alias("return_count")
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .withColumn("avg_sale_value", spark_round(col("avg_sale_value"), 2))
)

# Join with product details
df_product_gold = (
    df_product_performance
    .join(
        df_products.select(
            "product_id", "product_name", "category",
            "sub_category", "brand", "price",
            "cost_price", "is_available"
        ),
        on="product_id",
        how="left"
    )
    # Calculate profit
    .withColumn("total_cost",
        spark_round(col("cost_price") * col("total_quantity_sold"), 2)
    )
    .withColumn("total_profit",
        spark_round(col("total_revenue") - col("total_cost"), 2)
    )
    .withColumn("profit_margin_pct",
        spark_round(col("total_profit") / col("total_revenue") * 100, 2)
    )
    # Rank products by revenue
    .withColumn("revenue_rank",
        dense_rank().over(Window.orderBy(col("total_revenue").desc()))
    )
    .withColumn("_created_timestamp", current_timestamp())
)

print(f"   Product Performance rows: {df_product_gold.count()}")
df_product_gold.show(5, truncate=False)

# COMMAND ----------

# ==========================================
# GOLD TABLE 5: CATEGORY SUMMARY
# ==========================================
print("\n[5/6] Creating Category Summary...")

df_category_summary = (
    df_product_gold
    .groupBy("category")
    .agg(
        count("product_id").alias("total_products"),
        spark_sum("total_orders").alias("total_orders"),
        spark_sum("total_revenue").alias("total_revenue"),
        spark_sum("total_profit").alias("total_profit"),
        avg("profit_margin_pct").alias("avg_profit_margin"),
        spark_sum("total_quantity_sold").alias("total_items_sold")
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .withColumn("total_profit", spark_round(col("total_profit"), 2))
    .withColumn("avg_profit_margin", spark_round(col("avg_profit_margin"), 2))
    .withColumn("_created_timestamp", current_timestamp())
    .orderBy(col("total_revenue").desc())
)

print(f"   Category Summary rows: {df_category_summary.count()}")
df_category_summary.show(truncate=False)

# COMMAND ----------

# ==========================================
# GOLD TABLE 6: PAYMENT METHOD ANALYSIS
# ==========================================
print("\n[6/6] Creating Payment Analysis...")

df_payment_analysis = (
    df_orders
    .filter(col("order_status") != "Cancelled")
    .groupBy("payment_method")
    .agg(
        count("order_id").alias("total_transactions"),
        spark_sum("final_amount").alias("total_revenue"),
        avg("final_amount").alias("avg_transaction_value"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .withColumn("avg_transaction_value", spark_round(col("avg_transaction_value"), 2))
    .withColumn("_created_timestamp", current_timestamp())
    .orderBy(col("total_revenue").desc())
)

print(f"   Payment Analysis rows: {df_payment_analysis.count()}")
df_payment_analysis.show(truncate=False)

# COMMAND ----------

# ==========================================
# SAVE ALL GOLD TABLES
# ==========================================
print("\nSaving Gold layer tables...")

tables = {
    "daily_sales": df_daily_sales,
    "monthly_revenue": df_monthly_revenue,
    "customer_analytics": df_customer_gold,
    "product_performance": df_product_gold,
    "category_summary": df_category_summary,
    "payment_analysis": df_payment_analysis
}

for table_name, df in tables.items():
    save_path = f"{GOLD_PATH}/{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(save_path)
    print(f"   Saved: {table_name} -> {save_path}")

# COMMAND ----------

# ==========================================
# FINAL SUMMARY
# ==========================================
print("\n" + "=" * 60)
print("    GOLD LAYER SUMMARY")
print("=" * 60)
for table_name, df in tables.items():
    print(f"   {table_name}: {df.count()} rows")
print(f"   Timestamp: {PROCESSING_TIME}")
print("=" * 60)
print("    GOLD LAYER COMPLETE!")
print("=" * 60)