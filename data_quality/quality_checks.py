"""
==========================================
Data Quality Checks
Real-Time E-Commerce Data Pipeline
==========================================
Yeh module data quality validate karta hai:
- Null checks
- Duplicate checks
- Row count checks
- Value range checks
- Schema validation
==========================================
"""

import os
import sys
from pathlib import Path
import pandas as pd
from loguru import logger
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))
from config.config import PathConfig

# Logger setup
log_file = PathConfig.LOG_DIR / "data_quality.log"
logger.add(
    str(log_file),
    rotation="10 MB",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)


class DataQualityChecker:
    """Data quality checks for CSV files."""

    def __init__(self):
        """Initialize with results list."""
        self.results = []
        self.all_passed = True

    def check_file_exists(self, file_path, table_name):
        """Check if file exists."""
        exists = Path(file_path).exists()
        status = "PASS" if exists else "FAIL"
        if not exists:
            self.all_passed = False
        self.results.append({
            'table': table_name,
            'check': 'File Exists',
            'status': status,
            'details': str(file_path)
        })
        logger.info(f"[{status}] {table_name} - File exists: {file_path}")
        return exists

    def check_row_count(self, df, table_name, min_rows=1):
        """Check if table has minimum rows."""
        row_count = len(df)
        passed = row_count >= min_rows
        status = "PASS" if passed else "FAIL"
        if not passed:
            self.all_passed = False
        self.results.append({
            'table': table_name,
            'check': 'Row Count',
            'status': status,
            'details': f"{row_count} rows (min: {min_rows})"
        })
        logger.info(f"[{status}] {table_name} - Row count: {row_count}")
        return passed

    def check_no_nulls(self, df, table_name, columns):
        """Check if specified columns have no nulls."""
        all_ok = True
        for col in columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                passed = null_count == 0
                status = "PASS" if passed else "FAIL"
                if not passed:
                    self.all_passed = False
                    all_ok = False
                self.results.append({
                    'table': table_name,
                    'check': f'No Nulls: {col}',
                    'status': status,
                    'details': f"{null_count} nulls found"
                })
                logger.info(f"[{status}] {table_name}.{col} - Nulls: {null_count}")
        return all_ok

    def check_unique(self, df, table_name, column):
        """Check if column has unique values."""
        total = len(df)
        unique = df[column].nunique()
        duplicates = total - unique
        passed = duplicates == 0
        status = "PASS" if passed else "FAIL"
        if not passed:
            self.all_passed = False
        self.results.append({
            'table': table_name,
            'check': f'Unique: {column}',
            'status': status,
            'details': f"{duplicates} duplicates found"
        })
        logger.info(f"[{status}] {table_name}.{column} - Duplicates: {duplicates}")
        return passed

    def check_values_in_range(self, df, table_name, column, min_val, max_val):
        """Check if values are within expected range."""
        out_of_range = df[
            (df[column] < min_val) | (df[column] > max_val)
        ].shape[0]
        passed = out_of_range == 0
        status = "PASS" if passed else "WARN"
        self.results.append({
            'table': table_name,
            'check': f'Range: {column}',
            'status': status,
            'details': f"{out_of_range} out of [{min_val}, {max_val}]"
        })
        logger.info(f"[{status}] {table_name}.{column} - Out of range: {out_of_range}")
        return passed

    def check_accepted_values(self, df, table_name, column, accepted):
        """Check if column only has accepted values."""
        actual = set(df[column].dropna().unique())
        invalid = actual - set(accepted)
        passed = len(invalid) == 0
        status = "PASS" if passed else "FAIL"
        if not passed:
            self.all_passed = False
        self.results.append({
            'table': table_name,
            'check': f'Accepted Values: {column}',
            'status': status,
            'details': f"Invalid: {invalid}" if invalid else "All valid"
        })
        logger.info(f"[{status}] {table_name}.{column} - Invalid values: {invalid}")
        return passed

    def print_report(self):
        """Print quality check report."""
        print("\n" + "=" * 70)
        print("              DATA QUALITY REPORT")
        print("=" * 70)
        print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 70)
        print(f"  {'Table':<20} {'Check':<25} {'Status':<8} {'Details'}")
        print("-" * 70)

        pass_count = 0
        fail_count = 0
        warn_count = 0

        for r in self.results:
            status_icon = ""
            if r['status'] == 'PASS':
                status_icon = "PASS"
                pass_count += 1
            elif r['status'] == 'FAIL':
                status_icon = "FAIL"
                fail_count += 1
            else:
                status_icon = "WARN"
                warn_count += 1

            print(f"  {r['table']:<20} {r['check']:<25} {status_icon:<8} {r['details']}")

        print("-" * 70)
        print(f"  Total Checks: {len(self.results)}")
        print(f"  PASSED: {pass_count}  |  FAILED: {fail_count}  |  WARNINGS: {warn_count}")
        overall = "ALL CHECKS PASSED!" if self.all_passed else "SOME CHECKS FAILED!"
        print(f"  Result: {overall}")
        print("=" * 70)

        return self.all_passed


def run_quality_checks():
    """Run all quality checks on generated data."""
    print("\nStarting Data Quality Checks...")
    logger.info("Starting data quality checks...")

    checker = DataQualityChecker()

    # File paths
    customers_file = PathConfig.CUSTOMERS_DIR / "customers_latest.csv"
    products_file = PathConfig.PRODUCTS_DIR / "products_latest.csv"
    orders_file = PathConfig.ORDERS_DIR / "orders_latest.csv"

    # Check files exist
    checker.check_file_exists(customers_file, "Customers")
    checker.check_file_exists(products_file, "Products")
    checker.check_file_exists(orders_file, "Orders")

    # Load data
    df_cust = pd.read_csv(customers_file)
    df_prod = pd.read_csv(products_file)
    df_ord = pd.read_csv(orders_file)

    # --- CUSTOMERS CHECKS ---
    checker.check_row_count(df_cust, "Customers", min_rows=100)
    checker.check_unique(df_cust, "Customers", "customer_id")
    checker.check_unique(df_cust, "Customers", "email")
    checker.check_no_nulls(df_cust, "Customers", ["customer_id", "email", "first_name"])
    checker.check_accepted_values(df_cust, "Customers", "gender", ["Male", "Female", "Other"])

    # --- PRODUCTS CHECKS ---
    checker.check_row_count(df_prod, "Products", min_rows=50)
    checker.check_unique(df_prod, "Products", "product_id")
    checker.check_no_nulls(df_prod, "Products", ["product_id", "product_name", "price"])
    checker.check_values_in_range(df_prod, "Products", "price", 0.01, 10000)
    checker.check_accepted_values(df_prod, "Products", "category",
        ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports"])

    # --- ORDERS CHECKS ---
    checker.check_row_count(df_ord, "Orders", min_rows=1000)
    checker.check_unique(df_ord, "Orders", "order_id")
    checker.check_no_nulls(df_ord, "Orders", ["order_id", "customer_id", "product_id", "order_date"])
    checker.check_values_in_range(df_ord, "Orders", "quantity", 1, 100)
    checker.check_values_in_range(df_ord, "Orders", "final_amount", 0, 100000)
    checker.check_accepted_values(df_ord, "Orders", "order_status",
        ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"])
    checker.check_accepted_values(df_ord, "Orders", "payment_method",
        ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer", "Cash on Delivery"])

    # Print report
    all_passed = checker.print_report()

    logger.info(f"Data quality checks completed. All passed: {all_passed}")

    return all_passed


if __name__ == "__main__":
    run_quality_checks()