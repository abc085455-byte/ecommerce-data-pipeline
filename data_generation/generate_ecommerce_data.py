"""
==========================================
E-Commerce Fake Data Generator
Real-Time E-Commerce Data Pipeline
==========================================
"""

import os
import sys
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker
import pandas as pd
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))
from config.config import PathConfig, PipelineConfig

# Logger setup
log_file = PathConfig.LOG_DIR / "data_generation.log"
logger.add(
    str(log_file),
    rotation="10 MB",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

fake = Faker('en_US')
Faker.seed(42)
random.seed(42)


def generate_customers(num_records=500):
    """Fake customer data generate karo."""
    print(f"   Generating {num_records} customers...")
    logger.info(f"Generating {num_records} customers...")

    customers = []
    genders = ['Male', 'Female', 'Other']

    for i in range(num_records):
        customer = {
            'customer_id': f"CUST_{str(i+1).zfill(5)}",
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'country': 'US',
            'date_of_birth': fake.date_of_birth(
                minimum_age=18,
                maximum_age=75
            ).strftime('%Y-%m-%d'),
            'gender': random.choice(genders),
            'registration_date': fake.date_between(
                start_date='-3y',
                end_date='today'
            ).strftime('%Y-%m-%d'),
            'is_active': random.choices(
                [True, False],
                weights=[85, 15]
            )[0]
        }
        customers.append(customer)

    df = pd.DataFrame(customers)
    print(f"   Done! {len(df)} customers generated.")
    return df


def generate_products(num_records=100):
    """Fake product data generate karo."""
    print(f"   Generating {num_records} products...")
    logger.info(f"Generating {num_records} products...")

    categories = {
        'Electronics': {
            'sub_categories': ['Smartphones', 'Laptops', 'Tablets', 'Headphones', 'Cameras'],
            'brands': ['Samsung', 'Apple', 'Sony', 'LG', 'Dell', 'HP', 'Lenovo'],
            'price_range': (49.99, 1999.99)
        },
        'Clothing': {
            'sub_categories': ['T-Shirts', 'Jeans', 'Jackets', 'Dresses', 'Shoes'],
            'brands': ['Nike', 'Adidas', 'Zara', 'H&M', 'Levis', 'Puma'],
            'price_range': (9.99, 299.99)
        },
        'Home & Kitchen': {
            'sub_categories': ['Cookware', 'Furniture', 'Decor', 'Appliances', 'Bedding'],
            'brands': ['IKEA', 'Whirlpool', 'KitchenAid', 'Dyson', 'Philips'],
            'price_range': (14.99, 799.99)
        },
        'Books': {
            'sub_categories': ['Fiction', 'Non-Fiction', 'Science', 'History', 'Self-Help'],
            'brands': ['Penguin', 'HarperCollins', 'Random House', 'Oxford', 'Wiley'],
            'price_range': (4.99, 49.99)
        },
        'Sports': {
            'sub_categories': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Cycling'],
            'brands': ['Nike', 'Adidas', 'Under Armour', 'Puma', 'Reebok', 'Decathlon'],
            'price_range': (9.99, 499.99)
        }
    }

    products = []

    for i in range(num_records):
        category = random.choice(list(categories.keys()))
        cat_info = categories[category]
        price = round(random.uniform(*cat_info['price_range']), 2)
        cost_price = round(price * random.uniform(0.4, 0.7), 2)

        product = {
            'product_id': f"PROD_{str(i+1).zfill(4)}",
            'product_name': f"{random.choice(cat_info['brands'])} {fake.word().title()} {random.choice(cat_info['sub_categories'])}",
            'category': category,
            'sub_category': random.choice(cat_info['sub_categories']),
            'brand': random.choice(cat_info['brands']),
            'price': price,
            'cost_price': cost_price,
            'weight_kg': round(random.uniform(0.1, 25.0), 2),
            'is_available': random.choices(
                [True, False],
                weights=[90, 10]
            )[0],
            'created_date': fake.date_between(
                start_date='-2y',
                end_date='today'
            ).strftime('%Y-%m-%d')
        }
        products.append(product)

    df = pd.DataFrame(products)
    print(f"   Done! {len(df)} products generated.")
    return df


def generate_orders(num_records=5000, customers_df=None, products_df=None):
    """Fake order data generate karo."""
    print(f"   Generating {num_records} orders...")
    logger.info(f"Generating {num_records} orders...")

    customer_ids = customers_df['customer_id'].tolist()
    product_data = products_df[['product_id', 'price']].to_dict('records')

    payment_methods = [
        'Credit Card', 'Debit Card', 'PayPal',
        'Apple Pay', 'Google Pay', 'Bank Transfer',
        'Cash on Delivery'
    ]

    order_statuses = ['Delivered', 'Shipped', 'Processing', 'Cancelled', 'Returned']
    status_weights = [50, 20, 15, 10, 5]

    # Date strings ko date objects mein convert karo
    start_dt = datetime.strptime(PipelineConfig.ORDER_START_DATE, '%Y-%m-%d').date()
    end_dt = datetime.strptime(PipelineConfig.ORDER_END_DATE, '%Y-%m-%d').date()

    orders = []

    for i in range(num_records):
        customer_id = random.choice(customer_ids)
        product = random.choice(product_data)

        # Ab date objects use ho rahe hain - error nahi aayega
        order_date = fake.date_between(
            start_date=start_dt,
            end_date=end_dt
        )

        quantity = random.choices(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            weights=[40, 25, 15, 8, 5, 3, 1, 1, 1, 1]
        )[0]

        unit_price = product['price']
        total_amount = round(unit_price * quantity, 2)

        discount_percent = random.choices(
            [0, 5, 10, 15, 20, 25],
            weights=[40, 20, 15, 10, 10, 5]
        )[0]

        discount_amount = round(total_amount * discount_percent / 100, 2)
        final_amount = round(total_amount - discount_amount, 2)

        status = random.choices(
            order_statuses,
            weights=status_weights
        )[0]

        delivery_date = None
        if status == 'Delivered':
            delivery_date = (
                order_date + timedelta(days=random.randint(3, 15))
            ).strftime('%Y-%m-%d')

        order = {
            'order_id': f"ORD_{str(i+1).zfill(6)}",
            'customer_id': customer_id,
            'product_id': product['product_id'],
            'order_date': order_date.strftime('%Y-%m-%d'),
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'discount_percent': discount_percent,
            'discount_amount': discount_amount,
            'final_amount': final_amount,
            'payment_method': random.choice(payment_methods),
            'order_status': status,
            'shipping_address': fake.street_address(),
            'shipping_city': fake.city(),
            'shipping_state': fake.state_abbr(),
            'delivery_date': delivery_date
        }
        orders.append(order)

    df = pd.DataFrame(orders)
    print(f"   Done! {len(df)} orders generated.")
    return df


def save_to_csv(df, folder_path, filename):
    """DataFrame ko CSV file mein save karo."""
    Path(folder_path).mkdir(parents=True, exist_ok=True)
    file_path = Path(folder_path) / filename
    df.to_csv(file_path, index=False)
    file_size = os.path.getsize(file_path)
    file_size_kb = round(file_size / 1024, 2)
    print(f"   Saved: {filename} ({len(df)} rows, {file_size_kb} KB)")
    logger.info(f"Saved: {file_path} ({len(df)} rows, {file_size_kb} KB)")
    return str(file_path)


def run_data_generation():
    """Main function - poora data generation process."""

    print("")
    print("=" * 60)
    print("    STARTING E-COMMERCE DATA GENERATION")
    print("=" * 60)

    start_time = datetime.now()
    logger.info("Data generation process started")

    # Step 1
    print("\n[1/5] Generating Customers...")
    customers_df = generate_customers(PipelineConfig.NUM_CUSTOMERS)

    # Step 2
    print("\n[2/5] Generating Products...")
    products_df = generate_products(PipelineConfig.NUM_PRODUCTS)

    # Step 3
    print("\n[3/5] Generating Orders...")
    orders_df = generate_orders(
        PipelineConfig.NUM_ORDERS,
        customers_df,
        products_df
    )

    # Step 4
    print("\n[4/5] Saving to CSV files...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    save_to_csv(customers_df, PathConfig.CUSTOMERS_DIR, f"customers_{timestamp}.csv")
    save_to_csv(products_df, PathConfig.PRODUCTS_DIR, f"products_{timestamp}.csv")
    save_to_csv(orders_df, PathConfig.ORDERS_DIR, f"orders_{timestamp}.csv")

    save_to_csv(customers_df, PathConfig.CUSTOMERS_DIR, "customers_latest.csv")
    save_to_csv(products_df, PathConfig.PRODUCTS_DIR, "products_latest.csv")
    save_to_csv(orders_df, PathConfig.ORDERS_DIR, "orders_latest.csv")

    # Step 5
    print("\n[5/5] DATA SUMMARY:")
    print("-" * 40)
    print(f"   Customers : {len(customers_df)} rows")
    print(f"   Products  : {len(products_df)} rows")
    print(f"   Orders    : {len(orders_df)} rows")
    print(f"   Revenue   : ${orders_df['final_amount'].sum():,.2f}")
    print(f"   Avg Order : ${orders_df['final_amount'].mean():,.2f}")
    print("-" * 40)
    print("   Order Status Breakdown:")
    for status, count in orders_df['order_status'].value_counts().items():
        print(f"     {status}: {count}")
    print("-" * 40)
    print("   Payment Methods:")
    for method, count in orders_df['payment_method'].value_counts().items():
        print(f"     {method}: {count}")
    print("-" * 40)
    print("   Product Categories:")
    for cat, count in products_df['category'].value_counts().items():
        print(f"     {cat}: {count}")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("")
    print(f"   Time Taken : {duration:.2f} seconds")
    print(f"   Files at   : {PathConfig.RAW_DATA_DIR}")
    print("")
    print("=" * 60)
    print("    DATA GENERATION COMPLETE!")
    print("=" * 60)

    logger.info(f"Data generation completed in {duration:.2f} seconds")

    return customers_df, products_df, orders_df


# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    print("Starting script...")
    customers, products, orders = run_data_generation()
    print("Script finished!")