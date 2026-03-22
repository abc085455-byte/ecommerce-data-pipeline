
# 🚀 Real-Time E-Commerce Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.x-017CEE?style=for-the-badge&logo=apache-airflow)
![AWS S3](https://img.shields.io/badge/AWS%20S3-orange?style=for-the-badge&logo=amazon-s3)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks)

> An end-to-end production-grade data engineering pipeline for e-commerce analytics, 
> built with modern data stack technologies.

---

## 📌 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Pipeline Flow](#pipeline-flow)
- [Results](#results)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Data Quality](#data-quality)
- [dbt Models](#dbt-models)
- [Author](#author)

---

## 📖 Overview

This project demonstrates a complete end-to-end data engineering pipeline for an 
e-commerce platform. It covers every stage of the data lifecycle:

- **Data Generation** - Realistic fake e-commerce data using Python Faker
- **Data Lake** - Raw data storage on AWS S3
- **Data Processing** - PySpark transformations on Databricks (Bronze/Silver/Gold)
- **Data Warehouse** - Snowflake as the central analytics warehouse
- **Data Transformation** - dbt models for staging, intermediate, and mart layers
- **Orchestration** - Apache Airflow DAG for end-to-end scheduling
- **Data Quality** - 20+ automated quality checks

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  E-COMMERCE DATA PIPELINE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  [Python/Faker]  ──►  [Local CSV]  ──►  [AWS S3 Bucket]        │
│                                               │                  │
│                                    ┌──────────┼──────────┐      │
│                                    │          │          │      │
│                               raw_data/   bronze/    silver/    │
│                                    │       gold/               │
│                                    │          │                  │
│                                    ▼          │                  │
│                          [Databricks PySpark] │                  │
│                          Bronze → Silver → Gold                  │
│                                    │                             │
│                                    ▼                             │
│                          [Snowflake DWH]                         │
│                          RAW → STAGING → ANALYTICS               │
│                                    │                             │
│                                    ▼                             │
│                          [dbt Transformations]                   │
│                          stg → int → mart models                 │
│                                    │                             │
│                                    ▼                             │
│                          [Analytics Ready!]                      │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │            Apache Airflow (Orchestrates Everything)       │   │
│  │  schedule='0 6 * * *'  (Daily at 6 AM UTC)               │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| **Python** | 3.10+ | Scripting, data generation, automation |
| **Apache Airflow** | 3.x | Workflow orchestration & scheduling |
| **AWS S3** | - | Data lake (raw, bronze, silver, gold layers) |
| **Databricks** | - | PySpark data processing (Medallion Architecture) |
| **Snowflake** | - | Cloud data warehouse |
| **dbt Core** | 1.7+ | Data transformation & modeling |
| **Faker** | 22.5+ | Realistic fake data generation |
| **Pandas** | 2.1+ | Data manipulation |
| **boto3** | 1.34+ | AWS SDK for Python |

---

## 📁 Project Structure

```
ecommerce-data-pipeline/
│
├── 📂 dags/
│   └── ecommerce_pipeline_dag.py      # Airflow DAG - full pipeline
│
├── 📂 config/
│   ├── __init__.py
│   └── config.py                      # Central config (AWS, Snowflake, Databricks)
│
├── 📂 data_generation/
│   ├── __init__.py
│   └── generate_ecommerce_data.py     # Faker data generator
│
├── 📂 aws_utils/
│   ├── __init__.py
│   └── s3_utils.py                    # S3 upload/download utilities
│
├── 📂 databricks_notebooks/
│   ├── 01_bronze_layer.py             # Raw data ingestion to Delta
│   ├── 02_silver_layer.py             # Data cleaning & standardization
│   └── 03_gold_layer.py               # Business aggregations
│
├── 📂 snowflake_setup/
│   ├── 01_create_database.sql         # Database & warehouse setup
│   ├── 02_create_schemas.sql          # RAW, STAGING, ANALYTICS schemas
│   ├── 03_create_tables.sql           # Table definitions
│   ├── 04_create_roles.sql            # Access control
│   └── 05_create_s3_integration.sql   # S3 external stage
│
├── 📂 dbt_ecommerce/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   └── 📂 models/
│       ├── 📂 staging/
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_customers.sql
│       │   ├── stg_products.sql
│       │   └── stg_orders.sql
│       ├── 📂 intermediate/
│       │   ├── schema.yml
│       │   └── int_order_details.sql
│       └── 📂 marts/
│           ├── schema.yml
│           ├── fct_daily_sales.sql
│           ├── fct_monthly_revenue.sql
│           ├── dim_customers.sql
│           └── dim_products.sql
│
├── 📂 data_quality/
│   ├── __init__.py
│   └── quality_checks.py              # 20+ automated checks
│
├── 📂 raw_data/                        # Generated CSV files (gitignored)
│   ├── customers/
│   ├── products/
│   └── orders/
│
├── 📂 logs/                            # Application logs (gitignored)
├── 📂 docs/                            # Documentation
│
├── .env.example                        # Environment variables template
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 📊 Data Model

### Source Data (Generated)

| Entity | Records | Key Fields |
|--------|---------|-----------|
| **Customers** | 500 | customer_id, name, email, city, state, gender |
| **Products** | 100 | product_id, name, category, brand, price |
| **Orders** | 5,000 | order_id, customer_id, product_id, amount, status |

### Snowflake Schemas

```
ECOMMERCE_DB
├── RAW          # Raw data as-is from source
├── STAGING      # Cleaned & standardized (dbt views)
└── ANALYTICS    # Business-ready tables (dbt tables)
```

### dbt Models

#### Staging Layer (Views)
| Model | Description |
|-------|-------------|
| `stg_customers` | Cleaned customers with age calculation |
| `stg_products` | Products with profit margin & price tier |
| `stg_orders` | Orders with delivery days & status flags |

#### Intermediate Layer (Views)
| Model | Description |
|-------|-------------|
| `int_order_details` | Orders enriched with customer & product info |

#### Marts Layer (Tables)
| Model | Description | Rows |
|-------|-------------|------|
| `fct_daily_sales` | Daily revenue, orders, profit | 365 |
| `fct_monthly_revenue` | Monthly trends with growth % | 12 |
| `dim_customers` | Customer dimensions + lifetime metrics | 500 |
| `dim_products` | Product dimensions + sales performance | 100 |

### Customer Segments

| Segment | Criteria |
|---------|----------|
| 🥇 Premium | Lifetime spend ≥ $5,000 |
| 🥈 Gold | Lifetime spend ≥ $2,000 |
| 🥉 Silver | Lifetime spend ≥ $500 |
| 🔷 Bronze | Lifetime spend > $0 |
| ⚫ Inactive | No orders placed |

---

## 🔄 Pipeline Flow (Airflow DAG)

```
ecommerce_data_pipeline (Schedule: Daily 6 AM UTC)
│
├── 📦 data_ingestion/
│   ├── generate_data      → Faker generates 5,600 records
│   ├── quality_checks     → 20 automated validations
│   └── upload_s3          → 6 CSV files to AWS S3
│
├── ❄️  data_loading/
│   └── load_snowflake     → Bulk load to RAW schema
│
├── ⚡ databricks_processing/
│   ├── bronze_layer       → Raw CSV → Delta format on S3
│   ├── silver_layer       → Clean, deduplicate, validate
│   └── gold_layer         → Business aggregations
│
├── 🔧 dbt_transformations/
│   ├── dbt_run            → 8 models (staging/intermediate/marts)
│   ├── dbt_test           → 42 data tests
│   └── dbt_docs           → Auto-generate documentation
│
└── 📋 pipeline_summary    → Execution report & metrics
```

---

## 📈 Results

| Metric | Value |
|--------|-------|
| Total Customers | 500 |
| Total Products | 100 |
| Total Orders | 5,000 |
| Total Revenue | $4,404,253.45 |
| Avg Order Value | $880.85 |
| Delivered Orders | ~49% (2,471) |
| Returned Orders | ~4.5% (226) |
| Data Quality Checks | 20 (All Passed ✅) |
| dbt Models | 8 (All Passed ✅) |
| dbt Tests | 42 (All Passed ✅) |
| Snowflake Load Time | 17.26 seconds |
| Pipeline Runtime | ~2 minutes |

---

## ✅ Prerequisites

Before running this project, you need:

- **Python** 3.10+
- **Git**
- **Docker Desktop** (for Airflow)
- **AWS Account** (Free Tier works)
- **Snowflake Account** (30-day free trial)
- **Databricks Account** (Community Edition - Free)

---

## ⚙️ Setup Instructions

### Step 1: Clone the Repository

```bash
git clone https://github.com/abc085455-byte/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Configure Environment Variables

```bash
# Copy example file
cp .env.example .env

# Edit .env with your credentials
notepad .env
```

Fill in your credentials:
```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
```

### Step 4: Setup Snowflake

Run SQL scripts in order in Snowflake worksheet:
```
snowflake_setup/01_create_database.sql
snowflake_setup/02_create_schemas.sql
snowflake_setup/03_create_tables.sql
snowflake_setup/04_create_roles.sql
```

### Step 5: Setup dbt

```bash
cd dbt_ecommerce

# Update profiles.yml with your Snowflake credentials
notepad profiles.yml

# Test connection
dbt debug

# Install packages
dbt deps
```

### Step 6: Setup Airflow (Docker)

```bash
# Copy DAG to Airflow dags folder
cp dags/ecommerce_pipeline_dag.py /path/to/airflow/dags/

# Or for Docker:
docker cp dags/ecommerce_pipeline_dag.py airflow-scheduler:/opt/airflow/dags/
```

---

## 🚀 Running the Pipeline

### Option 1: Run Individual Components

```bash
# 1. Generate fake data
python data_generation/generate_ecommerce_data.py

# 2. Run quality checks
python data_quality/quality_checks.py

# 3. Load to Snowflake (bulk)
python docs/test_snowflake_load.py

# 4. Upload to S3
python docs/test_s3_upload.py

# 5. Run dbt transformations
cd dbt_ecommerce
dbt run
dbt test
dbt docs generate
```

### Option 2: Run via Airflow (Recommended)

```bash
# Start Airflow
cd /path/to/airflow
docker-compose up -d

# Open UI
# http://localhost:8080 (admin/admin)

# Enable and trigger DAG: ecommerce_data_pipeline
```

---

## 🔍 Data Quality

20+ automated checks covering:

| Check Type | Tables | Description |
|-----------|--------|-------------|
| **File Exists** | All | CSV files present |
| **Row Count** | All | Minimum records check |
| **Unique** | All | No duplicate IDs or emails |
| **Not Null** | All | Required fields filled |
| **Value Range** | Orders/Products | Price, quantity in valid range |
| **Accepted Values** | Orders | Valid statuses & payment methods |

```bash
# Run quality checks
python data_quality/quality_checks.py

# Expected output:
# Total Checks: 20
# PASSED: 20 | FAILED: 0 | WARNINGS: 0
# Result: ALL CHECKS PASSED!
```

---

## 🔧 dbt Models

### Run dbt

```bash
cd dbt_ecommerce

# Run all models
dbt run

# Test all models  
dbt test

# Generate docs
dbt docs generate
dbt docs serve
# Open: http://localhost:8080
```

### dbt Test Coverage

| Test Type | Count | Description |
|-----------|-------|-------------|
| `unique` | 7 | Primary key uniqueness |
| `not_null` | 12 | Required field validation |
| `relationships` | 2 | FK integrity checks |
| `accepted_values` | 21 | Domain value validation |
| **Total** | **42** | **All passing** ✅ |

---

## 🌟 Key Features

- ✅ **Medallion Architecture** - Bronze/Silver/Gold data layers
- ✅ **Bulk Loading** - Fast Snowflake loading (5,600 rows in 17 seconds)
- ✅ **Data Quality** - 20+ automated checks with detailed reporting
- ✅ **dbt Testing** - 42 tests for data integrity
- ✅ **Structured Logging** - File + console logging with loguru
- ✅ **Error Handling** - Retry logic in Airflow tasks
- ✅ **Modular Code** - Clean separation of concerns
- ✅ **Environment Config** - No hardcoded credentials
- ✅ **Documentation** - Auto-generated dbt docs
- ✅ **Task Groups** - Organized Airflow DAG with task groups

---

## 🔮 Future Enhancements

- [ ] Real-time streaming with Apache Kafka
- [ ] BI Dashboard with Apache Superset or Metabase
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Incremental dbt models (instead of full refresh)
- [ ] Data lineage tracking with OpenLineage
- [ ] Cost optimization (Snowflake warehouse auto-suspend)
- [ ] Containerize project with Docker

---

## 👨‍💻 Author

**Muhammad Shawail**

[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat&logo=github)](https://github.com/abc085455-byte)

---

## 📄 License

This project is open source and available for educational and portfolio purposes.

