# AWS Big Data E-commerce Lakehouse Pipeline

An industry-grade, end-to-end Big Data Engineering pipeline simulating e-commerce customer transaction events, processing them via PySpark, executing Slowly Changing Dimensions (SCD Type 1 & 2), and cataloging results for analytical queries in AWS.

---

## 🏗️ Architecture & Flow

```
                                  [ Orchestration: Apache Airflow (MWAA) ]
                                                     |
                                                     v
[ Numpy.Random Source Generator ] ---> [ AWS S3 Raw (Bronze) ] ---> [ AWS Glue PySpark (ETL & SCD) ]
                                                                                   |
                                                                                   v
[ BI Analytics: Redshift/Athena (Gold) ] <--- [ AWS Glue Catalog ] <--- [ AWS S3 Curated Parquet (Silver) ]
```

1. **Ingestion (`scripts/generate_data.py`)**: A Python engine generates mock customer profiles, products catalog, and historical orders. It simulates daily runs with real-world changes:
   - Some customers move houses (generating changes in Address, City, State fields).
   - Some product prices/stock counts fluctuate.
   - New orders are created linking to updated values.
2. **Ingestion Upload (`scripts/upload_to_s3.py`)**: Connects to AWS S3 using Boto3 to partition raw files (`raw/customers/ingest_date=YYYY-MM-DD/`, etc.). Fallback dry-run allows execution without credentials.
3. **Core Processing (`src/jobs/spark_etl.py`)**: A distributed Spark engine executes transformations:
   - **SCD Type 2 (Customers)**: Keeps track of historical records. Expired profile rows get `is_current = False` and `end_date = run_date`. The updated profile gets `is_current = True`, `start_date = run_date`, and a unique MD5 surrogate key.
   - **SCD Type 1 (Products)**: Overwrites details. The catalog remains updated with the latest price/stock without history.
   - **Fact Table (`fact_orders`)**: Resolves surrogate keys by joining order timestamps against the specific customer details active *at that timestamp* (essential for billing/tax accuracy).
4. **Data Lakehouse / DWH (`config/redshift_ddl.sql`)**: Exposes the Parquet datasets in AWS S3 Curated folder as dimensional tables via Glue Data Catalog, allowing queries using Athena or Amazon Redshift.
5. **Orchestration (`dags/ecommerce_etl_dag.py`)**: Coordinates execution dependencies daily.
6. **CI/CD (`.github/workflows/ci-cd.yml`)**: Continuous Integration running Python tests, PySpark checks, and syncing scripts to AWS.

---

## 📊 Slowly Changing Dimensions (SCD) Concept

This project implements two SCD types:

### SCD Type 1 (Overwrites) - Products Catalog
If a product's price changes, we update the existing row directly:
* **Before Price Update:**
  | product_key (MD5) | product_id | product_name | price |
  |---|---|---|---|
  | `a1b2...` | 5001 | Smartphone model-10 | **499.99** |

* **After Price Update (Type 1):**
  | product_key (MD5) | product_id | product_name | price |
  |---|---|---|---|
  | `a1b2...` | 5001 | Smartphone model-10 | **450.00** |

---

### SCD Type 2 (Historical Tracking) - Customer Profile
If a customer moves, the historical address is preserved for past orders:
* **Before Address Update:**
  | customer_key (MD5) | customer_id | name | address | start_date | end_date | is_current |
  |---|---|---|---|---|---|---|
  | `c1001_initial` | 1001 | Alice Smith | 123 Main St, NY | 2026-01-01 | 9999-12-31 | **True** |

* **After Address Update (Type 2):**
  | customer_key (MD5) | customer_id | name | address | start_date | end_date | is_current |
  |---|---|---|---|---|---|---|
  | `c1001_initial` | 1001 | Alice Smith | 123 Main St, NY | 2026-01-01 | **2026-06-02** | **False** |
  | `c1001_updated` | 1001 | Alice Smith | 789 Pine Rd, CA | **2026-06-02** | 9999-12-31 | **True** |

When an order is created on `2026-02-15`, the Spark job joins against the customer table and picks `c1001_initial` (NY). If a new order is created on `2026-06-10`, it joins and picks `c1001_updated` (CA).

---

## 🚀 How to Run and Test Locally

Although you don't have an active AWS account, we have set up the project so you can simulate and test everything locally:

### 1. Generate Mock Data
Initialize python virtual environment and run the generator:
```powershell
# Create environment
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# 1. Generate Baseline Historical Data (Batch 1: ingest_date=2026-06-01)
python scripts/generate_data.py --batch initial

# 2. Generate Next-Day Updates and New Orders (Batch 2: ingest_date=2026-06-02)
python scripts/generate_data.py --batch incremental
```
This generates the CSV files locally inside `data/raw/ingest_date=2026-06-01` and `data/raw/ingest_date=2026-06-02`. You can open these folders and inspect them.

### 2. Verify Ingestion S3 Upload (Dry-Run)
Verify Boto3 script mapping to S3 keys:
```powershell
python scripts/upload_to_s3.py --date 2026-06-01 --dry-run
python scripts/upload_to_s3.py --date 2026-06-02 --dry-run
```

### 3. Run Unit Tests (PySpark)
Verify SCD transformations with Spark calculations using PyTest:
```powershell
pytest -v
```

---

## ☁️ Deploying to AWS Production

### AWS S3 Setup
Create an S3 bucket named `ecommerce-lakehouse-bucket` (or update bucket name in `config/config.yaml`). Make sure it has three core prefixes:
- `raw/`
- `curated/`
- `scripts/`

### AWS Glue Job Config
1. Create a **Glue Spark Job** in AWS Console.
2. Select **Spark 3.3 / Python 3** (or latest).
3. Upload `src/jobs/spark_etl.py` to `s3://ecommerce-lakehouse-bucket/scripts/spark_etl.py` and point Glue to this file.
4. Set Job Parameters:
   - `--run_date`
   - `--raw_path` = `s3://ecommerce-lakehouse-bucket/raw`
   - `--curated_path` = `s3://ecommerce-lakehouse-bucket/curated`

### AWS MWAA Orchestration
Copy `dags/ecommerce_etl_dag.py` to your Managed Airflow S3 bucket's `dags/` folder. Add connection details for `aws_default` if needed.

### Amazon Athena/Redshift Cataloging
Run `config/redshift_ddl.sql` in Athena or Redshift Query Editor to define your analytical star schema on top of the S3 Curated folder.
