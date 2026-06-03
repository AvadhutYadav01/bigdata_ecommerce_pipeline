#!/usr/bin/env python
"""
E-commerce Big Data Pipeline Orchestration DAG
Author: Antigravity

This Apache Airflow DAG defines the workflow for our e-commerce big data pipeline:
  1. Generate raw transactional data (simulated hourly/daily extraction).
  2. Upload raw files to AWS S3.
  3. Trigger the AWS Glue PySpark job to clean data, run SCD calculations, and write to Parquet.
  4. Run Athena/Redshift DDL or aggregations on the curated dataset.

To deploy this in AWS MWAA (Managed Workflows for Apache Airflow), place this file
in the dags/ folder of your MWAA S3 bucket.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# -------------------------------------------------------------
# Configuration Constants
# -------------------------------------------------------------
DAG_ID = "ecommerce_bigdata_etl_pipeline"
AWS_CONN_ID = "aws_default" # Airflow connection name for AWS
GLUE_JOB_NAME = "ecommerce-pyspark-etl-job"
S3_BUCKET = "ecommerce-lakehouse-bucket"
ATHENA_DATABASE = "ecommerce_dwh"
ATHENA_WORKGROUP = "primary"

# Paths to python scripts (adjusted to run in execution environments)
# In production, these scripts might be stored in the DAGs folder or S3
PROJECT_ROOT = "/usr/local/airflow" # Standard path in MWAA
GENERATE_SCRIPT = f"{PROJECT_ROOT}/scripts/generate_data.py"
UPLOAD_SCRIPT = f"{PROJECT_ROOT}/scripts/upload_to_s3.py"

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Orchestrates ingestion, S3 upload, Glue Spark ETL, and Athena DWH transformations.",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ecommerce", "aws", "pyspark", "glue", "scd"],
) as dag:

    # 1. Generate Raw Transactional Data
    # Runs the python generation script for the execution date.
    # We pass '{{ ds }}' (Airflow logical date, YYYY-MM-DD) to specify the batch date.
    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        bash_command=f"python {GENERATE_SCRIPT} --batch incremental",
        env={"PYTHONPATH": PROJECT_ROOT},
    )

    # 2. Upload Raw Data to AWS S3 Raw Bucket
    # Uploads customer, product, and order files to S3.
    upload_raw_to_s3 = BashOperator(
        task_id="upload_raw_to_s3",
        bash_command=f"python {UPLOAD_SCRIPT} --date '{{ ds }}'",
        env={"PYTHONPATH": PROJECT_ROOT},
    )

    # 3. Trigger AWS Glue PySpark ETL Job
    # GlueJobOperator triggers the PySpark ETL script running on AWS managed Spark cluster.
    # It passes parameters (like run_date, raw_path, curated_path) dynamically.
    trigger_glue_etl = GlueJobOperator(
        task_id="trigger_glue_etl",
        job_name=GLUE_JOB_NAME,
        aws_conn_id=AWS_CONN_ID,
        # Script arguments are passed to sys.argv in the Glue script
        script_args={
            "--run_date": "{{ ds }}",
            "--raw_path": f"s3://{S3_BUCKET}/raw",
            "--curated_path": f"s3://{S3_BUCKET}/curated",
        },
        num_of_dpus=2, # Glue DPUs control compute power (WorkerType=G.1X, NumberOfWorkers=2)
        region_name="us-east-1",
    )

    # 4. Refresh Athena Catalog & Tables
    # In AWS, external Athena tables map to S3 folders. 
    # Since we partition fact_orders by 'order_date_key', we must run MSDCK REPAIR TABLE
    # or create a partition update to discover the new partition.
    refresh_fact_partitions = AthenaOperator(
        task_id="refresh_fact_partitions",
        aws_conn_id=AWS_CONN_ID,
        query=f"MSCK REPAIR TABLE {ATHENA_DATABASE}.fact_orders;",
        database=ATHENA_DATABASE,
        output_location=f"s3://{S3_BUCKET}/query-results/athena/",
        workgroup=ATHENA_WORKGROUP,
    )

    # 5. Build Aggregated KPI Summary (Gold Layer/BI View)
    # Aggregates orders to build a daily sales summary for business dashboards.
    # Demonstrates writing SQL on top of SCD Type 2 tables.
    calculate_daily_sales_kpis = AthenaOperator(
        task_id="calculate_daily_sales_kpis",
        aws_conn_id=AWS_CONN_ID,
        query=f"""
            CREATE TABLE IF NOT EXISTS {ATHENA_DATABASE}.summary_daily_sales AS
            SELECT 
                d.year,
                d.month,
                o.order_date_key,
                c.state as customer_state,
                p.category as product_category,
                SUM(o.quantity) as total_items_sold,
                SUM(o.total_amount) as total_sales_usd,
                COUNT(DISTINCT o.order_id) as total_order_count
            FROM {ATHENA_DATABASE}.fact_orders o
            JOIN {ATHENA_DATABASE}.dim_customers_scd2 c ON o.customer_key = c.customer_key
            JOIN {ATHENA_DATABASE}.dim_products_scd1 p ON o.product_key = p.product_key
            JOIN {ATHENA_DATABASE}.dim_date d ON o.order_date_key = d.date_key
            GROUP BY d.year, d.month, o.order_date_key, c.state, p.category;
        """,
        database=ATHENA_DATABASE,
        output_location=f"s3://{S3_BUCKET}/query-results/athena/",
        workgroup=ATHENA_WORKGROUP,
    )

    # -------------------------------------------------------------
    # Define Dependencies / Flow
    # -------------------------------------------------------------
    generate_raw_data >> upload_raw_to_s3 >> trigger_glue_etl >> refresh_fact_partitions >> calculate_daily_sales_kpis
