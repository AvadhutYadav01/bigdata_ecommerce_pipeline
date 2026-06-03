#!/usr/bin/env python
"""
AWS PySpark ETL Job with SCD Type 1 and Type 2
Author: Antigravity

This Spark job:
  1. Reads raw customers, products, and orders data from S3 (or local directory for testing).
  2. Cleans and validates schemas.
  3. Implements SCD Type 2 (History tracking) for Customers.
  4. Implements SCD Type 1 (Overwrite) for Products.
  5. Enriches Orders with SCD2 Customer Keys and SCD1 Product Keys to build the Orders Fact Table.
  6. Writes the final tables back to S3 in Parquet format.

It is designed to run seamlessly on AWS Glue (using glueContext if available)
or standard PySpark (for local testing/validation).
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_date, to_timestamp, md5, concat_ws, expr

# Check if running in AWS Glue environment
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    GLUE_ENV = True
except ImportError:
    GLUE_ENV = False

def init_spark():
    """Initializes Spark Session or Glue Context based on environment."""
    if GLUE_ENV:
        print("[INFO] Running in AWS Glue environment.")
        from pyspark.context import SparkContext
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        # Parse Glue arguments
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'run_date', 'raw_path', 'curated_path'])
        job.init(args['JOB_NAME'], args)
        return spark, glueContext, job, args
    else:
        print("[INFO] Running in local/standard Spark environment.")
        # Local fallback configuration
        spark = SparkSession.builder \
            .appName("Ecommerce-Spark-ETL") \
            .config("spark.sql.warehouse.dir", "spark-warehouse") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        
        # Mock arguments for local testing
        args = {
            'run_date': '2026-06-02', # Default fallback run date
            'raw_path': 'C:/Users/Avadhut/bigdata_ecommerce_pipeline/data/raw',
            'curated_path': 'C:/Users/Avadhut/bigdata_ecommerce_pipeline/data/curated'
        }
        return spark, None, None, args

def process_customers_scd2(spark, df_new_raw, curated_path, run_date):
    """
    Implements Slowly Changing Dimension Type 2 (SCD2) for Customers.
    
    Logic:
    1. Read existing curated dim_customers (history) from S3/local parquet.
    2. If no existing history, initialize dim_customers with new records:
       - customer_key = md5(customer_id + signup_date)
       - start_date = signup_date
       - end_date = 9999-12-31 00:00:00 (active indicator)
       - is_current = True
    3. If history exists:
       - Identify active records in history (is_current = True).
       - Join active history records with new raw batch on customer_id.
       - Separate into:
         - New customers (no record in history).
         - Unchanged customers (matching customer_id, details are same).
         - Changed customers (matching customer_id, details differ).
       - For Changed Customers:
         - Old history record is closed: set end_date = run_date, is_current = False.
         - New record is inserted: customer_key = md5(customer_id + run_date),
           start_date = run_date, end_date = 9999-12-31, is_current = True.
    """
    history_file_path = os.path.join(curated_path, "dim_customers")
    run_timestamp = f"{run_date} 00:00:00"
    high_date = "9999-12-31 23:59:59"
    
    # Standardize schema of incoming new batch
    df_new = df_new_raw.select(
        col("customer_id").cast("int"),
        col("name"),
        col("email"),
        col("phone"),
        col("address"),
        col("city"),
        col("state"),
        to_date(col("signup_date")).alias("signup_date"),
        to_timestamp(col("updated_at")).alias("updated_at")
    ).dropDuplicates(["customer_id"])

    # Check if curated dim_customers already exists
    history_exists = False
    try:
        df_history = spark.read.parquet(history_file_path)
        if df_history.count() > 0:
            history_exists = True
            print(f"[INFO] Found existing customer history in {history_file_path}")
    except Exception:
        print("[INFO] No existing customer history found. Starting initial load.")
        
    if not history_exists:
        # Initial run: Create SCD2 fields
        df_scd2 = df_new.withColumn(
            "start_date", to_timestamp(col("signup_date"))
        ).withColumn(
            "end_date", to_timestamp(lit(high_date))
        ).withColumn(
            "is_current", lit(True)
        ).withColumn(
            "customer_key", md5(concat_ws("_", col("customer_id"), col("start_date")))
        ).select(
            "customer_key", "customer_id", "name", "email", "phone", 
            "address", "city", "state", "signup_date", "start_date", "end_date", "is_current", "updated_at"
        )
        return df_scd2

    # SCD Type 2 logic using join
    # 1. Separate current active records and historical inactive records
    df_history_active = df_history.filter(col("is_current") == True)
    df_history_inactive = df_history.filter(col("is_current") == False)

    # 2. Join active history with new batch to detect changes
    df_join = df_history_active.join(
        df_new.alias("new"), 
        on="customer_id", 
        how="outer"
    )

    # 3. Handle 3 scenarios:
    # Scenario A: Brand New Customers (exist only in new batch)
    df_new_records = df_join.filter(col("customer_key").isNull()).select(
        md5(concat_ws("_", col("new.customer_id"), to_timestamp(col("new.signup_date")))).alias("customer_key"),
        col("new.customer_id"),
        col("new.name"),
        col("new.email"),
        col("new.phone"),
        col("new.address"),
        col("new.city"),
        col("new.state"),
        col("new.signup_date"),
        to_timestamp(col("new.signup_date")).alias("start_date"),
        to_timestamp(lit(high_date)).alias("end_date"),
        lit(True).alias("is_current"),
        col("new.updated_at")
    )

    # Scenario B: Existing Customers with changes
    # We define change if address, city, or state changed (you can add name, phone, etc. as well)
    change_condition = (
        (col("customer_key").isNotNull()) & (col("new.customer_id").isNotNull()) &
        (
            (col("address") != col("new.address")) |
            (col("city") != col("new.city")) |
            (col("state") != col("new.state"))
        )
    )

    # B.1: Expire old active records (set end_date = run_date, is_current = False)
    df_expired_records = df_join.filter(change_condition).select(
        col("customer_key"),
        col("customer_id"),
        col("name"),
        col("email"),
        col("phone"),
        col("address"),
        col("city"),
        col("state"),
        col("signup_date"),
        col("start_date"),
        to_timestamp(lit(run_timestamp)).alias("end_date"),
        lit(False).alias("is_current"),
        to_timestamp(lit(run_timestamp)).alias("updated_at")
    )

    # B.2: Insert new active records with updated details
    df_updated_active_records = df_join.filter(change_condition).select(
        md5(concat_ws("_", col("new.customer_id"), to_timestamp(lit(run_timestamp)))).alias("customer_key"),
        col("new.customer_id"),
        col("new.name"),
        col("new.email"),
        col("new.phone"),
        col("new.address"),
        col("new.city"),
        col("new.state"),
        col("new.signup_date"),
        to_timestamp(lit(run_timestamp)).alias("start_date"),
        to_timestamp(lit(high_date)).alias("end_date"),
        lit(True).alias("is_current"),
        col("new.updated_at")
    )

    # Scenario C: Existing Customers with NO changes (keep as-is)
    no_change_condition = (
        (col("customer_key").isNotNull()) & 
        (
            col("new.customer_id").isNull() | # Not in the new daily batch (no transaction today)
            (
                (col("address") == col("new.address")) &
                (col("city") == col("new.city")) &
                (col("state") == col("new.state"))
            )
        )
    )
    df_unchanged_records = df_join.filter(no_change_condition).select(
        col("customer_key"),
        col("customer_id"),
        col("name"),
        col("email"),
        col("phone"),
        col("address"),
        col("city"),
        col("state"),
        col("signup_date"),
        col("start_date"),
        col("end_date"),
        col("is_current"),
        col("updated_at")
    )

    # Union all segments together: Inactive history + Unchanged active + Expired active + New active + Updated active
    df_final_scd2 = df_history_inactive \
        .unionByName(df_unchanged_records) \
        .unionByName(df_expired_records) \
        .unionByName(df_new_records) \
        .unionByName(df_updated_active_records)

    return df_final_scd2

def process_products_scd1(spark, df_new_raw, curated_path):
    """
    Implements Slowly Changing Dimension Type 1 (SCD1) for Products.
    Type 1 overwrites values. We maintain a product_key surrogate key, but update
    its attributes (price, stock_quantity, updated_at) to reflect latest.
    
    Logic:
      1. If dim_products does not exist, initialize using md5(product_id).
      2. If it exists, perform outer join on product_id.
         - If product exists in incoming batch, take values from incoming batch (upsert).
         - Otherwise, keep existing values.
    """
    history_file_path = os.path.join(curated_path, "dim_products")
    
    df_new = df_new_raw.select(
        col("product_id").cast("int"),
        col("product_name"),
        col("category"),
        col("price").cast("decimal(10,2)"),
        col("stock_quantity").cast("int"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at")
    ).dropDuplicates(["product_id"])

    history_exists = False
    try:
        df_history = spark.read.parquet(history_file_path)
        if df_history.count() > 0:
            history_exists = True
            print(f"[INFO] Found existing products in {history_file_path}")
    except Exception:
        print("[INFO] No existing product catalog found. Starting initial load.")

    if not history_exists:
        # Initial run
        df_scd1 = df_new.withColumn(
            "product_key", md5(col("product_id").cast("string"))
        ).select(
            "product_key", "product_id", "product_name", "category", "price", "stock_quantity", "created_at", "updated_at"
        )
        return df_scd1

    # SCD Type 1 Upsert (Join and select)
    df_join = df_history.alias("hist").join(
        df_new.alias("new"),
        on="product_id",
        how="outer"
    )

    df_final_scd1 = df_join.select(
        # Surrogate key stays the same (based on natural ID)
        md5(col("product_id").cast("string")).alias("product_key"),
        col("product_id"),
        # Use new product details if updated, otherwise keep history
        when(col("new.product_id").isNotNull(), col("new.product_name")).otherwise(col("hist.product_name")).alias("product_name"),
        when(col("new.product_id").isNotNull(), col("new.category")).otherwise(col("hist.category")).alias("category"),
        when(col("new.product_id").isNotNull(), col("new.price")).otherwise(col("hist.price")).alias("price"),
        when(col("new.product_id").isNotNull(), col("new.stock_quantity")).otherwise(col("hist.stock_quantity")).alias("stock_quantity"),
        when(col("new.product_id").isNotNull(), col("new.created_at")).otherwise(col("hist.created_at")).alias("created_at"),
        when(col("new.product_id").isNotNull(), col("new.updated_at")).otherwise(col("hist.updated_at")).alias("updated_at")
    )

    return df_final_scd1

def process_fact_orders(spark, df_orders_raw, df_customers, df_products):
    """
    Builds the fact_orders table.
    
    For each order:
      - Clean columns.
      - Link to dim_customers_scd2: Find customer_key where customer_id matches and
        orders.order_date falls within the customer's effective range (start_date <= order_date <= end_date).
      - Link to dim_products_scd1: Find product_key matching product_id.
      - Generate order_date_key (YYYYMMDD integer) for date dimension joins.
    """
    df_orders = df_orders_raw.select(
        col("order_id").cast("int"),
        col("customer_id").cast("int"),
        col("product_id").cast("int"),
        col("quantity").cast("int"),
        to_date(col("order_date")).alias("order_date"),
        col("total_amount").cast("decimal(12,2)"),
        col("order_status")
    ).dropDuplicates(["order_id"])

    # 1. Join with dim_customers on SCD Type 2 date ranges
    # Cast order_date to timestamp to compare with start_date and end_date
    df_orders_timestamped = df_orders.withColumn("order_timestamp", to_timestamp(col("order_date")))

    df_join_cust = df_orders_timestamped.alias("o").join(
        df_customers.alias("c"),
        (col("o.customer_id") == col("c.customer_id")) &
        (col("o.order_timestamp") >= col("c.start_date")) &
        (col("o.order_timestamp") <= col("c.end_date")),
        how="inner"
    )

    # 2. Join with dim_products
    df_fact = df_join_cust.join(
        df_products.alias("p"),
        on="product_id",
        how="inner"
    )

    # 3. Create Date Key (YYYYMMDD) and select fact columns
    df_fact_orders = df_fact.select(
        col("o.order_id"),
        col("c.customer_key"),
        col("p.product_key"),
        expr("cast(date_format(o.order_date, 'yyyyMMdd') as int)").alias("order_date_key"),
        col("o.quantity"),
        col("p.price").alias("price_each"),
        col("o.total_amount"),
        col("o.order_status"),
        col("o.order_timestamp").alias("created_at")
    )

    return df_fact_orders

def main():
    spark, glueContext, job, args = init_spark()
    
    run_date = args['run_date']
    raw_path = args['raw_path']
    curated_path = args['curated_path']
    
    print(f"[START] Running ETL for date: {run_date}")
    print(f"  Raw data path: {raw_path}")
    print(f"  Curated data path: {curated_path}")

    # Paths for current daily raw batch
    batch_raw_dir = os.path.join(raw_path, f"ingest_date={run_date}")
    
    cust_raw_path = os.path.join(batch_raw_dir, "customers.csv")
    prod_raw_path = os.path.join(batch_raw_dir, "products.csv")
    ord_raw_path = os.path.join(batch_raw_dir, "orders.csv")

    # Verify input files exist
    # In S3, we might check using boto3, but Spark will throw an error if the directory is missing.
    # For robust local/AWS execution, we read CSVs.
    print("[INFO] Loading raw data files...")
    try:
        df_cust_raw = spark.read.option("header", "true").csv(cust_raw_path)
        df_prod_raw = spark.read.option("header", "true").csv(prod_raw_path)
        df_ord_raw = spark.read.option("header", "true").csv(ord_raw_path)
    except Exception as e:
        print(f"[ERROR] Failed to read raw CSV files: {e}")
        if GLUE_ENV:
            job.commit()
        sys.exit(1)

    # 1. Process Customers SCD Type 2
    print("[ETL] Processing dim_customers (SCD2)...")
    df_customers_updated = process_customers_scd2(spark, df_cust_raw, curated_path, run_date)
    # Save back to curated (Overwrite the entire table as we maintain full history inside it)
    cust_curated_path = os.path.join(curated_path, "dim_customers")
    df_customers_updated.write.mode("overwrite").parquet(cust_curated_path)
    print(f"[ETL] Saved customer dimensions to: {cust_curated_path}")

    # 2. Process Products SCD Type 1
    print("[ETL] Processing dim_products (SCD1)...")
    df_products_updated = process_products_scd1(spark, df_prod_raw, curated_path)
    # Save back to curated (Overwrite catalog, keeping current product lists with Type 1 updates)
    prod_curated_path = os.path.join(curated_path, "dim_products")
    df_products_updated.write.mode("overwrite").parquet(prod_curated_path)
    print(f"[ETL] Saved product dimensions to: {prod_curated_path}")

    # 3. Process Orders Fact Table
    print("[ETL] Processing fact_orders...")
    df_orders_fact = process_fact_orders(spark, df_ord_raw, df_customers_updated, df_products_updated)
    
    # Save back to curated. Partitioning by order_date_key is an industry best practice for fast querying.
    fact_curated_path = os.path.join(curated_path, "fact_orders")
    df_orders_fact.write.mode("append").partitionBy("order_date_key").parquet(fact_curated_path)
    print(f"[ETL] Saved order facts (appended) to: {fact_curated_path}")

    print("[SUCCESS] ETL Spark Job finished successfully!")
    
    if GLUE_ENV:
        job.commit()

if __name__ == "__main__":
    main()
