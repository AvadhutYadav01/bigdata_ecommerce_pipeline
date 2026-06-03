#!/usr/bin/env python
"""
PySpark ETL Unit Tests
Author: Antigravity

This test suite uses pytest and a local SparkSession to validate the core transformation
and slowly changing dimension logic in our Spark job.
"""

import os
import shutil
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Import ETL functions
from src.jobs.spark_etl import process_customers_scd2, process_products_scd1, process_fact_orders

@pytest.fixture(scope="module")
def spark_session():
    """Provides a local Spark Session for testing."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("SparkUnitTests") \
        .config("spark.sql.warehouse.dir", "spark-warehouse-test") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    yield spark
    spark.stop()
    
    # Cleanup spark-warehouse folder if exists
    if os.path.exists("spark-warehouse-test"):
        shutil.rmtree("spark-warehouse-test")

@pytest.fixture
def temp_curated_dir(tmpdir):
    """Provides a temporary folder for curated Parquet outputs."""
    yield str(tmpdir)

def test_customers_scd2_initial_load(spark_session, temp_curated_dir):
    """Tests the initial load of customers (no prior history exists)."""
    # 1. Arrange: Define schema and initial mock raw batch
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])
    
    data = [
        ("1001", "Alice Smith", "alice@test.com", "555-0101", "123 Main St", "New York", "NY", "2026-01-01", "2026-01-01 10:00:00"),
        ("1002", "Bob Jones", "bob@test.com", "555-0102", "456 Oak Ave", "Chicago", "IL", "2026-01-02", "2026-01-02 11:30:00")
    ]
    
    df_raw = spark_session.createDataFrame(data, schema)
    
    # 2. Act: Run process_customers_scd2 on empty curated path
    df_result = process_customers_scd2(spark_session, df_raw, temp_curated_dir, run_date="2026-06-01")
    
    # 3. Assert: Verify initial output fields
    assert df_result.count() == 2
    
    # Verify is_current = True and high date end_date
    rows = df_result.collect()
    for row in rows:
        assert row["is_current"] == True
        assert row["end_date"].strftime("%Y-%m-%d") == "9999-12-31"
        assert row["customer_key"] is not None

def test_customers_scd2_incremental_load(spark_session, temp_curated_dir):
    """Tests the incremental daily run where a customer changes address."""
    # 1. Arrange: Setup initial history parquet in the temporary folder
    schema_hist = StructType([
        StructField("customer_key", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("is_current", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])
    
    # Existing baseline history
    # Customer 1001 lives in NY
    # Customer 1002 lives in IL
    history_data = [
        ("key_1", 1001, "Alice Smith", "alice@test.com", "555-0101", "123 Main St", "New York", "NY", "2026-01-01", "2026-01-01 00:00:00", "9999-12-31 23:59:59", "true", "2026-01-01 10:00:00"),
        ("key_2", 1002, "Bob Jones", "bob@test.com", "555-0102", "456 Oak Ave", "Chicago", "IL", "2026-01-02", "2026-01-02 00:00:00", "9999-12-31 23:59:59", "true", "2026-01-02 11:30:00")
    ]
    
    # Parse dates to timestamp before writing to match parquet schema
    df_hist = spark_session.createDataFrame(history_data, schema_hist)
    df_hist_typed = df_hist \
        .withColumn("signup_date", to_date(col("signup_date"))) \
        .withColumn("start_date", to_timestamp(col("start_date"))) \
        .withColumn("end_date", to_timestamp(col("end_date"))) \
        .withColumn("is_current", col("is_current").cast("boolean")) \
        .withColumn("updated_at", to_timestamp(col("updated_at")))
        
    df_hist_typed.write.mode("overwrite").parquet(os.path.join(temp_curated_dir, "dim_customers"))

    # 2. Arrange: Incoming raw updates
    # Alice (1001) has changed address to "789 Pine Rd, San Jose, CA"
    # Plus a brand new customer (1003)
    schema_raw = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])
    
    raw_data = [
        ("1001", "Alice Smith", "alice@test.com", "555-0101", "789 Pine Rd", "San Jose", "CA", "2026-01-01", "2026-06-02 10:00:00"),
        ("1003", "Charlie Brown", "charlie@test.com", "555-0103", "111 Elm St", "Miami", "FL", "2026-06-02", "2026-06-02 09:00:00")
    ]
    df_raw = spark_session.createDataFrame(raw_data, schema_raw)

    # 3. Act: Run process_customers_scd2
    run_date = "2026-06-02"
    df_result = process_customers_scd2(spark_session, df_raw, temp_curated_dir, run_date)
    
    # 4. Assert: Check the resulting history table contents
    # We expect 4 total records now:
    # - Bob (1002) unchanged: is_current = True
    # - Alice (1001) old record: is_current = False, end_date = 2026-06-02
    # - Alice (1001) new record: is_current = True, address = 789 Pine Rd, start_date = 2026-06-02
    # - Charlie (1003) new customer: is_current = True, start_date = 2026-06-02
    rows = df_result.collect()
    assert len(rows) == 4
    
    # Query specific users from the output
    alice_records = [r for r in rows if r["customer_id"] == 1001]
    assert len(alice_records) == 2
    
    # Verify the expired record
    expired_alice = [r for r in alice_records if not r["is_current"]][0]
    assert expired_alice["address"] == "123 Main St"
    assert expired_alice["end_date"].strftime("%Y-%m-%d") == "2026-06-02"
    
    # Verify the new active record
    active_alice = [r for r in alice_records if r["is_current"]][0]
    assert active_alice["address"] == "789 Pine Rd"
    assert active_alice["start_date"].strftime("%Y-%m-%d") == "2026-06-02"
    assert active_alice["end_date"].strftime("%Y-%m-%d") == "9999-12-31"

def test_products_scd1_overwrite(spark_session, temp_curated_dir):
    """Tests the product catalog updates (SCD Type 1 - overwrite)."""
    # 1. Arrange: Setup initial history
    schema_hist = StructType([
        StructField("product_key", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])
    hist_data = [
        ("key_p1", 5001, "Smartphone model-1", "Electronics", 499.99, 100, "2026-01-01 00:00:00", "2026-01-01 00:00:00")
    ]
    df_hist = spark_session.createDataFrame(hist_data, schema_hist)
    df_hist_typed = df_hist \
        .withColumn("price", col("price").cast("decimal(10,2)")) \
        .withColumn("created_at", to_timestamp(col("created_at"))) \
        .withColumn("updated_at", to_timestamp(col("updated_at")))
    df_hist_typed.write.mode("overwrite").parquet(os.path.join(temp_curated_dir, "dim_products"))

    # 2. Arrange: Incoming raw product with a price/stock update
    schema_raw = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("stock_quantity", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])
    raw_data = [
        # Price is now 450.00 and stock is 85
        ("5001", "Smartphone model-1", "Electronics", "450.00", "85", "2026-01-01 00:00:00", "2026-06-02 12:00:00")
    ]
    df_raw = spark_session.createDataFrame(raw_data, schema_raw)

    # 3. Act: Run process_products_scd1
    df_result = process_products_scd1(spark_session, df_raw, temp_curated_dir)

    # 4. Assert: Check catalog overwritten values
    rows = df_result.collect()
    assert len(rows) == 1
    assert float(rows[0]["price"]) == 450.00
    assert rows[0]["stock_quantity"] == 85
    assert rows[0]["updated_at"].strftime("%Y-%m-%d") == "2026-06-02"
