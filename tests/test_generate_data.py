#!/usr/bin/env python
"""
Unit Tests for Data Ingestion Layer
Author: Antigravity

Validates that generate_data.py produces correct schemas, formats, and outputs.
"""

import pytest
import pandas as pd
from datetime import datetime
from scripts.generate_data import generate_customers, generate_products, generate_orders

def test_generate_customers():
    """Validates schema and count of generated customers."""
    num_customers = 50
    df = generate_customers(num_customers, seed=123)
    
    # Assert DataFrame characteristics
    assert isinstance(df, pd.DataFrame)
    assert len(df) == num_customers
    
    # Assert correct columns
    expected_cols = ["customer_id", "name", "email", "phone", "address", "city", "state", "signup_date", "updated_at"]
    assert list(df.columns) == expected_cols
    
    # Assert data types and values
    assert df["customer_id"].iloc[0] == 1001
    assert df["customer_id"].iloc[-1] == 1050
    assert df["email"].str.contains("@example.com").all()

def test_generate_products():
    """Validates schema and count of generated products catalog."""
    num_products = 20
    df = generate_products(num_products, seed=123)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == num_products
    
    expected_cols = ["product_id", "product_name", "category", "price", "stock_quantity", "created_at", "updated_at"]
    assert list(df.columns) == expected_cols
    
    assert df["product_id"].iloc[0] == 5001
    assert df["price"].min() > 0.0

def test_generate_orders():
    """Validates schema, foreign keys, and date alignment of generated orders."""
    # 1. Generate base tables
    customers_df = generate_customers(10, seed=1)
    products_df = generate_products(5, seed=2)
    
    # 2. Generate orders
    num_orders = 100
    start_dt = datetime(2026, 1, 1)
    end_dt = datetime(2026, 2, 1)
    df_orders = generate_orders(customers_df, products_df, num_orders, start_dt, end_dt, seed=3)
    
    assert isinstance(df_orders, pd.DataFrame)
    assert len(df_orders) == num_orders
    
    expected_cols = ["order_id", "customer_id", "product_id", "quantity", "order_date", "total_amount", "order_status"]
    assert list(df_orders.columns) == expected_cols
    
    # Verify foreign keys are valid reference references
    assert df_orders["customer_id"].isin(customers_df["customer_id"]).all()
    assert df_orders["product_id"].isin(products_df["product_id"]).all()
    
    # Verify order dates fall within the bounds
    order_dates = pd.to_datetime(df_orders["order_date"])
    assert order_dates.min() >= start_dt
    assert order_dates.max() <= end_dt
