#!/usr/bin/env python
"""
E-commerce Mock Data Generator
Author: Antigravity

This script simulates customer, product, and order data for an e-commerce platform.
It generates two kinds of batches:
  1. 'initial' - The baseline history of customers, products, and historical orders.
  2. 'incremental' - Simulates updates (SCD testing) and new daily orders.
     - Some customers change their address (to test SCD Type 2).
     - Some products change their price/stock (to test SCD Type 1).
     - New daily orders are created.
"""

import os
import argparse
import yaml
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def load_config(config_path="config/config.yaml"):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def generate_customers(num_customers, seed=42):
    np.random.seed(seed)
    customer_ids = np.arange(1001, 1001 + num_customers)
    
    first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah",
                   "Ian", "Julia", "Kevin", "Laura", "Matthew", "Natalie", "Oliver", "Penelope", "Ryan", "Sophia"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson",
                  "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez", "Moore", "Martin", "Jackson", "Thompson", "White"]
    
    states = ["NY", "CA", "TX", "FL", "IL", "PA", "OH", "MI", "GA", "NC", "WA", "AZ", "CO", "VA", "MA"]
    cities_by_state = {
        "NY": ["New York", "Buffalo", "Rochester"],
        "CA": ["Los Angeles", "San Francisco", "San Diego"],
        "TX": ["Houston", "Austin", "Dallas"],
        "FL": ["Miami", "Orlando", "Tampa"],
        "IL": ["Chicago", "Springfield", "Peoria"],
        "PA": ["Philadelphia", "Pittsburgh", "Allentown"],
        "OH": ["Columbus", "Cleveland", "Cincinnati"],
        "MI": ["Detroit", "Grand Rapids", "Lansing"],
        "GA": ["Atlanta", "Savannah", "Augusta"],
        "NC": ["Charlotte", "Raleigh", "Greensboro"],
        "WA": ["Seattle", "Spokane", "Tacoma"],
        "AZ": ["Phoenix", "Tucson", "Mesa"],
        "CO": ["Denver", "Colorado Springs", "Aurora"],
        "VA": ["Richmond", "Virginia Beach", "Norfolk"],
        "MA": ["Boston", "Worcester", "Springfield"]
    }
    
    streets = ["Main St", "Oak Ave", "Pine Rd", "Maple Dr", "Cedar Ln", "Elm St", "View Rd", "Park Pl", "Sunset Blvd", "Broadway"]
    
    data = []
    start_date = datetime(2026, 1, 1)
    
    for c_id in customer_ids:
        fn = np.random.choice(first_names)
        ln = np.random.choice(last_names)
        name = f"{fn} {ln}"
        email = f"{fn.lower()}.{ln.lower()}{c_id}@example.com"
        phone = f"+1-{np.random.randint(100, 999)}-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}"
        
        state = np.random.choice(states)
        city = np.random.choice(cities_by_state[state])
        street_num = np.random.randint(1, 9999)
        street_name = np.random.choice(streets)
        address = f"{street_num} {street_name}"
        
        signup_days = np.random.randint(0, 120)
        signup_date = start_date + timedelta(days=signup_days)
        
        data.append({
            "customer_id": int(c_id),
            "name": name,
            "email": email,
            "phone": phone,
            "address": address,
            "city": city,
            "state": state,
            "signup_date": signup_date.strftime("%Y-%m-%d"),
            "updated_at": signup_date.strftime("%Y-%m-%d %H:%M:%S")
        })
        
    return pd.DataFrame(data)

def generate_products(num_products, seed=42):
    np.random.seed(seed)
    product_ids = np.arange(5001, 5001 + num_products)
    
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Beauty", "Sports"]
    products_by_category = {
        "Electronics": ["Smartphone", "Laptop", "Wireless Headphones", "Smartwatch", "Bluetooth Speaker", "Tablet"],
        "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Socks", "Sweater"],
        "Home & Kitchen": ["Coffee Maker", "Blender", "Air Fryer", "Cookware Set", "Vacuum Cleaner", "Toaster"],
        "Books": ["Fiction Novel", "Sci-Fi Trilogy", "Self-Help Book", "Biography", "Cookbook", "History Book"],
        "Beauty": ["Moisturizer", "Sunscreen", "Perfume", "Shampoo", "Face Wash", "Lip Balm"],
        "Sports": ["Yoga Mat", "Dumbbells", "Water Bottle", "Running Shoes", "Backpack", "Resistance Bands"]
    }
    
    data = []
    created_date = datetime(2026, 1, 1).strftime("%Y-%m-%d %H:%M:%S")
    
    for p_id in product_ids:
        cat = np.random.choice(categories)
        p_name_base = np.random.choice(products_by_category[cat])
        p_name = f"{p_name_base} model-{np.random.randint(10, 99)}"
        
        # Prices based on category
        if cat == "Electronics":
            price = np.round(np.random.uniform(50.0, 1000.0), 2)
        elif cat == "Clothing":
            price = np.round(np.random.uniform(10.0, 150.0), 2)
        elif cat == "Home & Kitchen":
            price = np.round(np.random.uniform(20.0, 300.0), 2)
        elif cat == "Books":
            price = np.round(np.random.uniform(5.0, 40.0), 2)
        else:
            price = np.round(np.random.uniform(8.0, 120.0), 2)
            
        stock = np.random.randint(10, 500)
        
        data.append({
            "product_id": int(p_id),
            "product_name": p_name,
            "category": cat,
            "price": float(price),
            "stock_quantity": int(stock),
            "created_at": created_date,
            "updated_at": created_date
        })
        
    return pd.DataFrame(data)

def generate_orders(customers_df, products_df, num_orders, start_dt, end_dt, seed=42):
    np.random.seed(seed)
    order_ids = np.arange(100001, 100001 + num_orders)
    
    customer_pool = customers_df["customer_id"].values
    product_pool = products_df["product_id"].values
    product_price_map = products_df.set_index("product_id")["price"].to_dict()
    
    delta_days = (end_dt - start_dt).days
    order_dates = [start_dt + timedelta(days=int(np.random.randint(0, delta_days))) for _ in range(num_orders)]
    order_dates.sort()
    
    statuses = ["Delivered", "Delivered", "Delivered", "Shipped", "Processing", "Cancelled"]
    
    data = []
    for o_id, o_date in zip(order_ids, order_dates):
        c_id = np.random.choice(customer_pool)
        p_id = np.random.choice(product_pool)
        qty = np.random.choice([1, 1, 1, 2, 3]) # Bias towards buying 1 item
        price = product_price_map[p_id]
        total = np.round(price * qty, 2)
        status = np.random.choice(statuses)
        
        data.append({
            "order_id": int(o_id),
            "customer_id": int(c_id),
            "product_id": int(p_id),
            "quantity": int(qty),
            "order_date": o_date.strftime("%Y-%m-%d"),
            "total_amount": float(total),
            "order_status": status
        })
        
    return pd.DataFrame(data)

def make_incremental_changes(customers_df, products_df, config, update_date_str):
    # 1. Update existing customer addresses (SCD Type 2 simulation)
    cust_change_prob = config["data_generation"]["address_change_probability"]
    cust_mask = np.random.rand(len(customers_df)) < cust_change_prob
    num_updates = cust_mask.sum()
    
    # Predefined cities and streets
    states = ["NY", "CA", "TX", "FL", "IL"]
    cities_by_state = {
        "NY": ["New York", "Syracuse"],
        "CA": ["San Jose", "Fresno"],
        "TX": ["San Antonio", "Fort Worth"],
        "FL": ["Jacksonville", "Tallahassee"],
        "IL": ["Naperville", "Rockford"]
    }
    streets = ["Pine Rd", "Oak Ave", "Maple Dr", "Broadway", "Industrial Way", "Commerce St"]
    
    # Create copy of customers to return as updates
    cust_updates = customers_df[cust_mask].copy()
    if num_updates > 0:
        for idx in cust_updates.index:
            new_state = np.random.choice(states)
            new_city = np.random.choice(cities_by_state[new_state])
            new_address = f"{np.random.randint(10, 9999)} {np.random.choice(streets)}"
            
            cust_updates.loc[idx, "address"] = new_address
            cust_updates.loc[idx, "city"] = new_city
            cust_updates.loc[idx, "state"] = new_state
            cust_updates.loc[idx, "updated_at"] = update_date_str
            
    # Add some brand new customers (e.g. 5% of original size)
    num_new_cust = int(config["data_generation"]["num_customers"] * 0.05)
    max_cust_id = customers_df["customer_id"].max()
    new_cust_ids = np.arange(max_cust_id + 1, max_cust_id + 1 + num_new_cust)
    
    new_cust_data = []
    for c_id in new_cust_ids:
        state = np.random.choice(states)
        city = np.random.choice(cities_by_state[state])
        new_cust_data.append({
            "customer_id": int(c_id),
            "name": f"NewUser {c_id}",
            "email": f"newuser{c_id}@example.com",
            "phone": "+1-555-555-5555",
            "address": f"{np.random.randint(10, 9999)} NewWay Blvd",
            "city": city,
            "state": state,
            "signup_date": update_date_str[:10],
            "updated_at": update_date_str
        })
    new_cust_df = pd.DataFrame(new_cust_data)
    customers_update_batch = pd.concat([cust_updates, new_cust_df], ignore_index=True)

    # 2. Update product prices and stock (SCD Type 1 simulation)
    prod_change_prob = config["data_generation"]["price_change_probability"]
    prod_mask = np.random.rand(len(products_df)) < prod_change_prob
    num_prod_updates = prod_mask.sum()
    
    prod_updates = products_df[prod_mask].copy()
    if num_prod_updates > 0:
        for idx in prod_updates.index:
            # Change price by +/- 10%
            old_price = prod_updates.loc[idx, "price"]
            multiplier = np.random.uniform(0.9, 1.1)
            new_price = np.round(old_price * multiplier, 2)
            prod_updates.loc[idx, "price"] = float(new_price)
            # Add some stock
            prod_updates.loc[idx, "stock_quantity"] = int(np.random.randint(50, 600))
            prod_updates.loc[idx, "updated_at"] = update_date_str
            
    # Add a couple of new products
    max_prod_id = products_df["product_id"].max()
    new_prod_data = [{
        "product_id": int(max_prod_id + 1),
        "product_name": "New Product X-90",
        "category": "Electronics",
        "price": 199.99,
        "stock_quantity": 100,
        "created_at": update_date_str,
        "updated_at": update_date_str
    }]
    new_prod_df = pd.DataFrame(new_prod_data)
    products_update_batch = pd.concat([prod_updates, new_prod_df], ignore_index=True)

    return customers_update_batch, products_update_batch

def main():
    parser = argparse.ArgumentParser(description="Generate mock e-commerce datasets.")
    parser.add_argument("--batch", type=str, choices=["initial", "incremental"], default="initial",
                        help="Choose generation batch type: 'initial' or 'incremental'")
    args = parser.parse_args()
    
    config = load_config()
    raw_dir = config["local"]["raw_dir"]
    os.makedirs(raw_dir, exist_ok=True)
    
    num_cust = config["data_generation"]["num_customers"]
    num_prod = config["data_generation"]["num_products"]
    num_orders = config["data_generation"]["num_orders"]
    
    start_date_val = config["data_generation"]["start_date"]
    end_date_val = config["data_generation"]["end_date"]
    start_date_str = start_date_val.strftime("%Y-%m-%d") if hasattr(start_date_val, 'strftime') else str(start_date_val)
    end_date_str = end_date_val.strftime("%Y-%m-%d") if hasattr(end_date_val, 'strftime') else str(end_date_val)
    
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # -------------------------------------------------------------
    # BATCH 1: INITIAL DATA LOAD
    # -------------------------------------------------------------
    if args.batch == "initial":
        print(f"Generating initial baseline history up to {end_dt.strftime('%Y-%m-%d')}...")
        
        customers = generate_customers(num_cust)
        products = generate_products(num_prod)
        orders = generate_orders(customers, products, num_orders, start_dt, end_dt)
        
        # Save initial dataset under batch execution date (e.g. 2026-06-01)
        batch_date = end_dt.strftime("%Y-%m-%d")
        batch_dir = os.path.join(raw_dir, f"ingest_date={batch_date}")
        os.makedirs(batch_dir, exist_ok=True)
        
        customers.to_csv(os.path.join(batch_dir, "customers.csv"), index=False)
        products.to_csv(os.path.join(batch_dir, "products.csv"), index=False)
        orders.to_csv(os.path.join(batch_dir, "orders.csv"), index=False)
        
        # We also write active reference copies to help create updates for the next incremental batch
        customers.to_csv(os.path.join(raw_dir, ".customers_ref.csv"), index=False)
        products.to_csv(os.path.join(raw_dir, ".products_ref.csv"), index=False)
        
        print(f"Initial batch generated at: {batch_dir}")
        print(f"  - Customers: {len(customers)} rows")
        print(f"  - Products: {len(products)} rows")
        print(f"  - Orders: {len(orders)} rows")
        
    # -------------------------------------------------------------
    # BATCH 2: INCREMENTAL DAILY RUN (SCD SIMULATION)
    # -------------------------------------------------------------
    elif args.batch == "incremental":
        ref_cust_file = os.path.join(raw_dir, ".customers_ref.csv")
        ref_prod_file = os.path.join(raw_dir, ".products_ref.csv")
        
        if not os.path.exists(ref_cust_file) or not os.path.exists(ref_prod_file):
            print("Error: Reference files not found. Please run '--batch initial' first.")
            return
            
        print("Generating incremental daily update run (SCD testing)...")
        customers = pd.read_csv(ref_cust_file)
        products = pd.read_csv(ref_prod_file)
        
        # Run date is next day (e.g. 2026-06-02)
        incremental_date = end_dt + timedelta(days=1)
        incremental_date_str = incremental_date.strftime("%Y-%m-%d")
        incremental_timestamp_str = incremental_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # Generate Updates
        cust_updates, prod_updates = make_incremental_changes(customers, products, config, incremental_timestamp_str)
        
        # Generate brand new orders for the day
        # Customers and products can come from the updated list or original list
        full_customers_pool = pd.concat([customers, cust_updates]).drop_duplicates(subset=["customer_id"], keep="last")
        full_products_pool = pd.concat([products, prod_updates]).drop_duplicates(subset=["product_id"], keep="last")
        
        daily_orders = generate_orders(
            customers_df=full_customers_pool,
            products_df=full_products_pool,
            num_orders=150, # Representing typical daily volume
            start_dt=incremental_date,
            end_dt=incremental_date + timedelta(days=1),
            seed=43
        )
        
        batch_dir = os.path.join(raw_dir, f"ingest_date={incremental_date_str}")
        os.makedirs(batch_dir, exist_ok=True)
        
        cust_updates.to_csv(os.path.join(batch_dir, "customers.csv"), index=False)
        prod_updates.to_csv(os.path.join(batch_dir, "products.csv"), index=False)
        daily_orders.to_csv(os.path.join(batch_dir, "orders.csv"), index=False)
        
        # Update our reference copies to reflect latest state
        full_customers_pool.to_csv(ref_cust_file, index=False)
        full_products_pool.to_csv(ref_prod_file, index=False)
        
        print(f"Incremental batch generated at: {batch_dir}")
        print(f"  - Customer updates & inserts: {len(cust_updates)} rows")
        print(f"  - Product updates & inserts: {len(prod_updates)} rows")
        print(f"  - New Daily Orders: {len(daily_orders)} rows")

if __name__ == "__main__":
    main()
