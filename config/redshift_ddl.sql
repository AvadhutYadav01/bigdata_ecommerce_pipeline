-- ============================================================================
-- AWS Redshift / Athena DDL Schema
-- E-commerce Dimensional Data Model (Star Schema)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. DIMENSION: dim_customers_scd2
-- Description: Customer dimension implementing Slowly Changing Dimension Type 2.
-- It keeps historical records of customer profile changes (e.g. changing address).
-- Every change inserts a new row with a new surrogate key, setting the old row's
-- end_date to the transaction date and setting is_current = FALSE.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.dim_customers_scd2 (
    customer_key VARCHAR(64) NOT NULL,       -- Surrogate Key: md5(customer_id + start_date)
    customer_id INT NOT NULL,                -- Natural Key (from source system)
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    signup_date DATE,
    start_date TIMESTAMP NOT NULL,           -- Effective date when this version became active
    end_date TIMESTAMP,                     -- Expiry date. NULL (or '9999-12-31') if active
    is_current BOOLEAN DEFAULT TRUE,        -- TRUE if this is the active record, FALSE otherwise
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (customer_key)
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (is_current, customer_id);

-- ----------------------------------------------------------------------------
-- 2. DIMENSION: dim_products_scd1
-- Description: Product dimension implementing Slowly Changing Dimension Type 1.
-- Type 1 updates overwrite the existing record. No historical tracking is kept.
-- For example, if a product price or stock count changes, we just update it.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.dim_products_scd1 (
    product_key VARCHAR(64) NOT NULL,        -- Surrogate Key: md5(product_id)
    product_id INT NOT NULL,                 -- Natural Key (from source system)
    product_name VARCHAR(150),
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INT,
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (product_key)
)
DISTSTYLE ALL                                -- Small dimension table; distribute to all nodes for fast join performance
SORTKEY (product_id);

-- ----------------------------------------------------------------------------
-- 3. DIMENSION: dim_date
-- Description: Date dimension to facilitate easy slicing and dicing by time period.
-- Pre-populated dimension.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key INT NOT NULL,                   -- YYYYMMDD format integer
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date);

-- ----------------------------------------------------------------------------
-- 4. FACT TABLE: fact_orders
-- Description: Fact table storing the individual order transactions.
-- Foreign keys point to the surrogate keys of the dimension tables.
-- Points to the customer record that was active *at the time the order was placed* (SCD2).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.fact_orders (
    order_id INT NOT NULL,                   -- Natural Transaction ID
    customer_key VARCHAR(64) NOT NULL,       -- FK to dim_customers_scd2 (joins on active record during order placement)
    product_key VARCHAR(64) NOT NULL,        -- FK to dim_products_scd1
    order_date_key INT NOT NULL,             -- FK to dim_date
    quantity INT NOT NULL,
    price_each DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    order_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (order_id)
)
DISTSTYLE KEY
DISTKEY (customer_key)                       -- Distributed on customer_key to optimize joins with dim_customers_scd2
SORTKEY (order_date_key);
