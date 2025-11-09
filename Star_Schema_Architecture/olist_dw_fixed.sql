-- ============================================================
-- BID3000 - Olist Data Warehouse (Star Schema)
-- Based on ERD_olist_dw-2.png (Updated Version)
-- ============================================================

DROP SCHEMA IF EXISTS olist_dw CASCADE;
CREATE SCHEMA olist_dw;
SET search_path TO olist_dw;

-- ============================================================
-- DIMENSIONS
-- ============================================================

-- 1. Date Dimension
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(15),
    quarter INT
);

-- 2. Customer Dimension
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE,
    customer_unique_id VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

-- 3. Product Dimension
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE,
    product_category_name VARCHAR(100),
    product_weight_g NUMERIC,
    product_length_cm NUMERIC,
    product_height_cm NUMERIC,
    product_width_cm NUMERIC
);

-- 4. Seller Dimension
CREATE TABLE dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

-- 5. Order Dimension
CREATE TABLE dim_order (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE,
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- 6. Geolocation Dimension
CREATE TABLE dim_geolocation (
    geolocation_key SERIAL PRIMARY KEY,
    geolocation_zip_code_prefix VARCHAR(20),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10),
    latitude NUMERIC,
    longitude NUMERIC
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- 1. Fact Order Items
CREATE TABLE fact_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    customer_id VARCHAR(50),
    date_key INT REFERENCES dim_date(date_key),
    price NUMERIC,
    freight_value NUMERIC,
    payment_value NUMERIC,
    review_score INT,
    quantity INT DEFAULT 1,
    FOREIGN KEY (order_id) REFERENCES dim_order(order_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    -- Add this line for your ETL ON CONFLICT to work
    UNIQUE (order_id, product_id, seller_id)
);


-- 2. Fact Order Delivery
CREATE TABLE fact_order_delivery (
    delivery_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    date_key INT REFERENCES dim_date(date_key),
    geolocation_id INT REFERENCES dim_geolocation(geolocation_key),
    num_items INT,
    total_order_value NUMERIC,
    order_estimated_delivery_days INT,
    order_actual_delivery_days INT,
    delivery_delay_days INT,
    FOREIGN KEY (order_id) REFERENCES dim_order(order_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    -- Add this UNIQUE constraint so ON CONFLICT(order_id) works
    UNIQUE (order_id)
);

-- ============================================================
-- END OF SCHEMA
-- ============================================================

