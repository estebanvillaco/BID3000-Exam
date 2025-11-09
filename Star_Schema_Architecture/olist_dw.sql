DROP SCHEMA IF EXISTS olist_dw;
CREATE SCHEMA olist_dw;
SET search_path TO olist_dw;

-- DIMENSIONS
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(10),
    quarter INT
);

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_weight_g NUMERIC,
    product_length_cm NUMERIC,
    product_height_cm NUMERIC,
    product_width_cm NUMERIC
);

CREATE TABLE dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

CREATE TABLE dim_order (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE dim_geolocation (
    geolocation_key SERIAL PRIMARY KEY,
    geolocation_zip_code_prefix VARCHAR(20),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10),
    latitude NUMERIC,
    longitude NUMERIC
);

-- FACTS
CREATE TABLE fact_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    customer_id VARCHAR(50),
    date_id INT REFERENCES dim_date(date_key),
    price NUMERIC,
    freight_value NUMERIC,
    payment_value NUMERIC,
    review_score INT,
    quantity INT DEFAULT 1
);

CREATE TABLE fact_order_delivery (
    delivery_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    date_id INT REFERENCES dim_date(date_key),
    geolocation_id INT REFERENCES dim_geolocation(geolocation_key),
    num_items INT,
    total_order_value NUMERIC,
    order_estimated_delivery_days INT,
    order_actual_delivery_days INT,
    delivery_delay_days INT
);