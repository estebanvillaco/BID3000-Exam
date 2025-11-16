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
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
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
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
);

-- ============================================================
-- END OF SCHEMA
-- ============================================================



-- ============================================================
-- BID3000 - Olist Data Warehouse (Star Schema) - Sample Inserts
-- ============================================================

-- ============================================================
-- DIMENSIONS
-- ============================================================

-- 1. dim_date
INSERT INTO dim_date (full_date, day, month, year, weekday, quarter) VALUES
('2016-11-05', 5, 11, 2016, 'Saturday', 4),
('2016-12-22', 22, 12, 2016, 'Thursday', 4),
('2017-03-15', 15, 3, 2017, 'Wednesday', 1),
('2017-07-10', 10, 7, 2017, 'Monday', 3),
('2017-11-25', 25, 11, 2017, 'Saturday', 4),
('2018-01-12', 12, 1, 2018, 'Friday', 1),
('2018-04-09', 9, 4, 2018, 'Monday', 2),
('2018-09-30', 30, 9, 2018, 'Sunday', 3),
('2018-12-15', 15, 12, 2018, 'Saturday', 4),
('2017-05-22', 22, 5, 2017, 'Monday', 2);

-- 2. dim_customer
INSERT INTO dim_customer (customer_id, customer_unique_id, customer_city, customer_state) VALUES
('c_2016_1', 'u2016_1', 'sao paulo', 'SP'),
('c_2016_2', 'u2016_2', 'rio de janeiro', 'RJ'),
('c_2017_1', 'u2017_1', 'curitiba', 'PR'),
('c_2017_2', 'u2017_2', 'belo horizonte', 'MG'),
('c_2018_1', 'u2018_1', 'salvador', 'BA'),
('c_2018_2', 'u2018_2', 'fortaleza', 'CE'),
('c_2017_3', 'u2017_3', 'porto alegre', 'RS'),
('c_2018_3', 'u2018_3', 'recife', 'PE');

-- 3. dim_product
INSERT INTO dim_product (product_id, product_category_name, product_weight_g, product_length_cm, product_height_cm, product_width_cm) VALUES
('p_2016_1', 'books_general', 250, 22, 3, 15),
('p_2016_2', 'toys_babies', 300, 10, 10, 10),
('p_2017_1', 'computers_accessories', 1500, 40, 20, 30),
('p_2017_2', 'automotive', 1200, 60, 30, 40),
('p_2018_1', 'fashion_bags_accessories', 600, 20, 25, 15),
('p_2018_2', 'health_beauty', 500, 15, 8, 12),
('p_2018_3', 'sports', 1000, 50, 25, 25),
('p_2017_3', 'furniture_living_room', 5000, 150, 75, 80);

-- 4. dim_seller
INSERT INTO dim_seller (seller_id, seller_city, seller_state) VALUES
('s_2016_1', 'porto alegre', 'RS'),
('s_2016_2', 'manaus', 'AM'),
('s_2017_1', 'brasilia', 'DF'),
('s_2017_2', 'niteroi', 'RJ'),
('s_2018_1', 'curitiba', 'PR'),
('s_2018_2', 'guarulhos', 'SP'),
('s_2017_3', 'belo horizonte', 'MG'),
('s_2018_3', 'salvador', 'BA');

-- 5. dim_order
INSERT INTO dim_order (order_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date) VALUES
('o_2016_1', 'delivered', '2016-11-05 09:15:00', '2016-11-05 10:00:00', '2016-11-10 08:00:00', '2016-11-15 17:00:00', '2016-11-14 17:00:00'),
('o_2016_2', 'canceled', '2016-12-22 14:20:00', NULL, NULL, NULL, '2016-12-28 17:00:00'),
('o_2017_1', 'delivered', '2017-07-10 12:34:00', '2017-07-10 13:00:00', '2017-07-15 09:00:00', '2017-07-20 17:00:00', '2017-07-18 17:00:00'),
('o_2017_2', 'shipped', '2017-11-25 16:45:00', '2017-11-25 17:10:00', '2017-11-30 11:00:00', '2017-12-04 17:00:00', '2017-12-03 17:00:00'),
('o_2018_1', 'delivered', '2018-01-12 10:05:00', '2018-01-12 10:45:00', '2018-01-17 09:30:00', '2018-01-22 17:00:00', '2018-01-20 17:00:00'),
('o_2018_2', 'delivered', '2018-09-30 11:30:00', '2018-09-30 12:00:00', '2018-10-05 08:00:00', '2018-10-10 17:00:00', '2018-10-09 17:00:00'),
('o_2017_3', 'delivered', '2017-05-22 09:00:00', '2017-05-22 10:00:00', '2017-05-27 08:00:00', '2017-05-30 17:00:00', '2017-05-29 17:00:00');

-- 6. dim_geolocation
INSERT INTO dim_geolocation (geolocation_zip_code_prefix, geolocation_city, geolocation_state, latitude, longitude) VALUES
('11000', 'santos', 'SP', -23.9540, -46.3333),
('22000', 'rio de janeiro', 'RJ', -22.9083, -43.1964),
('30000', 'belo horizonte', 'MG', -19.9208, -43.9378),
('40000', 'salvador', 'BA', -12.9714, -38.5014),
('50000', 'recife', 'PE', -8.0476, -34.8770),
('60000', 'fortaleza', 'CE', -3.7172, -38.5431),
('70000', 'brasilia', 'DF', -15.7942, -47.8822);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- 7. fact_order_items
INSERT INTO fact_order_items (order_id, product_id, seller_id, customer_id, date_key, price, freight_value, payment_value, review_score, quantity) VALUES
('o_2016_1', 'p_2016_1', 's_2016_1', 'c_2016_1', 1, 30.00, 5.00, 35.00, 4, 1),
('o_2016_2', 'p_2016_2', 's_2016_2', 'c_2016_2', 2, 25.00, 4.00, 29.00, NULL, 2),
('o_2017_1', 'p_2017_1', 's_2017_1', 'c_2017_1', 3, 200.00, 20.00, 220.00, 5, 1),
('o_2017_2', 'p_2017_2', 's_2017_2', 'c_2017_2', 4, 150.00, 15.00, 165.00, 3, 1),
('o_2018_1', 'p_2018_1', 's_2018_1', 'c_2018_1', 5, 60.00, 10.00, 70.00, 4, 2),
('o_2018_2', 'p_2018_2', 's_2018_2', 'c_2018_2', 6, 45.00, 7.00, 52.00, 5, 1),
('o_2017_3', 'p_2017_3', 's_2017_3', 'c_2017_3', 10, 300.00, 30.00, 330.00, 4, 1);

-- 8. fact_order_delivery
INSERT INTO fact_order_delivery (order_id, customer_id, date_key, geolocation_id, num_items, total_order_value, order_estimated_delivery_days, order_actual_delivery_days, delivery_delay_days) VALUES
('o_2016_1', 'c_2016_1', 1, 1, 1, 35.00, 9, 11, 2),
('o_2016_2', 'c_2016_2', 2, 2, 2, 29.00, 6, NULL, NULL),
('o_2017_1', 'c_2017_1', 3, 3, 1, 220.00, 8, 10, 2),
('o_2017_2', 'c_2017_2', 4, 4, 1, 165.00, 7, 8, 1),
('o_2018_1', 'c_2018_1', 5, 5, 2, 70.00, 7, 9, 2),
('o_2018_2', 'c_2018_2', 6, 6, 1, 52.00, 5, 5, 0),
('o_2017_3', 'c_2017_3', 10, 7, 1, 330.00, 7, 8, 1);