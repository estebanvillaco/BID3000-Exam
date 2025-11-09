-- ============================================================
-- 📊 Olist Data Warehouse - Data Quality Validation Script
-- ============================================================
-- Run this after each ETL load to verify referential integrity,
-- null handling, and value sanity across all tables in schema olist_dw.
-- ============================================================

SET search_path TO olist_dw;

-- ============================================================
-- 1️⃣ Basic Row Count Summary
-- ============================================================
SELECT 
    'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL
SELECT 'dim_geolocation', COUNT(*) FROM dim_geolocation
UNION ALL
SELECT 'fact_order_items', COUNT(*) FROM fact_order_items
UNION ALL
SELECT 'fact_order_delivery', COUNT(*) FROM fact_order_delivery
ORDER BY table_name;

-- ============================================================
-- 2️⃣ Null / Missing Key Checks
-- ============================================================
SELECT 
    'dim_customer' AS table_name,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_id,
    COUNT(*) FILTER (WHERE customer_city IS NULL) AS null_city,
    COUNT(*) FILTER (WHERE customer_state IS NULL) AS null_state
FROM dim_customer
UNION ALL
SELECT 
    'dim_product',
    COUNT(*) FILTER (WHERE product_id IS NULL),
    COUNT(*) FILTER (WHERE product_category_name IS NULL),
    COUNT(*) FILTER (WHERE product_weight_g IS NULL)
FROM dim_product
UNION ALL
SELECT 
    'dim_seller',
    COUNT(*) FILTER (WHERE seller_id IS NULL),
    COUNT(*) FILTER (WHERE seller_city IS NULL),
    COUNT(*) FILTER (WHERE seller_state IS NULL)
FROM dim_seller;

-- ============================================================
-- 3️⃣ Referential Integrity Checks (Fact → Dimension)
-- ============================================================
SELECT 
    'fact_order_items' AS fact_table,
    SUM(CASE WHEN p.product_id IS NULL THEN 1 ELSE 0 END) AS missing_products,
    SUM(CASE WHEN s.seller_id IS NULL THEN 1 ELSE 0 END) AS missing_sellers,
    SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customers
FROM fact_order_items f
LEFT JOIN dim_product p ON f.product_id = p.product_id
LEFT JOIN dim_seller s ON f.seller_id = s.seller_id
LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
UNION ALL
SELECT 
    'fact_order_delivery',
    NULL,
    NULL,
    SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END)
FROM fact_order_delivery f
LEFT JOIN dim_customer c ON f.customer_id = c.customer_id;

-- ============================================================
-- 4️⃣ Missing Date Keys
-- ============================================================
SELECT 
    'fact_order_items' AS table_name,
    COUNT(*) FILTER (WHERE date_key IS NULL) AS missing_date_keys
FROM fact_order_items
UNION ALL
SELECT 
    'fact_order_delivery',
    COUNT(*) FILTER (WHERE date_key IS NULL)
FROM fact_order_delivery;

-- ============================================================
-- 5️⃣ Duplicate Fact Rows Check
-- ============================================================
-- Each (order_id, product_id, seller_id) in fact_order_items should be unique
SELECT 
    'fact_order_items' AS table_name,
    COUNT(*) - COUNT(DISTINCT (order_id, product_id, seller_id)) AS duplicate_rows
FROM fact_order_items
UNION ALL
SELECT 
    'fact_order_delivery',
    COUNT(*) - COUNT(DISTINCT order_id)
FROM fact_order_delivery;

-- ============================================================
-- 6️⃣ Sanity Check: Delivery Delay Distribution
-- ============================================================
SELECT 
    ROUND(AVG(delivery_delay_days)::numeric, 2) AS avg_delay,
    MIN(delivery_delay_days) AS min_delay,
    MAX(delivery_delay_days) AS max_delay
FROM fact_order_delivery;

-- ============================================================
-- 7️⃣ Outlier Detection (Extreme Order Values)
-- ============================================================
SELECT 
    COUNT(*) FILTER (WHERE total_order_value < 0) AS negative_values,
    COUNT(*) FILTER (WHERE total_order_value > 10000) AS suspiciously_high_values
FROM fact_order_delivery;

-- ============================================================
-- ✅ Summary Message
-- ============================================================
SELECT '✅ Data quality checks completed successfully!' AS status_message;

