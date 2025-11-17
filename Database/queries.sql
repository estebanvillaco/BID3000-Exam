-- Query 1: Time based analysis
SELECT d.year,
    SUM(f.price) AS toatal_sales
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key 
GROUP BY d.year
ORDER BY d.year;


-- Query 2: Aggregation analysis
SELECT d.year, d.month, SUM(f.payment_value) AS total_sales
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY ROLLUP (d.year, d.month);


-- Query 3: Window Function
SELECT 
    d.year,
    p.product_category_name,
    SUM(f.price) AS total_sales,
    RANK() OVER (PARTITION BY d.year ORDER BY SUM(f.price) DESC) AS rank_per_year
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY d.year, p.product_category_name
ORDER BY d.year, rank_per_year;


-- Query 4: Complex filtering  Subqueries or EXISTS/IN clauses
SELECT c.customer_id, c.customer_city
FROM fact_order_items f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE d.year = 2017
AND c.customer_id NOT IN (
    SELECT f2.customer_id
    FROM fact_order_items f2
    JOIN dim_date d2 ON f2.date_key = d2.date_key
    WHERE d2.year = 2016
);

-- Query 5: Business Metrics (KPI calculations specific to your domain)
SELECT d.year,
    ROUND(AVG(f.order_actual_delivery_days), 2) AS avg_delivery_days
FROM fact_order_delivery f
JOIN dim_date d ON d.date_key = f.date_key
WHERE f.order_actual_delivery_days IS NOT NULL
GROUP BY d.year
ORDER BY d.year;


-- Query 6: Business Metrics (Customer or product performance analysis)
SELECT p.product_id,
    SUM(f.price) AS most_sold_product
FROM fact_order_items f
JOIN dim_product p ON p.product_id = f.product_id
GROUP BY p.product_id 
ORDER BY most_sold_product DESC;
