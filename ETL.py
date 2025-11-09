import pandas as pd
import numpy as np
from sqlalchemy import text
from db_config import get_engine

# ============================================================
# 1. EXTRACT
# ============================================================
print("Extracting CSV files...")

datasets_path = "datasets/"

customers = pd.read_csv(datasets_path + "olist_customers_dataset.csv")
orders = pd.read_csv(datasets_path + "olist_orders_dataset.csv")
order_items = pd.read_csv(datasets_path + "olist_order_items_dataset.csv")
products = pd.read_csv(datasets_path + "olist_products_dataset.csv")
sellers = pd.read_csv(datasets_path + "olist_sellers_dataset.csv")
geolocation = pd.read_csv(datasets_path + "olist_geolocation_dataset.csv")
reviews = pd.read_csv(datasets_path + "olist_order_reviews_dataset.csv")
payments = pd.read_csv(datasets_path + "olist_order_payments_dataset.csv")

print("Data loaded successfully.")

# ============================================================
# 2. TRANSFORM
# ============================================================
print("Transforming data...")

# --- Remove duplicates ---
customers = customers.drop_duplicates(subset='customer_id')
products = products.drop_duplicates(subset='product_id')
sellers = sellers.drop_duplicates(subset='seller_id')
orders = orders.drop_duplicates(subset='order_id')

# --- Handle missing values ---
products['product_category_name'] = products['product_category_name'].fillna('Unknown')
customers = customers.fillna({'customer_city': 'Unknown', 'customer_state': 'Unknown'})
sellers = sellers.fillna({'seller_city': 'Unknown', 'seller_state': 'Unknown'})
geolocation = geolocation.rename(columns={'geolocation_lat': 'latitude', 'geolocation_lng': 'longitude'})
geolocation = geolocation.fillna({'geolocation_city': 'Unknown', 'geolocation_state': 'Unknown'})

# --- Convert to datetime ---
date_cols = [
    'order_purchase_timestamp', 'order_approved_at',
    'order_delivered_carrier_date', 'order_delivered_customer_date',
    'order_estimated_delivery_date'
]
for col in date_cols:
    orders[col] = pd.to_datetime(orders[col], errors='coerce')

# --- Calculate delivery delays ---
orders['order_actual_delivery_days'] = (
    (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).dt.days
)
orders['order_estimated_delivery_days'] = (
    (orders['order_estimated_delivery_date'] - orders['order_purchase_timestamp']).dt.days
)
orders['delivery_delay_days'] = orders['order_actual_delivery_days'] - orders['order_estimated_delivery_days']

# ============================================================
# 3. LOAD INTO POSTGRESQL
# ============================================================
engine = get_engine()
print("Connected to database.")

# --- Helper functions ---
def load_dimension_with_chunks(df, table_name, chunksize=5000):
    """Load dimension table safely: truncate first, then insert in chunks."""
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE olist_dw.{table_name} RESTART IDENTITY CASCADE"))
    df.to_sql(table_name, engine, schema='olist_dw', if_exists='append', index=False, chunksize=chunksize)
    print(f"Dimension '{table_name}' loaded ({len(df)} rows)")

def load_fact_with_chunks(df, table_name, pk_columns, chunksize=5000):
    """Load fact table with ON CONFLICT DO NOTHING to avoid duplicates."""
    df_columns = ','.join(df.columns)
    values_placeholders = ','.join([f":{col}" for col in df.columns])
    conflict_cols = ','.join(pk_columns)
    insert_sql = f"""
    INSERT INTO olist_dw.{table_name} ({df_columns})
    VALUES ({values_placeholders})
    ON CONFLICT ({conflict_cols}) DO NOTHING
    """
    for i in range(0, len(df), chunksize):
        chunk = df.iloc[i:i+chunksize].replace({np.nan: None})
        with engine.begin() as conn:
            conn.execute(text(insert_sql), chunk.to_dict(orient='records'))
        print(f"Inserted rows {i+1}–{i+len(chunk)} into '{table_name}'")

# ============================================================
# DIMENSIONS
# ============================================================

# --- DIM_DATE ---
dim_date = pd.DataFrame({
    'full_date': pd.to_datetime(orders['order_purchase_timestamp'].dropna()).dt.date
}).drop_duplicates(subset='full_date')
dim_date['full_date'] = pd.to_datetime(dim_date['full_date'])
dim_date['day'] = dim_date['full_date'].dt.day
dim_date['month'] = dim_date['full_date'].dt.month
dim_date['year'] = dim_date['full_date'].dt.year
dim_date['weekday'] = dim_date['full_date'].dt.day_name()
dim_date['quarter'] = dim_date['full_date'].dt.quarter
load_dimension_with_chunks(dim_date, 'dim_date')

# --- DIM_CUSTOMER ---
load_dimension_with_chunks(
    customers[['customer_id', 'customer_unique_id', 'customer_city', 'customer_state']],
    'dim_customer'
)

# --- DIM_PRODUCT ---
load_dimension_with_chunks(
    products[['product_id', 'product_category_name', 'product_weight_g',
              'product_length_cm', 'product_height_cm', 'product_width_cm']],
    'dim_product'
)

# --- DIM_SELLER ---
load_dimension_with_chunks(
    sellers[['seller_id', 'seller_city', 'seller_state']],
    'dim_seller'
)

# --- DIM_GEOLOCATION ---
load_dimension_with_chunks(
    geolocation[['geolocation_zip_code_prefix', 'geolocation_city',
                 'geolocation_state', 'latitude', 'longitude']],
    'dim_geolocation'
)

# --- DIM_ORDER ---
dim_order = orders[['order_id', 'order_status', 'order_purchase_timestamp',
                    'order_approved_at', 'order_delivered_carrier_date',
                    'order_delivered_customer_date', 'order_estimated_delivery_date']].copy()
load_dimension_with_chunks(dim_order, 'dim_order')

# ============================================================
# FACT TABLES
# ============================================================

# --- Map dates to date_keys ---
dim_date_map = pd.read_sql("SELECT date_key, full_date FROM olist_dw.dim_date", engine)
dim_date_map['full_date'] = pd.to_datetime(dim_date_map['full_date']).dt.date
orders['order_date'] = orders['order_purchase_timestamp'].dt.date
orders_date_key = orders[['order_id', 'order_date']].merge(
    dim_date_map, left_on='order_date', right_on='full_date', how='left'
)
orders_date_key['date_key'] = orders_date_key['date_key'].fillna(1).astype(np.int64)

# --- FACT_ORDER_ITEMS ---
fact_order_items = order_items.merge(
    orders[['order_id', 'customer_id']], on='order_id', how='inner'
).merge(
    reviews[['order_id', 'review_score']], on='order_id', how='left'
).merge(
    orders_date_key[['order_id', 'date_key']], on='order_id', how='left'
)
fact_order_items['quantity'] = 1

# Ensure foreign key references exist
dim_order_ids = pd.read_sql("SELECT order_id FROM olist_dw.dim_order", engine)['order_id']
dim_product_ids = pd.read_sql("SELECT product_id FROM olist_dw.dim_product", engine)['product_id']
dim_seller_ids = pd.read_sql("SELECT seller_id FROM olist_dw.dim_seller", engine)['seller_id']

fact_order_items = fact_order_items[
    fact_order_items['order_id'].isin(dim_order_ids) &
    fact_order_items['product_id'].isin(dim_product_ids) &
    fact_order_items['seller_id'].isin(dim_seller_ids)
]

fact_order_items = fact_order_items[[
    'order_id', 'product_id', 'seller_id', 'customer_id', 'date_key',
    'price', 'freight_value', 'review_score', 'quantity'
]]

load_fact_with_chunks(fact_order_items, 'fact_order_items', pk_columns=['order_id','product_id','seller_id'])

# --- FACT_ORDER_DELIVERY ---
fact_order_delivery = orders[['order_id', 'customer_id',
                              'order_estimated_delivery_days',
                              'order_actual_delivery_days',
                              'delivery_delay_days']].copy()

# Add num_items and total_order_value
order_item_counts = order_items.groupby('order_id')['order_item_id'].count()
order_total_values = order_items.groupby('order_id')['price'].sum()
fact_order_delivery['num_items'] = fact_order_delivery['order_id'].map(order_item_counts).fillna(0).astype(np.int64)
fact_order_delivery['total_order_value'] = fact_order_delivery['order_id'].map(order_total_values).fillna(0).astype(float)

fact_order_delivery = fact_order_delivery.merge(
    orders_date_key[['order_id','date_key']], on='order_id', how='left'
)
fact_order_delivery['geolocation_id'] = None

fact_order_delivery = fact_order_delivery[[
    'order_id','customer_id','date_key','geolocation_id',
    'num_items','total_order_value',
    'order_estimated_delivery_days','order_actual_delivery_days','delivery_delay_days'
]]

load_fact_with_chunks(fact_order_delivery, 'fact_order_delivery', pk_columns=['order_id'])

# ============================================================
# 4. DATA QUALITY CHECKS
# ============================================================
with engine.connect() as conn:
    for tbl in ['dim_customer', 'dim_product', 'dim_seller', 'dim_order', 'fact_order_items', 'fact_order_delivery']:
        res = conn.execute(text(f"SELECT COUNT(*) FROM olist_dw.{tbl}"))
        print(f"{tbl}: {res.scalar()} rows")

print("ETL complete.")
