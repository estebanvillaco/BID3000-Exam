# BID3000 – Task 2 Descriptive Analytics

import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import text
from db_config import get_engine

OUTPUT_DIR = "outputs_delivery"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_data():
    engine = get_engine()
    sql = text("""
        SELECT
            f.order_id,
            c.customer_state,
            s.seller_state,
            d.year,
            d.month,
            f.quantity,
            f.price,
            f.freight_value,

            -- Delivery time (days)
            CASE 
                WHEN o.order_delivered_customer_date IS NOT NULL 
                 AND o.order_purchase_timestamp IS NOT NULL
                THEN EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0
                ELSE NULL
            END AS delivery_days,

            -- Delivery delay (days)
            CASE 
                WHEN o.order_delivered_customer_date IS NOT NULL
                 AND o.order_estimated_delivery_date IS NOT NULL
                THEN EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date))/86400.0
                ELSE NULL
            END AS delay_days,

            f.review_score

        FROM olist_dw.fact_order_items f
        JOIN olist_dw.dim_customer c ON f.customer_id = c.customer_id
        JOIN olist_dw.dim_seller   s ON f.seller_id   = s.seller_id
        JOIN olist_dw.dim_date     d ON f.date_key    = d.date_key
        JOIN olist_dw.dim_order    o ON f.order_id    = o.order_id;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    df = df[df["delivery_days"].notna()]
    return df


def descriptive_plots(df):
    # 1. Distribution of delivery days
    plt.figure()
    df["delivery_days"].plot(kind="hist", bins=40, edgecolor="black")
    plt.title("Distribution of Delivery Days")
    plt.xlabel("Days")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/hist_delivery_days.png")
    plt.close()

    # 2. Distribution of delivery delays
    plt.figure()
    df["delay_days"].dropna().plot(kind="hist", bins=40, edgecolor="black")
    plt.title("Distribution of Delivery Delays")
    plt.xlabel("Delay Days (positive = late)")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/hist_delay_days.png")
    plt.close()

    # 3. Monthly average delivery days
    monthly = (
        df.groupby(["year", "month"])["delivery_days"]
          .mean()
          .reset_index()
          .sort_values(["year", "month"])
    )
    plt.figure()
    plt.plot(monthly.index, monthly["delivery_days"])
    plt.title("Monthly Average Delivery Days")
    plt.ylabel("Avg Delivery Days")
    plt.xlabel("Month Index")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/monthly_delivery_trend.png")
    plt.close()

    # 4. Delivery days by customer state
    plt.figure()
    df.boxplot(column="delivery_days", by="customer_state", rot=90, grid=False)
    plt.title("Delivery Days by Customer State")
    plt.suptitle("")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/boxplot_delivery_by_customer_state.png")
    plt.close()

    # 5. Delivery days by seller state
    plt.figure()
    df.boxplot(column="delivery_days", by="seller_state", rot=90, grid=False)
    plt.title("Delivery Days by Seller State")
    plt.suptitle("")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/boxplot_delivery_by_seller_state.png")
    plt.close()

    # 6. Correlation matrix
    corr = df[[
        "delivery_days", "delay_days", "freight_value",
        "price", "quantity", "review_score"
    ]].corr()

    plt.figure()
    plt.imshow(corr, cmap="coolwarm", interpolation="nearest")
    plt.colorbar()
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=45)
    plt.yticks(range(len(corr.columns)), corr.columns)
    plt.title("Correlation Matrix")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/correlation_matrix.png")
    plt.close()


def main():
    df = load_data()
    print(f"Loaded {len(df):,} rows with valid delivery_days.")
    descriptive_plots(df)
    print("Descriptive analytics completed. Files saved in:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
