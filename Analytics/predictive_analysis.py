# BID3000 – Task 2 Predictive Analytics
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import text
from db_config import get_engine

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

OUTPUT_DIR = "outputs_delivery"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_data():
    engine = get_engine()
    sql = text("""
        SELECT
            c.customer_state,
            s.seller_state,
            d.year,
            d.month,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm,
            f.quantity,
            f.price,
            f.freight_value,

            CASE 
                WHEN o.order_delivered_customer_date IS NOT NULL 
                 AND o.order_purchase_timestamp IS NOT NULL
                THEN EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0
                ELSE NULL
            END AS delivery_days

        FROM olist_dw.fact_order_items f
        JOIN olist_dw.dim_customer c ON f.customer_id = c.customer_id
        JOIN olist_dw.dim_seller   s ON f.seller_id   = s.seller_id
        JOIN olist_dw.dim_date     d ON f.date_key    = d.date_key
        JOIN olist_dw.dim_product  p ON f.product_id  = p.product_id
        JOIN olist_dw.dim_order    o ON f.order_id    = o.order_id;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    df = df[df["delivery_days"].notna()]
    if len(df) > 20000:
        df = df.sample(n=20000, random_state=42)
    return df


def train_model(df):
    categorical = ["customer_state", "seller_state"]
    numerical = [
        "year", "month",
        "product_weight_g", "product_length_cm",
        "product_height_cm", "product_width_cm",
        "quantity", "price", "freight_value"
    ]

    X = df[categorical + numerical]
    y = df["delivery_days"]

    preprocessor = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ("num", SimpleImputer(strategy="median"), numerical)
    ])

    model = RandomForestRegressor( n_estimators=80,
    max_depth=20,
    random_state=42,
    n_jobs=-1)

    pipe = Pipeline([
        ("prep", preprocessor),
        ("model", model)
    ])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )

    pipe.fit(X_train, y_train)
    preds = pipe.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    # Feature importances
    prep = pipe.named_steps["prep"]
    model = pipe.named_steps["model"]

    try:
        names = prep.get_feature_names_out()
    except:
        names = [f"f{i}" for i in range(len(model.feature_importances_))]

    importances = model.feature_importances_

    fi = pd.DataFrame({"feature": names, "importance": importances})
    fi = fi.sort_values("importance", ascending=False)

    fi.to_csv(f"{OUTPUT_DIR}/feature_importances.csv", index=False)

    # Plot
    top = fi.head(20)
    plt.figure()
    plt.barh(top["feature"], top["importance"])
    plt.gca().invert_yaxis()
    plt.title("Top 20 Feature Importances – Delivery Days")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/feature_importances_top20.png")
    plt.close()

    metrics = {
        "MAE": float(mae),
        "R2": float(r2),
        "train_size": len(X_train),
        "test_size": len(X_test)
    }

    with open(f"{OUTPUT_DIR}/metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    return metrics, fi


def main():
    df = load_data()
    print("Rows:", len(df))

    metrics, fi = train_model(df)
    print("\n=== MODEL RESULTS ===")
    print("MAE:", metrics["MAE"])
    print("R²:", metrics["R2"])
    print("Train size:", metrics["train_size"])
    print("Test size:", metrics["test_size"])
    print("\nTop 10 features:\n", fi.head(10))

    print("\nSaved all outputs to:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
