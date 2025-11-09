from sqlalchemy import create_engine

def get_engine():
    """
    Creates and returns a SQLAlchemy engine for connecting to the PostgreSQL database.
    """
    # --- Connection settings ---
    username = "postgres"         # Change if you use a different PostgreSQL user
    password = "Z4mb0Es27An13Vi04"    # ← replace with your real password
    host = "localhost"            # or your server's hostname/IP
    port = "5432"                 # default PostgreSQL port
    database = "olist_dw_db"      # your database name

    # --- Create connection URL ---
    connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

    # --- Create SQLAlchemy engine ---
    engine = create_engine(connection_url)

    return engine
