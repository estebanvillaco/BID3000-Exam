BID3000, Business Intelligence & Data Warehousing (Home Exam)

This project is part of the BID3000, Business Intelligence & Data Warehousing (Autumn 2025) home exam at University of South-Eastern Norway (USN).
The project covers Task 1A (Star Schema Design) and Task 1B (ETL Process) based on the Brazilian E-commerce Dataset (Olist).

Project Structure
```bash
BID3000_Exam/
│
├── Datasets/                 # Raw data (CSV files)
│
├── Database/ # SQL and ERD for the data warehouse
│   ├── schema_creation.sql
│   └── ERD.pdf
│
├── db_config.py              # Database connection setup (SQLAlchemy engine)
│
└── ETL.py                    # Python ETL process
```


1. Install Python

If you don’t have Python installed:

Go to https://www.python.org/downloads/

Download Python 3.10+

During installation:

Check “Add Python to PATH”

Click Install Now

Verify installation:
```bash
python --version
```


2. Install PostgreSQL and pgAdmin 4

Download and install PostgreSQL (includes pgAdmin 4):

https://www.postgresql.org/download/


After installation:

Launch pgAdmin 4

Log in using the user postgres

You can now create databases, schemas, and run SQL via the Query Tool


3. Create the database in pgAdmin 4

Open pgAdmin 4

Right-click Databases → Create → Database

Fill in:

Database name: olist_dw_db
Owner: postgres


Click Save


4. Create schema and tables

Open Query Tool in pgAdmin 4

Open the file schema_creation.sql and run the script


5. Install required Python packages

Open terminal in VS Code or cmd, then run:

```bash
pip install pandas sqlalchemy psycopg2-binary
```

These libraries are used for:

pandas → reading CSV files

SQLAlchemy → connecting Python to PostgreSQL

psycopg2 → running SQL queries


6. Configure db_config.py

The project uses SQLAlchemy to connect Python to the database. Example setup:

```bash
from sqlalchemy import create_engine

def get_engine():
    """
    Creates and returns a SQLAlchemy engine for connecting to the PostgreSQL database.
    """
    username = "postgres"         # PostgreSQL username
    password = "your-password"    # Replace with your actual password
    host = "localhost"            # Server address (default: localhost)
    port = "5432"                 # Default PostgreSQL port
    database = "olist_dw_db"      # Database name

    connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_url)

    return engine
```

Important: Change the password to your actual PostgreSQL password and ensure the database olist_dw_db exists before running the ETL script.


7. Run the ETL process

Execute the Python file ETL.py to load and transform data:

python ETL.py


ETL.py does the following:

Reads data from Datasets/ (inside this project make you own dataset folder and extract the dataset files to your new folder.)

Cleans and transforms data (handles nulls, duplicates, etc.)

Creates dimension and fact tables

Loads data into PostgreSQL via SQLAlchemy engine


8. Verify results in pgAdmin 4

After the ETL script has finished:

Open pgAdmin 4

Navigate to:

Databases → olist_dw_db → Schemas → datawarehouse → Tables


Right-click a table → View/Edit Data → All Rows

Or use Query Tool:

```bash
SELECT COUNT(*) FROM datawarehouse.dim_customers;
SELECT COUNT(*) FROM datawarehouse.fact_orders;
```
You can use these commands above or use olist_dw_data_quality_checks queries file

Technologies Used
Component	Tool
Database	PostgreSQL + pgAdmin 4
ETL	Python (pandas, SQLAlchemy, psycopg2-binary)
Data Modeling	Lucidchart / pgAdmin built-in ERD maker
Dataset	Olist Brazilian E-commerce Dataset
Star Schema Overview
```bash
         dim_customers
              │
              │
dim_products──┼──fact_orders──dim_sellers
              │
              └──dim_time
```
References

Kaggle – Olist E-commerce Dataset

BID3000 – USN course material

Student Candidates
Group work from University of South-Eastern Norway (USN)  Autumn 2025

Campus Ringerike:
Esteban Villacorta, Adrian Lafjell ED, Jonas Ambaya

2025, BID3000 Home Exam, University of South-Eastern Norway
