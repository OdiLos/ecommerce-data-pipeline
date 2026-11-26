import pandas as pd
import sqlite3
from pathlib import Path
import logging
import time
from sqlalchemy import create_engine, text, event

# E-commerce ETL pipeline
# Data quality validation and reporting

# Initiate pipeline timer
pipeline_start = time.perf_counter()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Locate project directory
project_root = Path(__file__).resolve().parent.parent

# Define the path to the data warehouse database
database_file = project_root / "database" / "warehouse.db"

# Create a SQLAlchemy engine to manage database connections and transactions
engine = create_engine(f"sqlite:///{database_file}")
# Configure SQLite to use explicit transaction control
@event.listens_for(engine, "connect")
def configure_sqlite(dbapi_connection, connection_record):
    dbapi_connection.isolation_level = None


@event.listens_for(engine, "begin")
def begin_transaction(connection):
    connection.exec_driver_sql("BEGIN")

# Define the path to raw CSV files
orders_file = project_root / "data" / "raw" / "orders.csv"
customers_file = project_root / "data" / "raw" / "customers.csv"
products_file = project_root / "data" / "raw" / "products.csv"

# Locate raw data files for orders
try:
    # Extract orders data
    orders_df = pd.read_csv(orders_file)
    required_columns = [
        "order_id", "customer_id", "product",
        "price", "quantity", "payment_status"
    ]

    missing_columns = [
        column for column in required_columns
        if column not in orders_df.columns
    ]

    if missing_columns:
        logging.error(
            f"Missing required columns: {missing_columns}"
        )
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    logging.info("Orders extracted and validated successfully.")
except FileNotFoundError:
    logging.error("Orders CSV file could not be found!")
    raise
except pd.errors.ParserError:
    logging.error("Orders CSV file could not be parsed.")
    raise
except Exception:
    logging.exception("Unexpected error while extracting orders data.")
    raise

# Locate raw data files for customers
try:
    # Extract customers data
    customers_df = pd.read_csv(customers_file)
    required_columns = [
        'customer_id', 'name', 'city', 'email'
    ]

    missing_columns = [
        column for column in required_columns
        if column not in customers_df.columns
    ]

    if missing_columns:
        logging.error(
            f"Missing required columns: {missing_columns}"
        )
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    logging.info("Customers extracted and validated successfully.")
except FileNotFoundError:
    logging.error("Customers CSV file could not be found!")
    raise
except pd.errors.ParserError:
    logging.error("Customers CSV file could not be parsed.")
    raise
except Exception:
    logging.exception("Unexpected error while extracting customers data.")
    raise

# Locate raw data files for products
try:
    # Extract products data
    products_df = pd.read_csv(products_file)
    required_columns = [
        'product_id', 'product', 'category'
    ]

    missing_columns = [
        column for column in required_columns
        if column not in products_df.columns
    ]

    if missing_columns:
        logging.error(
            f"Missing required columns: {missing_columns}"
        )
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    logging.info("Products extracted and validated successfully.")
except FileNotFoundError:
    logging.error("Products CSV file could not be found!")
    raise
except pd.errors.ParserError:
    logging.error("Products CSV file could not be parsed.")
    raise
except Exception:
    logging.exception("Unexpected error while extracting products data.")
    raise

# Inspect Orders data quality
logging.debug(f"Missing values: {orders_df.isnull().sum()}")
logging.debug(f"Duplicate order records: {orders_df.duplicated().sum()}")
logging.debug(f"Invalid quantities: {(orders_df['quantity']<=0).sum()}")

# Revome exact duplicate records from orders
original_count = len(orders_df)

orders_df = orders_df.drop_duplicates()

total_orders = len(orders_df)

duplicates_removed = original_count - total_orders


logging.info(f"Duplicates removed: {duplicates_removed}")
logging.info(f"Remaining records: {total_orders}")

# Identify invalid records
missing_price = orders_df["price"].isna()
invalid_quantity = orders_df["quantity"] <= 0
missing_quantity = orders_df["quantity"].isna()

# Combine the conditions
invalid_mask = missing_price | invalid_quantity | missing_quantity

# Seperate valid from invalid orders
invalid_orders = orders_df[invalid_mask].copy()
valid_orders = orders_df[~invalid_mask].copy()

valid_order_count = len(valid_orders)
invalid_order_count = len(invalid_orders)

logging.warning(
    f"Invalid Orders: {invalid_order_count}"
)
logging.info(
    f"Valid Orders: {valid_order_count}"
)

valid_percentage = (valid_order_count / total_orders) * 100
invalid_percentage = (invalid_order_count / total_orders) * 100

logging.info(f"Total orders processed: {total_orders}")
logging.info(f"Valid data percentage: {valid_percentage:.2f}%")
logging.info(f"Invalid data percentage: {invalid_percentage:.2f}%")

MAX_INVALID_PERCENTAGE = 30

if invalid_percentage > MAX_INVALID_PERCENTAGE:
    logging.error(
        f"Invalid data percentage ({invalid_percentage:.2f}%)"
        f"exceeds threshold ({MAX_INVALID_PERCENTAGE}%)"
    )
    raise ValueError("Data quality threshold exceeded.")

logging.info(f"Data quality threshold passed: "
             f"{invalid_percentage:.2f}% invalid "
             f"(maximum allowed: {MAX_INVALID_PERCENTAGE:.2f}%)")

# Inspect Customer data quality
required_fields = ["customer_id", "name", "city"]

logging.debug(f"Duplicate customer IDs: {customers_df['customer_id'].duplicated().sum()}")

missing_values = customers_df[required_fields].isnull().any(axis=1)
duplicate_id = customers_df["customer_id"].duplicated(keep=False)

# Combine the conditions
invalid_mask = missing_values | duplicate_id

# Seperate valid from invalid customers
invalid_customers = customers_df[invalid_mask].copy()
valid_customers = customers_df[~invalid_mask].copy()

valid_customer_count = len(valid_customers)
invalid_customer_count = len(invalid_customers)

logging.info(
    f"Customers validated: {valid_customer_count} valid, {invalid_customer_count} rejected"
)

# Inspect Product data quality
required_fields = ['product_id', 'product', 'category']

logging.debug(f"Duplicate product IDs: {products_df['product_id'].duplicated().sum()}")

missing_values = products_df[required_fields].isnull().any(axis=1)
duplicate_id = products_df["product_id"].duplicated(keep=False)

# Combine the conditions
invalid_mask = missing_values | duplicate_id

# Seperate valid from invalid products
invalid_products = products_df[invalid_mask].copy()
valid_products = products_df[~invalid_mask].copy()

valid_product_count = len(valid_products)
invalid_product_count = len(invalid_products)

logging.info(
    f"Products validated: {valid_product_count} valid, {invalid_product_count} rejected"
)


# Function for Record validation
def validate_record_count(table_name, expected_count, actual_count):
    if expected_count != actual_count:
        logging.error(
            f"{table_name} count validation failed: "
            f"Expected: {expected_count}, Actual: {actual_count}"
        )
        raise ValueError(f"{table_name} data integrity check failed.")

    logging.info(f"{table_name} count validation passed: "
                 f"Expected: {expected_count}, Actual: {actual_count}")

# Load data and Build Star Schema
try:
    # Load cleaned data into SQLite
    with engine.begin() as connection:

        valid_orders.to_sql(
            "orders",
            connection,
            if_exists="replace",
            index=False
        )

        invalid_orders.to_sql(
            "rejected_orders",
            connection,
            if_exists="replace",
            index=False
        )

        valid_customers.to_sql(
            "customers",
            connection,
            if_exists="replace",
            index=False
        )

        invalid_customers.to_sql(
            "rejected_customers",
            connection,
            if_exists="replace",
            index=False
        )

        valid_products.to_sql(
            "products",
            connection,
            if_exists="replace",
            index=False
        )

        invalid_products.to_sql(
            "rejected_products",
            connection,
            if_exists="replace",
            index=False
        )



        # Building Star Schema
        # Create dimensional tables
        connection.execute(text("DROP TABLE IF EXISTS dim_customers"))
        connection.execute(text("DROP TABLE IF EXISTS dim_products"))

        connection.execute(text("""
        CREATE TABLE dim_customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT,
            city TEXT,
            email TEXT
        );
        """))

        connection.execute(text("""
        INSERT INTO dim_customers
        SELECT customer_id, name, city, email
        FROM customers;
        """))

        connection.execute(text("""
        CREATE TABLE dim_products (
            product_id TEXT PRIMARY KEY,
            product TEXT,
            category TEXT);
        """))

        connection.execute(text("""
        INSERT INTO dim_products
        SELECT product_id, product, category
        FROM products;
        """))

        # Create fact table
        connection.execute(text("DROP TABLE IF EXISTS fact_orders"))

        connection.execute(text("""
        CREATE TABLE fact_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            product_id TEXT,
            quantity INTEGER,
            revenue REAL,
            payment_status TEXT
        );
        """))
        
        # Referential integrity validation for fact table
        missing_records = pd.read_sql_query("""
        SELECT 
            o.order_id, 
            c.customer_id, 
            p.product_id, 
            o.quantity, 
            o.price * o.quantity AS revenue, 
            o.payment_status
        FROM orders AS o
        LEFT JOIN customers AS c
            ON o.customer_id = c.customer_id
        LEFT JOIN products AS p
            ON o.product = p.product
        WHERE c.customer_id IS NULL
            OR p.product_id IS NULL;
        """, connection)

        if not missing_records.empty:
            raise ValueError(
                f"Fact table validation failed: "
                f"{len(missing_records)} orders have missing references"
            )

        connection.execute(text("""
        INSERT INTO fact_orders
        SELECT 
            o.order_id, 
            c.customer_id, 
            p.product_id, 
            o.quantity, 
            o.price * o.quantity AS revenue, 
            o.payment_status
        FROM orders AS o
        JOIN customers AS c
            ON o.customer_id = c.customer_id
        JOIN products AS p
            ON o.product = p.product
        """))

except Exception:
    logging.exception("Database loading failed")
    raise

logging.info("Data successfully loaded and star schema created in warehouse.db")


# Open a SQLite connection to verify the loaded data
connection = sqlite3.connect(database_file)

#Verify database Loading
clean_orders = connection.execute(
    "SELECT COUNT(*) FROM orders"
).fetchone()[0]

rejected_orders = connection.execute(
    "SELECT COUNT(*) FROM rejected_orders"
).fetchone()[0]

clean_customers = connection.execute(
    "SELECT COUNT(*) FROM customers"
).fetchone()[0]

rejected_customers = connection.execute(
    "SELECT COUNT(*) FROM rejected_customers"
).fetchone()[0]

clean_products = connection.execute(
    "SELECT COUNT(*) FROM products"
).fetchone()[0]

rejected_products = connection.execute(
    "SELECT COUNT(*) FROM rejected_products"
).fetchone()[0]

dim_customers_count = connection.execute(
    "SELECT COUNT(*) FROM dim_customers"
).fetchone()[0]

dim_products_count = connection.execute(
    "SELECT COUNT(*) FROM dim_products"
).fetchone()[0]

fact_orders_count = connection.execute(
    "SELECT COUNT(*) FROM fact_orders"
).fetchone()[0]

# Validity check for data integrity
validate_record_count("orders", valid_order_count, clean_orders)
validate_record_count("rejected_orders", invalid_order_count, rejected_orders)
validate_record_count("customers", valid_customer_count, clean_customers)
validate_record_count("rejected_customers", invalid_customer_count, rejected_customers)
validate_record_count("products", valid_product_count, clean_products)
validate_record_count("rejected_products", invalid_product_count, rejected_products)
validate_record_count("dim_customers", valid_customer_count, dim_customers_count)
validate_record_count("dim_products", valid_product_count, dim_products_count)
validate_record_count("fact_orders", valid_order_count, fact_orders_count)

# Referential integrity validation for loaded star schema
query = """SELECT
                    fo.order_id,
                    fo.customer_id,
                    fo.product_id
            FROM fact_orders AS fo
            LEFT JOIN dim_customers As dc
                ON fo.customer_id = dc.customer_id
            LEFT JOIN dim_products AS dp
                ON fo.product_id = dp.product_id
            WHERE dc.customer_id IS NULL
                OR dp.product_id IS NULL;"""

integrity_issues = connection.execute(query).fetchall()

if integrity_issues:
    logging.error(f"Referential integrity validation: FAILED "
                  f"{len(integrity_issues)} unmatched records found.")
    raise ValueError("Referential integrity validation failed.")
else:
    logging.info("Referential integrity validation: PASSED")

connection.close()


logging.info("========== PIPELINE SUMMARY ==========")
logging.info(f"Orders processed: {total_orders}")
logging.info(f"Valid orders: {valid_order_count}")
logging.info(f"Rejected orders: {invalid_order_count}")
logging.info(f"Customers loaded: {valid_customer_count}")
logging.info(f"Products loaded: {valid_product_count}")

# End pipeline timer
pipeline_end = time.perf_counter()

# Calculate pipeline duration
execution_time = pipeline_end - pipeline_start

logging.info(
    f"Pipeline execution time: {execution_time:.2f} seconds"
)

# Dispose of SQLAlchemy engine and release pooled connections
engine.dispose()