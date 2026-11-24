import pandas as pd
import sqlite3
from pathlib import Path
import logging

# E-commerce ETL pipeline

# Add logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Locate project directory
project_root = Path(__file__).resolve().parent.parent

# Locate raw data files for orders
try:
    orders_file = project_root / "data" / "raw" / "orders.csv"

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

    logging.info("Orders successfully extracted.")
    logging.info("CSV structure successfully validated.") 

except FileNotFoundError:
    logging.error("Orders CSV file could not be found!")
    raise
except pd.errors.ParserError:
    logging.error("CSV file could not be parsed.")
    raise

# Locate raw data files for customers
try:
    customers_file = project_root / "data" / "raw" / "customers.csv"
    
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

    logging.info("Customers extraced successfully.")
    logging.info("CSV structure successfully validated.")
except FileNotFoundError:
    logging.error("Customers CSV file could not be found!")
    raise
except pd.errors.ParserError:
    logging.error("CSV file could not be parsed.")
    raise

# Locate raw data files for products
try:
    products_file = project_root / "data" / "raw" / "products.csv"
    
    # Extract customers data
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

    logging.info("Products extraced successfully.")
    logging.info("CSV structure successfully validated.")
except FileNotFoundError:
    logging.error("Products CSV file could not be found!")
    raise
except pd.errors.ParserError:
    logging.error("CSV file could not be parsed.")
    raise

# Inspect Orders data quality
logging.warning(f"Missing values: {orders_df.isnull().sum()}")
logging.warning(f"Duplicate records: {orders_df.duplicated().sum()}")
logging.warning(f"Invalid quantities: {(orders_df["quantity"]<=0).sum()}")

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

# Combine the conditions
invalid_mask = missing_price | invalid_quantity

# Seperate valid from invalid orders
invalid_orders = orders_df[invalid_mask].copy()
valid_orders = orders_df[~invalid_mask].copy()

logging.warning(
    f"Invalid Orders: {len(invalid_orders)}"
)
logging.info(
    f"Valid Orders: {len(valid_orders)}"
)

valid_percentage = (len(valid_orders) / total_orders) * 100
invalid_percentage = (len(invalid_orders) / total_orders) * 100

logging.info(f"Total orders processed: {total_orders}")
logging.info(f"Valid data percentage: {valid_percentage:.2f}%")
logging.info(f"Invalid data percentage: {invalid_percentage:.2f}%")

MAX_INVALID_PERCENTAGE = 40

if invalid_percentage > MAX_INVALID_PERCENTAGE:
    logging.error(
        f"exceeds threshold ({MAX_INVALID_PERCENTAGE}%)"
    )
    raise ValueError("Data quality threshold exceeded.")

else:
    logging.info("Data quality threshold passed.")

# Inspect customer records
required_fields = ["customer_id", "name", "city"]
missing_values = customers_df[required_fields].isnull().any(axis=1)
duplicate_id = customers_df["customer_id"].duplicated(keep=False)

# Combine the conditions
invalid_mask = missing_values | duplicate_id

# Seperate valid from invalid orders
invalid_customers = customers_df[invalid_mask].copy()
valid_customers = customers_df[~invalid_mask].copy()

logging.warning(
    f"Invalid Customers: {len(invalid_customers)}"
)
logging.info(
    f"Valid Customers: {len(valid_customers)}"
)

# Inspect product records
required_fields = ['product_id', 'product', 'category']
missing_values = products_df[required_fields].isnull().any(axis=1)
duplicate_id = products_df["product_id"].duplicated(keep=False)

# Combine the conditions
invalid_mask = missing_values | duplicate_id

# Seperate valid from invalid orders
invalid_products = products_df[invalid_mask].copy()
valid_products = products_df[~invalid_mask].copy()

logging.warning(
    f"Invalid Products: {len(invalid_products)}"
)
logging.info(
    f"Valid Products: {len(valid_products)}"
)

# Function for Record validation
def validate_record_count(table_name, expected_count, actual_count):
    if expected_count == actual_count:
        logging.info(f"{table_name} count validation passed.")
    else:
        logging.error(
            f"{table_name} count mismatch! "
            f"Expected: {expected_count}, Actual: {actual_count}"
        )
        raise ValueError(f"{table_name} data integrity check faild.")

# Load data and Build Star Schema
try:

    # Load cleaned data into SQLite
    database_file = project_root / "database" / "warehouse.db"

    connection = sqlite3.connect(database_file)

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
    connection.execute("DROP TABLE IF EXISTS dim_customers")
    connection.execute("DROP TABLE IF EXISTS dim_products")

    connection.execute("""
    CREATE TABLE dim_customers (
        customer_id TEXT PRIMARY KEY,
        name TEXT,
        city TEXT,
        email TEXT
    );
    """)

    connection.execute("""
    INSERT INTO dim_customers
    SELECT customer_id, name, city, email
    FROM customers;
    """)

    connection.execute("""
    CREATE TABLE dim_products (
        product_id TEXT PRIMARY KEY,
        product TEXT,
        category TEXT);
    """)

    connection.execute("""
    INSERT INTO dim_products
    SELECT product_id, product, category
    FROM products;
    """)

    # Create fact table
    connection.execute("DROP TABLE IF EXISTS fact_orders")

    connection.execute("""
    CREATE TABLE fact_orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        product_id TEXT,
        quantity INTEGER,
        revenue REAL,
        payment_status TEXT
    );
    """)

    connection.execute("""
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
        ON o.product = p.product;
    """)

    connection.commit()
except Exception:
    connection.rollback()
    logging.exception("Database loading failed")
    raise

finally:
    connection.close()

logging.info("Data successfully loaded into warehouse.db")
logging.info("Dimensional tables successfully created in warehouse.db")
logging.info("Fact table successfully created in warehouse.db")

#Verify database Loading
connection = sqlite3.connect(database_file)

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

# Validity check for data integrity
validate_record_count("orders", len(valid_orders), clean_orders)
validate_record_count("rejected_orders", len(invalid_orders), rejected_orders)
validate_record_count("customers", len(valid_customers), clean_customers)
validate_record_count("rejected_customers", len(invalid_customers), rejected_customers)
validate_record_count("products", len(valid_products), clean_products)
validate_record_count("rejected_products", len(invalid_products), rejected_products)

# Referential integrity validation
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
print(connection.execute(query).fetchall())

query = """SELECT dc.city, dp.category, SUM(revenue) AS total_revenue
            FROM fact_orders AS fo
            JOIN dim_customers As dc
                ON fo.customer_id = dc.customer_id
            JOIN dim_products AS dp
                ON fo.product_id = dp.product_id
            GROUP BY dp.category, dc.city"""
total_revenue = connection.execute(query).fetchall()

print(total_revenue)

connection.close()
