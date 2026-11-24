import sqlite3
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
database_file = project_root/"database"/"orders.db"

connection = sqlite3.connect(database_file)

# Create customers table
connection.executescript("""
                            DROP TABLE IF EXISTS customers;
                            CREATE TABLE IF NOT EXISTS customers (
                            customer_id TEXT PRIMARY KEY,
                            name TEXT,
                            city TEXT);
                            """)

# Insert sample customers
customers = [
    ("C001", "Alex", "Munich"),
    ("C002", "Sarah", "Berlin"),
    ("C003", "James", "Hamburg"),
    ("C004", "Lisa", "Munich"),
    ("C005", "Michael", "Frankfurt"),
    ("C006", "Emma", "Cologne"),
    ("C007", "Daniel", "Stuttgart")
]

connection.executemany(""" 
                        INSERT OR REPLACE INTO customers
                        (customer_id, name, city)
                        VALUES (?, ?, ?)
                        """, customers)

connection.commit()

# Run INNER JOIN
query = """ 
        SELECT customers.name, orders.product, orders.price
        FROM customers
        INNER JOIN orders
        ON customers.customer_id = orders.customer_id;
        """

result = connection.execute(query).fetchall()

for row in result:
     print(row)


# Run LEFT JOIN
query = """ 
        SELECT customers.name, orders.product, orders.price
        FROM customers
        LEFT JOIN orders
        ON customers.customer_id = orders.customer_id;
        """

result = connection.execute(query).fetchall()

for row in result:
    print(row)

# Create table for customers that have never placed an order
query = """ 
        SELECT customers.name
        FROM customers
        LEFT JOIN orders
        ON customers.customer_id = orders.customer_id
        WHERE orders.customer_id IS NULL;
        """

result = connection.execute(query).fetchall()

for row in result:
    print(row)

# Create table for how many orders each customer has placed
query = """ 
        SELECT customers.name, COUNT(orders.order_id) AS number_of_orders
        FROM customers
        LEFT JOIN orders
        ON customers.customer_id = orders.customer_id
        GROUP BY customers.customer_id, customers.name;
        """

result = connection.execute(query).fetchall()

for row in result:
    print(row)

connection.close()