import sqlite3
from pathlib import Path

# Locate the project directory
project_root = Path(__file__).resolve().parent.parent

# Locate the database
database_file = project_root / "database" / "orders.db"

# Connect to the database
connection = sqlite3.connect(database_file)

print("Database connection successful!")

query = """
SELECT SUM(price * quantity)
FROM orders
WHERE payment_status = 'paid'
AND price IS NOT NULL
AND quantity > 0;
"""

result = connection.execute(query)

revenue = result.fetchone()[0]

print(f"Total valid paid revenue: €{revenue:.2f}")

query = """
        SELECT COUNT(*)
        FROM orders;
        """
result = connection.execute(query)

order_count = result.fetchone()[0]

print(f"Order count: {order_count}")

connection.close()