
import sqlite3
from pathlib import Path

# Locate the project and database
project_root = Path(__file__).resolve().parent.parent
database_file = project_root / "database" / "orders.db"

connection = sqlite3.connect(database_file)


# --------------------------------------------------
# 1. BASIC SUBQUERY
# Find orders with a price above the average price
# --------------------------------------------------

query = """
SELECT *
FROM orders
WHERE price > (
    SELECT AVG(price)
    FROM orders
);
"""

result = connection.execute(query).fetchall()
print("\n1. Orders above average price:")
for row in result:
    print(row)


# --------------------------------------------------
# 2. IN
# Find customers who have placed at least one order
# --------------------------------------------------

query = """
SELECT *
FROM customers
WHERE customer_id IN (
    SELECT customer_id
    FROM orders
);
"""

result = connection.execute(query).fetchall()
print("\n2. Customers who have placed orders:")
for row in result:
    print(row)


# --------------------------------------------------
# 3. NOT IN
# Find customers who have never placed an order
# --------------------------------------------------

query = """
SELECT *
FROM customers
WHERE customer_id NOT IN (
    SELECT customer_id
    FROM orders
);
"""

result = connection.execute(query).fetchall()
print("\n3. Customers without orders:")
for row in result:
    print(row)


# --------------------------------------------------
# 4. EXISTS
# Check whether a customer has at least one order
# --------------------------------------------------

query = """
SELECT *
FROM customers
WHERE EXISTS (
    SELECT 1
    FROM orders
    WHERE orders.customer_id = customers.customer_id
);
"""

result = connection.execute(query).fetchall()
print("\n4. Customers with matching orders (EXISTS):")
for row in result:
    print(row)


# --------------------------------------------------
# 5. NOT EXISTS
# Find customers without matching orders
# --------------------------------------------------

query = """
SELECT *
FROM customers
WHERE NOT EXISTS (
    SELECT 1
    FROM orders
    WHERE orders.customer_id = customers.customer_id
);
"""

result = connection.execute(query).fetchall()
print("\n5. Customers without matching orders (NOT EXISTS):")
for row in result:
    print(row)


# --------------------------------------------------
# 6. CORRELATED SUBQUERY
# EXISTS references the outer customers table
# --------------------------------------------------

query = """
SELECT *
FROM customers
WHERE EXISTS (
    SELECT 1
    FROM orders
    WHERE orders.customer_id = customers.customer_id
);
"""

result = connection.execute(query).fetchall()
print("\n6. Correlated subquery:")
for row in result:
    print(row)


# --------------------------------------------------
# 7. AGGREGATE SUBQUERY WITH HAVING
# Find customers whose total spending is higher
# than the average paid order value
# --------------------------------------------------

query = """
SELECT
    customer_id,
    SUM(price * quantity) AS total_spent
FROM orders
WHERE payment_status = 'paid'
GROUP BY customer_id
HAVING SUM(price * quantity) > (
    SELECT AVG(price * quantity)
    FROM orders
    WHERE payment_status = 'paid'
);
"""

result = connection.execute(query).fetchall()
print("\n7. Customers spending above average order value:")
for row in result:
    print(row)


# Close the database connection
connection.close()
