import pandas as pd
import time
import sqlite3
from pathlib import Path

# Create a dataset with 100,000 orders
df = pd.DataFrame({
    "order_id": range(100000),
    "quantity": [1, 2, 0, 3, -1] * 20000
})

# Measure vectorised operation
start = time.perf_counter()

invalid_quantity = df["quantity"] <= 0

vectorised_time = time.perf_counter() - start

print(f"Vectorised execution: {vectorised_time:.6f} seconds")
print(f"Invalid records: {invalid_quantity.sum()}")

# Measure Python loop
start = time.perf_counter()

invalid_count = 0

for quantity in df["quantity"]:
    if quantity <= 0:
        invalid_count += 1

loop_time = time.perf_counter() - start

print(f"Loop execution: {loop_time:.6f} seconds")
print(f"Invalid records: {invalid_count}")

project_root = Path(__file__).resolve().parent.parent
database_file = project_root / "database" / "performance_test.db"

connection = sqlite3.connect(database_file)

connection.execute("""
CREATE TABLE IF NOT EXISTS performance_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id TEXT,
    amount REAL
);
""")

# Generate 100,000 test orders
orders = [
    (i, f"C{i % 1000:04d}", 50.0 + (i % 100))
    for i in range(100000)
]

# Insert orders into SQLite
connection.executemany("""
INSERT OR REPLACE INTO performance_orders
(order_id, customer_id, amount)
VALUES (?, ?, ?);
""", orders)

connection.commit()

print("100,000 test orders inserted.")

# Search without an index
start = time.perf_counter()

result = connection.execute("""
SELECT *
FROM performance_orders
WHERE customer_id = 'C0001';
""").fetchall()

without_index_time = time.perf_counter() - start

print(f"Records found: {len(result)}")
print(f"Search without index: {without_index_time:.6f} seconds")

# Create an index
connection.execute("""
CREATE INDEX IF NOT EXISTS idx_performance_customer
ON performance_orders(customer_id);
""")

# Search with an index
start = time.perf_counter()

result = connection.execute("""
SELECT *
FROM performance_orders
WHERE customer_id = 'C0001';
""").fetchall()

with_index_time = time.perf_counter() - start

print(f"Records found: {len(result)}")
print(f"Search with index: {with_index_time:.6f} seconds")

connection.close()