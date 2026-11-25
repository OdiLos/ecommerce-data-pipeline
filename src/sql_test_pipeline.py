import sqlite3
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
database_file = project_root / "database" / "warehouse.db"

connection = sqlite3.connect(database_file)

# Create test table
connection.execute("""
CREATE TABLE IF NOT EXISTS upsert_test (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    quantity INTEGER
);
""")

connection.execute("""
CREATE TABLE IF NOT EXISTS pipeline_checkpoint (
    pipeline_name TEXT PRIMARY KEY,
    last_processed_batch INTEGER
);
""")

connection.execute("""
INSERT OR IGNORE INTO pipeline_checkpoint
(pipeline_name, last_processed_batch)
VALUES ('orders_pipeline', 6);
""")

# Insert or update initial record
connection.execute("""
INSERT INTO upsert_test
(order_id, customer_id, quantity)
VALUES ('1001', 'C001', 2)
ON CONFLICT(order_id)
DO UPDATE SET
    quantity = excluded.quantity;
""")

# Insert or update second record
connection.execute("""
INSERT INTO upsert_test
(order_id, customer_id, quantity)
VALUES ('1002', 'C002', 1)
ON CONFLICT(order_id)
DO UPDATE SET
    quantity = excluded.quantity;
""")

connection.commit()

# Verify initial record
records = connection.execute("""
SELECT * FROM upsert_test;
""").fetchall()

print("Current records:", records)

checkpoint = connection.execute("""
SELECT last_processed_batch
FROM pipeline_checkpoint
WHERE pipeline_name = 'orders_pipeline';
""").fetchone()

if checkpoint:
    last_processed_batch = checkpoint[0]
else:
    last_processed_batch = 0

next_batch = last_processed_batch + 1

print("Last successful batch:", checkpoint[0])
print("Resuming from batch:", next_batch)

for batch in range(next_batch, 11):
    print(f"Processing batch {batch}")

    connection.execute("""
    INSERT OR REPLACE INTO pipeline_checkpoint
    (pipeline_name, last_processed_batch)
    VALUES ('orders_pipeline', ?);
    """, (batch,))

    connection.commit()

    print(f"Checkpoint saved: batch {batch}")

connection.close()