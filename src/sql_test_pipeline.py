import sqlite3
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
database_file = project_root / "database" / "warehouse.db"

connection = sqlite3.connect(database_file)

connection.execute("""
CREATE TABLE IF NOT EXISTS transaction_test (
    test_id INTEGER PRIMARY KEY,
    description TEXT
);
""")

connection.commit()
connection.close()

connection = sqlite3.connect(database_file)

try:
    connection.execute("""
    INSERT INTO transaction_test (description)
    VALUES ('First record');
    """)

    connection.execute("""
    INSERT INTO transaction_test (description)
    VALUES ('Second record');
    """)



    connection.commit()

except Exception:
    connection.rollback
    print("Transaction rolled back.")

finally:
    connection.close()

connection = sqlite3.connect(database_file)

records = connection.execute("""
SELECT * FROM transaction_test;
""").fetchall()

print("Records remaining:", records)

connection.close()