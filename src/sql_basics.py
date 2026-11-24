import pandas as pd
import sqlite3
from pathlib import Path

#Create database folder if it doesn't exist
Path("database").mkdir(exist_ok=True)

#Read our original CSV dataset
df = pd.read_csv("data/raw/orders.csv")

#Connect to SQLite database
connection = sqlite3.connect("database/orders.db")

#Load CSV data into a database table
df.to_sql("orders", connection, if_exists="replace", index=False)

#Run our first SQL query
query = "SELECT product, SUM(price * quantity) AS total_revenue " \
        "FROM orders " \
        "WHERE payment_status = 'paid' " \
        "GROUP BY product;"

result = pd.read_sql_query(query, connection)

print(result)

