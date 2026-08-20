import random
import time
from datetime import datetime
import pyodbc

# Azure SQL Connection Details
server = "kafka-demo-sql-server.database.windows.net"
database = "TransactionAnalyticsDB"
username = "sqladmin"
password = ""  # Replace with your actual SQL admin password
driver = "{ODBC Driver 18 for SQL Server}"

# Connection string
connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

print("Connecting to Azure SQL Database...")
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
print("Connected successfully! Starting transaction generation...")

# Sample data lists for realistic generation
customers = [
    "Alice Smith",
    "Bob Jones",
    "Charlie Brown",
    "Diana Prince",
    "Ethan Hunt",
]
categories = ["Electronics", "Clothing", "Groceries", "Home", "Entertainment"]

try:
    while True:
        # Generate random transaction details
        customer = random.choice(customers)
        amount = round(random.uniform(10.0, 1000.0), 2)
        category = random.choice(categories)
        timestamp = datetime.now()

        # SQL Insert Query
        insert_query = """
            INSERT INTO dbo.SourceTransactions (CustomerName, TransactionAmount, Category, TransactionDate)
            VALUES (?, ?, ?, ?)
        """

        cursor.execute(insert_query, (customer, amount, category, timestamp))
        conn.commit()

        print(
            f"Inserted -> Customer: {customer}, Amount: ${amount}, Category: {category}"
        )

        # Wait 3 seconds before generating the next transaction
        time.sleep(3)
       

except KeyboardInterrupt:
    print("\nStopping transaction generator...")
finally:
    cursor.close()
    conn.close()
    print("Database connection closed.")
