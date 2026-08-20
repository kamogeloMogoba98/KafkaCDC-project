import json
import time
from kafka import KafkaProducer
import pyodbc

# Azure SQL Connection details
server = "kafka-demo-sql-server.database.windows.net"
database = "TransactionAnalyticsDB"
username = "sqladmin"
password = ""
driver = "{ODBC Driver 18 for SQL Server}"

connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"

KAFKA_BOOTSTRAP_SERVERS = ["40.123.240.176:9094"]
KAFKA_TOPIC = "financial-transactions-cdc"

print("Connecting to Kafka Producer...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
print("Connected to Kafka successfully!")

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Track the last check time to only fetch new/modified records
last_checked_time = "2026-01-01 00:00:00"

print("Listening for changes (Inserts, Updates, Soft Deletes)...")

try:
    while True:
        query = """
            SELECT TransactionID, CustomerName, TransactionAmount, Category, TransactionDate, IsDeleted, LastModifiedDate
            FROM SourceTransactions
            WHERE LastModifiedDate > ?
            ORDER BY LastModifiedDate ASC
        """
        cursor.execute(query, (last_checked_time,))
        rows = cursor.fetchall()

        for row in rows:
            tx_id = str(row[0])
            customer = row[1]
            amount = float(row[2]) if row[2] is not None else 0.0
            category = row[3]
            tx_date = str(row[4])
            is_deleted = bool(row[5])
            modified_date = row[6]

            # Determine operation type based on columns
            op_type = "DELETE" if is_deleted else "UPSERT"

            transaction = {
                "operation": op_type,
                "transaction_id": tx_id,
                "customer_name": customer,
                "amount": amount,
                "category": category,
                "transaction_date": tx_date,
            }

            producer.send(KAFKA_TOPIC, value=transaction)
            print(f"Produced [{op_type}] -> ID: {tx_id}")
           

            # Update our watermark to the latest modified timestamp processed
            last_checked_time = modified_date

        producer.flush()
        time.sleep(5)

except KeyboardInterrupt:
    print("Producer stopped by user.")
finally:
    cursor.close()
    conn.close()
    producer.close()
