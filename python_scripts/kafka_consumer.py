from datetime import datetime
import json
from kafka import KafkaConsumer
import pyodbc

# Azure SQL Connection details
server = "kafka-demo-sql-server.database.windows.net"
database = "TransactionAnalyticsDB"
username = "sqladmin"
password = ""
driver = "{ODBC Driver 18 for SQL Server}"

connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"

# Kafka Broker details
KAFKA_BOOTSTRAP_SERVERS = ["40.123.240.176:9094"]
KAFKA_TOPIC = "financial-transactions-cdc"

print("Connecting to Kafka Consumer...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="transaction-analytics-group-v5",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
print("Connected to Kafka successfully! Listening for events...")

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

try:
    for message in consumer:
        tx = message.value
        op_type = tx.get("operation")
        tx_id = tx.get("transaction_id")
        amount = tx.get("amount")
        category = tx.get("category")
        tx_date_str = tx.get("transaction_date")

        try:
            summary_date = datetime.strptime(
                tx_date_str.split()[0], "%Y-%m-%d"
            ).date()
        except Exception:
            summary_date = datetime.now().date()

        print(
            f"Processing Event [{op_type}] -> ID: {tx_id}, Amount: {amount}, Date: {summary_date}"
        )

        # IDEMPOTENT RECALCULATION APPROACH:
        # Instead of incrementing/decrementing blindly (which causes duplicates if messages replay),
        # we recalculate the exact totals for that day directly from the source state or use a safe MERGE.
        
        cursor.execute(
            """
            MERGE INTO TransactionAnalytics AS target
            USING (
                SELECT 
                    CAST(TransactionDate AS DATE) AS SummaryDate,
                    COUNT(TransactionID) AS TotalTransactions,
                    SUM(TransactionAmount) AS TotalRevenue,
                    (
                        SELECT TOP 1 Category 
                        FROM SourceTransactions st2 
                        WHERE CAST(st2.TransactionDate AS DATE) = CAST(st1.TransactionDate AS DATE) 
                          AND st2.IsDeleted = 0
                        GROUP BY Category 
                        ORDER BY COUNT(*) DESC
                    ) AS DominantCategory
                FROM SourceTransactions st1
                WHERE CAST(st1.TransactionDate AS DATE) = ? AND st1.IsDeleted = 0
                GROUP BY CAST(TransactionDate AS DATE)
            ) AS source
            ON target.SummaryDate = source.SummaryDate
            WHEN MATCHED THEN
                UPDATE SET 
                    target.TotalTransactions = source.TotalTransactions,
                    target.TotalRevenue = source.TotalRevenue,
                    target.DominantCategory = source.DominantCategory,
                    target.LastUpdatedTimestamp = SYSDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (SummaryDate, TotalTransactions, TotalRevenue, DominantCategory, LastUpdatedTimestamp)
                VALUES (source.SummaryDate, source.TotalTransactions, source.TotalRevenue, source.DominantCategory, SYSDATETIME());
        """,
            summary_date,
        )

        conn.commit()

except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    cursor.close()
    conn.close()
    consumer.close()
