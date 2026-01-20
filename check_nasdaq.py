import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\MSSQLSERVER01;"
    "DATABASE=stockdata_db;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Check NASDAQ table
cursor.execute("SELECT COUNT(*) FROM nasdaq_top100")
nasdaq_total = cursor.fetchone()[0]
print(f"Total NASDAQ tickers in table: {nasdaq_total}")

cursor.execute("SELECT TOP 5 ticker, company_name FROM nasdaq_top100")
print("\nSample NASDAQ tickers:")
for row in cursor.fetchall():
    print(f"  {row[0]} - {row[1]}")

# Check if fundamental table exists
cursor.execute("SELECT COUNT(*) FROM nasdaq_100_fundamentals")
nasdaq_funds = cursor.fetchone()[0]
print(f"\nRecords in nasdaq_100_fundamentals: {nasdaq_funds}")

conn.close()
