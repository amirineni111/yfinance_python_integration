"""Quick check of fundamental data in both tables"""
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\MSSQLSERVER01;"
    "DATABASE=stockdata_db;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Check NSE fundamentals
cursor.execute("SELECT COUNT(*) as cnt, MAX(fetch_date) as max_date FROM nse_500_fundamentals")
row = cursor.fetchone()
print(f"NSE 500 fundamentals: {row[0]} total rows, latest date: {row[1]}")

cursor.execute("SELECT COUNT(*) FROM nse_500_fundamentals WHERE fetch_date = CAST(GETDATE() AS DATE)")
print(f"NSE 500 fundamentals today: {cursor.fetchone()[0]} rows")

# Check NASDAQ fundamentals
cursor.execute("SELECT COUNT(*) as cnt, MAX(fetch_date) as max_date FROM nasdaq_100_fundamentals")
row = cursor.fetchone()
print(f"NASDAQ 100 fundamentals: {row[0]} total rows, latest date: {row[1]}")

cursor.execute("SELECT COUNT(*) FROM nasdaq_100_fundamentals WHERE fetch_date = CAST(GETDATE() AS DATE)")
print(f"NASDAQ 100 fundamentals today: {cursor.fetchone()[0]} rows")

# Last 5 dates for NASDAQ
cursor.execute("SELECT TOP 5 fetch_date, COUNT(*) as cnt FROM nasdaq_100_fundamentals GROUP BY fetch_date ORDER BY fetch_date DESC")
print("\nNASDAQ 100 fundamentals last 5 dates:")
for r in cursor.fetchall():
    print(f"  {r[0]}: {r[1]} tickers")

# Last 5 dates for NSE
cursor.execute("SELECT TOP 5 fetch_date, COUNT(*) as cnt FROM nse_500_fundamentals GROUP BY fetch_date ORDER BY fetch_date DESC")
print("\nNSE 500 fundamentals last 5 dates:")
for r in cursor.fetchall():
    print(f"  {r[0]}: {r[1]} tickers")

conn.close()
