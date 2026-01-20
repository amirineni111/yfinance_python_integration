import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\MSSQLSERVER01;"
    "DATABASE=stockdata_db;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Update all NASDAQ tickers to process_flag='y'
cursor.execute("UPDATE nasdaq_top100 SET process_flag = 'y'")
conn.commit()

cursor.execute("SELECT COUNT(*) FROM nasdaq_top100 WHERE process_flag='y'")
count = cursor.fetchone()[0]
print(f"✅ Updated {count} NASDAQ tickers to process_flag='y'")

conn.close()
