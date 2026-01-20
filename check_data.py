import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\MSSQLSERVER01;"
    "DATABASE=stockdata_db;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Check NASDAQ table
cursor.execute("SELECT COUNT(*) FROM nasdaq_top100 WHERE process_flag='y'")
nasdaq_flagged = cursor.fetchone()[0]
print(f"NASDAQ tickers with process_flag='y': {nasdaq_flagged}")

cursor.execute("SELECT COUNT(*) FROM nasdaq_100_fundamentals")
nasdaq_funds = cursor.fetchone()[0]
print(f"Records in nasdaq_100_fundamentals: {nasdaq_funds}")

# Check NSE table
cursor.execute("SELECT COUNT(*) FROM nse_500 WHERE process_flag='y'")
nse_flagged = cursor.fetchone()[0]
print(f"NSE tickers with process_flag='y': {nse_flagged}")

cursor.execute("SELECT COUNT(*) FROM nse_500_fundamentals")
nse_funds = cursor.fetchone()[0]
print(f"Records in nse_500_fundamentals: {nse_funds}")

conn.close()
