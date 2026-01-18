import pyodbc

try:
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\MSSQLSERVER01;"
        "DATABASE=stockdata_db;"
        "Trusted_Connection=yes;"
    )
    print("✅ Connected successfully")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM nasdaq_top100 WHERE process_flag='y'")
    print(f"Tickers with process_flag='y': {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM nasdaq_top100")
    print(f"Total tickers: {cursor.fetchone()[0]}")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
