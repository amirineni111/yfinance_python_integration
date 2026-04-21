"""Delete today's incomplete market_context_daily row"""
import pyodbc
from datetime import date

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost\\MSSQLSERVER01;'
    'DATABASE=stockdata_db;'
    'Trusted_Connection=yes;'
)

cursor = conn.cursor()
today = date(2026, 4, 21)
cursor.execute("DELETE FROM market_context_daily WHERE trading_date = ?", today)
conn.commit()
print(f'Deleted {cursor.rowcount} row(s) for {today}')
conn.close()
