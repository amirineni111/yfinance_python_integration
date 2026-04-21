"""Quick check of market_context_daily table to see which columns have NULL values"""

import pyodbc
import pandas as pd

server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)

# Get the most recent 5 rows
query = """
SELECT TOP 5 *
FROM market_context_daily
ORDER BY trading_date DESC
"""

df = pd.read_sql(query, conn)

print("Most recent 5 rows in market_context_daily:")
print("=" * 80)
print(df.to_string())
print("\n")

# Check for NULL columns in the most recent row
latest_row = df.iloc[0]
null_cols = [col for col in df.columns if pd.isna(latest_row[col])]

print(f"\nColumns with NULL values in most recent row ({latest_row['trading_date']}):")
print("=" * 80)
if null_cols:
    for col in null_cols:
        print(f"  - {col}")
else:
    print("  No NULL columns found!")

# Check for columns that have data in previous rows but not in latest
print("\n\nColumns that had data before but are NULL now:")
print("=" * 80)
if len(df) > 1:
    prev_row = df.iloc[1]
    newly_null = []
    for col in df.columns:
        if col != 'data_fetched_at' and col != 'trading_date':
            if pd.notna(prev_row[col]) and pd.isna(latest_row[col]):
                newly_null.append(col)
    
    if newly_null:
        for col in newly_null:
            print(f"  - {col}: was {prev_row[col]} on {prev_row['trading_date']}, now NULL")
    else:
        print("  No newly NULL columns detected!")

conn.close()
