"""
Script to:
1. Export existing tickers from nasdaq_top100 and nse_500 tables
2. Fetch top 1000 NASDAQ and NSE 1000 ticker lists
3. Compare and find missing tickers
4. Generate SQL INSERT scripts for the missing ones
"""
import pyodbc
import json

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost\\MSSQLSERVER01;'
    'DATABASE=stockdata_db;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Get all existing NASDAQ tickers
cursor.execute('SELECT ticker FROM nasdaq_top100')
existing_nasdaq = set(r[0] for r in cursor.fetchall())

# Get all existing NSE tickers
cursor.execute('SELECT ticker FROM nse_500')
existing_nse = set(r[0] for r in cursor.fetchall())

# Check table columns
cursor.execute("""
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'nasdaq_top100' ORDER BY ORDINAL_POSITION
""")
nasdaq_cols = [r[0] for r in cursor.fetchall()]
print('nasdaq_top100 columns:', nasdaq_cols)

cursor.execute("""
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'nse_500' ORDER BY ORDINAL_POSITION
""")
nse_cols = [r[0] for r in cursor.fetchall()]
print('nse_500 columns:', nse_cols)

# Save existing tickers to files
with open('existing_nasdaq_tickers.txt', 'w') as f:
    for t in sorted(existing_nasdaq):
        f.write(t + '\n')

with open('existing_nse_tickers.txt', 'w') as f:
    for t in sorted(existing_nse):
        f.write(t + '\n')

print(f'\nExisting NASDAQ tickers: {len(existing_nasdaq)}')
print(f'Existing NSE tickers: {len(existing_nse)}')

# Print first 10 of each for verification
print('\nSample NASDAQ tickers:', sorted(existing_nasdaq)[:10])
print('Sample NSE tickers:', sorted(existing_nse)[:10])

# Check if NSE tickers have .NS suffix
nse_with_ns = [t for t in existing_nse if t.endswith('.NS')]
nse_without_ns = [t for t in existing_nse if not t.endswith('.NS')]
print(f'\nNSE with .NS suffix: {len(nse_with_ns)}')
print(f'NSE without .NS suffix: {len(nse_without_ns)}')
if nse_without_ns:
    print('NSE without .NS:', nse_without_ns[:5])

conn.close()
print('\nDone. Now run step 2 to fetch top 1000 lists.')
