"""
This script updates the NSE_500 table with industry classification data
It fetches sector, industry, and sub-industry information from Yahoo Finance
"""

import yfinance as yf
import pyodbc
from datetime import datetime
import time

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
table_name = "nse_500"

# Establish connection using Windows Authentication
try:
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    print("✅ Connected to SQL Server successfully.")
except Exception as e:
    print("❌ Failed to connect to SQL Server:", e)
    exit()

# Fetch only tickers where sector is NULL (not yet populated)
print("\n📊 Fetching tickers from NSE_500 table where sector is NULL...")
cursor.execute(f"""
    SELECT ticker, company_name 
    FROM {table_name} 
    WHERE sector IS NULL OR sector = ''
""")
tickers = cursor.fetchall()

if not tickers:
    print("✅ No tickers found that need updating. All records already have industry data!")
    cursor.close()
    conn.close()
    exit()

print(f"✅ Found {len(tickers)} tickers that need updating.\n")

# Update each ticker with industry information
successful_updates = 0
failed_updates = 0

for idx, (ticker, company_name) in enumerate(tickers, 1):
    print(f"[{idx}/{len(tickers)}] Processing {ticker} - {company_name}...")
    
    try:
        # Fetch stock information from Yahoo Finance
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Extract industry classification data
        sector = info.get('sector', None)
        industry = info.get('industry', None)
        
        # Yahoo Finance doesn't provide sub_industry separately, 
        # so we'll use industryDisp or industryKey as fallback
        sub_industry = info.get('industryDisp', info.get('industryKey', None))
        
        # Get business summary
        business_summary = info.get('longBusinessSummary', None)
        
        # Truncate business_summary if too long (SQL VARCHAR(MAX) can handle it, but let's be safe)
        if business_summary and len(business_summary) > 5000:
            business_summary = business_summary[:5000]
        
        # Update the database
        update_query = f"""
        UPDATE {table_name}
        SET 
            sector = ?,
            industry = ?,
            sub_industry = ?,
            business_summary = ?,
            last_updated = ?
        WHERE ticker = ?
        """
        
        cursor.execute(
            update_query,
            sector,
            industry,
            sub_industry,
            business_summary,
            datetime.now(),
            ticker
        )
        conn.commit()
        
        print(f"   ✅ Updated: Sector={sector}, Industry={industry}")
        successful_updates += 1
        
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
        
    except Exception as e:
        print(f"   ⚠ Error updating {ticker}: {e}")
        failed_updates += 1
        continue

# Summary
print("\n" + "="*60)
print("📊 Update Summary:")
print("="*60)
print(f"✅ Successfully updated: {successful_updates} tickers")
print(f"❌ Failed updates: {failed_updates} tickers")
print(f"📈 Total processed: {len(tickers)} tickers")
print("="*60)

# Display sample of updated data
print("\n📋 Sample of updated data:")
cursor.execute(f"""
    SELECT TOP 10 
        ticker, 
        company_name, 
        sector, 
        industry, 
        sub_industry,
        last_updated
    FROM {table_name}
    WHERE sector IS NOT NULL
    ORDER BY ticker
""")

results = cursor.fetchall()
for row in results:
    print(f"  {row[0]}: {row[1]}")
    print(f"    Sector: {row[2]}")
    print(f"    Industry: {row[3]}")
    print(f"    Sub-Industry: {row[4]}")
    print(f"    Updated: {row[5]}")
    print()

# Close connection
cursor.close()
conn.close()
print("✅ Database connection closed. Update complete!")
