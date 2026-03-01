"""
Step 2: Fetch NASDAQ 1000 and NSE 1000 ticker lists, compare with DB, 
and generate SQL INSERT scripts for missing tickers.
"""
import pandas as pd
import requests
from io import StringIO
import pyodbc

# Browser-like headers to avoid 403 from Wikipedia
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# =====================================================
# 1. FETCH NASDAQ 1000 (S&P 500 + Russell 1000)
# Russell 1000 gives us the top ~1000 US stocks by market cap
# =====================================================
print("=" * 60)
print("FETCHING NASDAQ / US TOP 1000 TICKERS")
print("=" * 60)

# Method 1: Wikipedia S&P 500
sp500_tickers = []
try:
    resp = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=HEADERS, timeout=15)
    resp.raise_for_status()
    sp500_tables = pd.read_html(StringIO(resp.text))
    sp500_df = sp500_tables[0]
    sp500_tickers = sp500_df['Symbol'].str.strip().tolist()
    print(f"S&P 500 tickers fetched: {len(sp500_tickers)}")
except Exception as e:
    print(f"Failed to fetch S&P 500: {e}")

# Method 2: Russell 1000 from Wikipedia (top ~1000 US stocks by market cap)
russell_tickers = []
try:
    resp = requests.get("https://en.wikipedia.org/wiki/Russell_1000_Index", headers=HEADERS, timeout=15)
    resp.raise_for_status()
    russell_tables = pd.read_html(StringIO(resp.text))
    # Find the table with 'Symbol' column and ~1000 rows
    for t in russell_tables:
        if 'Symbol' in t.columns and len(t) > 500:
            russell_tickers = t['Symbol'].astype(str).str.strip().tolist()
            print(f"Russell 1000 tickers fetched: {len(russell_tickers)}")
            break
    if not russell_tickers:
        print("Could not find Russell 1000 ticker table")
except Exception as e:
    print(f"Failed to fetch Russell 1000: {e}")

# Combine all US tickers (deduplicated)
all_us_tickers = set()
for t in sp500_tickers:
    all_us_tickers.add(t.strip())
for t in russell_tickers:
    all_us_tickers.add(str(t).strip())

# Clean up tickers - remove any NaN or empty
all_us_tickers = {t for t in all_us_tickers if t and t != 'nan' and len(t) <= 10}
print(f"\nTotal unique US tickers (combined): {len(all_us_tickers)}")

# =====================================================
# 2. FETCH NSE 1000 (NIFTY 500 + NIFTY Next 50 + more)
# =====================================================
print("\n" + "=" * 60)
print("FETCHING NSE TOP 1000 TICKERS")
print("=" * 60)

nse_tickers_all = set()

# Nifty 500
try:
    nse500_url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
    response = requests.get(nse500_url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    nse500_df = pd.read_csv(StringIO(response.text))
    nse500_symbols = nse500_df['Symbol'].tolist()
    nse_tickers_all.update(nse500_symbols)
    print(f"Nifty 500 tickers fetched: {len(nse500_symbols)}")
except Exception as e:
    print(f"Failed to fetch Nifty 500: {e}")

# Nifty Total Market (broader index) 
try:
    nse_total_url = "https://archives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv"
    response = requests.get(nse_total_url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    nse_total_df = pd.read_csv(StringIO(response.text))
    if 'Symbol' in nse_total_df.columns:
        nse_total_symbols = nse_total_df['Symbol'].tolist()
        nse_tickers_all.update(nse_total_symbols)
        print(f"Nifty Total Market tickers fetched: {len(nse_total_symbols)}")
    else:
        print(f"Nifty Total Market columns: {nse_total_df.columns.tolist()}")
except Exception as e:
    print(f"Failed to fetch Nifty Total Market: {e}")

# Nifty Microcap 250
try:
    nse_micro_url = "https://archives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv"
    response = requests.get(nse_micro_url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    nse_micro_df = pd.read_csv(StringIO(response.text))
    if 'Symbol' in nse_micro_df.columns:
        nse_micro_symbols = nse_micro_df['Symbol'].tolist()
        nse_tickers_all.update(nse_micro_symbols)
        print(f"Nifty Microcap 250 tickers fetched: {len(nse_micro_symbols)}")
    else:
        print(f"Nifty Microcap columns: {nse_micro_df.columns.tolist()}")
except Exception as e:
    print(f"Failed to fetch Nifty Microcap 250: {e}")

# Nifty MidSmallcap 400
try:
    nse_midsm_url = "https://archives.nseindia.com/content/indices/ind_niftymidsmallcap400list.csv"
    response = requests.get(nse_midsm_url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    nse_midsm_df = pd.read_csv(StringIO(response.text))
    if 'Symbol' in nse_midsm_df.columns:
        nse_midsm_symbols = nse_midsm_df['Symbol'].tolist()
        nse_tickers_all.update(nse_midsm_symbols)
        print(f"Nifty MidSmallcap 400 tickers fetched: {len(nse_midsm_symbols)}")
    else:
        print(f"Nifty MidSmallcap columns: {nse_midsm_df.columns.tolist()}")
except Exception as e:
    print(f"Failed to fetch Nifty MidSmallcap 400: {e}")

# Clean NSE tickers
nse_tickers_all = {t.strip() for t in nse_tickers_all if t and str(t) != 'nan'}
print(f"\nTotal unique NSE tickers (combined): {len(nse_tickers_all)}")

# =====================================================
# 3. CONNECT TO DB AND COMPARE
# =====================================================
print("\n" + "=" * 60)
print("COMPARING WITH DATABASE")
print("=" * 60)

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost\\MSSQLSERVER01;'
    'DATABASE=stockdata_db;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Get existing NASDAQ tickers
cursor.execute('SELECT ticker FROM nasdaq_top100')
existing_nasdaq = set(r[0] for r in cursor.fetchall())

# Get existing NSE tickers (they have .NS suffix in DB)
cursor.execute('SELECT ticker FROM nse_500')
existing_nse_raw = set(r[0] for r in cursor.fetchall())
# Extract base symbols (remove .NS suffix) for comparison
existing_nse_base = set()
for t in existing_nse_raw:
    if t.endswith('.NS'):
        existing_nse_base.add(t[:-3])
    elif t.endswith('.BO'):
        existing_nse_base.add(t[:-3])
    else:
        existing_nse_base.add(t)

conn.close()

# Find missing NASDAQ tickers
# Take top 1000 from the combined list (we may have more than 1000)
missing_nasdaq = sorted(all_us_tickers - existing_nasdaq)
print(f"\nNASDAQ: {len(existing_nasdaq)} in DB, {len(all_us_tickers)} in top list")
print(f"Missing NASDAQ tickers: {len(missing_nasdaq)}")

# Find missing NSE tickers
missing_nse = sorted(nse_tickers_all - existing_nse_base)
print(f"\nNSE: {len(existing_nse_raw)} in DB, {len(nse_tickers_all)} in top list")
print(f"Missing NSE tickers: {len(missing_nse)}")

# Also find tickers in DB but NOT in the top lists (potentially delisted/removed)
extra_nasdaq = sorted(existing_nasdaq - all_us_tickers)
extra_nse = sorted(existing_nse_base - nse_tickers_all)
print(f"\nNASDAQ tickers in DB but NOT in top list: {len(extra_nasdaq)}")
print(f"NSE tickers in DB but NOT in top list: {len(extra_nse)}")

# =====================================================
# 4. GENERATE SQL INSERT SCRIPTS
# =====================================================
print("\n" + "=" * 60)
print("GENERATING SQL INSERT SCRIPTS")
print("=" * 60)

# Generate NASDAQ INSERT script
nasdaq_sql_lines = []
nasdaq_sql_lines.append("-- =====================================================")
nasdaq_sql_lines.append("-- INSERT missing NASDAQ tickers into nasdaq_top100")
nasdaq_sql_lines.append(f"-- Generated: Missing {len(missing_nasdaq)} tickers")
nasdaq_sql_lines.append(f"-- Current count: {len(existing_nasdaq)}, Target: ~{len(existing_nasdaq) + len(missing_nasdaq)}")
nasdaq_sql_lines.append("-- =====================================================")
nasdaq_sql_lines.append("USE stockdata_db;")
nasdaq_sql_lines.append("GO")
nasdaq_sql_lines.append("")

for ticker in missing_nasdaq:
    safe_ticker = ticker.replace("'", "''")
    nasdaq_sql_lines.append(
        f"INSERT INTO nasdaq_top100 (ticker, company_name, process_flag) "
        f"VALUES ('{safe_ticker}', 'TBD', 1);"
    )

nasdaq_sql_lines.append("")
nasdaq_sql_lines.append(f"-- Total inserts: {len(missing_nasdaq)}")
nasdaq_sql_lines.append(f"-- After insert, run update_nasdaq100_industry_data.py to populate company_name, sector, industry")
nasdaq_sql_lines.append("GO")

with open('insert_missing_nasdaq_tickers.sql', 'w') as f:
    f.write('\n'.join(nasdaq_sql_lines))
print(f"Generated: insert_missing_nasdaq_tickers.sql ({len(missing_nasdaq)} inserts)")

# Generate NSE INSERT script (tickers stored with .NS suffix)
nse_sql_lines = []
nse_sql_lines.append("-- =====================================================")
nse_sql_lines.append("-- INSERT missing NSE tickers into nse_500")
nse_sql_lines.append(f"-- Generated: Missing {len(missing_nse)} tickers")
nse_sql_lines.append(f"-- Current count: {len(existing_nse_raw)}, Target: ~{len(existing_nse_raw) + len(missing_nse)}")
nse_sql_lines.append("-- =====================================================")
nse_sql_lines.append("USE stockdata_db;")
nse_sql_lines.append("GO")
nse_sql_lines.append("")

for ticker in missing_nse:
    safe_ticker = ticker.replace("'", "''")
    nse_sql_lines.append(
        f"INSERT INTO nse_500 (ticker, company_name, process_flag) "
        f"VALUES ('{safe_ticker}.NS', 'TBD', 1);"
    )

nse_sql_lines.append("")
nse_sql_lines.append(f"-- Total inserts: {len(missing_nse)}")
nse_sql_lines.append(f"-- After insert, run update_nse500_industry_data.py to populate company_name, sector, industry")
nse_sql_lines.append("GO")

with open('insert_missing_nse_tickers.sql', 'w') as f:
    f.write('\n'.join(nse_sql_lines))
print(f"Generated: insert_missing_nse_tickers.sql ({len(missing_nse)} inserts)")

# =====================================================
# 5. PRINT SUMMARY
# =====================================================
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"\nNASDAQ:")
print(f"  Currently in DB: {len(existing_nasdaq)}")
print(f"  Top US stocks found: {len(all_us_tickers)}")
print(f"  Missing (to add): {len(missing_nasdaq)}")
print(f"  After insert: {len(existing_nasdaq) + len(missing_nasdaq)}")
if missing_nasdaq:
    print(f"  Sample missing: {missing_nasdaq[:20]}")

print(f"\nNSE:")
print(f"  Currently in DB: {len(existing_nse_raw)}")
print(f"  Top NSE stocks found: {len(nse_tickers_all)}")
print(f"  Missing (to add): {len(missing_nse)}")
print(f"  After insert: {len(existing_nse_raw) + len(missing_nse)}")
if missing_nse:
    print(f"  Sample missing: {missing_nse[:20]}")

print(f"\nExtra tickers in DB not in top lists (may be delisted/removed):")
if extra_nasdaq:
    print(f"  NASDAQ extra ({len(extra_nasdaq)}): {extra_nasdaq[:20]}...")
if extra_nse:
    print(f"  NSE extra ({len(extra_nse)}): {extra_nse[:20]}...")

print("\nFiles generated:")
print("  1. insert_missing_nasdaq_tickers.sql")
print("  2. insert_missing_nse_tickers.sql")
print("\nRun these SQL scripts in SSMS to add missing tickers, then run:")
print("  python update_nasdaq100_industry_data.py  (to fill company_name/sector/industry)")
print("  python update_nse500_industry_data.py     (to fill company_name/sector/industry)")
