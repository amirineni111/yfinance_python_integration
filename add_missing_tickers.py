"""
Add missing stock symbols to nasdaq_top100 and nse_500 tables.
- NASDAQ: Top 2000 US stocks by market cap from nasdaq.com screener API
- NSE: Top 2000 Indian stocks from NSE India index APIs + all-equity listing
Fetches company_name, sector, industry, sub_industry from yfinance.
All new tickers get process_flag='Y'.
"""
import pyodbc
import requests
import yfinance as yf
import time
import csv
import io
from datetime import datetime


def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\MSSQLSERVER01;"
        "DATABASE=stockdata_db;"
        "Trusted_Connection=yes;"
    )


def get_existing_tickers(cursor, table_name):
    cursor.execute(f"SELECT ticker FROM {table_name}")
    return set(row[0] for row in cursor.fetchall())


# ─── NASDAQ: Top 2000 by market cap from screener API ────────────────
def fetch_nasdaq_top2000():
    """
    Fetch all US stocks from NASDAQ screener API across 3 exchanges,
    parse market cap, sort descending, return top 2000 symbols.
    """
    print("Fetching US stocks from NASDAQ screener API...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
    }
    
    stocks = []  # list of (symbol, market_cap_float)
    
    for exchange in ['nasdaq', 'nyse', 'amex']:
        url = f"https://api.nasdaq.com/api/screener/stocks?tableType=most_active&exchange={exchange}&limit=5000&offset=0"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                rows = resp.json().get('data', {}).get('table', {}).get('rows', [])
                count = 0
                for row in rows:
                    sym = row.get('symbol', '').strip()
                    mcap_str = row.get('marketCap', '0').replace(',', '').strip()
                    
                    # Filter out bad symbols
                    if not sym or '/' in sym or '^' in sym or '$' in sym:
                        continue
                    
                    try:
                        mcap = float(mcap_str) if mcap_str else 0
                    except ValueError:
                        mcap = 0
                    
                    stocks.append((sym, mcap))
                    count += 1
                print(f"  {exchange.upper()}: {count} stocks")
            time.sleep(1)
        except Exception as e:
            print(f"  {exchange.upper()}: error - {e}")
    
    # Sort by market cap descending, take top 2000
    stocks.sort(key=lambda x: x[1], reverse=True)
    top2000 = [sym for sym, mcap in stocks[:2000]]
    
    print(f"  Total fetched: {len(stocks)} | Top 2000 selected")
    if stocks:
        print(f"  Smallest in top 2000: ${stocks[min(1999, len(stocks)-1)][1]:,.0f} market cap")
    
    return set(top2000)


# ─── NSE: Comprehensive stock list from index APIs ───────────────────
def create_nse_session():
    """Create a requests session with NSE cookies."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/',
    })
    try:
        session.get('https://www.nseindia.com/', timeout=15)
        time.sleep(2)
    except:
        pass
    return session


def fetch_nse_from_indices(session):
    """Fetch NSE stock symbols from all available index APIs."""
    print("Fetching NSE stocks from index APIs...")
    
    tickers = set()
    
    indices = [
        'NIFTY 500',
        'NIFTY TOTAL MARKET',
        'NIFTY MICROCAP 250',
        'NIFTY MIDCAP 150',
        'NIFTY SMALLCAP 250',
        'NIFTY LARGEMIDCAP 250',
        'NIFTY MIDSMALLCAP 400',
        'SECURITIES IN F&O',
        'NIFTY NEXT 50',
        'NIFTY 100',
        'NIFTY 200',
        'NIFTY500 MULTICAP 50:25:25',
        'NIFTY MIDCAP SELECT',
        'NIFTY MIDCAP 50',
        'NIFTY SMALLCAP 50',
        'NIFTY SMALLCAP 100',
    ]
    
    for idx in indices:
        try:
            url = f'https://www.nseindia.com/api/equity-stockIndices?index={requests.utils.quote(idx)}'
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                before = len(tickers)
                for item in data.get('data', []):
                    sym = item.get('symbol', '').strip()
                    if sym and not sym.startswith('NIFTY') and 'India VIX' not in sym:
                        tickers.add(sym)
                added_count = len(tickers) - before
                if added_count > 0:
                    print(f"  {idx}: +{added_count} new (total: {len(tickers)})")
            else:
                print(f"  {idx}: HTTP {resp.status_code}")
            time.sleep(1)
        except Exception as e:
            print(f"  {idx}: error - {e}")
            # Re-init session if needed
            try:
                session.get('https://www.nseindia.com/', timeout=15)
                time.sleep(2)
            except:
                pass
    
    return tickers


def fetch_nse_sector_indices(session):
    """Fetch stocks from NSE sector/thematic indices for broader coverage."""
    print("Fetching NSE sector/thematic indices...")
    
    tickers = set()
    
    sector_indices = [
        'NIFTY BANK', 'NIFTY IT', 'NIFTY FINANCIAL SERVICES', 
        'NIFTY AUTO', 'NIFTY PHARMA', 'NIFTY FMCG', 'NIFTY METAL',
        'NIFTY REALTY', 'NIFTY ENERGY', 'NIFTY INFRASTRUCTURE',
        'NIFTY PSE', 'NIFTY PSU BANK', 'NIFTY PRIVATE BANK',
        'NIFTY MEDIA', 'NIFTY COMMODITIES', 'NIFTY CONSUMPTION',
        'NIFTY CPSE', 'NIFTY FIN SERVICE', 'NIFTY GROWSECT 15',
        'NIFTY100 QUALITY 30', 'NIFTY HEALTHCARE INDEX',
        'NIFTY OIL & GAS', 'NIFTY MNC', 'NIFTY INDIA DIGITAL',
        'NIFTY INDIA DEFENCE', 'NIFTY INDIA MANUFACTURING',
        'NIFTY ALPHA 50', 'NIFTY HIGH BETA 50',
        'NIFTY LOW VOLATILITY 50', 'NIFTY100 EQUAL WEIGHT',
        'NIFTY100 LOW VOLATILITY 30', 'NIFTY DIVIDEND OPPORTUNITIES 50',
        'NIFTY50 VALUE 20', 'NIFTY HOUSING',
        'NIFTY TRANSPORTATION & LOGISTICS', 'NIFTY MOBILITY',
        'NIFTY INDIA TOURISM', 'NIFTY REITs & InvITs',
        'NIFTY NON-CYCLICAL CONSUMER', 'NIFTY CAPITAL MARKETS',
        'NIFTY RURAL', 'NIFTY TOTAL MARKET',
    ]
    
    for idx in sector_indices:
        try:
            url = f'https://www.nseindia.com/api/equity-stockIndices?index={requests.utils.quote(idx)}'
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get('data', []):
                    sym = item.get('symbol', '').strip()
                    if sym and not sym.startswith('NIFTY') and 'India VIX' not in sym:
                        tickers.add(sym)
            time.sleep(0.8)
        except:
            pass
    
    print(f"  Sector/thematic indices added up to: {len(tickers)} unique symbols")
    return tickers


def fetch_nse_all_equity(session):
    """Try to get all NSE listed equities from market data APIs."""
    print("Fetching NSE all-equity listing...")
    
    tickers = set()
    
    # Try pre-open market data (has all actively traded stocks)
    try:
        url = 'https://www.nseindia.com/api/market-data-pre-open?key=ALL'
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get('data', []):
                meta = item.get('metadata', {})
                sym = meta.get('symbol', '').strip()
                if sym:
                    tickers.add(sym)
            print(f"  Pre-open ALL: {len(tickers)} symbols")
    except Exception as e:
        print(f"  Pre-open error: {e}")
    
    # Try all listed stocks by letter
    for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
        try:
            url = f'https://www.nseindia.com/api/equity-master?index=allAlpha&key={letter}'
            resp = session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    for item in data:
                        sym = item.get('symbol', '').strip() if isinstance(item, dict) else ''
                        if sym:
                            tickers.add(sym)
            time.sleep(0.3)
        except:
            pass
    
    if tickers:
        print(f"  All-equity listing: {len(tickers)} symbols")
    return tickers


def fetch_nse_top2000():
    """Combine multiple NSE sources to get as many stocks as possible."""
    session = create_nse_session()
    
    all_tickers = set()
    
    # Method 1: Index-based
    all_tickers |= fetch_nse_from_indices(session)
    
    # Method 2: Sector indices (re-init session as cookie may expire)
    session = create_nse_session()
    all_tickers |= fetch_nse_sector_indices(session)
    
    # Method 3: All equity listing
    session = create_nse_session()
    all_tickers |= fetch_nse_all_equity(session)
    
    # Add .NS suffix for yfinance compatibility
    result = set(f"{sym}.NS" for sym in all_tickers)
    
    print(f"\n  Total unique NSE symbols collected: {len(result)}")
    return result


# ─── yfinance details fetch ──────────────────────────────────────────
def fetch_details_yf(ticker):
    """Fetch company_name, sector, industry, sub_industry from yfinance."""
    try:
        info = yf.Ticker(ticker).info
        name = info.get('longName') or info.get('shortName')
        sector = info.get('sector') or None
        industry = info.get('industry') or None
        sub_industry = info.get('industryDisp') or info.get('industry') or None
        return name, sector, industry, sub_industry
    except Exception:
        return None, None, None, None


def insert_ticker(cursor, conn, table_name, ticker, name, sector, industry, sub_industry):
    cursor.execute(
        f"""INSERT INTO {table_name} 
            (ticker, company_name, process_flag, sector, industry, sub_industry, last_updated) 
            VALUES (?, ?, 'Y', ?, ?, ?, GETDATE())""",
        ticker, name, sector, industry, sub_industry
    )
    conn.commit()


# ─── Process NASDAQ ──────────────────────────────────────────────────
def process_nasdaq():
    conn = get_connection()
    cursor = conn.cursor()
    
    existing = get_existing_tickers(cursor, 'nasdaq_top100')
    print(f"\nExisting NASDAQ tickers in DB: {len(existing)}")
    
    top2000 = fetch_nasdaq_top2000()
    
    missing = sorted(top2000 - existing)
    print(f"Missing tickers to add (top 2000 - existing): {len(missing)}")
    
    if not missing:
        print("All top 2000 NASDAQ stocks are already in the DB!")
        cursor.close()
        conn.close()
        return 0
    
    added = 0
    skipped = 0
    
    for i, ticker in enumerate(missing):
        name, sector, industry, sub_industry = fetch_details_yf(ticker)
        if name:
            try:
                insert_ticker(cursor, conn, 'nasdaq_top100', ticker, name, sector, industry, sub_industry)
                print(f"  [{i+1}/{len(missing)}] ADDED: {ticker} -> {name} | {sector} | {industry}")
                added += 1
            except pyodbc.IntegrityError:
                print(f"  [{i+1}/{len(missing)}] DUPLICATE: {ticker}")
                skipped += 1
            except Exception as e:
                print(f"  [{i+1}/{len(missing)}] DB ERROR: {ticker} - {e}")
                skipped += 1
        else:
            print(f"  [{i+1}/{len(missing)}] SKIPPED: {ticker} (yfinance returned no data)")
            skipped += 1
        
        if (i + 1) % 100 == 0:
            print(f"\n  --- Progress: {i+1}/{len(missing)} | Added: {added} | Skipped: {skipped} ---\n")
        time.sleep(0.15)
    
    print(f"\nNASDAQ: Added={added}, Skipped={skipped}")
    cursor.close()
    conn.close()
    return added


# ─── Process NSE ─────────────────────────────────────────────────────
def process_nse():
    conn = get_connection()
    cursor = conn.cursor()
    
    existing = get_existing_tickers(cursor, 'nse_500')
    print(f"\nExisting NSE tickers in DB: {len(existing)}")
    
    nse_all = fetch_nse_top2000()
    
    missing = sorted(nse_all - existing)
    print(f"Missing NSE tickers to add: {len(missing)}")
    
    if not missing:
        print("All collected NSE stocks are already in the DB!")
        cursor.close()
        conn.close()
        return 0
    
    added = 0
    skipped = 0
    
    for i, ticker in enumerate(missing):
        name, sector, industry, sub_industry = fetch_details_yf(ticker)
        if name:
            try:
                insert_ticker(cursor, conn, 'nse_500', ticker, name, sector, industry, sub_industry)
                print(f"  [{i+1}/{len(missing)}] ADDED: {ticker} -> {name} | {sector} | {industry}")
                added += 1
            except pyodbc.IntegrityError:
                print(f"  [{i+1}/{len(missing)}] DUPLICATE: {ticker}")
                skipped += 1
            except Exception as e:
                print(f"  [{i+1}/{len(missing)}] DB ERROR: {ticker} - {e}")
                skipped += 1
        else:
            print(f"  [{i+1}/{len(missing)}] SKIPPED: {ticker} (yfinance returned no data)")
            skipped += 1
        
        if (i + 1) % 100 == 0:
            print(f"\n  --- Progress: {i+1}/{len(missing)} | Added: {added} | Skipped: {skipped} ---\n")
        time.sleep(0.15)
    
    print(f"\nNSE: Added={added}, Skipped={skipped}")
    cursor.close()
    conn.close()
    return added


def main():
    print("=" * 70)
    print("ADD MISSING TICKERS (TOP 2000) TO nasdaq_top100 AND nse_500")
    print(f"Started: {datetime.now()}")
    print("=" * 70)
    
    print("\n" + "=" * 70)
    print("PHASE 1: NASDAQ — Top 2000 US stocks by market cap")
    print("=" * 70)
    nasdaq_added = process_nasdaq()
    
    print("\n" + "=" * 70)
    print("PHASE 2: NSE — Indian stocks from all index APIs")
    print("=" * 70)
    nse_added = process_nse()
    
    # Final counts
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM nasdaq_top100")
    nasdaq_total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM nse_500")
    nse_total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM nasdaq_top100 WHERE process_flag='Y'")
    nasdaq_y = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM nse_500 WHERE process_flag='Y'")
    nse_y = cursor.fetchone()[0]
    conn.close()
    
    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    print(f"NASDAQ: +{nasdaq_added} new | Total: {nasdaq_total} | process_flag=Y: {nasdaq_y}")
    print(f"NSE:    +{nse_added} new | Total: {nse_total} | process_flag=Y: {nse_y}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
