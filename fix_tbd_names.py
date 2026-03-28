"""
Fix 'TBD' company names in nasdaq_top100 and nse_500 tables.
Fetches real company names from yfinance and updates SQL Server.
"""
import pyodbc
import yfinance as yf
import time

def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\MSSQLSERVER01;"
        "DATABASE=stockdata_db;"
        "Trusted_Connection=yes;"
    )

def get_tbd_tickers(cursor, table_name):
    cursor.execute(f"SELECT ticker FROM {table_name} WHERE company_name='TBD'")
    return [row[0] for row in cursor.fetchall()]

def fetch_company_name(ticker):
    """Fetch company name from yfinance."""
    try:
        info = yf.Ticker(ticker).info
        name = info.get('longName') or info.get('shortName')
        return name
    except Exception as e:
        print(f"  ERROR fetching {ticker}: {e}")
        return None

def update_company_name(cursor, conn, table_name, ticker, company_name):
    cursor.execute(
        f"UPDATE {table_name} SET company_name = ? WHERE ticker = ?",
        company_name, ticker
    )
    conn.commit()

def main():
    conn = get_connection()
    cursor = conn.cursor()

    # --- NASDAQ ---
    print("=" * 60)
    print("Fixing nasdaq_top100 TBD entries")
    print("=" * 60)
    nasdaq_tbd = get_tbd_tickers(cursor, 'nasdaq_top100')
    print(f"Found {len(nasdaq_tbd)} TBD tickers\n")

    for ticker in nasdaq_tbd:
        name = fetch_company_name(ticker)
        if name:
            update_company_name(cursor, conn, 'nasdaq_top100', ticker, name)
            print(f"  UPDATED: {ticker} -> {name}")
        else:
            print(f"  SKIPPED: {ticker} (could not fetch name)")
        time.sleep(0.3)

    # --- NSE ---
    print("\n" + "=" * 60)
    print("Fixing nse_500 TBD entries")
    print("=" * 60)
    nse_tbd = get_tbd_tickers(cursor, 'nse_500')
    print(f"Found {len(nse_tbd)} TBD tickers\n")

    for ticker in nse_tbd:
        name = fetch_company_name(ticker)
        if name:
            update_company_name(cursor, conn, 'nse_500', ticker, name)
            print(f"  UPDATED: {ticker} -> {name}")
        else:
            print(f"  SKIPPED: {ticker} (could not fetch name)")
        time.sleep(0.3)

    cursor.close()
    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
