# This PY script fetches fundamental data for NSE500 and NASDAQ100 stocks
import yfinance as yf
import pandas as pd
import pyodbc
from datetime import datetime
import time

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"

# ✅ Establish connection to SQL Server
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

# ✅ Create fundamental data tables
def create_fundamental_tables():
    # NSE 500 Fundamentals Table
    nse_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'nse_500_fundamentals')
    BEGIN
        CREATE TABLE nse_500_fundamentals (
            ticker VARCHAR(50),
            company_name VARCHAR(255),
            fetch_date DATE,
            market_cap BIGINT,
            enterprise_value BIGINT,
            trailing_pe FLOAT,
            forward_pe FLOAT,
            price_to_book FLOAT,
            price_to_sales FLOAT,
            peg_ratio FLOAT,
            trailing_eps FLOAT,
            forward_eps FLOAT,
            book_value FLOAT,
            profit_margin FLOAT,
            operating_margin FLOAT,
            gross_margin FLOAT,
            return_on_equity FLOAT,
            return_on_assets FLOAT,
            total_revenue BIGINT,
            revenue_per_share FLOAT,
            revenue_growth FLOAT,
            earnings_growth FLOAT,
            dividend_rate FLOAT,
            dividend_yield FLOAT,
            payout_ratio FLOAT,
            total_cash BIGINT,
            total_debt BIGINT,
            debt_to_equity FLOAT,
            current_ratio FLOAT,
            quick_ratio FLOAT,
            free_cashflow BIGINT,
            operating_cashflow BIGINT,
            beta FLOAT,
            fifty_two_week_high FLOAT,
            fifty_two_week_low FLOAT,
            fifty_day_avg FLOAT,
            two_hundred_day_avg FLOAT,
            PRIMARY KEY (ticker, fetch_date)
        );
    END
    """
    
    # NASDAQ 100 Fundamentals Table
    nasdaq_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'nasdaq_100_fundamentals')
    BEGIN
        CREATE TABLE nasdaq_100_fundamentals (
            ticker VARCHAR(50),
            company_name VARCHAR(255),
            fetch_date DATE,
            market_cap BIGINT,
            enterprise_value BIGINT,
            trailing_pe FLOAT,
            forward_pe FLOAT,
            price_to_book FLOAT,
            price_to_sales FLOAT,
            peg_ratio FLOAT,
            trailing_eps FLOAT,
            forward_eps FLOAT,
            book_value FLOAT,
            profit_margin FLOAT,
            operating_margin FLOAT,
            gross_margin FLOAT,
            return_on_equity FLOAT,
            return_on_assets FLOAT,
            total_revenue BIGINT,
            revenue_per_share FLOAT,
            revenue_growth FLOAT,
            earnings_growth FLOAT,
            dividend_rate FLOAT,
            dividend_yield FLOAT,
            payout_ratio FLOAT,
            total_cash BIGINT,
            total_debt BIGINT,
            debt_to_equity FLOAT,
            current_ratio FLOAT,
            quick_ratio FLOAT,
            free_cashflow BIGINT,
            operating_cashflow BIGINT,
            beta FLOAT,
            fifty_two_week_high FLOAT,
            fifty_two_week_low FLOAT,
            fifty_day_avg FLOAT,
            two_hundred_day_avg FLOAT,
            PRIMARY KEY (ticker, fetch_date)
        );
    END
    """
    
    cursor.execute(nse_table_query)
    cursor.execute(nasdaq_table_query)
    conn.commit()
    print("✅ Fundamental tables created successfully.")

create_fundamental_tables()

# ✅ Function to fetch fundamental data
def fetch_fundamentals(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Extract fundamental data
        fundamentals = {
            'market_cap': info.get('marketCap'),
            'enterprise_value': info.get('enterpriseValue'),
            'trailing_pe': info.get('trailingPE'),
            'forward_pe': info.get('forwardPE'),
            'price_to_book': info.get('priceToBook'),
            'price_to_sales': info.get('priceToSalesTrailing12Months'),
            'peg_ratio': info.get('pegRatio'),
            'trailing_eps': info.get('trailingEps'),
            'forward_eps': info.get('forwardEps'),
            'book_value': info.get('bookValue'),
            'profit_margin': info.get('profitMargins'),
            'operating_margin': info.get('operatingMargins'),
            'gross_margin': info.get('grossMargins'),
            'return_on_equity': info.get('returnOnEquity'),
            'return_on_assets': info.get('returnOnAssets'),
            'total_revenue': info.get('totalRevenue'),
            'revenue_per_share': info.get('revenuePerShare'),
            'revenue_growth': info.get('revenueGrowth'),
            'earnings_growth': info.get('earningsGrowth'),
            'dividend_rate': info.get('dividendRate'),
            'dividend_yield': info.get('dividendYield'),
            'payout_ratio': info.get('payoutRatio'),
            'total_cash': info.get('totalCash'),
            'total_debt': info.get('totalDebt'),
            'debt_to_equity': info.get('debtToEquity'),
            'current_ratio': info.get('currentRatio'),
            'quick_ratio': info.get('quickRatio'),
            'free_cashflow': info.get('freeCashflow'),
            'operating_cashflow': info.get('operatingCashflow'),
            'beta': info.get('beta'),
            'fifty_two_week_high': info.get('fiftyTwoWeekHigh'),
            'fifty_two_week_low': info.get('fiftyTwoWeekLow'),
            'fifty_day_avg': info.get('fiftyDayAverage'),
            'two_hundred_day_avg': info.get('twoHundredDayAverage')
        }
        
        return fundamentals
    except Exception as e:
        print(f"⚠ Error fetching fundamentals for {ticker}: {e}")
        return None

# ✅ Function to insert fundamental data
def insert_fundamentals(ticker, company_name, fundamentals, target_table):
    if fundamentals is None:
        return
    
    fetch_date = datetime.now().date()
    
    # Check if record exists for today
    check_query = f"SELECT COUNT(*) FROM {target_table} WHERE ticker = ? AND fetch_date = ?"
    cursor.execute(check_query, ticker, fetch_date)
    exists = cursor.fetchone()[0] > 0
    
    if exists:
        # Update existing record
        update_query = f"""
        UPDATE {target_table} SET
            company_name = ?, market_cap = ?, enterprise_value = ?, trailing_pe = ?, forward_pe = ?,
            price_to_book = ?, price_to_sales = ?, peg_ratio = ?, trailing_eps = ?, forward_eps = ?,
            book_value = ?, profit_margin = ?, operating_margin = ?, gross_margin = ?,
            return_on_equity = ?, return_on_assets = ?, total_revenue = ?, revenue_per_share = ?,
            revenue_growth = ?, earnings_growth = ?, dividend_rate = ?, dividend_yield = ?,
            payout_ratio = ?, total_cash = ?, total_debt = ?, debt_to_equity = ?,
            current_ratio = ?, quick_ratio = ?, free_cashflow = ?, operating_cashflow = ?,
            beta = ?, fifty_two_week_high = ?, fifty_two_week_low = ?, fifty_day_avg = ?,
            two_hundred_day_avg = ?
        WHERE ticker = ? AND fetch_date = ?
        """
        cursor.execute(update_query, 
            company_name, fundamentals['market_cap'], fundamentals['enterprise_value'],
            fundamentals['trailing_pe'], fundamentals['forward_pe'], fundamentals['price_to_book'],
            fundamentals['price_to_sales'], fundamentals['peg_ratio'], fundamentals['trailing_eps'],
            fundamentals['forward_eps'], fundamentals['book_value'], fundamentals['profit_margin'],
            fundamentals['operating_margin'], fundamentals['gross_margin'], fundamentals['return_on_equity'],
            fundamentals['return_on_assets'], fundamentals['total_revenue'], fundamentals['revenue_per_share'],
            fundamentals['revenue_growth'], fundamentals['earnings_growth'], fundamentals['dividend_rate'],
            fundamentals['dividend_yield'], fundamentals['payout_ratio'], fundamentals['total_cash'],
            fundamentals['total_debt'], fundamentals['debt_to_equity'], fundamentals['current_ratio'],
            fundamentals['quick_ratio'], fundamentals['free_cashflow'], fundamentals['operating_cashflow'],
            fundamentals['beta'], fundamentals['fifty_two_week_high'], fundamentals['fifty_two_week_low'],
            fundamentals['fifty_day_avg'], fundamentals['two_hundred_day_avg'],
            ticker, fetch_date
        )
    else:
        # Insert new record
        insert_query = f"""
        INSERT INTO {target_table} (
            ticker, company_name, fetch_date, market_cap, enterprise_value, trailing_pe, forward_pe,
            price_to_book, price_to_sales, peg_ratio, trailing_eps, forward_eps, book_value,
            profit_margin, operating_margin, gross_margin, return_on_equity, return_on_assets,
            total_revenue, revenue_per_share, revenue_growth, earnings_growth, dividend_rate,
            dividend_yield, payout_ratio, total_cash, total_debt, debt_to_equity, current_ratio,
            quick_ratio, free_cashflow, operating_cashflow, beta, fifty_two_week_high,
            fifty_two_week_low, fifty_day_avg, two_hundred_day_avg
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query,
            ticker, company_name, fetch_date, fundamentals['market_cap'], fundamentals['enterprise_value'],
            fundamentals['trailing_pe'], fundamentals['forward_pe'], fundamentals['price_to_book'],
            fundamentals['price_to_sales'], fundamentals['peg_ratio'], fundamentals['trailing_eps'],
            fundamentals['forward_eps'], fundamentals['book_value'], fundamentals['profit_margin'],
            fundamentals['operating_margin'], fundamentals['gross_margin'], fundamentals['return_on_equity'],
            fundamentals['return_on_assets'], fundamentals['total_revenue'], fundamentals['revenue_per_share'],
            fundamentals['revenue_growth'], fundamentals['earnings_growth'], fundamentals['dividend_rate'],
            fundamentals['dividend_yield'], fundamentals['payout_ratio'], fundamentals['total_cash'],
            fundamentals['total_debt'], fundamentals['debt_to_equity'], fundamentals['current_ratio'],
            fundamentals['quick_ratio'], fundamentals['free_cashflow'], fundamentals['operating_cashflow'],
            fundamentals['beta'], fundamentals['fifty_two_week_high'], fundamentals['fifty_two_week_low'],
            fundamentals['fifty_day_avg'], fundamentals['two_hundred_day_avg']
        )
    
    conn.commit()

# ✅ Process NSE 500 stocks
print("\n📊 Fetching NSE 500 fundamental data...")
cursor.execute("SELECT ticker, company_name FROM nse_500")
nse_tickers = cursor.fetchall()

total_nse = len(nse_tickers)
for idx, (ticker, company_name) in enumerate(nse_tickers, 1):
    print(f"[{idx}/{total_nse}] Fetching fundamentals for {ticker}...")
    fundamentals = fetch_fundamentals(ticker)
    insert_fundamentals(ticker, company_name, fundamentals, 'nse_500_fundamentals')
    print(f"✅ {ticker} fundamentals saved.")
    time.sleep(1)  # Add 1 second delay to avoid rate limiting

# ✅ Process NASDAQ 100 stocks
print("\n📊 Fetching NASDAQ 100 fundamental data...")
cursor.execute("SELECT ticker, company_name FROM nasdaq_top100")
nasdaq_tickers = cursor.fetchall()

total_nasdaq = len(nasdaq_tickers)
for idx, (ticker, company_name) in enumerate(nasdaq_tickers, 1):
    print(f"[{idx}/{total_nasdaq}] Fetching fundamentals for {ticker}...")
    fundamentals = fetch_fundamentals(ticker)
    insert_fundamentals(ticker, company_name, fundamentals, 'nasdaq_100_fundamentals')
    print(f"✅ {ticker} fundamentals saved.")
    time.sleep(1)  # Add 1 second delay to avoid rate limiting

# ✅ Close connection
cursor.close()
conn.close()
print("\n✅ All fundamental data fetched and stored successfully!")
