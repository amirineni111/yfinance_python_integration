# This PY script fetches fundamental data for NSE500 and NASDAQ100 stocks
import yfinance as yf
import pandas as pd
import pyodbc
from datetime import datetime
import time
import argparse
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ✅ Parse command-line arguments
parser = argparse.ArgumentParser(description="Fetch fundamental data for NSE and/or NASDAQ stocks")
parser.add_argument('--market', choices=['nse', 'nasdaq', 'all'], default='all',
                    help='Which market to fetch: nse, nasdaq, or all (default: all)')
args = parser.parse_args()

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"

# ✅ Email configuration (set STOCK_EMAIL_PASSWORD env var with Gmail App Password)
EMAIL_SENDER = os.environ.get('STOCK_EMAIL_SENDER', 'sree.amiri@gmail.com')
EMAIL_PASSWORD = os.environ.get('STOCK_EMAIL_PASSWORD', '')
EMAIL_RECIPIENT = os.environ.get('STOCK_EMAIL_RECIPIENT', 'sree.amiri@gmail.com')
SMTP_SERVER = os.environ.get('STOCK_SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('STOCK_SMTP_PORT', '587'))

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

# ✅ Email notification function
def send_failure_email(subject, body):
    """Send email notification when failures occur."""
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT]):
        print("⚠ Email not configured. Set STOCK_EMAIL_SENDER, STOCK_EMAIL_PASSWORD, STOCK_EMAIL_RECIPIENT env vars.")
        print(f"📧 Would have sent email:")
        print(f"   Subject: {subject}")
        print(f"   Body: {body[:500]}")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECIPIENT
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"📧 Failure notification email sent to {EMAIL_RECIPIENT}")
    except Exception as e:
        print(f"❌ Failed to send email notification: {e}")

# ✅ Function to fetch fundamental data
def fetch_fundamentals(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Extract fundamental data
        trailing_pe = info.get('trailingPE')
        earnings_growth = info.get('earningsGrowth')
        
        # ✅ Calculate PEG ratio manually (yfinance pegRatio is unreliable/always 0)
        # PEG = Trailing P/E ÷ Earnings Growth %
        # earningsGrowth from yfinance is decimal (0.25 = 25%), so multiply by 100
        calculated_peg = None
        if trailing_pe and earnings_growth and earnings_growth != 0:
            growth_pct = earnings_growth * 100
            if growth_pct > 0:  # PEG only meaningful with positive growth
                calculated_peg = round(trailing_pe / growth_pct, 4)
        
        fundamentals = {
            'market_cap': info.get('marketCap'),
            'enterprise_value': info.get('enterpriseValue'),
            'trailing_pe': trailing_pe,
            'forward_pe': info.get('forwardPE'),
            'price_to_book': info.get('priceToBook'),
            'price_to_sales': info.get('priceToSalesTrailing12Months'),
            'peg_ratio': calculated_peg,
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

# ✅ Batch size for DB inserts (accumulate N tickers before committing)
BATCH_SIZE = 100

# ✅ Function to insert a batch of fundamental data (single commit per batch)
def insert_fundamentals_batch(batch, target_table):
    """Insert/update a batch of (ticker, company_name, fundamentals) tuples with a single commit."""
    if not batch:
        return
    
    fetch_date = datetime.now().date()
    
    for ticker, company_name, fundamentals in batch:
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
    print(f"   💾 Batch committed: {len(batch)} tickers written to {target_table}")

# ✅ Process a market's tickers with failure tracking (batch DB inserts)
def process_market(market_label, master_table, target_table):
    """Fetch fundamentals one ticker at a time, batch-insert to DB every BATCH_SIZE tickers."""
    print(f"\n📊 Fetching {market_label} fundamental data...")
    cursor.execute(f"SELECT ticker, company_name FROM {master_table}")
    tickers = cursor.fetchall()

    total = len(tickers)
    success_count = 0
    failed_tickers = []
    batch = []  # accumulate (ticker, company_name, fundamentals) tuples
    batch_num = 0

    for idx, (ticker, company_name) in enumerate(tickers, 1):
        try:
            print(f"[{idx}/{total}] Fetching fundamentals for {ticker}...")
            fundamentals = fetch_fundamentals(ticker)
            if fundamentals is None:
                failed_tickers.append(ticker)
                print(f"⚠ {ticker} — no data returned (API returned None)")
            else:
                batch.append((ticker, company_name, fundamentals))
                success_count += 1
                print(f"✅ {ticker} fundamentals fetched.")
        except Exception as e:
            failed_tickers.append(ticker)
            print(f"❌ {ticker} failed with error: {e}")
        
        # Flush batch to DB every BATCH_SIZE tickers
        if len(batch) >= BATCH_SIZE:
            batch_num += 1
            print(f"\n📦 Writing batch {batch_num} ({len(batch)} tickers) to {target_table}...")
            insert_fundamentals_batch(batch, target_table)
            batch = []
        
        time.sleep(1)  # Delay to avoid rate limiting

    # Flush remaining tickers
    if batch:
        batch_num += 1
        print(f"\n📦 Writing final batch {batch_num} ({len(batch)} tickers) to {target_table}...")
        insert_fundamentals_batch(batch, target_table)

    total_batches = batch_num
    print(f"\n📊 {market_label} Summary: {success_count}/{total} succeeded, {len(failed_tickers)} failed, {total_batches} DB batch commits")
    if failed_tickers:
        print(f"⚠ Failed tickers: {', '.join(failed_tickers[:50])}{'...' if len(failed_tickers) > 50 else ''}")

    return success_count, failed_tickers, total

# ✅ Run based on --market argument
run_date = datetime.now().strftime('%Y-%m-%d %H:%M')
all_failures = {}  # market_label -> (failed_tickers, total, success)

if args.market in ('nse', 'all'):
    try:
        nse_success, nse_failed, nse_total = process_market('NSE 500', 'nse_500', 'nse_500_fundamentals')
        if nse_failed:
            all_failures['NSE 500'] = (nse_failed, nse_total, nse_success)
    except Exception as e:
        print(f"❌ NSE 500 processing crashed: {e}")
        all_failures['NSE 500'] = ([f'ENTIRE BATCH CRASHED: {e}'], 0, 0)

if args.market in ('nasdaq', 'all'):
    try:
        nasdaq_success, nasdaq_failed, nasdaq_total = process_market('NASDAQ', 'nasdaq_top100', 'nasdaq_100_fundamentals')
        if nasdaq_failed:
            all_failures['NASDAQ'] = (nasdaq_failed, nasdaq_total, nasdaq_success)
    except Exception as e:
        print(f"❌ NASDAQ processing crashed: {e}")
        all_failures['NASDAQ'] = ([f'ENTIRE BATCH CRASHED: {e}'], 0, 0)

# ✅ Send email if there were any failures
if all_failures:
    subject = f"⚠ Fundamental Data Fetch Failures — {run_date}"
    body_lines = [f"Fundamental data fetch completed with failures on {run_date}\n"]
    for market, (failed, total, success) in all_failures.items():
        body_lines.append(f"\n{'='*50}")
        body_lines.append(f"{market}: {success}/{total} succeeded, {len(failed)} failed")
        body_lines.append(f"Failed tickers: {', '.join(failed)}")
    body_lines.append(f"\n{'='*50}")
    body_lines.append(f"\nMarkets processed: {args.market}")
    body_lines.append(f"Script: get_fundamental_data.py")
    send_failure_email(subject, '\n'.join(body_lines))
else:
    print("\n🎉 All markets processed with zero failures!")

# ✅ Close connection
cursor.close()
conn.close()
print(f"\n✅ Fundamental data fetch completed. Markets: {args.market}")
if all_failures:
    print(f"⚠ There were failures — check email or logs above for details.")
    exit(1)  # Non-zero exit so batch file can detect failure
