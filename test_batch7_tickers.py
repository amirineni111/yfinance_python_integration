# Quick test to identify problematic ticker in batch 7 (tickers 601-700)
import yfinance as yf
import pyodbc
from datetime import datetime
import math

# SQL Server Connection
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Get tickers 601-700
cursor.execute("SELECT ticker, company_name FROM nasdaq_top100")
all_tickers = cursor.fetchall()
test_tickers = all_tickers[600:700]  # Batch 7 range

print(f"Testing {len(test_tickers)} tickers from batch 7...")

def clean_fundamentals(fundamentals):
    """Clean fundamental data to ensure proper NULL handling."""
    cleaned = {}
    for key, value in fundamentals.items():
        if value is None or value == '' or value == 'None':
            cleaned[key] = None
        elif isinstance(value, str):
            if value.lower() in ('n/a', 'na', '-', 'nan', 'inf', '-inf', 'null', 'none'):
                cleaned[key] = None
            else:
                try:
                    numeric_value = float(value)
                    if math.isinf(numeric_value) or math.isnan(numeric_value):
                        cleaned[key] = None
                    else:
                        cleaned[key] = numeric_value
                except (ValueError, TypeError):
                    cleaned[key] = None
        elif isinstance(value, float) and (math.isinf(value) or math.isnan(value)):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned

def fetch_fundamentals(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        trailing_pe = info.get('trailingPE')
        earnings_growth = info.get('earningsGrowth')
        
        calculated_peg = None
        if trailing_pe and earnings_growth and earnings_growth != 0:
            growth_pct = earnings_growth * 100
            if growth_pct > 0:
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
        print(f"Error fetching {ticker}: {e}")
        return None

# Test each ticker
for idx, (ticker, company_name) in enumerate(test_tickers, 601):
    print(f"\n[{idx}/700] Testing {ticker}...")
    try:
        fundamentals = fetch_fundamentals(ticker)
        if fundamentals is None:
            print(f"  Skipped (no data)")
            continue
        
        # Clean the data
        fundamentals = clean_fundamentals(fundamentals)
        
        # Check if record exists
        fetch_date = datetime.now().date()
        check_query = "SELECT COUNT(*) FROM nasdaq_100_fundamentals WHERE ticker = ? AND fetch_date = ?"
        cursor.execute(check_query, ticker, fetch_date)
        exists = cursor.fetchone()[0] > 0
        
        if exists:
            # Try UPDATE
            update_query = """
            UPDATE nasdaq_100_fundamentals SET
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
            print(f"  OK - {ticker} UPDATE successful")
        else:
            # Try INSERT
            insert_query = """
            INSERT INTO nasdaq_100_fundamentals (
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
            print(f"  OK - {ticker} INSERT successful")
        
    except Exception as e:
        print(f"  ERROR - {ticker} failed: {e}")
        print(f"  Company: {company_name}")
        print(f"  Fundamentals: {fundamentals}")
        conn.rollback()
        break

cursor.close()
conn.close()
print("\nTest complete")
