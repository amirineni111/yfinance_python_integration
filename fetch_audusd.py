import yfinance as yf
from datetime import datetime

# Fetch AUD/USD data
print("Fetching AUD/USD data from yfinance...")
ticker = yf.Ticker('AUDUSD=X')

# Get historical data
hist = ticker.history(period='5d')
info = ticker.info

print("\n" + "="*50)
print("      AUD/USD EXCHANGE RATE DATA")
print("="*50)

if not hist.empty:
    latest_price = hist['Close'].iloc[-1]
    prev_price = hist['Close'].iloc[-2] if len(hist) > 1 else latest_price
    change = latest_price - prev_price
    change_pct = (change / prev_price * 100) if prev_price != 0 else 0
    
    print(f"\nSymbol: AUDUSD=X")
    print(f"Latest Price: ${latest_price:.4f}")
    print(f"Previous Close: ${prev_price:.4f}")
    print(f"Change: ${change:+.4f}")
    print(f"Change %: {change_pct:+.2f}%")
    
    print(f"\n5-Day History:")
    print("-" * 80)
    print(hist[['Open', 'High', 'Low', 'Close', 'Volume']].to_string())
    
    print(f"\n\nAdditional Information:")
    print("-" * 50)
    for key, value in info.items():
        print(f"{key}: {value}")
else:
    print("No data available for AUDUSD=X")

print("\n" + "="*50)
