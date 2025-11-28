import yfinance as yf

ticker = "VTRS"  # Replace with the one you're debugging
stock = yf.Ticker(ticker)
df = stock.history(period="5d", interval="1d")

print(df)
