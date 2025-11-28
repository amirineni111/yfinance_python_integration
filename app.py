
import yfinance as yf

stock = yf.Ticker("Zomato.NS")
data = stock.history(period="20d")  # Get daily data
print(data)
