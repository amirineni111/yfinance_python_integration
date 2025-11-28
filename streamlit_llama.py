# this one is useful to analyse stock data with streamlit and NLP
# run the python file on terminal
# then it produce the url to open streamlit application and enter that
import streamlit as st
import yfinance as yf
import pandas as pd
import ollama
from datetime import datetime, timedelta

# Initialize Streamlit App
st.title("Apple Stock Analysis with Llama Insights")
st.write("Real-time analysis of Apple (AAPL) stock and Dow Jones index using Streamlit and Llama")

# Fetching historical data for Apple (AAPL) and Dow Jones (DJI)
stock = yf.Ticker("AAPL")
dow_jones = yf.Ticker("^DJI")
data = stock.history(period="20d", interval="1d")
dow_data = dow_jones.history(period="20d", interval="1d")

# Global variables to store rolling data
rolling_window = pd.DataFrame()
dow_rolling_window = pd.DataFrame()
daily_high = float('-inf')
daily_low = float('inf')
buying_momentum = 0
selling_momentum = 0

# Function to process a new stock update every minute
def process_stock_update():
    global rolling_window, data, dow_rolling_window, dow_data
    global daily_high, daily_low, buying_momentum, selling_momentum

    if not data.empty and not dow_data.empty:
        update = data.iloc[0].to_frame().T
        dow_update = dow_data.iloc[0].to_frame().T
        data = data.iloc[1:]
        dow_data = dow_data.iloc[1:]

        rolling_window = pd.concat([rolling_window, update], ignore_index=False)
        dow_rolling_window = pd.concat([dow_rolling_window, dow_update], ignore_index=False)

        daily_high = max(daily_high, update['Close'].values[0])
        daily_low = min(daily_low, update['Close'].values[0])

        if len(rolling_window) >= 2:
            price_change = update['Close'].values[0] - rolling_window['Close'].iloc[-2]
            if price_change > 0:
                buying_momentum += price_change
            else:
                selling_momentum += abs(price_change)

def calculate_insights(window, dow_window):
    if len(window) >= 5:
        rolling_avg = window['Close'].rolling(window=5).mean().iloc[-1]
        ema = window['Close'].ewm(span=5, adjust=False).mean().iloc[-1]

        std = window['Close'].rolling(window=5).std().iloc[-1]
        bollinger_upper = rolling_avg + (2 * std)
        bollinger_lower = rolling_avg - (2 * std)

        delta = window['Close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14, min_periods=1).mean().iloc[-1]
        avg_loss = loss.rolling(window=14, min_periods=1).mean().iloc[-1]
        rs = avg_gain / avg_loss if avg_loss != 0 else float('nan')
        rsi = 100 - (100 / (1 + rs))

        dow_rolling_avg = dow_window['Close'].rolling(window=5).mean().iloc[-1]
        get_natural_language_insights(
            rolling_avg, ema, rsi, bollinger_upper, bollinger_lower,
            price_change=0, volume_change=0, dow_rolling_avg=dow_rolling_avg,
            market_open_duration=0, dow_price_change=0, dow_volume_change=0, 
            daily_high=daily_high, daily_low=daily_low, 
            buying_momentum=buying_momentum, selling_momentum=selling_momentum
        )

def get_natural_language_insights(
    rolling_avg, ema, rsi, bollinger_upper, bollinger_lower,
    price_change, volume_change, dow_rolling_avg, market_open_duration, 
    dow_price_change, dow_volume_change, daily_high, daily_low, 
    buying_momentum, selling_momentum
):
    prompt = f"""
    Apple's stock has a 5-minute rolling average of {rolling_avg:.2f}.
    The EMA is {ema:.2f}, RSI is {rsi:.2f}, with Bollinger Bands between {bollinger_lower:.2f} and {bollinger_upper:.2f}.
    The buying momentum is {buying_momentum:.2f}, and the selling momentum is {selling_momentum:.2f}.
    Provide insights into the current stock trend and general market sentiment.
    """

    response = ollama.chat(
        model="llama3",
        messages=[{"role": "user", "content": prompt}]
    )
    response_text = response['message']['content'].strip()
    
    st.subheader("Natural Language Insights")
    st.write(response_text)

# Streamlit Button to Trigger Updates
if st.button("Update Stock Data"):
    process_stock_update()
    calculate_insights(rolling_window, dow_rolling_window)
    st.write("Stock data updated and insights generated.")

# Display Stock Data
st.subheader("Apple Stock Data")
st.dataframe(rolling_window)

st.subheader("Dow Jones Data")
st.dataframe(dow_rolling_window)
