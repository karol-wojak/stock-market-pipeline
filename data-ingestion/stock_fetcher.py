import yfinance as yf
from datetime import datetime

def fetch_stock_data(symbol):
    """Fetch current stock data for a given symbol"""
    try:
        ticker = yf.Ticker(symbol)
        
        # Get today's data with 5-minute intervals (more reliable)
        data = ticker.history(period="1d", interval="5m")
        
        if data.empty:
            print(f"No data found for {symbol}")
            return None
            
        # Get the latest row with actual volume
        latest = data[data['Volume'] > 0].tail(1)
        
        if latest.empty:
            latest = data.tail(1)  # fallback to last row even if volume is 0

        # Clean up the timestamp
        timestamp = latest.index[0]
        clean_time = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                
        print(f"\n--- {symbol} Latest Data ---")
        print(f"Time: {clean_time} (Eastern Time)")
        print(f"Window Start Price: ${latest['Open'].iloc[0]:.2f}")
        print(f"Current Price: ${latest['Close'].iloc[0]:.2f}")
        print(f"Volume (5min): {latest['Volume'].iloc[0]:,}")
        
        # Also get basic info
        info = ticker.info
        print(f"Previous Close: ${info.get('previousClose', 'N/A')}")
        print(f"Market State: {info.get('marketState', 'N/A')}")
        
        return latest
        
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

if __name__ == "__main__":
    print("Testing Yahoo Finance API...")
    fetch_stock_data("AAPL")