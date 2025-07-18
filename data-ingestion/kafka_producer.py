import json
import time
from kafka import KafkaProducer
from datetime import datetime, timezone
import yfinance as yf
import pytz

class StockKafkaProducer:
    def __init__(self, kafka_servers=['localhost:9092']):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'stock-prices'
        
    def fetch_and_send_stock_data(self, symbol):
        """Fetch stock data and send to Kafka"""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d", interval="1m")
            
            if data.empty:
                print(f"No data for {symbol}")
                return False
            
            # Get the most recent COMPLETE data (skip current minute if it has 0 volume)
            latest = data.tail(1)
            
            # If the latest minute has 0 volume, use the previous minute
            if latest['Volume'].iloc[0] == 0 and len(data) > 1:
                latest = data.tail(2).head(1)  # Second to last row
                
            latest_timestamp = latest.index[0]
            
            # Check market hours (NYSE: 9:30 AM - 4:00 PM ET)
            et_tz = pytz.timezone('US/Eastern')
            now_et = datetime.now(et_tz)
            
            # Get today's market hours in ET
            today_et = now_et.date()
            market_open = et_tz.localize(datetime.combine(today_et, datetime.min.time().replace(hour=9, minute=30)))
            market_close = et_tz.localize(datetime.combine(today_et, datetime.min.time().replace(hour=16, minute=0)))
            
            is_weekday = now_et.weekday() < 5  # Monday = 0, Friday = 4
            is_market_hours = market_open <= now_et <= market_close and is_weekday
            
            # Convert latest timestamp to ET for comparison
            if latest_timestamp.tz is None:
                latest_timestamp = latest_timestamp.tz_localize('UTC')
            latest_et = latest_timestamp.astimezone(et_tz)
            
            time_diff = now_et - latest_et
            
            # Create message with original timestamp
            message = {
                'symbol': symbol,
                'timestamp': latest_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'open': float(latest['Open'].iloc[0]),
                'high': float(latest['High'].iloc[0]),
                'low': float(latest['Low'].iloc[0]),
                'close': float(latest['Close'].iloc[0]),
                'volume': int(latest['Volume'].iloc[0]),
                'market_hours': is_market_hours,
                'data_age_minutes': round(time_diff.total_seconds()/60, 1)
            }
            
            # Send to Kafka
            self.producer.send(self.topic, value=message)
            
            if is_market_hours and time_diff.total_seconds() < 300:
                print(f"✅ {symbol}: ${message['close']:.2f}")
            elif is_market_hours:
                print(f"⚠️  {symbol}: ${message['close']:.2f} (data {message['data_age_minutes']:.1f} min old)")
            else:
                print(f"🌙 {symbol}: ${message['close']:.2f} (after hours)")
            
            return True
            
        except Exception as e:
            print(f"❌ Error with {symbol}: {e}")
            return False
    
    def start_streaming(self, symbols, interval_seconds=60):
        """Start streaming stock data"""
        print(f"🚀 Starting stock data streaming...")
        print(f"📊 Symbols: {', '.join(symbols)}")
        print(f"⏱️  Interval: {interval_seconds} seconds")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                print(f"📈 {datetime.now().strftime('%H:%M:%S')}")
                
                for symbol in symbols:
                    self.fetch_and_send_stock_data(symbol)
                
                print(f"⏳ Waiting {interval_seconds} seconds...\n")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping producer...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    # Stock symbols to track
    symbols = ['AAPL', 'GOOGL', 'MSFT']
    
    # Create producer and start streaming
    producer = StockKafkaProducer()
    producer.start_streaming(symbols)
