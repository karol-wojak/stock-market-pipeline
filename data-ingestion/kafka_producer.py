import json
import time
from kafka import KafkaProducer
from datetime import datetime
import yfinance as yf

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
                
            # Get latest data
            latest = data.tail(1)
            
            # Create message
            message = {
                'symbol': symbol,
                'timestamp': latest.index[0].strftime('%Y-%m-%d %H:%M:%S'),
                'open': float(latest['Open'].iloc[0]),
                'high': float(latest['High'].iloc[0]),
                'low': float(latest['Low'].iloc[0]),
                'close': float(latest['Close'].iloc[0]),
                'volume': int(latest['Volume'].iloc[0])
            }
            
            # Send to Kafka
            self.producer.send(self.topic, value=message)
            print(f"✅ Sent {symbol}: ${message['close']:.2f}")
            
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
                print(f"📈 Fetching data at {datetime.now().strftime('%H:%M:%S')}")
                
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
    producer.start_streaming(symbols, interval_seconds=30)  # Every 30 seconds for testing