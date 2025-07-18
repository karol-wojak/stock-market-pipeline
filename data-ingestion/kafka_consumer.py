import json
import sys
import os

# Add the project root to the Python path
# This allows us to import from the 'config' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka import KafkaConsumer
from config.database import DatabaseConnection

class StockKafkaConsumer:
    def __init__(self, kafka_servers=['localhost:9092'], topic='stock-prices'):
        """Initialize Kafka consumer"""
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers,
            group_id='stock-consumer-fresh',  # New group ID to start fresh
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'  # Only read NEW messages
        )
        self.db = DatabaseConnection()
    
    def process_messages(self):
        """Process messages from Kafka and save to MySQL database"""
        if not self.db.connect():
            print("❌ Failed to connect to database. Exiting...")
            return
        
        print("✅ Database connected. Listening for messages...")
        print("Press Ctrl+C to stop\n")
        
        message_count = 0
        
        try:
            for message in self.consumer:
                message_count += 1
                stock_data = message.value
                
                # Validate data structure
                required_fields = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
                missing_fields = [field for field in required_fields if field not in stock_data]
                if missing_fields:
                    print(f"❌ Missing fields for {stock_data.get('symbol', 'unknown')}: {missing_fields}")
                    continue
                
                # Save to database
                if self.db.insert_stock_data(stock_data):
                    market_status = "🟢" if stock_data.get('market_hours', False) else "🔴"
                    print(f"{market_status} Stored {stock_data['symbol']}: ${stock_data['close']:.2f}")
                else:
                    print(f"❌ Failed to store {stock_data['symbol']}")
                    
        except KeyboardInterrupt:
            print(f"\n🛑 Stopping consumer... (Processed {message_count} messages)")
        except Exception as e:
            print(f"\n💥 Unexpected error: {e}")
        finally:
            print("🔒 Closing connections...")
            self.consumer.close()
            self.db.close()
            print("✅ Cleanup complete")

if __name__ == "__main__":
    consumer = StockKafkaConsumer()
    consumer.process_messages()
