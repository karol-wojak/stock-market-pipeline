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
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        self.db = DatabaseConnection()
    
    def process_messages(self):
        """Process messages from Kafka and save to MySQL database"""
        if not self.db.connect():
            print("❌ Failed to connect to database. Exiting...")
            return
        
        print("📡 Listening for stock data messages...")
        print("Press Ctrl+C to stop\n")
        
        try:
            for message in self.consumer:
                stock_data = message.value
                print(f"📊 Received: {stock_data}")
                
                # Save to database
                if self.db.insert_stock_data(stock_data):
                    print(f"✅ Stored {stock_data['symbol']} in database")
                else:
                    print(f"❌ Failed to store {stock_data['symbol']} in database")
        except KeyboardInterrupt:
            print("\n🛑 Stopping consumer...")
        finally:
            self.consumer.close()
            self.db.close()

if __name__ == "__main__":
    consumer = StockKafkaConsumer()
    consumer.process_messages()