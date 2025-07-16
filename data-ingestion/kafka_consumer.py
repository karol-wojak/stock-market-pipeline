import json
from kafka import KafkaConsumer

def test_kafka_consumer():
    """Simple consumer to verify Kafka messages"""
    print("🔍 Testing Kafka consumer...")
    print("📡 Listening for stock data messages...")
    print("Press Ctrl+C to stop\n")
    
    try:
        consumer = KafkaConsumer(
            'stock-prices',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'  # Only get new messages
        )
        
        for message in consumer:
            data = message.value
            print(f"📊 {data['symbol']}: ${data['close']:.2f} at {data['timestamp']} (Volume: {data['volume']:,})")
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer...")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_kafka_consumer()