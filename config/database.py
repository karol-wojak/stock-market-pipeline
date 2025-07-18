import mysql.connector
from mysql.connector import Error
import json

class DatabaseConnection:
    def __init__(self):
        """Initialize database connection"""
        self.connection_params = {
            'host': '127.0.0.1',
            'port': 3306,
            'database': 'stockdb',
            'user': 'admin',
            'password': 'password'
        }
        self.connection = None
    
    def connect(self):
        """Connect to MySQL database"""
        try:
            self.connection = mysql.connector.connect(**self.connection_params)
            return True
        except Error as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def insert_stock_data(self, stock_data):
        """Insert stock data into database"""
        try:
            cursor = self.connection.cursor()
            
            insert_query = """
            INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                stock_data['symbol'],
                stock_data['timestamp'],
                stock_data['open'],
                stock_data['high'],
                stock_data['low'],
                stock_data['close'],
                stock_data['volume']
            )
            
            cursor.execute(insert_query, values)
            self.connection.commit()
            cursor.close()
            return True
            
        except Error as e:
            print(f"❌ Database insert failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("🔌 Database connection closed")

# Test the connection
if __name__ == "__main__":
    db = DatabaseConnection()
    if db.connect():
        print("🎉 Database test successful!")
        db.close()
    else:
        print("💥 Database test failed!")