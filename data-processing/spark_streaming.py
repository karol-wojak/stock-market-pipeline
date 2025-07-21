from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session with manually added Kafka JARs
spark = SparkSession.builder \
    .appName("StockStreamProcessor") \
    .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar,/opt/spark/jars/kafka-clients-3.5.0.jar") \
    .getOrCreate()

# Read from Kafka stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-prices") \
    .load()

# Print schema to verify Kafka connection
print("✅ Kafka stream connected")
df.printSchema()

# Decode Kafka key-value messages
df_decoded = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Write data to console for testing
query = df_decoded.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for termination
query.awaitTermination()