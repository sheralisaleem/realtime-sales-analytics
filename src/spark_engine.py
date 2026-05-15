from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("⏳ Initializing PySpark Engine...")

# 1. Start the Spark Session and load the Kafka connector
spark = SparkSession.builder \
    .appName("RealTimeAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Keep the console clean by only showing Warnings, not standard Info logs
spark.sparkContext.setLogLevel("WARN")

# 2. Define the schema (structure) of our incoming JSON data
schema = StructType([
    StructField("OrderID", StringType()),
    StructField("Category", StringType()),
    StructField("Region", StringType()),
    StructField("Sales", DoubleType())
])

print("📡 Waiting for live data from Kafka...")

# 3. Connect to Kafka and read the stream
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "superstore_sales") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse the raw Kafka bytes into a readable DataFrame using our schema
parsed_df = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. The Analytics: Calculate Total Revenue grouped by Region
# This table will update continuously as new data arrives
live_aggregation = parsed_df.groupBy("Region").agg(_sum("Sales").alias("Total_Revenue"))

# 6. Output the live results to the terminal console
query = live_aggregation.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Keep the engine running indefinitely
query.awaitTermination()