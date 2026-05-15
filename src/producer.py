import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

# 1. Configure the Kafka Producer
# This converts our Python data into JSON format before streaming it
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = 'superstore_sales'

print("🚀 Starting Live Data Stream...")

# 2. Load the CSV Data
# Note: Adjust the path if you run this from a different directory
csv_path = os.path.join(os.path.dirname(__file__), '../data/sales.csv')
df = pd.read_csv(csv_path)

# 3. Stream data row-by-row
for index, row in df.iterrows():
    data_dict = row.to_dict()
    
    # Send the row to the Kafka topic
    producer.send(topic_name, value=data_dict)
    print(f"Sent: {data_dict}")
    
    # Wait 2 seconds before sending the next row
    time.sleep(2)

print("✅ Streaming Complete.")
# 3. Stream data infinitely to simulate a 24/7 business
while True:
    for index, row in df.iterrows():
        data_dict = row.to_dict()
        producer.send(topic_name, value=data_dict)
        print(f"Sent: {data_dict}")
        time.sleep(2)