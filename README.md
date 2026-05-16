# 🚀 Real-Time Sales Analytics Engine

![Real-Time Data Streaming](https://img.shields.io/badge/Data-Streaming-blue)
![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-orange?logo=apachespark)
![Python](https://img.shields.io/badge/Python-3.x-yellow?logo=python)

A robust, real-time Business Intelligence (BI) pipeline that simulates continuous live sales data streams and performs on-the-fly aggregations using **Apache Kafka** and **Apache Spark**.
..... 

---

## 📖 Overview

This project demonstrates a modern real-time data streaming architecture. It consists of two main components:
1. **Kafka Producer (`producer.py`)**: Simulates a live data stream by reading a historical sales CSV dataset and publishing each transaction as a JSON message to a Kafka topic in real-time.
2. **PySpark Engine (`spark_engine.py`)**: Subscribes to the Kafka topic, parses the incoming JSON data using a defined schema, and continuously calculates total revenue aggregated by region.

## 🛠️ Tech Stack

- **Python**: Core programming language
- **Apache Kafka**: Message broker for handling the live data stream
- **Apache Spark (PySpark)**: Distributed data processing engine for real-time analytics
- **Pandas**: Used for initial CSV data manipulation

## 📂 Project Structure

```text
BI_Project/
│
├── data/
│   └── sales.csv             # Source dataset containing historical sales
│
├── src/
│   ├── producer.py           # Python script to stream data to Kafka
│   └── spark_engine.py       # PySpark script to consume and analyze data
│
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation (You are here)
```

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed on your local machine:
- Python 3.8+
- [Apache Kafka](https://kafka.apache.org/downloads) (and Zookeeper)
- Java 8 or 11 (required for PySpark)

### 1. Install Dependencies

Clone this repository and install the required Python packages:

```bash
pip install -r requirements.txt
```

### 2. Start Kafka Environment

Before running the scripts, you need to start your local Kafka environment. 
Open your terminal and run the following commands (from your Kafka installation directory):

**Start Zookeeper:**
```bash
bin/windows/zookeeper-server-start.bat config/zookeeper.properties
```
*(Use `bin/zookeeper-server-start.sh` on Mac/Linux)*

**Start Kafka Broker:**
```bash
bin/windows/kafka-server-start.bat config/server.properties
```
*(Use `bin/kafka-server-start.sh` on Mac/Linux)*

### 3. Run the Application

You will need two separate terminal windows to run the producer and the analytics engine concurrently.

**Terminal 1: Start the Real-Time Data Stream (Producer)**
This script reads `data/sales.csv` and starts sending records to the `superstore_sales` Kafka topic.
```bash
cd src
python producer.py
```

**Terminal 2: Start the PySpark Analytics Engine**
This script listens to the `superstore_sales` topic and prints real-time revenue aggregations grouped by region.
```bash
cd src
python spark_engine.py
```

## 📊 How it Works

1. The **Producer** converts tabular data (CSV) into structured JSON payloads:
   ```json
   {"OrderID": "CA-2023-152156", "Category": "Furniture", "Region": "South", "Sales": 261.96}
   ```
2. The **PySpark Engine** reads the stream, enforces a strict `StructType` schema, and applies a stateful aggregation:
   `groupBy("Region").agg(sum("Sales"))`
3. As new data points enter the Kafka topic, the PySpark console output updates continuously, reflecting the true real-time revenue across different regions.

---
*Built with ❤️ for real-time data engineering.*
