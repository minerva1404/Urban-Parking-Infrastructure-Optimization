# 🅿️ Urban Parking Infrastructure Optimization

## Real-Time HDB Carpark Streaming Pipeline (Kafka + Spark | Bronze → Silver → Gold)

---

## 📌 Project Description

This project implements a production-style, real-time data pipeline for Singapore HDB carpark infrastructure data using Kafka and Apache Spark Structured Streaming.

We ingest live data from data.gov.sg, stream it through Kafka, and progressively refine it across Bronze, Silver, and Gold layers—a classic Lakehouse architecture that slaps in interviews.

### 🔥 Key Features
- Real public government API ingestion
- Kafka-based event streaming
- Event-time aware processing
- Spark Structured Streaming transformations
- Entity-level Gold aggregations
- Infrastructure scoring logic
- Fault tolerance via checkpoints

This pipeline is designed to simulate real-world urban analytics systems used in smart cities, mobility platforms, and infra optimization teams.

---

## 🏗️ Architecture Overview

data.gov.sg API
↓
Kafka Producer
↓
Kafka Topic (Bronze)
↓
Spark Structured Streaming (Silver)
↓
Cleaned JSON Storage
↓
Spark Structured Streaming (Gold)
↓
Aggregated Parquet (Analytics-Ready)

Bronze = raw events  
Silver = cleaned, typed, validated  
Gold = analytics + scoring layer  

Simple. Scalable. Recruiter-friendly.

---

## ⚙️ Installation Instructions

### 1️⃣ Prerequisites

Make sure you have the following installed:
- Python 3.9+
- Apache Kafka
- Apache Spark 3.5+
- Java 8 / 11
- Git

---

### 2️⃣ Clone the Repository

```bash
git clone https://github.com/your-username/Urban-Parking-Infrastructure-Optimization.git
cd Urban-Parking-Infrastructure-Optimization
```

⸻

3️⃣ Install Python Dependencies

```python
pip install kafka-python requests pyspark
```

⸻

4️⃣ Start Kafka
### Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

### Start Kafka Broker
bin/kafka-server-start.sh config/server.properties
⸻

5️⃣ Create Kafka Topic

bin/kafka-topics.sh --create \
  --topic hdb_carpark_bronze \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1


⸻

▶️ Usage Examples

🚀 Step 1: Start Kafka Producer (Bronze Ingestion)

python producer.py

What happens:
	•	Pulls HDB carpark data from data.gov.sg
	•	Adds event_time + source
	•	Streams records into Kafka every 5 seconds
	•	Prints each event (full observability, no vibes-based debugging)

⸻

👂 Step 2: (Optional) Run Kafka Consumer

python consumer.py

Purpose:
	•	Validate Kafka messages
	•	Inspect partitions, offsets, and event-time
	•	Useful for debugging and demos

⸻

🧪 Step 3: Run Silver Streaming Job

spark-submit silver.py

Silver Layer does:
	•	Parses Kafka JSON
	•	Enforces schema
	•	Casts numeric fields
	•	Filters invalid records
	•	Writes clean JSON to disk
	•	Uses checkpointing for recovery

⸻

🏆 Step 4: Run Gold Streaming Job

spark-submit gold.py

Gold Layer produces:
	•	Entity-level aggregations per car park
	•	Infrastructure KPIs:
	•	Average decks
	•	Gantry height stats
	•	Night/free parking counts
	•	Infrastructure Score
	•	Writes analytics-ready Parquet

This is the layer dashboards and ML models actually care about.

⸻

🧠 Gold Layer Metrics Explained

Infrastructure Score Formula

(avg_decks * 10)
+ (avg_gantry_height * 15)
+ (night_parking_count * 2)
+ (free_parking_count * 2)

Higher score = better infrastructure capacity & accessibility.
