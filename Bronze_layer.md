## Bronze Layer – Kafka Producer & Consumer

### Kafka Producer
- Fetches car park infrastructure data from Singapore’s public government API.
- Enriches each record with ingestion-time metadata such as event timestamp and data source.
- Publishes records individually to a Kafka topic to simulate real-time streaming ingestion.
- Introduces controlled delays between messages to mimic continuous event flow instead of batch loads.

### Kafka Consumer
- Subscribes to the bronze Kafka topic and consumes messages from the earliest available offset.
- Deserializes JSON events and validates message structure, offsets, and partitions.
- Acts as a verification layer to ensure data is successfully ingested into Kafka.
- Provides real-time visibility into event movement across the streaming pipeline.

### Producer Code:
```python
import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# -----------------------------
# Kafka Config
# -----------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "hdb_carpark_bronze"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# HDB Carpark Information API
# -----------------------------
RESOURCE_ID = "d_23f946fa557947f93a8043bbef41dd09"
API_URL = (
    f"https://data.gov.sg/api/action/datastore_search"
    f"?resource_id={RESOURCE_ID}&limit=5000"
)

print("🚀 Starting HDB Carpark Producer...")

response = requests.get(API_URL)
response.raise_for_status()
data = response.json()

records = data["result"]["records"]
print(f"✅ Found {len(records)} carpark records")

# -----------------------------
# Produce to Kafka
# -----------------------------
for idx, record in enumerate(records, start=1):
    # ✅ REAL EVENT TIME (ingestion snapshot time)
    record["event_time"] = datetime.utcnow().isoformat()
    record["source"] = "data.gov.sg_hdb_carpark_info"

    # Send to Kafka
    producer.send(TOPIC_NAME, record)

    # 🔥 PRINT WHAT IS ACTUALLY GOING TO KAFKA
    print(f"📤 Sent {idx}/{len(records)} → Kafka:")
    print(json.dumps(record, indent=2))

    # 🛑 SLOW DOWN: 5 seconds between each record
    time.sleep(5)

producer.flush()
producer.close()

print("🏁 Producer finished successfully.")
```

Output:

<img width="1920" height="1080" alt="Urban_producer" src="https://github.com/user-attachments/assets/7b363dd1-1e8d-4d02-9264-a33491999c39" />


### Consumer Code:
```python
import json
import time
from kafka import KafkaConsumer

# -----------------------------
# Kafka Config
# -----------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "hdb_carpark_bronze"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",   # read from beginning
    enable_auto_commit=True,
    group_id="hdb_carpark_bronze_consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("👂 Listening to Kafka topic:", TOPIC_NAME)

# -----------------------------
# Consume Messages
# -----------------------------
for message in consumer:
    record = message.value

    print("📥 Received from Kafka")
    print(f"  🧱 Partition : {message.partition}")
    print(f"  📍 Offset    : {message.offset}")
    print(f"  ⏰ EventTime : {record.get('event_time')}")
    print(json.dumps(record, indent=2))
    print("-" * 80)

    # 🛑 Slow down consumption: 5 seconds between messages
    time.sleep(5)
```

Output:
<img width="1920" height="1080" alt="Urban_consumer" src="https://github.com/user-attachments/assets/aa53f09a-f22f-4482-a51f-1892871616a5" />
