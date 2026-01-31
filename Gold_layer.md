## Gold Layer – Aggregated Analytics & Business Metrics

- Reads the curated Silver data as a streaming source with an explicitly defined schema.
- Aggregates data at the car park entity level to compute operational and infrastructure metrics.
- Derives business-ready indicators such as average decks, gantry height, night parking counts, and composite infrastructure score.
- Maintains the latest observed event time per car park for freshness tracking.
- Writes analytics-ready parquet outputs optimized for BI tools, with streaming checkpoints for reliability.

## Code:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# --------------------------
# Paths
# --------------------------
SILVER_INPUT_PATH = r"C:\Urban-Parking-Infrastructure-Optimization\silver"
GOLD_OUTPUT_PATH = r"C:\Urban-Parking-Infrastructure-Optimization\gold\data"
CHECKPOINT_PATH = r"C:\Urban-Parking-Infrastructure-Optimization\gold\checkpoint"

os.makedirs(GOLD_OUTPUT_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# --------------------------
# Spark Session
# --------------------------
spark = SparkSession.builder \
    .appName("Gold Streaming Entity Layer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --------------------------
# Schema (MANDATORY)
# --------------------------
silver_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("car_park_no", StringType(), True),
    StructField("address", StringType(), True),
    StructField("x_coord", DoubleType(), True),
    StructField("y_coord", DoubleType(), True),
    StructField("car_park_type", StringType(), True),
    StructField("type_of_parking_system", StringType(), True),
    StructField("short_term_parking", StringType(), True),
    StructField("free_parking", StringType(), True),
    StructField("night_parking", StringType(), True),
    StructField("car_park_decks", DoubleType(), True),
    StructField("gantry_height", DoubleType(), True),
    StructField("car_park_basement", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("source", StringType(), True),
    StructField("event_time_ts", TimestampType(), True)
])

# --------------------------
# Read Silver as STREAM
# --------------------------
silver_df = spark.readStream \
    .schema(silver_schema) \
    .json(SILVER_INPUT_PATH)

# --------------------------
# Gold Aggregation (ENTITY LEVEL)
# --------------------------
gold_df = silver_df \
    .filter(col("car_park_no").isNotNull()) \
    .groupBy(
        "car_park_no",
        "car_park_type",
        "type_of_parking_system",
        "address"
    ).agg(
        max("event_time_ts").alias("latest_event_time"),
        avg("gantry_height").alias("avg_gantry_height"),
        max("gantry_height").alias("max_gantry_height"),
        min("gantry_height").alias("min_gantry_height"),
        avg("car_park_decks").alias("avg_decks"),
        max("car_park_decks").alias("max_decks"),
        sum(when(col("night_parking") == "YES", 1).otherwise(0)).alias("night_yes_count"),
        sum(when(col("free_parking") == "YES", 1).otherwise(0)).alias("free_yes_count"),
        avg("x_coord").alias("x_coord"),
        avg("y_coord").alias("y_coord")
    ).withColumn(
        "infrastructure_score",
        round(
            (col("avg_decks") * 10) +
            (col("avg_gantry_height") * 15) +
            (col("night_yes_count") * 2) +
            (col("free_yes_count") * 2),
            2
        )
    ).withColumn(
        "gold_updated_at",
        current_timestamp()
    )

# --------------------------
# foreachBatch Writer
# --------------------------
def write_gold(batch_df, batch_id):
    print(f"\n========== GOLD BATCH {batch_id} ==========\n")
    batch_df.show(truncate=False)

    batch_df.write \
        .mode("append") \
        .parquet(GOLD_OUTPUT_PATH)

# --------------------------
# Start Streaming
# --------------------------
query = gold_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_gold) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()
```

### Output:

<img width="1920" height="1080" alt="Urban_gold" src="https://github.com/user-attachments/assets/8c41194c-5e83-44fb-b495-44a892eb7400" />
