## Silver Layer – Stream Processing & Data Standardization

- Consumes real-time car park events from the Kafka bronze topic using Spark Structured Streaming.
- Parses incoming JSON messages into a strongly typed schema for consistent downstream processing.
- Performs data cleansing by filtering invalid records and casting fields into appropriate data types.
- Enriches records with a proper event-time timestamp to support time-aware aggregations.
- Persists the refined, analytics-ready data to disk while maintaining streaming checkpoints for fault tolerance.

## Code:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --------------------------
# 🔹 Paths
# --------------------------
SILVER_OUTPUT_PATH = r"C:\Urban-Parking-Infrastructure-Optimization\silver"
SILVER_CHECKPOINT_PATH = r"C:\Urban-Parking-Infrastructure-Optimization\silver\silver_checkpoint"

# --------------------------
# 🔹 Spark Session
# --------------------------
spark = SparkSession.builder \
    .appName("HDB Carpark Silver") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --------------------------
# 🔹 Kafka Source
# --------------------------
bronze_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hdb_carpark_bronze") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --------------------------
# 🔹 Schema
# --------------------------
schema = StructType([
    StructField("_id", StringType()),
    StructField("car_park_no", StringType()),
    StructField("address", StringType()),
    StructField("x_coord", StringType()),
    StructField("y_coord", StringType()),
    StructField("car_park_type", StringType()),
    StructField("type_of_parking_system", StringType()),
    StructField("short_term_parking", StringType()),
    StructField("free_parking", StringType()),
    StructField("night_parking", StringType()),
    StructField("car_park_decks", StringType()),
    StructField("gantry_height", StringType()),
    StructField("car_park_basement", StringType()),
    StructField("event_time", StringType()),
    StructField("source", StringType())
])

# --------------------------
# 🔹 Parse JSON
# --------------------------
parsed_df = bronze_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .selectExpr(f"from_json(json_str, '{schema.simpleString()}') AS data") \
    .select("data.*")

# --------------------------
# 🔹 Silver Transformations
# --------------------------
silver_df = parsed_df \
    .withColumn("event_time_ts", to_timestamp(col("event_time"))) \
    .filter(col("car_park_no").isNotNull()) \
    .withColumn("x_coord", col("x_coord").cast(DoubleType())) \
    .withColumn("y_coord", col("y_coord").cast(DoubleType())) \
    .withColumn("car_park_decks", col("car_park_decks").cast(DoubleType())) \
    .withColumn("gantry_height", col("gantry_height").cast(DoubleType()))

# --------------------------
# 🔹 Write Silver (Disk + Console)
# --------------------------
def process_batch(df, epoch_id):
    print(f"\n🔥 SILVER BATCH {epoch_id}")
    df.show(truncate=False)

query = silver_df.writeStream \
    .foreachBatch(
        lambda df, epoch_id: (
            df.write
              .mode("append")
              .json(SILVER_OUTPUT_PATH),
            process_batch(df, epoch_id)
        )
    ) \
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH) \
    .start()

query.awaitTermination()
```

## Output:

<img width="1920" height="1080" alt="Urban_silver" src="https://github.com/user-attachments/assets/c4eaf745-c3ab-4465-bd15-03d68bf4a00d" />
