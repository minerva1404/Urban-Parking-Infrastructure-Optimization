## Architecture Summary

This project follows a real-time **Bronze → Silver → Gold streaming architecture** to process and analyze Singapore HDB car park infrastructure data.

Live car park metadata is ingested from the **data.gov.sg API** and published to **Apache Kafka**, which acts as the raw (Bronze) streaming layer and ensures replayability.

**Spark Structured Streaming** consumes Kafka events, enforces schema, cleans data, casts coordinates and numeric fields, and normalizes event-time data in the **Silver layer**.

The **Gold layer** performs entity-level aggregations per car park, computing infrastructure metrics such as deck capacity, gantry height statistics, parking availability indicators, and a derived **Infrastructure Score** for analytical and decision-making use cases.

Checkpointing and event-time processing ensure fault tolerance, consistency, and production-grade reliability.

![Architecture](https://github.com/user-attachments/assets/17b85f78-c64e-4185-a4fe-15e410a051a5)
