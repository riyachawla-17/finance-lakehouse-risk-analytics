# Bronze Layer – Streaming Ingestion (Auto Loader)

This notebook ingests raw financial transaction files from cloud storage using Databricks Auto Loader and writes them to a Delta Lake Bronze table.

## Responsibilities
- Streaming ingestion of JSON files
- Schema enforcement
- Metadata capture
- Exactly-once processing

## Source
- Landing zone: cloud object storage
- Format: JSON

## Core Logic
Read-Stream:
``from pyspark.sql.functions import current_timestamp, input_file_name, col, to_timestamp

bronze_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .schema(transaction_schema)  
        .load(landing_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("ingest_time", to_timestamp(col("ingest_time")))
)

Write-Stream:
query = (
    bronze_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start(bronze_path)
)
