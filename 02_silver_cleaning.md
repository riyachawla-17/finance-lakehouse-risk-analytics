# Silver Layer – Data Cleaning & Standardization

This notebook transforms raw Bronze data into a trusted Silver dataset by applying validation rules, normalization, and deduplication.

## Responsibilities
- Trim and standardize fields
- Type casting
- Timestamp normalization
- Deduplication
- Filter valid records

#Code:
```silver_path = f"{root}delta/silver/transactions_clean/"
silver_checkpoint = f"{root}checkpoints/silver_transactions_clean/"

from pyspark.sql.functions import col, trim, upper, to_timestamp, current_timestamp

clean_df = (
    bronze_df
      .withColumn("txn_id", trim(col("txn_id")))
      .withColumn("customer_id", trim(col("customer_id")))
      .withColumn("merchant_id", trim(col("merchant_id")))
      .withColumn("currency", upper(trim(col("currency"))))
      .withColumn("country", upper(trim(col("country"))))
      .withColumn("city", trim(col("city")))
      .withColumn("event_time_ts", to_timestamp(col("event_time")))
      .withColumn("ingest_time_ts", to_timestamp(col("ingest_time")))
      .withColumn("amount_num", col("amount").cast("double"))
      .withColumn("_silver_processed_at", current_timestamp())
)

trusted_df = clean_df.filter(
    col("txn_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("merchant_id").isNotNull() &
    col("amount_num").isNotNull() &
    (col("amount_num") > 0)
)

dedup_df = trusted_df.withWatermark("event_time_ts", "2 days").dropDuplicates(["txn_id"])

q = (dedup_df.writeStream
      .format("delta")
      .option("checkpointLocation", silver_checkpoint)
      .outputMode("append")
      .trigger(availableNow=True)
      .start(silver_path))

q
