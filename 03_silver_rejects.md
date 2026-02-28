# Silver Layer – Rejected Records & Data Quality

This notebook captures invalid transaction records and assigns explicit reject reasons for monitoring and governance.

## Reject Rules
- Missing transaction ID
- Missing customer ID
- Non-positive transaction amount

``bronze_static = spark.read.format("delta").load(bronze_path)

from pyspark.sql.functions import lit

sample = bronze_static.limit(3)

bad_negative = sample.withColumn("amount", lit(-999))
bad_null_customer = sample.withColumn("customer_id", lit(None))
bad_null_txn = sample.withColumn("txn_id", lit(None))

intentional_bad = bad_negative \
    .union(bad_null_customer) \
    .union(bad_null_txn)

intentional_bad.write \
    .format("delta") \
    .mode("append") \
    .save(bronze_path)

print("Intentional bad records added to Bronze.")

from pyspark.sql.functions import col, trim, upper, to_timestamp, current_timestamp, lit, when

bronze_df = spark.readStream.format("delta").load(bronze_path)

base = (
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

rejected = (
    base
      .withColumn(
          "reject_reason",
          when(col("txn_id").isNull(), lit("missing_txn_id"))
          .when(col("customer_id").isNull(), lit("missing_customer_id"))
          .when(col("merchant_id").isNull(), lit("missing_merchant_id"))
          .when(col("amount_num").isNull(), lit("amount_not_numeric"))
          .when(col("amount_num") <= 0, lit("amount_non_positive"))
          .otherwise(lit(None))
      )

q_rej = (
    rejected.writeStream
      .format("delta")
      .option("checkpointLocation", silver_reject_ckpt)
      .outputMode("append")
      .trigger(availableNow=True)
      .start(silver_reject_path)
)

q_rej
      .filter(col("reject_reason").isNotNull())
)
