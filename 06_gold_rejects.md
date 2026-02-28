# Gold Layer – Data Quality Metrics

Aggregates rejected records for monitoring data quality trends.

``from pyspark.sql.functions import count

rejects = spark.read.format("delta").load(silver_reject_path)
rejects = rejects.withColumn("txn_date", to_date(col("event_time_ts")))

gold_rejects_daily = (
    rejects.groupBy("txn_date", "reject_reason")
           .agg(count("*").alias("reject_count"))
)

gold_rejects_daily.write.format("delta").mode("overwrite").save(gold_rejects_daily_path)
