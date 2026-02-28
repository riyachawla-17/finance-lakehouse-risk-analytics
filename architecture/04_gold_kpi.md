### Purpose
Business-level aggregation.

# Gold Layer – Daily Transaction KPIs
Aggregates trusted Silver transactions into daily KPIs by country and currency.
```
df = silver.filter(col("event_time_ts").isNotNull())

w_24h = Window.partitionBy("customer_id").orderBy(col("event_time_ts").cast("long")).rangeBetween(-24*3600, 0)
w_1h  = Window.partitionBy("customer_id").orderBy(col("event_time_ts").cast("long")).rangeBetween(-1*3600, 0)

features = (
    df
      .withColumn("txn_count_1h", count("*").over(w_1h))
      .withColumn("spend_1h", fsum("amount_num").over(w_1h))
      .withColumn("txn_count_24h", count("*").over(w_24h))
      .withColumn("spend_24h", fsum("amount_num").over(w_24h))
)

cust_stats = (
    df.groupBy("customer_id")
      .agg(
          avg("amount_num").alias("avg_amount"),
          stddev("amount_num").alias("std_amount")
      )
)

features = (
    features.join(cust_stats, "customer_id", "left")
            .withColumn("amount_zscore",
                        (col("amount_num") - col("avg_amount")) / col("std_amount"))
)

scored = (
    features
      .withColumn("fraud_score",
                  (col("txn_count_1h") * 5) +
                  (col("txn_count_24h") * 1) +
                  (col("spend_1h") / 200) +
                  greatest(col("amount_zscore"), expr("0")) * 10
                 )
      .withColumn("fraud_flag", col("fraud_score") >= 25)
)
