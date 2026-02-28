# Gold Layer – Customer Risk Metrics

Computes customer-level risk indicators based on transaction behavior.

``from pyspark.sql.functions import when, max as fmax

cust_daily = (
    silver.groupBy("txn_date", "customer_id")
         .agg(
             count("*").alias("txn_count_1d"),
             fsum("amount_num").alias("spend_1d"),
             avg("amount_num").alias("avg_amount_1d"),
             fmax("amount_num").alias("max_amount_1d"),
         )
)

cust_risk = (
    cust_daily
      .withColumn("high_activity_flag", col("txn_count_1d") >= 10)
      .withColumn("high_spend_flag", col("spend_1d") >= 1000)
      .withColumn("high_value_flag", col("max_amount_1d") >= 500)
      .withColumn(
          "risk_score",
          (col("txn_count_1d") * 1.0) +
          (col("spend_1d") / 200.0) +
          when(col("high_value_flag"), 10).otherwise(0) +
          when(col("high_activity_flag"), 5).otherwise(0) +
          when(col("high_spend_flag"), 5).otherwise(0)
      )
      .withColumn("risk_band",
          when(col("risk_score") >= 25, "HIGH")
          .when(col("risk_score") >= 12, "MEDIUM")
          .otherwise("LOW")
      )
)

cust_risk.write.format("delta").mode("overwrite").save(gold_customer_risk_path)
