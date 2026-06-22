# Databricks notebook — 03_gold_marts
# ---------------------------------------------------------------------------
# Business-ready marts + a demand forecast, built on silver_energy_hourly.
#   - gold_daily            : daily totals for trend charts
#   - gold_hour_of_day      : average price / demand profile by hour
#   - gold_demand_forecast  : actual vs predicted hourly demand (Spark ML)
# ===========================================================================


# ==== CELL 1 — config ======================================================
from pyspark.sql import functions as F
from datetime import timedelta

CATALOG = "raw"; SCHEMA = "electricity"
spark.sql(f"USE {CATALOG}.{SCHEMA}")
silver = spark.table("silver_energy_hourly")


# ==== CELL 2 — gold_daily ==================================================
daily = (silver.groupBy("date").agg(
            F.round(F.sum("consumption_mwh") / 1000, 1).alias("consumption_gwh"),
            F.round(F.sum("production_total_mwh") / 1000, 1).alias("production_gwh"),
            F.round(F.sum("net_balance_mwh") / 1000, 1).alias("net_balance_gwh"),
            F.round(F.avg("renewable_share_pct"), 1).alias("avg_renewable_share_pct"),
            F.round(F.avg("price_eur_mwh"), 1).alias("avg_price_eur_mwh"),
            F.round(F.avg("temperature_c"), 1).alias("avg_temp_c"))
         .orderBy("date"))
daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
     .saveAsTable("gold_daily")
print("gold_daily:", daily.count(), "rows")


# ==== CELL 3 — gold_hour_of_day (demand & price profile) ===================
hod = (silver.groupBy("hour").agg(
          F.round(F.avg("consumption_mwh"), 0).alias("avg_demand_mwh"),
          F.round(F.avg("price_eur_mwh"), 1).alias("avg_price_eur_mwh"),
          F.round(F.avg("renewable_share_pct"), 1).alias("avg_renewable_share_pct"))
       .orderBy("hour"))
hod.write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
   .saveAsTable("gold_hour_of_day")
print("gold_hour_of_day:", hod.count(), "rows")


# ==== CELL 4 — demand forecast (Spark ML) ==================================
# Predict hourly consumption from weather + calendar. Time-based holdout
# (train on history, test on the last 90 days) so it's an honest forecast.
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.window import Window

# Demand is strongly autocorrelated — add lag features (known at day-ahead time:
# yesterday's and last week's demand for the same hour). Without these the model
# can't beat the mean on a low-variance summer holdout (R2 went negative).
w = Window.orderBy("hour_utc")
base = (silver.select("hour_utc", "hour_local", "consumption_mwh",
                      "temperature_c", "wind_speed_ms", "hour", "weekday", "month")
        .withColumn("lag_24h",  F.lag("consumption_mwh", 24).over(w))
        .withColumn("lag_168h", F.lag("consumption_mwh", 168).over(w)))

feat = ["temperature_c", "wind_speed_ms", "hour", "weekday", "month", "lag_24h", "lag_168h"]
df = base.dropna(subset=["consumption_mwh"] + feat)

cutoff = df.agg(F.max("hour_utc")).first()[0] - timedelta(days=90)
train = df.filter(F.col("hour_utc") < F.lit(cutoff))
test  = df.filter(F.col("hour_utc") >= F.lit(cutoff))

pipe = Pipeline(stages=[
    VectorAssembler(inputCols=feat, outputCol="features"),
    GBTRegressor(featuresCol="features", labelCol="consumption_mwh",
                 maxIter=80, maxDepth=5),
])
model = pipe.fit(train)
pred = model.transform(test)

mae  = RegressionEvaluator(labelCol="consumption_mwh", predictionCol="prediction",
                           metricName="mae").evaluate(pred)
rmse = RegressionEvaluator(labelCol="consumption_mwh", predictionCol="prediction",
                           metricName="rmse").evaluate(pred)
r2   = RegressionEvaluator(labelCol="consumption_mwh", predictionCol="prediction",
                           metricName="r2").evaluate(pred)
avg_demand = test.agg(F.avg("consumption_mwh")).first()[0]
print(f"Demand forecast (90-day holdout): MAE={mae:,.0f} MWh  RMSE={rmse:,.0f}  "
      f"R2={r2:.3f}  (MAE = {100*mae/avg_demand:.1f}% of avg demand)")

forecast = (pred.select("hour_local", "consumption_mwh",
                        F.round("prediction", 0).alias("predicted_mwh"))
            .withColumn("error_mwh", F.round(F.col("consumption_mwh") - F.col("predicted_mwh"), 0))
            .orderBy("hour_local"))
forecast.write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable("gold_demand_forecast")
print("gold_demand_forecast:", forecast.count(), "rows (test period)")


# ==== CELL 5 — feature importances (nice for the write-up) =================
gbt_model = model.stages[-1]
for f, imp in sorted(zip(feat, gbt_model.featureImportances.toArray()),
                     key=lambda x: -x[1]):
    print(f"{f:16s} {imp:.3f}")
