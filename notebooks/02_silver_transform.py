# Databricks notebook — 02_silver_transform
# ---------------------------------------------------------------------------
# Cleans + conforms bronze into an hourly star: one fact + a date dimension.
#
# Key transforms:
#   - resample every source to HOURLY (bronze is 3-min / 15-min / hourly mixed)
#   - harmonise units to MWh per hour:
#       * MWh/h series (consumption, production, solar)  -> hourly mean = MWh
#       * MW series (wind, nuclear, hydro), 3-min samples -> hourly mean MW = MWh
#       * price €/MWh -> hourly mean
#   - join all sources on the hour, add a Helsinki-local calendar
# ===========================================================================


# ==== CELL 1 — config + hourly helper ======================================
from pyspark.sql import functions as F

CATALOG = "raw"
SCHEMA  = "electricity"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

def hourly_mean(table, out_col, ts_col="startTime", val_col="value"):
    """Read a bronze table, truncate its timestamp to the hour, average the value."""
    return (spark.table(table)
            .withColumn("hour_utc", F.date_trunc("hour", F.to_timestamp(ts_col)))
            .groupBy("hour_utc")
            .agg(F.avg(F.col(val_col).cast("double")).alias(out_col)))


# ==== CELL 2 — hourly series (resample + unit-harmonise) ===================
cons = hourly_mean("bronze_fingrid_consumption",     "consumption_mwh")
prod = hourly_mean("bronze_fingrid_production_total", "production_total_mwh")
wind = hourly_mean("bronze_fingrid_wind",            "wind_mwh")
nuc  = hourly_mean("bronze_fingrid_nuclear",         "nuclear_mwh")
hyd  = hourly_mean("bronze_fingrid_hydro",           "hydro_mwh")
sol  = hourly_mean("bronze_fingrid_solar",           "solar_mwh_fc")   # forecast

# ENTSO-E price (timestamp col is 'time')
price = hourly_mean("bronze_entsoe_price", "price_eur_mwh", ts_col="time", val_col="price_eur_mwh")

# FMI weather is long (parameter/value) -> pivot to columns
weather = (spark.table("bronze_fmi_weather")
           .withColumn("hour_utc", F.date_trunc("hour", F.to_timestamp("time")))
           .groupBy("hour_utc").pivot("parameter").agg(F.avg("value")))
# columns now: hour_utc, temperature, windspeedms
weather = (weather
           .withColumnRenamed("temperature", "temperature_c")
           .withColumnRenamed("windspeedms", "wind_speed_ms"))


# ==== CELL 3 — join into one hourly fact ===================================
fact = (cons
        .join(prod, "hour_utc", "inner")        # core grid balance must exist
        .join(wind, "hour_utc", "left")
        .join(nuc,  "hour_utc", "left")
        .join(hyd,  "hour_utc", "left")
        .join(sol,  "hour_utc", "left")
        .join(price, "hour_utc", "left")
        .join(weather, "hour_utc", "left"))

# Helsinki-local calendar (reporting is in local time; data is UTC)
fact = (fact
        .withColumn("hour_local", F.from_utc_timestamp("hour_utc", "Europe/Helsinki"))
        .withColumn("date",     F.to_date("hour_local"))
        .withColumn("year",     F.year("hour_local"))
        .withColumn("month",    F.month("hour_local"))
        .withColumn("day",      F.dayofmonth("hour_local"))
        .withColumn("hour",     F.hour("hour_local"))
        .withColumn("weekday",  F.dayofweek("hour_local"))           # 1=Sun..7=Sat
        .withColumn("is_weekend", F.col("weekday").isin(1, 7)))

# derived measures
fact = (fact
        .withColumn("renewable_mwh",
                    F.coalesce(F.col("wind_mwh"), F.lit(0.0))
                    + F.coalesce(F.col("hydro_mwh"), F.lit(0.0))
                    + F.coalesce(F.col("solar_mwh_fc"), F.lit(0.0)))
        .withColumn("renewable_share_pct",
                    F.round(100 * F.col("renewable_mwh") / F.col("production_total_mwh"), 1))
        .withColumn("net_balance_mwh",
                    F.round(F.col("production_total_mwh") - F.col("consumption_mwh"), 1)))

fact = fact.orderBy("hour_utc")
(fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable("silver_energy_hourly"))
print("silver_energy_hourly:", fact.count(), "rows")


# ==== CELL 4 — conformed date dimension ====================================
dim_date = (fact.select("date").distinct()
            .withColumn("year",        F.year("date"))
            .withColumn("quarter",     F.quarter("date"))
            .withColumn("month",       F.month("date"))
            .withColumn("month_name",  F.date_format("date", "MMMM"))
            .withColumn("week",        F.weekofyear("date"))
            .withColumn("day",         F.dayofmonth("date"))
            .withColumn("weekday_name", F.date_format("date", "EEEE"))
            .withColumn("is_weekend",  F.dayofweek("date").isin(1, 7))
            .orderBy("date"))
(dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable("silver_dim_date"))
print("silver_dim_date:", dim_date.count(), "rows")


# ==== CELL 5 — quick sanity peek ===========================================
display(spark.table("silver_energy_hourly")
        .select("hour_local", "consumption_mwh", "production_total_mwh",
                "wind_mwh", "nuclear_mwh", "hydro_mwh", "solar_mwh_fc",
                "renewable_share_pct", "net_balance_mwh",
                "price_eur_mwh", "temperature_c", "wind_speed_ms")
        .orderBy(F.desc("hour_local")).limit(20))
