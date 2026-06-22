-- Databricks AI/BI Dashboard — datasets for "Finnish Electricity Market Analytics"
-- Each query below becomes one Dataset on the dashboard's Data tab.
-- Catalog/schema: raw.electricity
-- ===========================================================================

-- DATASET 1: kpis  (drives the headline Counter tiles)
SELECT
  round(avg(consumption_mwh))            AS avg_demand_mwh,
  round(avg(production_total_mwh))       AS avg_production_mwh,
  round(avg(price_eur_mwh), 1)           AS avg_price_eur_mwh,
  round(avg(renewable_share_pct), 1)     AS avg_renewable_share_pct,
  round(avg(net_balance_mwh))            AS avg_net_balance_mwh   -- negative = Finland imports
FROM raw.electricity.silver_energy_hourly;

-- DATASET 2: daily_balance  (line: consumption vs production over time)
-- Exclude the final partial day so the lines don't dip to ~0 at the edge.
SELECT date, consumption_gwh, production_gwh, net_balance_gwh,
       avg_renewable_share_pct, avg_price_eur_mwh, avg_temp_c
FROM raw.electricity.gold_daily
WHERE date < (SELECT max(date) FROM raw.electricity.gold_daily)
ORDER BY date;

-- DATASET 3: generation_mix  (stacked bar: monthly GWh by source)
SELECT date_trunc('month', hour_local)        AS month,
       round(sum(nuclear_mwh)   / 1000)        AS nuclear_gwh,
       round(sum(hydro_mwh)     / 1000)        AS hydro_gwh,
       round(sum(wind_mwh)      / 1000)        AS wind_gwh,
       round(sum(solar_mwh_fc)  / 1000)        AS solar_gwh
FROM raw.electricity.silver_energy_hourly
GROUP BY 1
ORDER BY 1;

-- DATASET 4: hour_of_day  (demand & price profile across the day)
SELECT hour, avg_demand_mwh, avg_price_eur_mwh, avg_renewable_share_pct
FROM raw.electricity.gold_hour_of_day
ORDER BY hour;

-- DATASET 5: price_vs_temp  (scatter: does cold drive price up?)
SELECT date, avg_temp_c, avg_price_eur_mwh
FROM raw.electricity.gold_daily;

-- DATASET 6: forecast  (line: actual vs predicted demand, last 14 days, hourly)
-- Short hourly window so the model tracking the daily peaks/troughs is visible
-- (the full test period summed to millions and hid the fit).
SELECT hour_local, consumption_mwh AS actual_mwh, predicted_mwh
FROM raw.electricity.gold_demand_forecast
WHERE hour_local >= (SELECT max(hour_local) FROM raw.electricity.gold_demand_forecast) - INTERVAL 14 DAYS
ORDER BY hour_local;
