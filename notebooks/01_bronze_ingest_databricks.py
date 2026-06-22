# Databricks notebook — 01_bronze_ingest  (Databricks Free Edition / serverless)
# ---------------------------------------------------------------------------
# Pulls raw data from open APIs and lands it AS-IS into Unity Catalog (bronze).
# Same medallion design as the Fabric draft, adapted for Databricks:
#   - tables go to  {CATALOG}.{SCHEMA}.bronze_*   (Unity Catalog)
#   - serverless compute attaches automatically (no capacity limits)
#
# HOW TO USE:
#   New > Notebook. Paste each "# ==== CELL ====" block into its own cell.
#   Run CELL 0 first (connectivity smoke test) before the full ingest.
# ===========================================================================


# ==== CELL 0 — connectivity smoke test (RUN THIS FIRST) ====================
# Confirms serverless compute is allowed to reach the public internet.
import requests
r = requests.get("https://data.fingrid.fi/api/datasets/124/data",
                 headers={"x-api-key": "PASTE_YOUR_FINGRID_KEY_HERE"},
                 params={"startTime": "2025-01-01T00:00:00Z",
                         "endTime": "2025-01-01T03:00:00Z",
                         "format": "json", "pageSize": 3}, timeout=30)
print("Fingrid HTTP", r.status_code, "->", r.text[:200])
r2 = requests.get("https://opendata.fmi.fi/wfs",
                  params={"service": "WFS", "version": "2.0.0", "request": "GetCapabilities"},
                  timeout=30)
print("FMI HTTP", r2.status_code)
# Both 200 = egress OK, continue. If they hang/fail = egress blocked, tell me.


# ==== CELL 1 — config ======================================================
# NB: paste keys to run, but DO NOT commit them / show in screenshots.
FINGRID_API_KEY = "PASTE_YOUR_FINGRID_KEY_HERE"
ENTSOE_TOKEN    = "PASTE_YOUR_ENTSOE_TOKEN_HERE"

CATALOG = "workspace"        # Free Edition default Unity Catalog
SCHEMA  = "electricity"      # created below

START_DATE = "2024-01-01"    # YYYY-MM-DD (UTC)
END_DATE   = "2025-06-01"

FINGRID_DATASETS = {
    "consumption":     124,  # total consumption, Finland   MWh/h, 15 min
    "production_total": 74,  # total production, Finland     MWh/h, 15 min
    "wind":            181,  # wind power (real-time)        MW,    3 min
    "nuclear":         188,  # nuclear power (real-time)     MW,    3 min
    "hydro":           191,  # hydro power (real-time)       MW,    3 min
    "solar":           247,  # solar generation FORECAST     MWh/h, 15 min
}
FINGRID_PAUSE_S = 2.1        # throttle: 1 request / 2 s per key
PAGE_SIZE = 20000

FMI_PLACE = "Helsinki"
FMI_PARAMS = "temperature,windspeedms"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Writing bronze tables to {CATALOG}.{SCHEMA}")


# ==== CELL 2 — helpers =====================================================
import time
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

def month_chunks(start_date, end_date):
    cur = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    while cur < end:
        nxt = min(cur + relativedelta(months=1), end)
        yield cur, nxt
        cur = nxt

def iso_z(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def save_bronze(pdf, table):
    """Write a pandas frame as a managed Delta table in the current schema."""
    (spark.createDataFrame(pdf).write.format("delta")
        .mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{table}"))


# ==== CELL 3 — Fingrid → bronze ============================================
def fetch_fingrid(ds_id, start_date, end_date):
    headers = {"x-api-key": FINGRID_API_KEY}
    rows = []
    for c_start, c_end in month_chunks(start_date, end_date):
        params = {"startTime": iso_z(c_start), "endTime": iso_z(c_end),
                  "format": "json", "pageSize": PAGE_SIZE}
        r = requests.get(f"https://data.fingrid.fi/api/datasets/{ds_id}/data",
                         headers=headers, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        chunk = payload.get("data", payload if isinstance(payload, list) else [])
        rows.extend(chunk)
        if len(chunk) >= PAGE_SIZE:
            print(f"    WARN id {ds_id} {c_start:%Y-%m}: hit pageSize, may be truncated")
        time.sleep(FINGRID_PAUSE_S)
    return rows

for name, ds_id in FINGRID_DATASETS.items():
    print(f"Fingrid {name} (id {ds_id}) ...")
    raw = fetch_fingrid(ds_id, START_DATE, END_DATE)
    pdf = pd.DataFrame(raw)[["datasetId", "startTime", "endTime", "value"]]
    pdf["series"] = name
    save_bronze(pdf, f"bronze_fingrid_{name}")
    print(f"  -> bronze_fingrid_{name}: {pdf.shape[0]} rows")


# ==== CELL 4 — FMI weather → bronze ========================================
def week_chunks(start_date, end_date, days=7):
    # FMI WFS rejects long intervals (~168h max per request); chunk weekly.
    cur = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt

def fetch_fmi(place, parameters, start_date, end_date):
    ns = {"wml2": "http://www.opengis.net/waterml/2.0"}
    out = []
    for c_start, c_end in week_chunks(start_date, end_date, 7):
        params = {"service": "WFS", "version": "2.0.0", "request": "getFeature",
                  "storedquery_id": "fmi::observations::weather::timevaluepair",
                  "place": place, "parameters": parameters, "timestep": 60,
                  "starttime": iso_z(c_start), "endtime": iso_z(c_end)}
        r = requests.get("https://opendata.fmi.fi/wfs", params=params, timeout=120)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for ts in root.iter("{http://www.opengis.net/waterml/2.0}MeasurementTimeseries"):
            pname = ts.get("{http://www.opengis.net/gml/3.2}id", "").split("-")[-1]
            for p in ts.findall(".//wml2:MeasurementTVP", ns):
                t = p.find("wml2:time", ns)
                v = p.find("wml2:value", ns)
                if t is not None and v is not None and v.text:
                    try:
                        out.append({"time": t.text, "parameter": pname, "value": float(v.text)})
                    except ValueError:
                        pass
    return out

print("FMI weather ...")
weather = fetch_fmi(FMI_PLACE, FMI_PARAMS, START_DATE, END_DATE)
save_bronze(pd.DataFrame(weather), "bronze_fmi_weather")
print(f"  -> bronze_fmi_weather: {len(weather)} rows")


# ==== CELL 5 — ENTSO-E price → bronze ======================================
_RES_MIN = {"PT60M": 60, "PT30M": 30, "PT15M": 15}

def _local(tag):
    return tag.split("}")[-1]

def fetch_entsoe_price(start_date, end_date):
    out = []
    for c_start, c_end in month_chunks(start_date, end_date):
        params = {"securityToken": ENTSOE_TOKEN, "documentType": "A44",
                  "in_Domain": "10YFI-1--------U", "out_Domain": "10YFI-1--------U",
                  "periodStart": c_start.strftime("%Y%m%d%H%M"),
                  "periodEnd": c_end.strftime("%Y%m%d%H%M")}
        r = requests.get("https://web-api.tp.entsoe.eu/api", params=params, timeout=120)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for period in root.iter():
            if _local(period.tag) != "Period":
                continue
            res, base, prices = None, None, {}
            for child in period:
                lt = _local(child.tag)
                if lt == "resolution":
                    res = child.text
                elif lt == "timeInterval":
                    for c in child:
                        if _local(c.tag) == "start":
                            base = datetime.fromisoformat(c.text.replace("Z", "+00:00"))
            for pt in period.iter():
                if _local(pt.tag) != "Point":
                    continue
                pos = price = None
                for c in pt:
                    if _local(c.tag) == "position":
                        pos = int(c.text)
                    elif _local(c.tag) == "price.amount":
                        price = float(c.text)
                if pos is not None and price is not None:
                    prices[pos] = price
            if not prices or base is None:
                continue
            step = _RES_MIN.get(res, 60)
            last = None
            for i in range(1, max(prices) + 1):
                if i in prices:
                    last = prices[i]
                ts = base + timedelta(minutes=step * (i - 1))
                out.append({"time": iso_z(ts), "price_eur_mwh": last, "resolution": res})
    return out

print("ENTSO-E day-ahead price ...")
price = fetch_entsoe_price(START_DATE, END_DATE)
pdf_price = pd.DataFrame(price).drop_duplicates(subset="time")
save_bronze(pdf_price, "bronze_entsoe_price")
print(f"  -> bronze_entsoe_price: {pdf_price.shape[0]} rows")


# ==== CELL 6 — sanity check ================================================
for name in FINGRID_DATASETS:
    n = spark.table(f"{CATALOG}.{SCHEMA}.bronze_fingrid_{name}").count()
    print(f"bronze_fingrid_{name:16s}: {n:>8} rows")
print(f"{'bronze_fmi_weather':30s}: {spark.table(f'{CATALOG}.{SCHEMA}.bronze_fmi_weather').count():>8} rows")
print(f"{'bronze_entsoe_price':30s}: {spark.table(f'{CATALOG}.{SCHEMA}.bronze_entsoe_price').count():>8} rows")
