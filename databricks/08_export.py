# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Exportación para la API y la web
# MAGIC
# MAGIC Genera los archivos que consumen la API y la página web, y los sube a `s3://<bucket>/exports/`.
# MAGIC
# MAGIC | Destino | Archivo | Contenido |
# MAGIC |---|---|---|
# MAGIC | `exports/api/` | `model.txt.gz` | Modelo LightGBM (comprimido) |
# MAGIC | | `model_meta.json` | Features, vocabulario, umbral, métricas |
# MAGIC | | `lookups.json` | Tasas históricas más recientes, estaciones y líneas por estación |
# MAGIC | `exports/web/` | `stations.json` | Estaciones con puntualidad de los últimos 30 días |
# MAGIC | | `segments.json` | Tramos con mayor retraso generado |
# MAGIC | | `trends/{eva}.json` | Serie diaria de puntualidad (180 días) |
# MAGIC | | `meta.json` | Fecha de los datos, métricas y atribución |
# MAGIC
# MAGIC **Tasas "al día de hoy".** La API predice para fechas posteriores al último dato disponible. Por eso las tasas
# MAGIC se calculan con la ventana que termina en el **último día con datos**, con el mismo suavizado que en Gold:
# MAGIC `(retrasos + 20·global) / (paradas + 20)`. Son las features que tendría una parada del día siguiente.

# COMMAND ----------

import gzip
import json
import os
import shutil
import tempfile

import boto3
from pyspark.sql import functions as F

M = 20
ART = "/Volumes/workspace/ml/artifacts/delay_classifier"
OUT = tempfile.mkdtemp()                      # se escribe en local y luego se sube (Volumes no permiten seek)
os.makedirs(f"{OUT}/api", exist_ok=True)
os.makedirs(f"{OUT}/web/trends", exist_ok=True)

meta = json.load(open(f"{ART}/model_meta.json"))
silver = spark.table("workspace.silver.train_stops")
sd = spark.table("workspace.gold.station_daily")
rd = spark.table("workspace.gold.route_daily")
ms = spark.table("workspace.gold.model_stations")

# Último día con volumen real: hay unas pocas filas sueltas en el mes siguiente (trenes de la última noche
# con hora planeada pasada la medianoche) que no deben definir la fecha de corte.
last_date = (sd.groupBy("service_date").agg(F.sum("n_valid").alias("n")).filter("n >= 10000")
               .agg(F.max("service_date")).first()[0])
sd = sd.filter(F.col("service_date") <= F.lit(last_date))
rd = rd.filter(F.col("service_date") <= F.lit(last_date))
g = sd.agg((F.sum("n_delayed") / F.sum("n_valid")).alias("g")).first()["g"]   # misma tasa global que Gold
print(f"Último día con datos: {last_date} · tasa global: {g:.4f}")

def window_rate(daily, keys, days, name):
    """Tasa suavizada de los últimos `days` días que terminan en last_date."""
    return (daily.filter(F.col("service_date") > F.date_sub(F.lit(last_date), days))
            .groupBy(*keys)
            .agg(((F.sum("n_delayed") + M * g) / (F.sum("n_valid") + M)).alias(name),
                 F.sum("n_valid").alias(f"{name}_n")))

def dump(obj, path):
    with open(path, "w") as fh:
        json.dump(obj, fh, separators=(",", ":"), ensure_ascii=False, default=str)

# COMMAND ----------

# MAGIC %md ## 1. Tablas de consulta para la API

# COMMAND ----------

stations = (ms.select("eva", F.coalesce("station_name_official", "station_name").alias("name"),
                      "lat", "lon", "state_code").orderBy("name").toPandas())

eva_rates = (window_rate(sd, ["eva"], 7, "eva_rate_7d")
             .join(window_rate(sd, ["eva"], 28, "eva_rate_28d"), "eva", "outer")
             .join(ms.select("eva"), "eva").toPandas().set_index("eva"))

valid_recent = (silver
    .filter("NOT is_canceled AND NOT dq_delay_outlier AND delay_in_min IS NOT NULL AND train_category <> 'Bus'")
    .filter((F.col("service_date") > F.date_sub(F.lit(last_date), 28)) & (F.col("service_date") <= F.lit(last_date)))
    .join(F.broadcast(ms.select("eva")), "eva")
    .withColumn("label", (F.col("delay_in_min") > 5).cast("int")))

eva_hour = (valid_recent.groupBy("eva", "hour")
            .agg(((F.sum("label") + M * g) / (F.count("*") + M)).alias("rate")).toPandas())

route_rates = (window_rate(rd, ["route_key"], 7, "route_rate_7d")
               .join(window_rate(rd, ["route_key"], 28, "route_rate_28d"), "route_key", "outer"))

# Líneas que pasan por cada estación en las últimas 4 semanas (para los selectores de la web y la validación)
frequent = set(meta["vocab"]["train_type_grp"])
routes = (valid_recent.groupBy("eva", "route_key")
    .agg(F.count("*").alias("n"),
         F.mode("train_type").alias("train_type"),
         F.mode("train_category").alias("train_category"),
         F.percentile_approx("stop_seq", 0.5).alias("stop_seq"),
         F.sort_array(F.collect_set("hour")).alias("hours"),
         F.mode("final_destination_station").alias("destination"))
    .filter("n >= 8")                                            # al menos ~2 paradas por semana
    .join(route_rates, "route_key", "left")
    .toPandas())

lookups = {
    "as_of": str(last_date), "global_rate": g, "smoothing_m": M,
    "stations": {r.eva: {"name": r.name, "lat": r.lat, "lon": r.lon, "state_code": r.state_code}
                 for r in stations.itertuples()},
    "eva_rates": {eva: {"eva_rate_7d": r.eva_rate_7d, "eva_rate_28d": r.eva_rate_28d}
                  for eva, r in eva_rates.iterrows()},
    "eva_hour_rates": {f"{r.eva}|{int(r.hour)}": r.rate for r in eva_hour.itertuples()},
    "routes": {},
}
for r in routes.sort_values(["eva", "n"], ascending=[True, False]).itertuples():
    lookups["routes"].setdefault(r.eva, {})[r.route_key] = {
        "train_type": r.train_type,
        "train_type_grp": r.train_type if r.train_type in frequent else "OTHER",
        "train_category": r.train_category, "stop_seq": int(r.stop_seq), "hours": [int(h) for h in r.hours],
        "destination": r.destination, "n_28d": int(r.n),
        "route_rate_7d": r.route_rate_7d, "route_rate_28d": r.route_rate_28d,
        "route_n_28d": int(r.route_rate_28d_n or 0)}

dump(lookups, f"{OUT}/api/lookups.json")
with open(f"{ART}/model.txt", "rb") as src, gzip.open(f"{OUT}/api/model.txt.gz", "wb") as dst:
    shutil.copyfileobj(src, dst)
shutil.copyfile(f"{ART}/model_meta.json", f"{OUT}/api/model_meta.json")

n_routes = sum(len(v) for v in lookups["routes"].values())
print(f"API: {len(lookups['stations'])} estaciones · {n_routes:,} combinaciones estación-línea · "
      f"{len(lookups['eva_hour_rates']):,} tasas estación-hora")

# COMMAND ----------

# MAGIC %md ## 2. Archivos para la web

# COMMAND ----------

st30 = (sd.filter(F.col("service_date") > F.date_sub(F.lit(last_date), 30))
    .groupBy("eva").agg(F.sum("n_valid").alias("n"),
                        (1 - F.sum("n_delayed") / F.sum("n_valid")).alias("pct_on_time"),
                        (F.sum(F.col("avg_delay") * F.col("n_valid")) / F.sum("n_valid")).alias("avg_delay"),
                        (F.sum("n_canceled") / F.sum("n_scheduled")).alias("pct_canceled"))
    .join(ms.select("eva", F.coalesce("station_name_official", "station_name").alias("name"), "lat", "lon"), "eva")
    .select("eva", "name", "lat", "lon", "n",
            F.round("pct_on_time", 3).alias("pct_on_time"), F.round("avg_delay", 1).alias("avg_delay"),
            F.round("pct_canceled", 3).alias("pct_canceled"))
    .toPandas())
dump(st30.to_dict("records"), f"{OUT}/web/stations.json")

dim = spark.table("workspace.silver.dim_station")
seg = (spark.table("workspace.gold.segments")
    .join(dim.select(F.col("eva").alias("from_eva"), F.col("station_name_official").alias("from_name"),
                     F.col("lat").alias("lat1"), F.col("lon").alias("lon1")), "from_eva")
    .join(dim.select(F.col("eva").alias("to_eva"), F.col("station_name_official").alias("to_name"),
                     F.col("lat").alias("lat2"), F.col("lon").alias("lon2")), "to_eva")
    .orderBy(F.desc("avg_delay_gain")).limit(500)
    .select("from_eva", "to_eva", "from_name", "to_name", "lat1", "lon1", "lat2", "lon2", "n_trips",
            F.round("avg_delay_gain", 2).alias("avg_delay_gain"),
            F.round("pct_gain_over_2min", 3).alias("pct_gain_over_2min"), "main_category")
    .toPandas())
dump(seg.to_dict("records"), f"{OUT}/web/segments.json")

tr = (sd.filter(F.col("service_date") > F.date_sub(F.lit(last_date), 180))
    .join(ms.select("eva"), "eva")
    .select("eva", "service_date", F.round("pct_on_time", 3).alias("pct_on_time"),
            F.round("avg_delay", 1).alias("avg_delay"), "n_valid")
    .toPandas())
for eva, grp in tr.groupby("eva"):
    dump(grp.drop(columns="eva").sort_values("service_date").to_dict("records"), f"{OUT}/web/trends/{eva}.json")

dump({"updated_at": str(last_date), "model_version": 1, "metrics_test": meta["metrics_test"],
      "threshold": meta["threshold"],
      "attribution": "Datos: Deutsche Bahn (CC BY 4.0) vía piebro/deutsche-bahn-data · Coordenadas: db-stations (CC BY 4.0)"},
     f"{OUT}/web/meta.json")
print(f"Web: {len(st30)} estaciones · {len(seg)} tramos · {tr['eva'].nunique()} series de tendencia")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tendencias y ficha del modelo
# MAGIC
# MAGIC Todo se calcula sobre `gold.features` (las 131 estaciones del modelo, sin buses ni cancelaciones).

# COMMAND ----------

import csv
import mlflow

feat = spark.table("workspace.gold.features").filter(F.col("service_date") <= F.lit(last_date))

# Puntualidad mensual por categoría de tren (toda la historia)
monthly = (feat.groupBy(F.date_format("service_date", "yyyy-MM").alias("month"), "train_category")
    .agg(F.count("*").alias("n"), F.round(1 - F.avg("label"), 4).alias("pct_on_time"),
         F.round(F.avg("delay_in_min"), 2).alias("avg_delay"))
    .filter("n >= 1000").orderBy("month").toPandas())
dump(monthly.to_dict("records"), f"{OUT}/web/trends_monthly.json")

# Mapa de calor día × hora (últimas 12 semanas), total y por categoría
recent = feat.filter(F.col("service_date") > F.date_sub(F.lit(last_date), 84))
def heat(d):
    return (d.groupBy("dow", "hour").agg(F.count("*").alias("n"), F.round(F.avg("label"), 4).alias("rate"))
             .orderBy("dow", "hour").toPandas().to_dict("records"))
heatmap = {"from": str(recent.agg(F.min("service_date")).first()[0]), "to": str(last_date),
           "all": heat(recent),
           "by_category": {c: heat(recent.filter(F.col("train_category") == c))
                           for c in ["Larga distancia", "Regional", "S-Bahn"]}}
dump(heatmap, f"{OUT}/web/heatmap.json")

# Ficha del modelo: métricas, split, hiperparámetros, importancia y calibración
mlflow.set_tracking_uri("databricks")
params = mlflow.get_run(meta["mlflow_run_id"]).data.params
importance = [{"feature": r[0], "gain": float(r[1])}
              for r in list(csv.reader(open(f"{ART}/feature_importance.csv")))[1:]]
calibration = [{k: float(v) for k, v in r.items()} for r in csv.DictReader(open(f"{ART}/calibration_test.csv"))]
counts = {"bronze_rows": spark.table("workspace.bronze.train_stops").count(),
          "silver_rows": silver.count(), "feature_rows": feat.count(),
          "stations_total": silver.select("eva").distinct().count(), "model_stations": len(st30)}
model_card = {
    "updated_at": str(last_date), "label": meta["label"], "threshold": meta["threshold"],
    "metrics_test": meta["metrics_test"], "split": meta["split"], "global_rate": g,
    "best_iteration": meta["best_iteration"],
    "params": {k: params[k] for k in ["learning_rate", "num_leaves", "min_child_samples", "subsample",
                                      "colsample_bytree", "reg_lambda", "train_rows"] if k in params},
    "importance": importance, "calibration": calibration, "counts": counts}
dump(model_card, f"{OUT}/web/model.json")
print(f"Tendencias: {monthly['month'].nunique()} meses · mapa de calor {heatmap['from']} → {heatmap['to']}")
print("Conteos:", counts)

# COMMAND ----------

# MAGIC %md ## 4. Copia al Volume y subida a S3

# COMMAND ----------

VOL = "/Volumes/workspace/gold/exports"
s3 = boto3.client("s3",
    aws_access_key_id=dbutils.secrets.get("aws", "access_key_id"),
    aws_secret_access_key=dbutils.secrets.get("aws", "secret_access_key"),
    region_name="us-east-1")
BUCKET = dbutils.secrets.get("aws", "bucket")

total, n_files = 0, 0
for root, _, files in os.walk(OUT):
    for name in files:
        local = os.path.join(root, name)
        rel = os.path.relpath(local, OUT)                         # api/lookups.json, web/trends/8000105.json
        os.makedirs(os.path.dirname(f"{VOL}/{rel}"), exist_ok=True)
        shutil.copyfile(local, f"{VOL}/{rel}")
        ctype = "application/json" if name.endswith(".json") else "application/gzip"
        s3.upload_file(local, BUCKET, f"exports/{rel}", ExtraArgs={"ContentType": ctype})
        total += os.path.getsize(local)
        n_files += 1

print(f"Subidos {n_files} archivos ({total / 1e6:.1f} MB) a s3://<bucket>/exports/ y a {VOL}")
for f in ["api/model.txt.gz", "api/model_meta.json", "api/lookups.json", "web/stations.json", "web/segments.json"]:
    print(f"  {f:22s} {os.path.getsize(f'{OUT}/{f}') / 1e6:6.2f} MB")
