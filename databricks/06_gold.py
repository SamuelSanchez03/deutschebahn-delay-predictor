# Databricks notebook source
# MAGIC %pip install holidays==0.62 --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 · Capa Gold
# MAGIC
# MAGIC **Principio:** tablas listas para consumir por el dashboard y por el modelo.
# MAGIC
# MAGIC | Tabla | Grano | Uso |
# MAGIC |---|---|---|
# MAGIC | `gold.station_daily` | estación × día | Mapa de puntualidad y tendencias |
# MAGIC | `gold.route_daily` | línea × día | Tasas históricas por línea |
# MAGIC | `gold.segments` | par de estaciones consecutivas | Mapa de tramos donde se *genera* el retraso |
# MAGIC | `gold.model_stations` | estación | Estaciones que usa el modelo (core, con coordenadas y activas) |
# MAGIC | `gold.features` | parada | Tabla de entrenamiento **sin leakage** |
# MAGIC
# MAGIC **Paradas válidas** (base de todo Gold): no canceladas, sin outlier de retraso y sin buses de reemplazo.
# MAGIC **Label:** `delay_in_min > 5` (DB considera puntual un retraso menor a 6 minutos).

# COMMAND ----------

from pyspark.sql import functions as F, Window
import holidays
import pandas as pd

silver = spark.table("workspace.silver.train_stops")
dim = spark.table("workspace.silver.dim_station")

valid = (silver
    .filter("NOT is_canceled AND NOT dq_delay_outlier AND delay_in_min IS NOT NULL AND train_category <> 'Bus'")
    .withColumn("label", (F.col("delay_in_min") > 5).cast("int")))

GLOBAL_RATE = valid.agg(F.avg("label")).first()[0]
print(f"Tasa global de retraso > 5 min: {GLOBAL_RATE:.4f}")

# COMMAND ----------

# MAGIC %md ## 1. `station_daily` y `route_daily`

# COMMAND ----------

station_daily = (valid.groupBy("eva", "service_date").agg(
        F.count("*").alias("n_valid"),
        F.sum("label").alias("n_delayed"),
        F.avg("delay_in_min").alias("avg_delay"),
        F.percentile_approx("delay_in_min", 0.9).alias("p90_delay"))
    .withColumn("pct_on_time", 1 - F.col("n_delayed") / F.col("n_valid")))

# Las cancelaciones se cuentan desde Silver (en 'valid' ya no están)
cancels = (silver.filter("train_category <> 'Bus'")
    .groupBy("eva", "service_date")
    .agg(F.count("*").alias("n_scheduled"), F.sum(F.col("is_canceled").cast("int")).alias("n_canceled")))

(station_daily.join(cancels, ["eva", "service_date"], "left")
    .withColumn("pct_canceled", F.col("n_canceled") / F.col("n_scheduled"))
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.station_daily"))

(valid.groupBy("route_key", "service_date")
    .agg(F.count("*").alias("n_valid"), F.sum("label").alias("n_delayed"))
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("workspace.gold.route_daily"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `segments`: dónde se genera el retraso
# MAGIC
# MAGIC Para cada viaje (`train_line_ride_id`) se compara cada parada con la anterior. `delay_gain` = retraso aquí −
# MAGIC retraso en la parada anterior: los minutos que se "ganaron" en ese tramo. Solo se usan datos desde 2025-11,
# MAGIC cuando el dataset cubre todas las estaciones; así las paradas consecutivas son realmente vecinas.

# COMMAND ----------

w_ride = Window.partitionBy("train_line_ride_id").orderBy("stop_seq")
seg = (valid.filter("service_date >= '2025-11-01'")
    .select("train_line_ride_id", "stop_seq", "eva", "delay_in_min", "train_category")
    .withColumn("prev_eva", F.lag("eva").over(w_ride))
    .withColumn("prev_seq", F.lag("stop_seq").over(w_ride))
    .withColumn("prev_delay", F.lag("delay_in_min").over(w_ride))
    .filter("prev_eva IS NOT NULL AND stop_seq - prev_seq = 1 AND prev_eva <> eva")
    .withColumn("delay_gain", F.col("delay_in_min") - F.col("prev_delay")))

segments = (seg.groupBy(F.col("prev_eva").alias("from_eva"), F.col("eva").alias("to_eva"))
    .agg(F.count("*").alias("n_trips"),
         F.avg("delay_gain").alias("avg_delay_gain"),
         F.avg((F.col("delay_gain") > 2).cast("int")).alias("pct_gain_over_2min"),
         F.first("train_category").alias("main_category"))
    .filter("n_trips >= 200"))                                # evita tramos con muy pocos datos

segments.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Estaciones del modelo
# MAGIC
# MAGIC De las 146 estaciones core, el modelo usa las que tienen coordenadas y siguen activas en los últimos 3 meses
# MAGIC (≥ 1,000 paradas válidas al mes). Así se descartan EVAs sin nombre o que dejaron de reportarse.

# COMMAND ----------

last_date = valid.agg(F.max("service_date")).first()[0]
recent = (valid.filter(F.col("service_date") > F.add_months(F.lit(last_date), -3))
    .filter("is_core_station").groupBy("eva").agg(F.count("*").alias("n_recent")))

STATE_CODES = {"Baden-Württemberg": "BW", "Bayern": "BY", "Berlin": "BE", "Brandenburg": "BB", "Bremen": "HB",
               "Hamburg": "HH", "Hessen": "HE", "Mecklenburg-Vorpommern": "MV", "Niedersachsen": "NI",
               "Nordrhein-Westfalen": "NW", "Rheinland-Pfalz": "RP", "Saarland": "SL", "Sachsen": "SN",
               "Sachsen-Anhalt": "ST", "Schleswig-Holstein": "SH", "Thüringen": "TH"}
state_map = F.create_map(*[F.lit(x) for kv in STATE_CODES.items() for x in kv])

names = silver.filter("is_core_station").groupBy("eva").agg(F.max("station_name").alias("station_name"))
model_stations = (recent.filter("n_recent >= 3000")
    .join(dim.select("eva", "station_name_official", "lat", "lon", "federalState"), "eva")
    .join(names, "eva", "left")
    .withColumn("state_code", state_map[F.col("federalState")]))
model_stations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.model_stations")
n_model = model_stations.count()
print(f"Estaciones del modelo: {n_model} (de 146 core)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Festivos por estado
# MAGIC
# MAGIC Los festivos cambian según el estado (Bundesland): por ejemplo, el Corpus Christi es festivo en Baviera y no en
# MAGIC Berlín. La librería `holidays` da los festivos nacionales y estatales para cada código de estado.

# COMMAND ----------

years = range(2024, 2028)
hol_rows = [(code, d, name) for code in STATE_CODES.values()
            for d, name in holidays.Germany(subdiv=code, years=years).items()]
hol = spark.createDataFrame(pd.DataFrame(hol_rows, columns=["state_code", "service_date", "holiday_name"]))
hol.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.holidays")
print(f"Festivos (estado × fecha): {hol.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `features`: tabla de entrenamiento sin leakage
# MAGIC
# MAGIC **Regla de oro:** toda feature debe conocerse *antes* de que llegue el tren. No se usan la hora real, los
# MAGIC cambios de horario ni el retraso en la parada anterior (eso sería predicción "en ruta", otro caso de uso).
# MAGIC
# MAGIC Las **tasas históricas** se calculan con ventanas que **terminan el día anterior** (`rangeBetween(-N, -1)`), y
# MAGIC se suavizan hacia la tasa global para que una línea con pocas observaciones no dé tasas extremas:
# MAGIC
# MAGIC `tasa = (retrasos + M · tasa_global) / (paradas + M)`, con `M = 20`.

# COMMAND ----------

M = 20

def rolling_rate(daily, keys, days, name):
    """Tasa de retraso de los últimos `days` días, terminando AYER, suavizada hacia la tasa global."""
    w = (Window.partitionBy(*keys).orderBy(F.unix_date("service_date")).rangeBetween(-days, -1))
    n = F.coalesce(F.sum("n_valid").over(w), F.lit(0))
    d = F.coalesce(F.sum("n_delayed").over(w), F.lit(0))
    return daily.select(*keys, "service_date",
                        ((d + M * GLOBAL_RATE) / (n + M)).alias(name),
                        n.alias(f"{name}_n"))

base = (valid.join(F.broadcast(model_stations.select("eva", "state_code")), "eva"))

eva_daily = base.groupBy("eva", "service_date").agg(F.count("*").alias("n_valid"), F.sum("label").alias("n_delayed"))
eva_hour_daily = base.groupBy("eva", "hour", "service_date").agg(F.count("*").alias("n_valid"), F.sum("label").alias("n_delayed"))
route_daily = spark.table("workspace.gold.route_daily")

eva_7 = rolling_rate(eva_daily, ["eva"], 7, "eva_rate_7d").drop("eva_rate_7d_n")
eva_28 = rolling_rate(eva_daily, ["eva"], 28, "eva_rate_28d").drop("eva_rate_28d_n")
evh_28 = rolling_rate(eva_hour_daily, ["eva", "hour"], 28, "eva_hour_rate_28d").drop("eva_hour_rate_28d_n")
rte_7 = rolling_rate(route_daily, ["route_key"], 7, "route_rate_7d").drop("route_rate_7d_n")
rte_28 = rolling_rate(route_daily, ["route_key"], 28, "route_rate_28d").withColumnRenamed("route_rate_28d_n", "route_n_28d")

# Tipos de tren poco frecuentes se agrupan en 'OTHER' (el modelo y la API comparten este vocabulario)
type_counts = base.groupBy("train_type").count()
frequent_types = [r.train_type for r in type_counts.filter("count >= 50000").collect()]
print(f"Tipos de tren con ≥ 50,000 paradas: {len(frequent_types)} → {sorted(frequent_types)}")

features = (base
    .join(eva_7, ["eva", "service_date"], "left")
    .join(eva_28, ["eva", "service_date"], "left")
    .join(evh_28, ["eva", "hour", "service_date"], "left")
    .join(rte_7, ["route_key", "service_date"], "left")
    .join(rte_28, ["route_key", "service_date"], "left")
    .join(F.broadcast(hol.select("state_code", "service_date", F.lit(1).alias("is_holiday"))),
          ["state_code", "service_date"], "left")
    .withColumn("is_holiday", F.coalesce("is_holiday", F.lit(0)))
    .withColumn("is_weekend", F.col("dow").isin(1, 7).cast("int"))
    .withColumn("train_type_grp",
                F.when(F.col("train_type").isin(frequent_types), F.col("train_type")).otherwise("OTHER"))
    .fillna({"eva_rate_7d": GLOBAL_RATE, "eva_rate_28d": GLOBAL_RATE, "eva_hour_rate_28d": GLOBAL_RATE,
             "route_rate_7d": GLOBAL_RATE, "route_rate_28d": GLOBAL_RATE, "route_n_28d": 0})
    .select("id", "service_date", "eva", "state_code", "train_category", "train_type_grp", "route_key",
            "hour", "dow", "month", "is_weekend", "is_holiday", "stop_seq",
            "eva_rate_7d", "eva_rate_28d", "eva_hour_rate_28d", "route_rate_7d", "route_rate_28d", "route_n_28d",
            "delay_in_min", "label"))

features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.features")

# COMMAND ----------

# MAGIC %md ## 6. Verificación

# COMMAND ----------

f = spark.table("workspace.gold.features")
s = f.agg(F.count("*").alias("n"), F.avg("label").alias("pos"), F.countDistinct("eva").alias("evas"),
          F.countDistinct("route_key").alias("routes"), F.min("service_date").alias("d0"),
          F.max("service_date").alias("d1"), F.avg("is_holiday").alias("hol")).first()
print(f"gold.features: {s['n']:,} filas · {s['evas']} estaciones · {s['routes']:,} líneas · "
      f"{s['d0']} → {s['d1']}")
print(f"  % label=1: {100 * s['pos']:.1f}% · % filas en festivo: {100 * s['hol']:.2f}%")

print("\nTasa real de retraso por categoría (features):")
for r in f.groupBy("train_category").agg(F.count("*").alias("n"), F.avg("label").alias("p")).orderBy(F.desc("n")).collect():
    print(f"  {r['train_category']:16s} {r['n']:>12,}  {100 * r['p']:5.1f}%")

# Prueba de que las tasas usan solo el pasado: el primer día de cada estación no tiene historia → tasa global
first_day = (f.groupBy("eva").agg(F.min("service_date").alias("d"))
             .join(f, ["eva"]).filter(F.col("service_date") == F.col("d"))
             .agg(F.max(F.abs(F.col("eva_rate_7d") - F.lit(GLOBAL_RATE))).alias("dif")).first()["dif"])
print(f"\nChequeo anti-leakage: en el primer día de cada estación, |eva_rate_7d − global| máx = {first_day:.6f} (debe ser 0)")

print("\nCorrelación de cada tasa con el label:")
for c in ["eva_rate_7d", "eva_rate_28d", "eva_hour_rate_28d", "route_rate_7d", "route_rate_28d"]:
    print(f"  {c:20s} {f.stat.corr(c, 'label'):.3f}")

for t in ["station_daily", "route_daily", "segments", "model_stations", "holidays"]:
    print(f"gold.{t}: {spark.table(f'workspace.gold.{t}').count():,} filas")
