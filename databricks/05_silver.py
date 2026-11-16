# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Capa Silver
# MAGIC
# MAGIC **Principio:** datos limpios, tipados, sin duplicados y con reglas de negocio aplicadas. Es la
# MAGIC "fuente de verdad" para el análisis y el modelo. Cada regla viene del perfil de Bronze (`04_bronze_profile`).
# MAGIC
# MAGIC | Problema (perfil de Bronze) | Tratamiento |
# MAGIC |---|---|
# MAGIC | 0 `id` duplicados, pero un mes reprocesado los generaría | Deduplicar por `id` conservando la versión ingerida más reciente |
# MAGIC | 1.49 M filas (0.7%) sin tipo de tren, sin retraso y sin hora planeada | Descartarlas: no se puede saber qué tren era ni cuándo pasaba |
# MAGIC | Llegada o salida planeada nula (~10.5% cada una) | Normal: la primera parada no tiene llegada y la última no tiene salida → `planned_time = coalesce(llegada, salida)` |
# MAGIC | Retrasos de −1,446 a 2,795 min (p99.9 = 86) | `dq_delay_outlier` si > 300 o < −30 min. Se marcan, no se borran, y se excluyen del modelo |
# MAGIC | 8.5 M paradas canceladas con `delay_in_min` | `is_canceled`. Su retraso no tiene sentido y se excluyen del label |
# MAGIC | EVA `08000001` en el dataset vs `8000001` en db-stations (match 0/146) | Normalizar con `CAST(eva AS BIGINT)` en ambos lados |
# MAGIC | 302 valores de `train_type`, muchos escritos a mano | `train_category`: Bus / Larga distancia / S-Bahn / Regional |
# MAGIC | Horas como `timestamp_ntz` en hora alemana | `hour`, `dow`, `month`, `service_date` se derivan directamente, sin conversión de zona |
# MAGIC
# MAGIC Silver conserva **todas las estaciones** con la bandera `is_core_station` (las 146 presentes antes de 2025-11):
# MAGIC el mapa de tramos necesita estaciones consecutivas, y el modelo filtra a las core en Gold.

# COMMAND ----------

from pyspark.sql import functions as F, Window

bronze = spark.table("workspace.bronze.train_stops")
n_bronze = bronze.count()

def norm_eva(col):
    """'08000001' y '8000001' -> '8000001'."""
    return F.col(col).cast("bigint").cast("string")

# COMMAND ----------

# MAGIC %md ## 1. Deduplicación y descarte de filas inutilizables

# COMMAND ----------

w = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc(), F.col("_file_modified_at").desc())
dedup = bronze.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

usable = (dedup
    .withColumn("planned_time", F.coalesce("arrival_planned_time", "departure_planned_time"))
    .filter(F.col("planned_time").isNotNull() & F.col("train_type").isNotNull()))

# COMMAND ----------

# MAGIC %md ## 2. Columnas derivadas y reglas de negocio

# COMMAND ----------

tt = F.upper(F.trim("train_type"))
line = F.upper(F.coalesce(F.trim("line_number"), F.lit("")))

LONG_DISTANCE = ["ICE", "IC", "EC", "ECE", "EN", "NJ", "RJ", "RJX", "TGV", "EST", "EUR", "ES", "D", "WB", "UEX"]

train_category = (
    F.when(tt.rlike("BUS|SEV|BSV|BVS|TAXI") | line.rlike("SEV|BUS"), "Bus")
     .when(tt.isin(LONG_DISTANCE) | tt.rlike("^FLX"), "Larga distancia")
     .when((tt == "S") | line.rlike(r"^S ?\d"), "S-Bahn")
     .otherwise("Regional"))

df = (usable
    .withColumn("eva", norm_eva("eva"))
    .withColumn("train_type", F.trim("train_type"))
    .withColumn("train_category", train_category)
    .withColumn("is_canceled",
                F.coalesce("arrival_is_canceled", F.lit(False)) | F.coalesce("departure_is_canceled", F.lit(False)))
    .withColumn("train_label", F.concat_ws(" ", "train_type", "train_number"))           # "ICE 603"
    # route_key: la línea (RE5, S3) o, si no tiene (larga distancia), el tren concreto (ICE 603)
    .withColumn("route_key", F.coalesce(F.nullif(F.trim("line_number"), F.lit("")), F.col("train_label")))
    .withColumn("service_date", F.to_date("planned_time"))
    .withColumn("hour", F.hour("planned_time"))
    .withColumn("dow", F.dayofweek("planned_time"))                                     # 1 = domingo … 7 = sábado
    .withColumn("month", F.month("planned_time"))
    .withColumn("stop_seq", F.col("train_line_station_num").cast("int"))
    .withColumn("dq_delay_outlier",
                F.coalesce((F.col("delay_in_min") > 300) | (F.col("delay_in_min") < -30), F.lit(False))))

# Estaciones core: las que existen desde el inicio (el dataset solo cubría ~100-150 estaciones antes de 2025-11)
core = (df.filter(F.col("planned_time") < "2025-11-01").select("eva").distinct()
          .withColumn("is_core_station", F.lit(True)))
df = (df.join(F.broadcast(core), "eva", "left")
        .withColumn("is_core_station", F.coalesce("is_core_station", F.lit(False))))

cols = ["id", "eva", "station_name", "train_type", "train_category", "train_number", "line_number",
        "train_label", "route_key", "final_destination_station", "train_line_ride_id", "stop_seq",
        "planned_time", "arrival_planned_time", "departure_planned_time", "service_date",
        "hour", "dow", "month", "delay_in_min", "is_canceled", "is_additional_stop",
        "is_replacement_train", "is_core_station", "dq_delay_outlier", "_source_file"]

df.select(*cols).createOrReplaceTempView("silver_train_stops_v")

# Liquid clustering por fecha y estación: las consultas de Gold filtran casi siempre por esas columnas
spark.sql("""
  CREATE OR REPLACE TABLE workspace.silver.train_stops
  CLUSTER BY (service_date, eva)
  COMMENT 'Paradas de tren limpias (todas las estaciones). Ver notebook 05_silver para las reglas.'
  AS SELECT * FROM silver_train_stops_v
""")

# COMMAND ----------

# MAGIC %md ## 3. Dimensión de estaciones con coordenadas

# COMMAND ----------

st = spark.table("workspace.bronze.stations")
dim = (st
    .select(F.explode(F.array_union(F.array("id"),
                                    F.coalesce("additionalIds", F.array().cast("array<string>")))).alias("eva_raw"),
            F.col("id").alias("station_id"),
            F.col("name").alias("station_name_official"),
            F.col("location.latitude").alias("lat"),
            F.col("location.longitude").alias("lon"),
            "ril100", "federalState", "category", "weight")
    .withColumn("eva", F.col("eva_raw").cast("bigint").cast("string"))
    .filter(F.col("eva").isNotNull() & F.col("lat").isNotNull())
    # si un EVA alternativo aparece en dos estaciones, nos quedamos con su estación principal (id == eva)
    .withColumn("_pri", (F.col("eva") == F.col("station_id")).cast("int"))
    .withColumn("_rn", F.row_number().over(Window.partitionBy("eva").orderBy(F.desc("_pri"), F.desc("weight"))))
    .filter("_rn = 1")
    .drop("eva_raw", "_pri", "_rn"))

dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.silver.dim_station")

# COMMAND ----------

# MAGIC %md ## 4. Resultados y métricas de calidad

# COMMAND ----------

s = spark.table("workspace.silver.train_stops")
n_bronze = spark.table("workspace.bronze.train_stops").count()
n_dedup = spark.table("workspace.bronze.train_stops").select("id").distinct().count()
n_silver = s.count()
print("Embudo de limpieza:")
print(f"  Bronze                         {n_bronze:>13,}")
print(f"  − duplicados por id            {n_bronze - n_dedup:>13,}")
print(f"  − sin tipo de tren / sin hora  {n_dedup - n_silver:>13,}")
print(f"  = Silver                       {n_silver:>13,}")

stats = s.agg(
    F.sum(F.col("is_core_station").cast("long")).alias("core_rows"),
    F.countDistinct(F.when(F.col("is_core_station"), F.col("eva"))).alias("core_stations"),
    F.countDistinct("eva").alias("stations"),
    F.sum(F.col("is_canceled").cast("long")).alias("canceled"),
    F.sum(F.col("dq_delay_outlier").cast("long")).alias("outliers"),
    F.sum(F.col("delay_in_min").isNull().cast("long")).alias("delay_null")).first()
print(f"\nEstaciones: {stats['stations']:,} · core: {stats['core_stations']} "
      f"({stats['core_rows']:,} filas, {100 * stats['core_rows'] / n_silver:.1f}%)")
print(f"Canceladas: {stats['canceled']:,} · outliers: {stats['outliers']:,} · retraso nulo: {stats['delay_null']:,}")

print("\nCategorías de tren (todas / core):")
for r in (s.groupBy("train_category")
           .agg(F.count("*").alias("n"), F.sum(F.col("is_core_station").cast("long")).alias("n_core"))
           .orderBy(F.desc("n")).collect()):
    print(f"  {r['train_category']:16s} {r['n']:>12,}  {r['n_core']:>12,}")

# COMMAND ----------

match = spark.sql("""
  WITH evas AS (
    SELECT eva, is_core_station, COUNT(*) AS filas FROM workspace.silver.train_stops GROUP BY eva, is_core_station)
  SELECT CASE WHEN e.is_core_station THEN 'core' ELSE 'no core' END AS grupo,
         COUNT(*) AS estaciones,
         SUM(CASE WHEN d.eva IS NOT NULL THEN 1 ELSE 0 END) AS con_coordenadas,
         ROUND(100 * SUM(CASE WHEN d.eva IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_estaciones,
         ROUND(100 * SUM(CASE WHEN d.eva IS NOT NULL THEN e.filas ELSE 0 END) / SUM(e.filas), 1) AS pct_filas
  FROM evas e LEFT JOIN workspace.silver.dim_station d USING (eva)
  GROUP BY 1 ORDER BY 1
""")
print("\nMatch de coordenadas (EVA normalizado):")
for r in match.collect():
    print(f"  {r['grupo']:8s} {r['con_coordenadas']:>5,}/{r['estaciones']:<5,} estaciones ({r['pct_estaciones']}%) · "
          f"{r['pct_filas']}% de las filas")

missing = spark.sql("""
  SELECT DISTINCT s.eva, s.station_name FROM workspace.silver.train_stops s
  LEFT ANTI JOIN workspace.silver.dim_station d USING (eva)
  WHERE s.is_core_station""").collect()
print("Core sin coordenadas:", [(r.eva, r.station_name) for r in missing] or "ninguna")

# COMMAND ----------

# MAGIC %md ## 5. Tabla de calidad de datos por mes

# COMMAND ----------

dq = spark.sql("""
  SELECT date_format(service_date, 'yyyy-MM')                   AS mes,
         COUNT(*)                                              AS filas,
         SUM(CAST(is_core_station AS INT))                     AS filas_core,
         COUNT(DISTINCT eva)                                   AS estaciones,
         SUM(CAST(is_canceled AS INT))                         AS canceladas,
         SUM(CASE WHEN delay_in_min IS NULL THEN 1 ELSE 0 END) AS retraso_nulo,
         SUM(CAST(dq_delay_outlier AS INT))                    AS outliers,
         COUNT(DISTINCT date_trunc('HOUR', planned_time))      AS horas_con_datos,
         ROUND(AVG(CASE WHEN NOT is_canceled AND NOT dq_delay_outlier AND delay_in_min IS NOT NULL
                        THEN CAST(delay_in_min > 5 AS INT) END) * 100, 1) AS pct_retraso_gt5
  FROM workspace.silver.train_stops
  GROUP BY 1 ORDER BY 1
""")
dq.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.data_quality_monthly")
print("\ngold.data_quality_monthly:")
print(f"  {'mes':8s} {'filas':>12s} {'core':>11s} {'estac':>6s} {'cancel':>9s} {'outl':>5s} {'horas':>5s} {'%>5m':>5s}")
for r in dq.collect():
    print(f"  {r['mes']:8s} {r['filas']:>12,} {r['filas_core']:>11,} {r['estaciones']:>6,} {r['canceladas']:>9,} "
          f"{r['outliers']:>5,} {r['horas_con_datos']:>5,} {r['pct_retraso_gt5']:>5}")
