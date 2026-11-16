# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Perfil de calidad de Bronze
# MAGIC
# MAGIC Mide los problemas de calidad **antes** de limpiar, para justificar cada regla de Silver con datos:
# MAGIC duplicados, nulos, rango de retrasos, cancelaciones, formato de EVAs y cobertura de estaciones.

# COMMAND ----------

from pyspark.sql import functions as F

b = spark.table("workspace.bronze.train_stops")
total = b.count()
print(f"Filas en Bronze: {total:,}\n")

# COMMAND ----------

# MAGIC %md ## Duplicados

# COMMAND ----------

ids = b.select(F.countDistinct("id").alias("ids")).first()["ids"]
print(f"ids distintos: {ids:,} · filas con id repetido: {total - ids:,}")
dup_ids = b.groupBy("id").count().filter("count > 1")
print(f"ids que aparecen más de una vez: {dup_ids.count():,}")
dup_ids.orderBy(F.desc("count")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## Nulos por columna

# COMMAND ----------

nulls = b.select([F.sum(F.col(c).isNull().cast("long")).alias(c) for c in b.columns if not c.startswith("_")]).first().asDict()
for c, n in sorted(nulls.items(), key=lambda kv: -kv[1]):
    print(f"  {c:28s} {n:>12,}  ({100 * n / total:5.1f}%)")

# COMMAND ----------

# MAGIC %md ## Retrasos, cancelaciones y tipo de parada

# COMMAND ----------

b2 = b.withColumn("is_canceled", F.coalesce("arrival_is_canceled", F.lit(False)) | F.coalesce("departure_is_canceled", F.lit(False)))

print("delay_in_min (no cancelados, no nulos):")
q = (b2.filter("NOT is_canceled AND delay_in_min IS NOT NULL")
       .select(F.percentile_approx("delay_in_min", [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 0.999, 1.0], 10000).alias("p"),
               F.avg("delay_in_min").alias("avg"),
               F.avg((F.col("delay_in_min") > 5).cast("double")).alias("pct_gt5"),
               F.sum((F.col("delay_in_min") > 300).cast("long")).alias("gt300"),
               F.sum((F.col("delay_in_min") > 180).cast("long")).alias("gt180"),
               F.sum((F.col("delay_in_min") < 0).cast("long")).alias("neg"),
               F.sum((F.col("delay_in_min") < -30).cast("long")).alias("lt_m30")).first())
print("  percentiles [0,1,5,25,50,75,95,99,99.9,100]:", q["p"])
print(f"  promedio {q['avg']:.2f} min · % > 5 min: {100 * q['pct_gt5']:.1f}%")
print(f"  > 180 min: {q['gt180']:,} · > 300 min: {q['gt300']:,} · negativos: {q['neg']:,} · < -30: {q['lt_m30']:,}")

print("\nCancelaciones y delay_in_min:")
b2.groupBy("is_canceled").agg(F.count("*").alias("filas"),
                              F.sum(F.col("delay_in_min").isNull().cast("long")).alias("delay_nulo"),
                              F.avg("delay_in_min").alias("delay_prom")).show()

print("Banderas de tipo de parada:")
b2.select(*[F.avg(F.col(c).cast("double")).alias(c) for c in
            ["is_additional_stop", "is_replacement_train", "has_plan_record"]]).show()

print("Sin hora planeada (ni llegada ni salida):",
      f"{b2.filter('arrival_planned_time IS NULL AND departure_planned_time IS NULL').count():,}")

# COMMAND ----------

# MAGIC %md ## Tipos de tren

# COMMAND ----------

b.groupBy("train_type").count().orderBy(F.desc("count")).show(25)

# COMMAND ----------

# MAGIC %md ## Estaciones core y formato de EVA

# COMMAND ----------

core = b.filter("time < '2025-11-01'").select("eva").distinct()
n_core = core.count()
core_rows = b.join(core, "eva").count()
print(f"Estaciones core (presentes antes de 2025-11): {n_core}")
print(f"Filas de estaciones core en todo el periodo: {core_rows:,} ({100 * core_rows / total:.1f}%)")
print(f"Estaciones totales: {b.select('eva').distinct().count():,}")

print("\nLongitud de los EVA en el dataset:")
b.select(F.length("eva").alias("len")).groupBy("len").count().orderBy("len").show()

st = spark.table("workspace.bronze.stations")
st_ids = (st.select(F.explode(F.array_union(F.array("id"), F.coalesce("additionalIds", F.array().cast("array<string>")))).alias("eva"))
            .distinct())
print("Ejemplos de id en db-stations:", [r.id for r in st.select("id").limit(5).collect()])
m = core.join(st_ids, "eva", "left_semi").count()
print(f"Core con coordenadas (match directo): {m}/{n_core}")
core.join(st_ids, "eva", "left_anti").join(b.select("eva", "station_name").distinct(), "eva").show(20, truncate=False)

print("¿Una estación core tiene varios nombres?")
(b.join(core, "eva").groupBy("eva").agg(F.countDistinct("station_name").alias("nombres"))
   .filter("nombres > 1").show(10))
