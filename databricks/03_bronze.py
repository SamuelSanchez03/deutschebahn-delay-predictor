# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa Bronze
# MAGIC
# MAGIC **Principio:** guardar los datos *tal como llegaron*, más columnas de linaje. Aquí no se limpia
# MAGIC ni se filtra nada; eso es trabajo de Silver.
# MAGIC
# MAGIC | Tabla | Fuente | Modo de carga |
# MAGIC |---|---|---|
# MAGIC | `bronze.train_stops` | `landing/monthly/*.parquet` | **Auto Loader** incremental: solo procesa archivos nuevos o modificados (lo controla un *checkpoint*) |
# MAGIC | `bronze.stations` | `landing/stations/db-stations.ndjson` | Reescritura completa (archivo pequeño) |
# MAGIC
# MAGIC Columnas de linaje: `_source_file` (de qué archivo viene la fila), `_file_modified_at` y `_ingested_at`.
# MAGIC
# MAGIC Si el autor reprocesa un mes, el puente lo reemplaza en el Volume y Auto Loader lo vuelve a leer
# MAGIC (`allowOverwrites`). Eso deja filas repetidas en Bronze, que Silver elimina quedándose con la versión más reciente.

# COMMAND ----------

from pyspark.sql import functions as F

LANDING = "/Volumes/workspace/bronze/landing"
SYS = "/Volumes/workspace/bronze/system"

# COMMAND ----------

# MAGIC %md ## Paradas de tren (Auto Loader)

# COMMAND ----------

TABLE = "workspace.bronze.train_stops"

def table_version() -> int:
    if not spark.catalog.tableExists(TABLE):
        return -1
    return spark.sql(f"DESCRIBE HISTORY {TABLE} LIMIT 1").first()["version"]

version_before = table_version()

query = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{SYS}/schemas/train_stops")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # si el autor agrega columnas, se incorporan
    .option("cloudFiles.allowOverwrites", "true")               # meses reprocesados se vuelven a leer
    .load(f"{LANDING}/monthly/")
    .withColumn("_source_file", F.col("_metadata.file_name"))
    .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", f"{SYS}/checkpoints/train_stops")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                                 # procesa lo pendiente y termina
    .toTable(TABLE))
query.awaitTermination()

# Filas agregadas por ESTA corrida: suma de los commits nuevos en el historial Delta
# (query.recentProgress no está disponible en serverless una vez terminada la query).
new_commits = (spark.sql(f"DESCRIBE HISTORY {TABLE}")
               .filter(f"version > {version_before} AND operation = 'STREAMING UPDATE'")
               .collect())
added = sum(int(c["operationMetrics"].get("numOutputRows", 0)) for c in new_commits)
print(f"Commits nuevos: {len(new_commits)} · filas agregadas en esta corrida: {added:,}")

# COMMAND ----------

# MAGIC %md ## Estaciones

# COMMAND ----------

(spark.read.json(f"{LANDING}/stations/db-stations.ndjson")
    .withColumn("_ingested_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("workspace.bronze.stations"))
print(f"bronze.stations: {spark.table('workspace.bronze.stations').count():,} estaciones")

# COMMAND ----------

# MAGIC %md ## Verificación

# COMMAND ----------

bronze = spark.table("workspace.bronze.train_stops")
print("Tipos de las columnas de tiempo:")
for f in bronze.schema.fields:
    if "time" in f.name:
        print(f"  {f.name:25s} {f.dataType.simpleString()}")

by_file = (bronze.groupBy("_source_file")
    .agg(F.count("*").alias("filas"),
         F.min("time").alias("desde"), F.max("time").alias("hasta"))
    .orderBy("_source_file"))
for r in by_file.collect():
    print(f"  {r['_source_file']:25s} {r['filas']:>12,}  {r['desde']} → {r['hasta']}")
print(f"\nTOTAL bronze.train_stops: {bronze.count():,} filas")
