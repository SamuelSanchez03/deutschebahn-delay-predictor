# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: esquemas y volumes de Unity Catalog
# MAGIC
# MAGIC Crea la estructura Medallion dentro del catálogo `workspace` (el catálogo por defecto de Free Edition):
# MAGIC
# MAGIC | Esquema | Contenido |
# MAGIC |---|---|
# MAGIC | `bronze` | Datos tal como llegaron + Volume `landing` (copia de S3) y `system` (checkpoints de Auto Loader) |
# MAGIC | `silver` | Datos limpios y tipados |
# MAGIC | `gold`   | Agregados y features + Volume `exports` (archivos para la API y la web) |
# MAGIC | `ml`     | Modelos y artefactos |
# MAGIC
# MAGIC Es idempotente (`IF NOT EXISTS`): se puede correr cuantas veces se quiera.

# COMMAND ----------

CATALOG = "workspace"

for schema in ["bronze", "silver", "gold", "ml"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

volumes = {
    "bronze.landing": "Copia de s3://<bucket>/landing/ (parquet mensuales y estaciones)",
    "bronze.system": "Checkpoints y esquemas inferidos por Auto Loader",
    "gold.exports": "Archivos generados para la API y la web",
    "ml.artifacts": "Modelos exportados",
}
for name, comment in volumes.items():
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{name} COMMENT '{comment}'")

# COMMAND ----------

display(spark.sql(f"""
  SELECT volume_schema AS esquema, volume_name AS volume, comment AS contenido
  FROM {CATALOG}.information_schema.volumes
  WHERE volume_schema IN ('bronze', 'silver', 'gold', 'ml')
  ORDER BY 1, 2
"""))
