# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Puente S3 → Volume
# MAGIC
# MAGIC Databricks Free Edition no permite registrar el bucket como *external location*, así que
# MAGIC copiamos `s3://<bucket>/landing/` al Volume `workspace.bronze.landing` con boto3.
# MAGIC
# MAGIC **Conversión de tipos (nanosegundos → microsegundos).** Los parquet del dataset los escribe DuckDB
# MAGIC con columnas `INT64 TIMESTAMP(NANOS)`. Spark solo soporta precisión de microsegundos y en serverless
# MAGIC no se puede activar `spark.sql.legacy.parquet.nanosAsLong`, así que leerlos falla con
# MAGIC `PARQUET_TYPE_ILLEGAL`. Al copiar cada mes, `pyarrow` reescribe esas columnas como
# MAGIC `TIMESTAMP(MICROS)`, sin zona horaria, igual que el original. No se pierde información: los horarios tienen
# MAGIC precisión de minutos. S3 conserva el archivo original intacto.
# MAGIC
# MAGIC **Incremental.** Un manifiesto (`bronze/system/bridge_manifest.json`) guarda el ETag (la huella)
# MAGIC de cada archivo de S3 ya procesado. Si el ETag no cambió, el archivo se salta. Si el autor reprocesa
# MAGIC un mes, el ETag cambia y el mes se vuelve a convertir.

# COMMAND ----------

import json
import os
import shutil
import tempfile
import time

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

BUCKET = dbutils.secrets.get("aws", "bucket")
VOL = "/Volumes/workspace/bronze/landing"
MANIFEST = "/Volumes/workspace/bronze/system/bridge_manifest.json"
ALLOWED = {"monthly/": ".parquet", "stations/": ".ndjson"}
BATCH_ROWS = 1_000_000

s3 = boto3.client(
    "s3",
    aws_access_key_id=dbutils.secrets.get("aws", "access_key_id"),
    aws_secret_access_key=dbutils.secrets.get("aws", "secret_access_key"),
    region_name="us-east-1",
)

manifest = json.load(open(MANIFEST)) if os.path.exists(MANIFEST) else {}

# COMMAND ----------

def spark_compatible_schema(schema: pa.Schema) -> pa.Schema:
    """Cambia timestamp[ns] por timestamp[us] y conserva la zona horaria (aquí ninguna)."""
    return pa.schema([
        pa.field(f.name, pa.timestamp("us", tz=f.type.tz), f.nullable)
        if pa.types.is_timestamp(f.type) and f.type.unit == "ns" else f
        for f in schema
    ])


def convert_parquet(src: str, dst: str) -> int:
    """Reescribe el parquet por bloques (poca memoria) con timestamps en microsegundos."""
    pf = pq.ParquetFile(src)
    target = spark_compatible_schema(pf.schema_arrow)
    rows = 0
    with pq.ParquetWriter(dst, target, compression="zstd") as writer:
        for batch in pf.iter_batches(batch_size=BATCH_ROWS):
            writer.write_table(pa.Table.from_batches([batch]).cast(target))
            rows += batch.num_rows
    return rows

# COMMAND ----------

processed, skipped = [], 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="landing/"):
    for obj in page.get("Contents", []):
        rel = obj["Key"].removeprefix("landing/")              # monthly/data-2024-07.parquet
        if not any(rel.startswith(p) and rel.endswith(ext) for p, ext in ALLOWED.items()):
            continue
        dest = f"{VOL}/{rel}"
        if manifest.get(rel, {}).get("etag") == obj["ETag"] and os.path.exists(dest):
            skipped += 1
            continue

        t0 = time.time()
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with tempfile.TemporaryDirectory() as tmp:
            raw = os.path.join(tmp, "raw")
            s3.download_file(BUCKET, obj["Key"], raw)
            if rel.endswith(".parquet"):
                out = os.path.join(tmp, "out.parquet")
                rows = convert_parquet(raw, out)
            else:
                out, rows = raw, None
            # Se escribe primero en el disco local y luego se copia completo al Volume,
            # para que Auto Loader nunca vea un archivo a medio escribir.
            shutil.copyfile(out, dest)

        manifest[rel] = {"etag": obj["ETag"], "s3_size": obj["Size"], "rows": rows,
                         "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        with open(MANIFEST, "w") as fh:                       # se guarda tras cada archivo
            json.dump(manifest, fh, indent=1)
        processed.append(rel)
        extra = f", {rows:,} filas" if rows else ""
        print(f"↓ {rel} ({obj['Size'] / 1e6:.0f} MB{extra}) en {time.time() - t0:.0f}s")

print(f"\nProcesados: {len(processed)} · sin cambios: {skipped}")

# COMMAND ----------

# Resumen del Volume
for sub in ALLOWED:
    d = f"{VOL}/{sub}"
    files = sorted(os.listdir(d)) if os.path.isdir(d) else []
    size = sum(os.path.getsize(f"{d}{f}") for f in files)
    rows = sum((manifest.get(f"{sub}{f}", {}).get("rows") or 0) for f in files)
    print(f"{sub:10s} {len(files):3d} archivos  {size / 1e9:6.2f} GB  {rows:,} filas")
