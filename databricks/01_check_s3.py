# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Prueba de conectividad Databricks → S3
# MAGIC
# MAGIC Databricks Free Edition restringe la salida a internet. Este notebook comprueba que el
# MAGIC cómputo serverless puede llegar a S3 con las llaves del usuario IAM `db-databricks`,
# MAGIC guardadas en el secret scope `aws`.
# MAGIC
# MAGIC - **Funciona** → el puente S3 → Volume se hace con boto3 desde Databricks.
# MAGIC - **Falla por red/timeout** → plan B: GitHub Actions copia al Volume con el Databricks CLI.

# COMMAND ----------

import socket
import boto3
from botocore.config import Config

BUCKET = dbutils.secrets.get("aws", "bucket")
s3 = boto3.client(
    "s3",
    aws_access_key_id=dbutils.secrets.get("aws", "access_key_id"),
    aws_secret_access_key=dbutils.secrets.get("aws", "secret_access_key"),
    region_name="us-east-1",
    config=Config(connect_timeout=10, read_timeout=20, retries={"max_attempts": 1}),
)

# 1) ¿Resuelve el DNS de S3?
host = "s3.us-east-1.amazonaws.com"
print("DNS", host, "→", socket.gethostbyname(host))

# 2) ¿Podemos listar landing/ con las llaves?
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="landing/")
for obj in resp.get("Contents", []):
    print(f"  {obj['Key']:50s} {obj['Size'] / 1e6:8.1f} MB")
print(f"✅ Conectividad OK: {resp.get('KeyCount', 0)} objetos en s3://{BUCKET}/landing/")
