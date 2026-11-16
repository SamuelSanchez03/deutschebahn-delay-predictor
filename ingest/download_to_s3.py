"""Ingesta: parquet mensuales de Hugging Face + estaciones de db-stations -> S3 (landing/).

Es idempotente: un archivo mensual solo se sube si no existe en S3 o si su tamaño cambió
(el autor del dataset reprocesa meses anteriores cuando cambia el esquema).

Las credenciales se toman de la cadena estándar de AWS:
  - local:          AWS_PROFILE=db-gh-actions
  - GitHub Actions: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY

Ejemplos:
  python ingest/download_to_s3.py --bucket <tu-bucket> --months 2024-07   # un mes, para probar
  python ingest/download_to_s3.py --bucket <tu-bucket> --since 2025-01
  python ingest/download_to_s3.py --bucket <tu-bucket> --months-back 2     # lo que usa el cron
  python ingest/download_to_s3.py --bucket <tu-bucket>                     # todo el histórico
"""
import argparse
import io
import os
import tarfile
import tempfile
import time

import boto3
import requests
from botocore.exceptions import ClientError
from huggingface_hub import HfApi, hf_hub_download

REPO_ID = "piebro/deutsche-bahn-data"
HF_DIR = "monthly_processed_data"
NPM_PACKAGE = "db-stations"
# full.ndjson (no data.ndjson): trae additionalIds (EVAs alternativos) y federalState.
STATIONS_FILE = "package/full.ndjson"


def month_of(path: str) -> str:
    """'monthly_processed_data/data-2024-07.parquet' -> '2024-07'."""
    return os.path.basename(path).removeprefix("data-")[:7]


def s3_size(s3, bucket: str, key: str) -> int | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "403"):
            # 403: sin s3:ListBucket, S3 responde 403 en vez de 404 a objetos inexistentes
            return None
        raise


def select_files(files, months, since, months_back):
    if months:
        files = [f for f in files if month_of(f.path) in set(months)]
    if since:
        files = [f for f in files if month_of(f.path) >= since]
    if months_back:
        files = files[-months_back:]
    return files


def ingest_monthly(s3, bucket, months, since, months_back, dry_run) -> None:
    api = HfApi()
    files = sorted(
        (f for f in api.list_repo_tree(REPO_ID, path_in_repo=HF_DIR, repo_type="dataset")
         if f.path.endswith(".parquet")),
        key=lambda f: f.path,
    )
    files = select_files(files, months, since, months_back)
    print(f"Meses seleccionados: {len(files)} ({sum(f.size for f in files) / 1e9:.2f} GB)")

    uploaded = skipped = 0
    for f in files:
        key = f"landing/monthly/{os.path.basename(f.path)}"
        if s3_size(s3, bucket, key) == f.size:
            print(f"  ✓ {key} ya está actualizado")
            skipped += 1
            continue
        if dry_run:
            print(f"  · {key} se subiría ({f.size / 1e6:.0f} MB)")
            continue
        with tempfile.TemporaryDirectory() as tmp:
            t0 = time.time()
            local = hf_hub_download(REPO_ID, f.path, repo_type="dataset", local_dir=tmp)
            t1 = time.time()
            s3.upload_file(local, bucket, key)
            t2 = time.time()
            print(f"  ↑ {key} ({f.size / 1e6:.0f} MB) — descarga {t1 - t0:.0f}s, subida {t2 - t1:.0f}s")
            uploaded += 1
    print(f"Mensuales: {uploaded} subidos, {skipped} sin cambios")


def ingest_stations(s3, bucket, dry_run) -> None:
    meta = requests.get(f"https://registry.npmjs.org/{NPM_PACKAGE}/latest", timeout=30)
    meta.raise_for_status()
    meta = meta.json()
    if dry_run:
        print(f"Estaciones: se subiría {NPM_PACKAGE} v{meta['version']}")
        return
    tgz = requests.get(meta["dist"]["tarball"], timeout=120)
    tgz.raise_for_status()
    with tarfile.open(fileobj=io.BytesIO(tgz.content)) as tar:
        data = tar.extractfile(STATIONS_FILE).read()
    s3.put_object(Bucket=bucket, Key="landing/stations/db-stations.ndjson", Body=data,
                  Metadata={"npm-version": meta["version"]})
    n_stations = data.count(b"\n")
    print(f"Estaciones: ↑ {NPM_PACKAGE} v{meta['version']} "
          f"({n_stations} estaciones, {len(data) / 1e6:.1f} MB)")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", default=os.environ.get("S3_BUCKET"),
                   help="Bucket destino (o variable S3_BUCKET)")
    p.add_argument("--months", nargs="+", metavar="YYYY-MM", help="Meses específicos")
    p.add_argument("--since", metavar="YYYY-MM", help="Primer mes a ingerir")
    p.add_argument("--months-back", type=int, metavar="N", help="Solo los N meses más recientes")
    p.add_argument("--skip-stations", action="store_true", help="No actualizar las estaciones")
    p.add_argument("--dry-run", action="store_true", help="Mostrar qué se subiría, sin subir")
    args = p.parse_args()
    if not args.bucket:
        p.error("falta --bucket (o la variable S3_BUCKET)")

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    ingest_monthly(s3, args.bucket, args.months, args.since, args.months_back, args.dry_run)
    if not args.skip_stations:
        ingest_stations(s3, args.bucket, args.dry_run)


if __name__ == "__main__":
    main()
