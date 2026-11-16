"""Lista los parquet mensuales del dataset en Hugging Face y su tamaño, sin descargarlos.

Sirve para decidir cuántos meses ingerir (decisión D2) antes de gastar ancho de banda o cuota.
Uso: python ingest/measure_dataset.py
"""
from huggingface_hub import HfApi

REPO_ID = "piebro/deutsche-bahn-data"

api = HfApi()
total = 0
files = sorted(
    (f for f in api.list_repo_tree(REPO_ID, path_in_repo="monthly_processed_data",
                                   repo_type="dataset")
     if f.path.endswith(".parquet")),
    key=lambda f: f.path,
)
for f in files:
    total += f.size
    print(f"{f.path:55s} {f.size / 1e6:8.1f} MB")
print(f"\nArchivos: {len(files)}   TOTAL: {total / 1e9:.2f} GB")
