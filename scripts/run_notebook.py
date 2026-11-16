"""Sube los notebooks de databricks/ al workspace, ejecuta uno en serverless y muestra su salida.

Uso:
  python scripts/run_notebook.py 01_check_s3
  python scripts/run_notebook.py 02_bronze --no-upload

Requiere el Databricks CLI autenticado (databricks auth login).
"""
import argparse
import base64
import json
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def cli(*args: str) -> str:
    res = subprocess.run(["databricks", *args, "-o", "json"], capture_output=True, text=True)
    if res.returncode != 0:
        sys.exit(f"Error en 'databricks {' '.join(args)}':\n{res.stderr}")
    return res.stdout


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("notebook", help="Nombre del notebook en databricks/, sin .py")
    p.add_argument("--no-upload", action="store_true", help="No volver a subir los notebooks")
    args = p.parse_args()

    user = json.loads(cli("current-user", "me"))["userName"]
    remote_dir = f"/Users/{user}/db-delay-predictor"

    if not args.no_upload:
        subprocess.run(["databricks", "workspace", "import-dir", str(REPO / "databricks"),
                        remote_dir, "--overwrite"], check=True, capture_output=True)
        print(f"↑ notebooks subidos a {remote_dir}")

    payload = {"run_name": args.notebook,
               "tasks": [{"task_key": "main",
                          "notebook_task": {"notebook_path": f"{remote_dir}/{args.notebook}"}}]}
    print(f"▶ ejecutando {args.notebook} en serverless (espera a que termine)...")
    run_id = json.loads(cli("jobs", "submit", "--json", json.dumps(payload), "--no-wait"))["run_id"]
    t0 = time.time()
    while True:
        run = json.loads(cli("jobs", "get-run", str(run_id)))
        state = run["state"]
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(15)
    task_run_id = run["tasks"][0]["run_id"]
    print(f"Estado: {state.get('result_state') or state.get('life_cycle_state')} "
          f"({(time.time() - t0) / 60:.1f} min)  {state.get('state_message', '')}")
    print(f"Corrida: {run.get('run_page_url')}\n")

    # La salida impresa de cada celda viene dentro del HTML exportado de la corrida.
    export = json.loads(cli("jobs", "export-run", str(task_run_id), "--views-to-export", "CODE"))
    html = export["views"][0]["content"]
    marker = "__DATABRICKS_NOTEBOOK_MODEL = '"
    start = html.index(marker) + len(marker)
    model = json.loads(urllib.parse.unquote(
        base64.b64decode(html[start:html.index("'", start)]).decode()))
    failed = False
    for cmd in model["commands"]:
        res = cmd.get("results") or {}
        items = res.get("data") if res.get("type") == "listResults" else [res]
        for item in items or []:
            if isinstance(item, dict) and item.get("type") in ("ansi", "text") and item.get("data"):
                print(item["data"])
        if cmd.get("error"):
            clean = re.sub(r"\x1b\[[0-9;]*m", "", cmd["error"]).strip().splitlines()
            print("❌ ERROR en celda:\n  " + "\n  ".join(clean[-6:]))
            failed = True

    if failed or state.get("result_state") != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
