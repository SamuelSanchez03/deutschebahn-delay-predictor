"""Crea (o actualiza) el Job mensual de Databricks que corre el pipeline de punta a punta.

  02_s3_to_volume -> 03_bronze -> 05_silver -> 06_gold -> 07_train -> 08_export

Cada tarea depende de la anterior; si una falla, las siguientes no corren y Databricks avisa por correo
al dueño del Job. Corre en serverless (environment version 4, necesaria para Spark ML).

Uso:
  python scripts/create_job.py              # sube los notebooks y crea/actualiza el Job
  python scripts/create_job.py --run-now    # además lo ejecuta una vez
  python scripts/create_job.py --paused     # crea el Job con la programación en pausa

Requiere el Databricks CLI autenticado (databricks auth login).
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_notebook import ENV_VERSION, REPO, cli  # noqa: E402

JOB_NAME = "db-delay-monthly"
TASKS = ["02_s3_to_volume", "03_bronze", "05_silver", "06_gold", "07_train", "08_export"]
CRON = "0 0 6 3 * ?"            # Quartz: día 3 de cada mes, 06:00 UTC (la ingesta corre el día 2)


def job_settings(remote_dir: str, user: str, paused: bool) -> dict:
    tasks = []
    for i, nb in enumerate(TASKS):
        task = {"task_key": nb, "environment_key": "default",
                "notebook_task": {"notebook_path": f"{remote_dir}/{nb}"},
                "timeout_seconds": 2 * 3600}
        if i:
            task["depends_on"] = [{"task_key": TASKS[i - 1]}]
        tasks.append(task)
    return {
        "name": JOB_NAME,
        "description": "Pipeline mensual: S3 -> Bronze -> Silver -> Gold -> entrenamiento -> exportación a S3",
        "max_concurrent_runs": 1,
        "environments": [{"environment_key": "default", "spec": {"environment_version": ENV_VERSION}}],
        "tasks": tasks,
        "schedule": {"quartz_cron_expression": CRON, "timezone_id": "UTC",
                     "pause_status": "PAUSED" if paused else "UNPAUSED"},
        "email_notifications": {"on_failure": [user]},
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-now", action="store_true", help="Ejecutar el Job una vez después de crearlo")
    p.add_argument("--paused", action="store_true", help="Dejar la programación mensual en pausa")
    args = p.parse_args()

    user = json.loads(cli("current-user", "me"))["userName"]
    remote_dir = f"/Users/{user}/db-delay-predictor"
    subprocess.run(["databricks", "workspace", "import-dir", str(REPO / "databricks"), remote_dir, "--overwrite"],
                   check=True, capture_output=True)
    print(f"↑ notebooks subidos a {remote_dir}")

    settings = job_settings(remote_dir, user, args.paused)
    jobs = json.loads(cli("jobs", "list", "--name", JOB_NAME) or "[]")
    if jobs:
        job_id = jobs[0]["job_id"]
        cli("jobs", "reset", "--json", json.dumps({"job_id": job_id, "new_settings": settings}))
        print(f"✓ Job {JOB_NAME} actualizado (id {job_id})")
    else:
        job_id = json.loads(cli("jobs", "create", "--json", json.dumps(settings)))["job_id"]
        print(f"✓ Job {JOB_NAME} creado (id {job_id})")
    print(f"  Programación: {CRON} UTC ({'en pausa' if args.paused else 'activa'})")

    if args.run_now:
        run = json.loads(cli("jobs", "run-now", str(job_id), "--no-wait"))
        print(f"▶ corrida lanzada: run_id {run['run_id']}")


if __name__ == "__main__":
    main()
