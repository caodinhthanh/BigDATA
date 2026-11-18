import os
import subprocess

import pendulum
from airflow.decorators import dag, task

VENV = "/home/airflow/pipeline-venv/bin"


@dag(
    dag_id="steam_02_lake_to_warehouse",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
)
def lake_to_warehouse():
    @task
    def load(**context):
        conf = context["dag_run"].conf or {}
        source = conf.get("source")
        snapshot = conf.get("snapshot")
        if source not in {"demo", "raw"} or not snapshot:
            raise ValueError("Trigger steam_01_ingest_to_lake with source demo or raw.")
        env = dict(os.environ, PYSPARK_SUBMIT_ARGS=f"--driver-memory {os.environ['SPARK_DRIVER_MEMORY']} pyspark-shell")
        subprocess.run([f"{VENV}/python", "/pipeline/spark/load_warehouse.py", "--source",
                        source, "--snapshot", snapshot], env=env, check=True)
        return "demo" if source == "demo" else "local"

    @task
    def build_and_test(target):
        subprocess.run([f"{VENV}/dbt", "build", "--project-dir", "/pipeline/dbt",
                        "--profiles-dir", "/pipeline/dbt", "--target", target], cwd="/tmp", check=True)

    build_and_test(load())


lake_to_warehouse()
