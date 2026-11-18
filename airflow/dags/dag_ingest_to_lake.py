import hashlib
import os
import subprocess

import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param

PYTHON = "/home/airflow/pipeline-venv/bin/python"


@dag(
    dag_id="steam_01_ingest_to_lake",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"source": Param("demo", enum=["demo", "raw"])},
)
def ingest_to_lake():
    @task
    def process(**context):
        source = context["params"]["source"]
        snapshot = hashlib.sha256(context["run_id"].encode()).hexdigest()[:20]
        env = dict(os.environ, PYSPARK_SUBMIT_ARGS=f"--driver-memory {os.environ['SPARK_DRIVER_MEMORY']} pyspark-shell")
        subprocess.run([PYTHON, "/pipeline/spark/ingest_job.py", "--source", source,
                        "--snapshot", snapshot], env=env, check=True)
        return {"source": source, "snapshot": snapshot}

    result = process()
    trigger = TriggerDagRunOperator(
        task_id="trigger_warehouse",
        trigger_dag_id="steam_02_lake_to_warehouse",
        trigger_run_id="ingest__{{ ti.xcom_pull(task_ids='process')['snapshot'] }}",
        conf={
            "source": "{{ ti.xcom_pull(task_ids='process')['source'] }}",
            "snapshot": "{{ ti.xcom_pull(task_ids='process')['snapshot'] }}",
        },
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )
    result >> trigger


ingest_to_lake()
