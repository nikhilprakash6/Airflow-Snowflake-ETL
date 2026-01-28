from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

INCLUDE_PATH = "/opt/airflow/include"
if INCLUDE_PATH not in sys.path:
    sys.path.append(INCLUDE_PATH)

from etl_runner import run_etl_job

default_args = {
    "owner": "adwbi",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="adwbi_etl_jobs",
    description="Snowflake ADWBI ETL runner (master table + audit table)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "etl", "adwbi"],
) as dag:
    job_codes = [
        "JOB_01",
        "JOB_02",
        "JOB_03",
        "JOB_04",
        "JOB_05",
        "JOB_06",
        "JOB_07",
        "JOB_08",
    ]

    # Create tasks dynamically
    tasks: dict[str, PythonOperator] = {}
    for job_code in job_codes:
        tasks[job_code] = PythonOperator(
            task_id=f"run_{job_code.lower()}",
            python_callable=run_etl_job,
            op_kwargs={
                "job_code": job_code,
                "snowflake_conn_id": "snowflake_adwbi",
            },
        )

    # - (JOB_03, JOB_04, JOB_02) must run sequentially: 03 -> 04 -> 02
    # - JOB_01, JOB_05, JOB_06 can run in parallel with that chain
    # - JOB_07 and JOB_08 must run after ALL above are done

    tasks["JOB_03"] >> tasks["JOB_04"] >> tasks["JOB_02"]

    # After all upstream jobs are complete, trigger JOB_07 and JOB_08
    upstream = [
        tasks["JOB_01"],
        tasks["JOB_05"],
        tasks["JOB_06"],
        tasks["JOB_02"],  # last in chain implies 03 and 04 finished
    ]
    downstream = [tasks["JOB_07"], tasks["JOB_08"]]

    for u in upstream:
        for d in downstream:
            u >> d

