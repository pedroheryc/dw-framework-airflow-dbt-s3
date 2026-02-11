from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags._shared.default_args import DEFAULT_ARGS
from lf.airflow.tasks.bootstrap import bootstrap_postgres_meta, smoke_test_s3


with DAG(
    dag_id="bootstrap_platform",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bootstrap", "platform"],
) as dag:
    t1 = PythonOperator(
        task_id="bootstrap_postgres_meta",
        python_callable=bootstrap_postgres_meta,
    )

    t2 = PythonOperator(
        task_id="smoke_test_s3",
        python_callable=smoke_test_s3,
    )

    t1 >> t2
