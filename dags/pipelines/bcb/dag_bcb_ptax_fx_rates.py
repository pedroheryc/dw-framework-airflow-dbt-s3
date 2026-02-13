from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags._shared.default_args import DEFAULT_ARGS
from lf.airflow.tasks.bcb_ptax import extract_ptax_to_raw, load_raw_to_stg


with DAG(
    dag_id="bcb_ptax_fx_rates",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bcb", "ptax", "finance"],
) as dag:
    t_extract = PythonOperator(
        task_id="extract_ptax_to_raw",
        python_callable=extract_ptax_to_raw,
    )

    t_load = PythonOperator(
        task_id="load_raw_to_stg",
        python_callable=load_raw_to_stg,
        op_args=[t_extract.output],
    )

    t_extract >> t_load
