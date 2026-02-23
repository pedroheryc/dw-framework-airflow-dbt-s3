from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags._shared.default_args import DEFAULT_ARGS
from lf.airflow.tasks.stock_market_regimes import (
    extract_stock_market_regimes_to_raw,
    extract_regime_by_year_to_raw,
    extract_regime_by_ticker_to_raw,
    load_raw_to_stg,
)

with DAG(
    dag_id="stock_market_regimes",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["kaggle", "stock", "regimes", "finance"],
) as dag:

    t_extract_regimes = PythonOperator(
        task_id="extract_stock_market_regimes_to_raw",
        python_callable=extract_stock_market_regimes_to_raw,
    )

    t_load_regimes = PythonOperator(
        task_id="load_stock_market_regimes_raw_to_stg",
        python_callable=load_raw_to_stg,
        op_args=[t_extract_regimes.output],
    )

    t_extract_year = PythonOperator(
        task_id="extract_regime_by_year_to_raw",
        python_callable=extract_regime_by_year_to_raw,
    )

    t_load_year = PythonOperator(
        task_id="load_regime_by_year_raw_to_stg",
        python_callable=load_raw_to_stg,
        op_args=[t_extract_year.output],
    )

    t_extract_ticker = PythonOperator(
        task_id="extract_regime_by_ticker_to_raw",
        python_callable=extract_regime_by_ticker_to_raw,
    )

    t_load_ticker = PythonOperator(
        task_id="load_regime_by_ticker_raw_to_stg",
        python_callable=load_raw_to_stg,
        op_args=[t_extract_ticker.output],
    )

    t_extract_regimes >> t_load_regimes
    t_extract_year >> t_load_year
    t_extract_ticker >> t_load_ticker