
from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from lf.airflow.context import build_context
from lf.datasets.contract import DatasetContract
from lf.datasets.naming import run_id as build_run_id, stg_table_name
from lf.ingest.raw_ingest import ingest_records_to_raw
from lf.load.from_manifest import from_manifest_ndjson_to_postgres

def _sql_quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ensure_stg_table_all_text(engine, table_name: str, columns: list[str]) -> None:
    if "." in table_name:
        schema, tbl = table_name.split(".", 1)
    else:
        schema, tbl = "public", table_name

    cols_sql = ",\n  ".join(f"{_sql_quote_ident(c)} TEXT" for c in columns)

    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {_sql_quote_ident(schema)};
    CREATE TABLE IF NOT EXISTS {_sql_quote_ident(schema)}.{_sql_quote_ident(tbl)} (
      {cols_sql},
      run_id TEXT,
      ingested_at TIMESTAMP
    );
    """

    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)

def _extract_csv_to_raw(
    *,
    source: str,
    dataset: str,
    domain: str,
    mode: str,
    csv_path: str,
) -> dict:
    """
    Reads a CSV, converts to records, writes RAW + manifest.
    Returns {run_id, manifest_bucket, manifest_key, stg_table, aws_region}.
    """
    ctx = build_context()

    contract = DatasetContract(
        source=source,
        dataset=dataset,
        domain=domain,
        mode=mode,
    )

    df = pd.read_csv(csv_path, sep=",", encoding="ascii")
    df.columns = [c.strip() for c in df.columns]

    rid = build_run_id(contract)

    now = datetime.utcnow().isoformat()
    records = df.to_dict(orient="records")
    for r in records:
        r["run_id"] = rid
        r["ingested_at"] = now

    result = ingest_records_to_raw(
        contract=contract,
        records=records,
        ext="json",
    )

    stg_table = stg_table_name(contract)

    return {
        "run_id": result.run_id,
        "manifest_bucket": result.raw_bucket,
        "manifest_key": result.manifest_key,
        "stg_table": stg_table,
        "aws_region": ctx.settings.aws_region,
        "columns": list(df.columns) + ["run_id", "ingested_at"],
    }


def extract_stock_market_regimes_to_raw(execution_date: str | None = None) -> dict:
    csv_path = Variable.get(
        "STOCK_REGIMES_CSV_PATH",
        default_var="/kaggle/input/stock-market-regimes-20002026/stock_market_regimes_2000_2026.csv",
    )
    return _extract_csv_to_raw(
        source="kaggle",
        dataset="stock_market_regimes_2000_2026",
        domain="finance",
        mode="append",
        csv_path=csv_path,
    )


def extract_regime_by_year_to_raw(execution_date: str | None = None) -> dict:
    csv_path = Variable.get(
        "STOCK_REGIMES_BY_YEAR_CSV_PATH",
        default_var="/kaggle/input/stock-market-regimes-20002026/regime_by_year.csv",
    )
    return _extract_csv_to_raw(
        source="kaggle",
        dataset="stock_market_regime_by_year",
        domain="finance",
        mode="append",
        csv_path=csv_path,
    )


def extract_regime_by_ticker_to_raw(execution_date: str | None = None) -> dict:
    csv_path = Variable.get(
        "STOCK_REGIMES_BY_TICKER_CSV_PATH",
        default_var="/kaggle/input/stock-market-regimes-20002026/regime_analysis_by_ticker.csv",
    )
    return _extract_csv_to_raw(
        source="kaggle",
        dataset="stock_market_regime_by_ticker",
        domain="finance",
        mode="append",
        csv_path=csv_path,
    )

def load_raw_to_stg(payload: dict) -> int:
    """
    Loads RAW NDJSON (from manifest) into Postgres STG.
    """
    ctx = build_context()

    run_id = payload["run_id"]
    manifest_bucket = payload["manifest_bucket"]
    manifest_key = payload["manifest_key"]
    stg_table = payload["stg_table"]
    columns = payload.get("columns") or []

    ensure_stg_table_all_text(ctx.pg_engine, table_name=stg_table, columns=columns)

    inserted = from_manifest_ndjson_to_postgres(
        aws_region=payload.get("aws_region", ctx.settings.aws_region),
        manifest_bucket=manifest_bucket,
        manifest_key=manifest_key,
        engine=ctx.pg_engine,
        stg_table=stg_table,
        run_id=run_id,
    )
    return inserted


with DAG(
    dag_id="FINANCE__STOCK_MARKET_REGIMES__RAW_TO_STG",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["finance", "kaggle", "raw_to_stg"],
) as dag:

    t_extract_regimes = PythonOperator(
        task_id="EXTRACT_STOCK_MARKET_REGIMES_TO_RAW",
        python_callable=extract_stock_market_regimes_to_raw,
    )
    t_load_regimes = PythonOperator(
        task_id="LOAD_STOCK_MARKET_REGIMES_RAW_TO_STG",
        python_callable=load_raw_to_stg,
        op_args=[{"__xcom__": True}],  # placeholder, see below
    )

    t_extract_year = PythonOperator(
        task_id="EXTRACT_REGIME_BY_YEAR_TO_RAW",
        python_callable=extract_regime_by_year_to_raw,
    )
    t_load_year = PythonOperator(
        task_id="LOAD_REGIME_BY_YEAR_RAW_TO_STG",
        python_callable=load_raw_to_stg,
        op_args=[{"__xcom__": True}],  
    )

    t_extract_ticker = PythonOperator(
        task_id="EXTRACT_REGIME_BY_TICKER_TO_RAW",
        python_callable=extract_regime_by_ticker_to_raw,
    )
    t_load_ticker = PythonOperator(
        task_id="LOAD_REGIME_BY_TICKER_RAW_TO_STG",
        python_callable=load_raw_to_stg,
        op_args=[{"__xcom__": True}],  
    )

    t_load_regimes.op_args = [t_extract_regimes.output]
    t_load_year.op_args = [t_extract_year.output]
    t_load_ticker.op_args = [t_extract_ticker.output]

    t_extract_regimes >> t_load_regimes
    t_extract_year >> t_load_year
    t_extract_ticker >> t_load_ticker