from __future__ import annotations

from datetime import date

from airflow.models import Variable

from lf.airflow.context import build_context
from lf.datasets.contract import DatasetContract
from lf.datasets.naming import run_id as build_run_id, stg_table_name
from lf.extract.bcb_ptax import fetch_ptax_latest_business_day, to_records_for_raw
from lf.ingest.raw_ingest import ingest_records_to_raw
from lf.load.from_manifest import ensure_stg_bcb_ptax_table, from_manifest_ndjson_to_postgres


def extract_ptax_to_raw(execution_date: str | None = None) -> dict:
    """
    Airflow task: extracts PTAX (official) and writes RAW + manifest.
    Returns {run_id, manifest_bucket, manifest_key, stg_table}.
    """
    ctx = build_context()

    contract = DatasetContract(
        source="bcb",
        dataset="ptax_fx_rates",
        domain="finance",
        mode="append",
    )

    anchor = date.today()

    currency = Variable.get("BCB_PTAX_CURRENCY", default_var="USD")

    q = fetch_ptax_latest_business_day(currency=currency, anchor_date=anchor)
    if q is None:
        raise RuntimeError(f"No PTAX quote found for {currency} in the last days before {anchor}")

    rid = build_run_id(contract)
    records = to_records_for_raw(quote=q, run_id=rid)

    result = ingest_records_to_raw(
        contract=contract,
        records=records,
        ext="json",
    )

    stg_table = stg_table_name(contract)  
    table_name = stg_table  

    return {
        "run_id": result.run_id,
        "manifest_bucket": result.raw_bucket,
        "manifest_key": result.manifest_key,
        "stg_table": table_name,
        "aws_region": ctx.settings.aws_region,
    }


def load_raw_to_stg(payload: dict) -> int:
    """
    Airflow task: loads RAW NDJSON (from manifest) into Postgres STG and updates meta.stg_rows.
    """
    ctx = build_context()
    run_id = payload["run_id"]
    manifest_bucket = payload["manifest_bucket"]
    manifest_key = payload["manifest_key"]
    stg_table = payload["stg_table"]

    ensure_stg_bcb_ptax_table(ctx.pg_engine, table_name=stg_table)

    inserted = from_manifest_ndjson_to_postgres(
        aws_region=ctx.settings.aws_region,
        manifest_bucket=manifest_bucket,
        manifest_key=manifest_key,
        engine=ctx.pg_engine,
        stg_table=stg_table,
        run_id=run_id,
    )
    return inserted
