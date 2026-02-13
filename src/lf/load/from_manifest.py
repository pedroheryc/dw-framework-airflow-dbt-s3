from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from lf.io.s3 import get_s3_client
from lf.meta.repo import update_stg_rows


def _read_s3_text(*, s3_client, bucket: str, key: str) -> str:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def _read_s3_bytes(*, s3_client, bucket: str, key: str) -> bytes:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def ensure_stg_bcb_ptax_table(engine: Engine, table_name: str) -> None:
    ddl = f"""
    create schema if not exists stg;

    create table if not exists stg.{table_name} (
      rate_date date not null,
      currency text not null,
      rate numeric(18,6) not null,
      quote_type text null,
      source text not null,
      ingested_at_utc timestamptz not null,
      run_id text not null
    );

    create index if not exists ix_{table_name}_date on stg.{table_name}(rate_date);
    create index if not exists ix_{table_name}_run on stg.{table_name}(run_id);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def from_manifest_ndjson_to_postgres(
    *,
    aws_region: str,
    manifest_bucket: str,
    manifest_key: str,
    engine: Engine,
    stg_table: str,
    run_id: str,
) -> int:
    """
    Reads manifest JSON, fetches NDJSON objects listed, inserts into Postgres STG.
    Returns inserted rows count, and updates meta.ingestion_runs.stg_rows.
    """
    s3 = get_s3_client(aws_region)

    manifest_text = _read_s3_text(s3_client=s3, bucket=manifest_bucket, key=manifest_key)
    manifest: dict[str, Any] = json.loads(manifest_text)

    objects = manifest.get("objects") or []
    if not objects:
        update_stg_rows(engine, run_id=run_id, stg_rows=0)
        return 0

    inserted = 0
    insert_sql = text(
        f"""
        insert into stg.{stg_table} (
          rate_date, currency, rate, quote_type, source, ingested_at_utc, run_id
        )
        values (
          :rate_date, :currency, :rate, :quote_type, :source, :ingested_at_utc, :run_id
        )
        """
    )

    with engine.begin() as conn:
        for objref in objects:
            bucket = objref["bucket"]
            key = objref["key"]

            raw_bytes = _read_s3_bytes(s3_client=s3, bucket=bucket, key=key)
            lines = raw_bytes.decode("utf-8").splitlines()

            rows: list[dict[str, Any]] = []
            for line in lines:
                if not line.strip():
                    continue
                rec = json.loads(line)

                rows.append(
                    {
                        "rate_date": rec["rate_date"],  # ISO string -> postgres date ok
                        "currency": rec["currency"],
                        "rate": rec["rate"],
                        "quote_type": rec.get("quote_type"),
                        "source": rec.get("source", "bcb_ptax"),
                        "ingested_at_utc": rec["ingested_at_utc"],
                        "run_id": rec.get("run_id", run_id),
                    }
                )

            if rows:
                conn.execute(insert_sql, rows)
                inserted += len(rows)

    update_stg_rows(engine, run_id=run_id, stg_rows=inserted)
    return inserted
