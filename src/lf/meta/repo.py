from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from lf.meta.models import IngestionRun


def ensure_meta_schema(engine: Engine) -> None:
    ddl = """
    create schema if not exists meta;

    create table if not exists meta.ingestion_runs (
      run_id text primary key,
      dataset text not null,
      started_at timestamptz not null,
      finished_at timestamptz null,
      status text not null
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def upsert_ingestion_run(engine: Engine, run: IngestionRun) -> None:
    sql = """
    insert into meta.ingestion_runs (run_id, dataset, started_at, finished_at, status)
    values (:run_id, :dataset, :started_at, :finished_at, :status)
    on conflict (run_id) do update
    set dataset = excluded.dataset,
        started_at = excluded.started_at,
        finished_at = excluded.finished_at,
        status = excluded.status;
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "run_id": run.run_id,
                "dataset": run.dataset,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
                "status": run.status,
            },
        )
