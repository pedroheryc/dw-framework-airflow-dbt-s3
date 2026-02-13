from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class RunStart:
    run_id: str
    source: str
    dataset: str
    domain: str
    started_at: datetime
    status: str = "running"


@dataclass(frozen=True)
class RunFinishSuccess:
    run_id: str
    finished_at: datetime
    raw_objects: Optional[int] = None
    raw_records: Optional[int] = None
    stg_rows: Optional[int] = None
    status: str = "success"


@dataclass(frozen=True)
class RunFinishFailed:
    run_id: str
    finished_at: datetime
    error_message: str
    status: str = "failed"


def ensure_meta_schema(engine: Engine) -> None:
    """
    Idempotente. Cria schema e tabela de tracking de runs.
    """
    ddl = """
    create schema if not exists meta;

    create table if not exists meta.ingestion_runs (
      run_id text primary key,
      source text not null,
      dataset text not null,
      domain text not null,
      status text not null,
      started_at timestamptz not null,
      finished_at timestamptz null,
      raw_objects int null,
      raw_records int null,
      stg_rows int null,
      error_message text null
    );

    create index if not exists ix_ingestion_runs_status on meta.ingestion_runs(status);
    create index if not exists ix_ingestion_runs_dataset on meta.ingestion_runs(source, dataset);
    create index if not exists ix_ingestion_runs_started_at on meta.ingestion_runs(started_at);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def start_run(engine: Engine, run: RunStart) -> None:
    """
    Marca/insere run como 'running'.
    Se o run_id já existir, atualiza os campos principais.
    """
    sql = """
    insert into meta.ingestion_runs (
      run_id, source, dataset, domain, status, started_at
    )
    values (
      :run_id, :source, :dataset, :domain, :status, :started_at
    )
    on conflict (run_id) do update
    set source = excluded.source,
        dataset = excluded.dataset,
        domain = excluded.domain,
        status = excluded.status,
        started_at = excluded.started_at;
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "run_id": run.run_id,
                "source": run.source,
                "dataset": run.dataset,
                "domain": run.domain,
                "status": run.status,
                "started_at": run.started_at,
            },
        )


def finish_run_success(engine: Engine, run: RunFinishSuccess) -> None:
    """
    Fecha run como success + métricas.
    """
    sql = """
    update meta.ingestion_runs
    set status = :status,
        finished_at = :finished_at,
        raw_objects = :raw_objects,
        raw_records = :raw_records,
        stg_rows = :stg_rows,
        error_message = null
    where run_id = :run_id;
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "run_id": run.run_id,
                "status": run.status,
                "finished_at": run.finished_at,
                "raw_objects": run.raw_objects,
                "raw_records": run.raw_records,
                "stg_rows": run.stg_rows,
            },
        )


def finish_run_failed(engine: Engine, run: RunFinishFailed) -> None:
    """
    Fecha run como failed + mensagem de erro.
    """
    sql = """
    update meta.ingestion_runs
    set status = :status,
        finished_at = :finished_at,
        error_message = :error_message
    where run_id = :run_id;
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "run_id": run.run_id,
                "status": run.status,
                "finished_at": run.finished_at,
                "error_message": run.error_message,
            },
        )
        
def update_stg_rows(engine: Engine, *, run_id: str, stg_rows: int) -> None:
    sql = """
    update meta.ingestion_runs
    set stg_rows = :stg_rows
    where run_id = :run_id;
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"run_id": run_id, "stg_rows": stg_rows})
