from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from lf.datasets.contract import DatasetContract


def join_key(*parts: str) -> str:
    cleaned = []
    for p in parts:
        p = (p or "").strip("/")
        if p:
            cleaned.append(p)
    return "/".join(cleaned)


def ingest_date_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def raw_prefix(contract: DatasetContract) -> str:
    return join_key("raw", contract.source, contract.dataset)


def raw_object_key(
    *,
    s3_prefix: str,
    contract: DatasetContract,
    run_id: str,
    ext: str,
    part: int = 1,
    ingest_date: Optional[str] = None,
) -> str:
    ingest_date = ingest_date or ingest_date_utc()
    return join_key(
        s3_prefix,
        raw_prefix(contract),
        f"ingest_date={ingest_date}",
        f"run_id={run_id}",
        f"part-{part:04d}.{ext.lstrip('.')}",
    )


def manifest_key(*, s3_prefix: str, contract: DatasetContract, run_id: str) -> str:
    return join_key(
        s3_prefix,
        raw_prefix(contract),
        "_manifests",
        f"run_id={run_id}.json",
    )
