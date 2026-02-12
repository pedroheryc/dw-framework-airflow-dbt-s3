from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from lf.config.settings import get_settings
from lf.datasets.contract import DatasetContract
from lf.datasets.naming import run_id as build_run_id
from lf.io.postgres import build_postgres_engine
from lf.io.s3 import (
    S3Path,
    get_s3_client,
    manifest_key,
    put_bytes,
    raw_object_key,
)
from lf.meta.manifest import IngestionManifest, RawObjectRef
from lf.meta.repo import (
    RunFinishFailed,
    RunFinishSuccess,
    RunStart,
    ensure_meta_schema,
    finish_run_failed,
    finish_run_success,
    start_run,
)


@dataclass(frozen=True)
class IngestResult:
    run_id: str
    raw_bucket: str
    raw_key: str
    manifest_key: str
    record_count: int


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_bytes(records: Sequence[dict[str, Any]]) -> bytes:
    lines = [
        json.dumps(r, ensure_ascii=False, separators=(",", ":"))
        for r in records
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def ingest_records_to_raw(
    *,
    contract: DatasetContract,
    records: Sequence[dict[str, Any]],
    ext: str = "json",
    content_type: Optional[str] = None,
    extra_manifest: Optional[dict[str, Any]] = None,
) -> IngestResult:
    settings = get_settings()
    pg = build_postgres_engine(settings)
    s3 = get_s3_client(settings.aws_region)

    ensure_meta_schema(pg)

    rid = build_run_id(contract)
    started_at = _utc_now()

    start_run(
        pg,
        RunStart(
            run_id=rid,
            source=contract.source,
            dataset=contract.dataset,
            domain=contract.domain,
            started_at=started_at,
        ),
    )

    try:
        record_count = len(records)
        ct = content_type or ("application/x-ndjson" if ext.lower() in {"json", "jsonl", "ndjson"} else None)

        raw_key = raw_object_key(
            s3_prefix=settings.s3_prefix,
            contract=contract,
            run_id=rid,
            ext=ext,
            part=1,
        )

        if ext.lower() in {"json", "jsonl", "ndjson"}:
            body = _json_bytes(records)
        else:
            raise ValueError(f"Unsupported ext for raw ingest right now: {ext}")

        put_bytes(
            client=s3,
            path=S3Path(bucket=settings.s3_bucket, key=raw_key),
            data=body,
            content_type=ct,
        )

        # 3) manifest
        created_at = _utc_now().isoformat(timespec="seconds")
        m_key = manifest_key(
            s3_prefix=settings.s3_prefix,
            contract=contract,
            run_id=rid,
        )

        manifest = IngestionManifest(
            run_id=rid,
            source=contract.source,
            dataset=contract.dataset,
            created_at_utc=created_at,
            objects=[
                RawObjectRef(
                    bucket=settings.s3_bucket,
                    key=raw_key,
                    content_type=ct,
                    record_count=record_count,
                )
            ],
            extra=extra_manifest,
        )

        put_bytes(
            client=s3,
            path=S3Path(bucket=settings.s3_bucket, key=m_key),
            data=manifest.to_json_bytes(),
            content_type="application/json",
        )

        finish_run_success(
            pg,
            RunFinishSuccess(
                run_id=rid,
                finished_at=_utc_now(),
                raw_objects=1,
                raw_records=record_count,
                stg_rows=None,  
            ),
        )

        return IngestResult(
            run_id=rid,
            raw_bucket=settings.s3_bucket,
            raw_key=raw_key,
            manifest_key=m_key,
            record_count=record_count,
        )

    except Exception as exc:
        finish_run_failed(
            pg,
            RunFinishFailed(
                run_id=rid,
                finished_at=_utc_now(),
                error_message=str(exc),
            ),
        )
        raise
