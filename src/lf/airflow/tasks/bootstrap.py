from __future__ import annotations

from datetime import datetime, timezone

from lf.airflow.context import build_context
from lf.io.s3 import S3Path, put_bytes
from lf.meta.repo import ensure_meta_schema


def bootstrap_postgres_meta() -> None:
    ctx = build_context()
    ensure_meta_schema(ctx.pg_engine)


def smoke_test_s3() -> None:
    ctx = build_context()

    now = datetime.now(timezone.utc).isoformat()
    key = f"{ctx.settings.s3_prefix}/_smoke/ingest_date={now[:10]}/smoke-test.txt"

    put_bytes(
        client=ctx.s3_client,
        path=S3Path(bucket=ctx.settings.s3_bucket, key=key),
        data=f"ok {now}\n".encode("utf-8"),
        content_type="text/plain",
    )
