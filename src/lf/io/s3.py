from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import boto3


@dataclass(frozen=True)
class S3Path:
    bucket: str
    key: str


def get_s3_client(region: str):
    return boto3.client("s3", region_name=region)


def put_bytes(
    *,
    client,
    path: S3Path,
    data: bytes,
    content_type: Optional[str] = None,
) -> None:
    extra = {}
    if content_type:
        extra["ContentType"] = content_type

    client.put_object(Bucket=path.bucket, Key=path.key, Body=data, **extra)

def join_key(*parts: str) -> str:
    cleaned = []
    for p in parts:
        p = (p or "").strip("/")
        if p:
            cleaned.append(p)
    return "/".join(cleaned)

