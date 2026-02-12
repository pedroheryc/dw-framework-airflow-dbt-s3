from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

IngestionMode = Literal["append", "upsert"]


@dataclass(frozen=True)
class DatasetContract:
    source: str                 # ex: "bcb", "github", "tlc"
    dataset: str                # ex: "fx_rates", "issues", "trips"
    domain: str                 # ex: "finance", "support", "ops"
    mode: IngestionMode         # "append" ou "upsert"
    primary_key: Optional[str] = None      # ex: "id" (upsert)
    watermark_field: Optional[str] = None  # ex: "updated_at" (incremental)
