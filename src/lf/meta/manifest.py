from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class RawObjectRef:
    bucket: str
    key: str
    content_type: Optional[str] = None
    record_count: Optional[int] = None


@dataclass(frozen=True)
class IngestionManifest:
    run_id: str
    source: str
    dataset: str
    created_at_utc: str
    objects: list[RawObjectRef]
    extra: Optional[dict[str, Any]] = None

    def to_json_bytes(self) -> bytes:
        import json
        return (json.dumps(asdict(self), ensure_ascii=False, indent=2) + "\n").encode("utf-8")
