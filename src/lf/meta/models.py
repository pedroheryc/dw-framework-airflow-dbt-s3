from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

Status = Literal["running", "success", "failed"]


@dataclass
class IngestionRun:
    run_id: str
    dataset: str
    started_at: datetime
    finished_at: Optional[datetime]
    status: Status
