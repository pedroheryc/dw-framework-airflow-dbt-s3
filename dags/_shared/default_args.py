from __future__ import annotations

from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "lakehouse-framework",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
