from __future__ import annotations

from datetime import datetime, timezone

from lf.datasets.contract import DatasetContract


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run_id(contract: DatasetContract) -> str:
    # ex: bcb__fx_rates__2026-02-12T22:15:00+00:00
    return f"{contract.source}__{contract.dataset}__{utc_now_iso()}"


def stg_table_name(contract: DatasetContract) -> str:
    # ex: stg_finance_bcb_fx_rates
    return f"stg_{contract.domain}_{contract.source}_{contract.dataset}"
