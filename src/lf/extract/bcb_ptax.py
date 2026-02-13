from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import requests


@dataclass(frozen=True)
class PtaxQuote:
    rate_date: date
    currency: str
    rate: float
    quote_type: str
    source: str = "bcb_ptax"


def _format_bcb_date(d: date) -> str:
    # BCB PTAX OData expects MM-DD-YYYY
    return d.strftime("%m-%d-%Y")


def _ptax_day_url(currency: str, d: date) -> str:
    # Official BCB PTAX endpoint (Olinda OData service)
    # Example:
    # .../CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)?@moeda='USD'&@dataCotacao='02-12-2026'&$format=json
    base = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"
    moeda = currency.upper()
    data = _format_bcb_date(d)
    return (
        f"{base}/CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)"
        f"?@moeda='{moeda}'&@dataCotacao='{data}'&$format=json"
    )


def fetch_ptax_for_date(
    *,
    currency: str = "USD",
    target_date: date,
    timeout_s: int = 20,
) -> list[PtaxQuote]:
    """
    Fetch PTAX official FX quote for a given date.
    Returns potentially multiple records during the day; we'll select one later (e.g., last).
    """
    url = _ptax_day_url(currency, target_date)
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    payload: dict[str, Any] = r.json()

    values = payload.get("value") or []
    out: list[PtaxQuote] = []

    for row in values:
        # Common PTAX fields returned by this service:
        # cotacaoCompra, cotacaoVenda, dataHoraCotacao, tipoBoletim, etc.
        # We'll prioritize 'cotacaoVenda' as standard "sell rate" usage in analytics.
        rate = row.get("cotacaoVenda")
        dt_str = row.get("dataHoraCotacao")  # ISO-like string
        quote_type = row.get("tipoBoletim") or "unknown"

        if rate is None:
            continue

        # We store rate_date as the target date, not the timestamp date
        out.append(
            PtaxQuote(
                rate_date=target_date,
                currency=currency.upper(),
                rate=float(rate),
                quote_type=str(quote_type),
            )
        )

    return out


def fetch_ptax_latest_business_day(
    *,
    currency: str = "USD",
    anchor_date: date,
    lookback_days: int = 7,
) -> Optional[PtaxQuote]:
    """
    If anchor_date has no quote (weekend/holiday), step back until a quote is found.
    Returns the last quote of that day (simple heuristic).
    """
    for i in range(0, lookback_days + 1):
        d = anchor_date - timedelta(days=i)
        quotes = fetch_ptax_for_date(currency=currency, target_date=d)
        if quotes:
            # choose the last record of the day
            return quotes[-1]
    return None


def to_records_for_raw(
    *,
    quote: PtaxQuote,
    run_id: str,
) -> list[dict[str, Any]]:
    """
    Normalize to dict records for NDJSON RAW.
    """
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return [
        {
            "rate_date": quote.rate_date.isoformat(),
            "currency": quote.currency,
            "rate": quote.rate,
            "quote_type": quote.quote_type,
            "source": quote.source,
            "ingested_at_utc": ingested_at,
            "run_id": run_id,
        }
    ]
