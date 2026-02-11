from __future__ import annotations

from dataclasses import dataclass

from lf.config.settings import Settings, get_settings
from lf.io.postgres import build_postgres_engine
from lf.io.s3 import get_s3_client


@dataclass(frozen=True)
class AppContext:
    settings: Settings
    pg_engine: object
    s3_client: object


def build_context() -> AppContext:
    settings = get_settings()
    engine = build_postgres_engine(settings)
    s3 = get_s3_client(settings.aws_region)
    return AppContext(settings=settings, pg_engine=engine, s3_client=s3)
