from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from lf.config.settings import Settings


def build_postgres_engine(settings: Settings) -> Engine:
    url = (
        f"postgresql+psycopg://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )
    return create_engine(url, pool_pre_ping=True)
