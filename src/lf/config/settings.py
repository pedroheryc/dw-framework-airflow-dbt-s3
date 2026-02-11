from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    aws_region: str = "sa-east-1"
    s3_bucket: str
    s3_prefix: str = "lakehouse-framework"

    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "lakehouse"
    postgres_user: str = "lakehouse"
    postgres_password: str = "lakehouse"


def get_settings() -> Settings:
    return Settings()
