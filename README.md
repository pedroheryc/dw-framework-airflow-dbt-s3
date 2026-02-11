# Lakehouse Framework (Airflow + dbt + S3 + Postgres)

Framework reutilizável para ingestão e transformação de dados:
- Bronze (RAW): S3 (particionado + manifests)
- Silver (STG): Postgres (idempotente, upsert/append)
- Gold (MARTS): dbt (staging/intermediate/marts)

## Features
- Pipelines por fonte/dataset
- Manifests e tracking de runs
- Padrões de idempotência (append e upsert)
- dbt tests + docs
- Docker Compose (Airflow + Postgres)

## Quickstart
1. Copie `.env.example` para `.env`
2. Suba: `docker compose up -d`
3. Rode o bootstrap DDL no Airflow
4. Execute um DAG de exemplo (GitHub/BCB/TLC)

## Arquitetura
(Será adicionado posteriormente)

## Como adicionar uma nova fonte
1. Implementar extractor em `src/lf/extract/`
2. Declarar dataset e partições
3. Criar DAG em `dags/pipelines/<source>/`
4. Criar `stg_` e `fct_/dim_` no dbt
