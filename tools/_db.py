"""Общий db-хелпер для tools — чтобы каждый модуль не переписывал параметры подключения."""
from __future__ import annotations

import os
import pathlib

import psycopg2
from dotenv import load_dotenv

# Идемпотентная загрузка .env при первом импорте tools — чтобы cron-скрипты,
# ad-hoc запуски и LLM-harness не зависели от того, что caller заранее сделал
# load_dotenv. Повторный load_dotenv безвреден (override=False по умолчанию).
_env_path = pathlib.Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    load_dotenv(dotenv_path=_env_path)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "172.20.0.2"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "knowledge_base"),
        user=os.getenv("DB_USER", "knowledge"),
        password=os.getenv("DB_PASSWORD"),
    )
