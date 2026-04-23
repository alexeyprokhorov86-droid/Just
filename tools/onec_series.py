"""
tools/onec_series — работа с Catalog_СерииНоменклатуры в 1С.

В 1С серия — это не per-номенклатура, а per-«Вид номенклатуры» (type_id).
Значит серия № 758 от 14.03.2026 одна на весь вид «Подконтрольное сырье
серии», а не отдельная копия на каждой позиции.

Функции:
  - find_series(vid_key, batch_number, production_date) → ref_key | None
  - create_series(vid_key, batch_number, production_date, expiry_date) → ref_key
  - find_or_create_series(...) → ref_key
  - resolve_vid_key_by_nomenclature(nomenclature_key) → vid_key (через БД nomenclature.type_id)
"""
from __future__ import annotations

import datetime
import logging
import os
import pathlib
from typing import Optional
from urllib.parse import quote

import psycopg2
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

_REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(_REPO / ".env")

logger = logging.getLogger("onec_series")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])

ENT = "Catalog_СерииНоменклатуры"


def _db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "172.20.0.2"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "knowledge_base"),
        user=os.getenv("DB_USER", "knowledge"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def resolve_vid_key_by_nomenclature(nomenclature_key: str) -> Optional[str]:
    """Возвращает ref_key вида номенклатуры (type_id) для данной номенклатуры."""
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT type_id FROM nomenclature WHERE id=%s", (nomenclature_key,))
            row = cur.fetchone()
    return str(row[0]) if row and row[0] else None


def _iso_dt(d: str | datetime.date) -> str:
    """Приводит YYYY-MM-DD к ISO datetime для OData."""
    if isinstance(d, datetime.date):
        return d.isoformat() + "T00:00:00"
    if isinstance(d, str):
        return d[:10] + "T00:00:00"
    raise ValueError(f"unsupported date: {d!r}")


def find_series(
    vid_key: str,
    batch_number: str,
    production_date: Optional[str] = None,
) -> Optional[str]:
    """Ищет серию по ВидНоменклатуры + Номер (+ дата произв. для уникальности).

    Кириллические поля в $filter ломают 1С OData (CLAUDE.md § quirks) —
    фильтруем только по тем полям которые можно слать, остальное в Python.
    В нашем случае Номер — Edm.String кириллицей: его тоже в Python.
    """
    # $filter только по ВидНоменклатуры_Key (латиница безопасна в OData).
    # Description/Номер фильтруем в Python.
    flt = f"ВидНоменклатуры_Key%20eq%20guid'{vid_key}'%20and%20DeletionMark%20eq%20false"
    url = (
        f"{BASE}/{quote(ENT, safe='_')}"
        f"?$format=json&$top=500&$filter={flt}"
    )
    r = requests.get(url, auth=AUTH, timeout=60)
    r.raise_for_status()
    items = r.json().get("value", [])
    # Python-фильтр по Номер + опционально ДатаПроизводства
    for it in items:
        if (it.get("Номер") or "").strip() != str(batch_number).strip():
            continue
        if production_date:
            pd = (it.get("ДатаПроизводства") or "")[:10]
            if pd and pd != production_date[:10]:
                continue
        return it.get("Ref_Key")
    return None


def create_series(
    vid_key: str,
    batch_number: str,
    production_date: str,
    expiry_date: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """POST новую серию. Возвращает Ref_Key."""
    payload = {
        "ВидНоменклатуры_Key": vid_key,
        "Номер": str(batch_number),
        "Description": description or f"№ {batch_number} от {production_date}",
        "ДатаПроизводства": _iso_dt(production_date),
        "bsg_Отдатировано": True,
    }
    if expiry_date:
        payload["ГоденДо"] = _iso_dt(expiry_date)
    url = f"{BASE}/{quote(ENT, safe='_')}?$format=json"
    r = requests.post(url, json=payload, auth=AUTH, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"create_series failed: HTTP {r.status_code}: {r.text[:400]}")
    data = r.json()
    ref = data.get("Ref_Key")
    if not ref:
        raise RuntimeError(f"no Ref_Key in response: {r.text[:300]}")
    logger.info("create_series: vid=%s batch=%s → %s", vid_key, batch_number, ref)
    return ref


def find_or_create_series(
    vid_key: str,
    batch_number: str,
    production_date: str,
    expiry_date: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """Идемпотентный find-or-create. Возвращает Ref_Key серии."""
    existing = find_series(vid_key, batch_number, production_date)
    if existing:
        logger.info("find_or_create_series: reusing existing %s", existing)
        return existing
    return create_series(vid_key, batch_number, production_date, expiry_date, description)


__all__ = [
    "find_series",
    "create_series",
    "find_or_create_series",
    "resolve_vid_key_by_nomenclature",
]
