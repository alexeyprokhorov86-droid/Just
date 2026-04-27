#!/usr/bin/env python3
"""Бэкфилл дополнительных полей в c1_bank_expenses из OData.

Сейчас sync_1c_full.py не выгружает: автора, ответственного, основание,
флаг «проведено банком». Добавляем эти поля и бэкфиллим.

Поля:
  - author_key            ← Автор_Key
  - responsible_key       ← Ответственный_Key
  - basis_doc_ref         ← ДокументОснование (часто guid Заказа поставщику)
  - basis_doc_type        ← ДокументОснование_Type (имя сущности)
  - bsg_order_ref         ← BSG_Заказ (BSG-кастом, тоже ссылка на заказ)
  - bsg_order_type        ← BSG_Заказ_Type
  - posted_by_bank        ← ПроведеноБанком (явный bool)
  - bank_post_date        ← ДатаПроведенияБанком

Запуск:
  python3 sync_bank_expenses_authors.py --alter-only
  python3 sync_bank_expenses_authors.py --quick      # 14 дней
  python3 sync_bank_expenses_authors.py --full       # с 2025-01-01
  python3 sync_bank_expenses_authors.py --date-from 2025-01-01 --date-to 2026-04-30
"""
from __future__ import annotations

import argparse
import os
import pathlib
import sys
import time
from datetime import date, datetime, timedelta
from urllib.parse import quote

import psycopg2
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

REPO = pathlib.Path(__file__).resolve().parent
load_dotenv(REPO / ".env")

ODATA_BASE = os.getenv("ODATA_BASE_URL")
ODATA_USER = os.getenv("ODATA_USERNAME")
ODATA_PASS = os.getenv("ODATA_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASS = os.getenv("DB_PASSWORD")

EMPTY_UUID = "00000000-0000-0000-0000-000000000000"

S = requests.Session()
S.auth = HTTPBasicAuth(ODATA_USER, ODATA_PASS)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _val(d: dict, key: str):
    v = d.get(key)
    if v in (EMPTY_UUID, "", None):
        return None
    return v


def _date10(s: str | None) -> str | None:
    if not s:
        return None
    s = s[:10]
    if s.startswith("0001"):
        return None
    return s


DDL = [
    "ALTER TABLE c1_bank_expenses ADD COLUMN IF NOT EXISTS author_key VARCHAR(50)",
    "ALTER TABLE c1_bank_expenses ADD COLUMN IF NOT EXISTS responsible_key VARCHAR(50)",
    "ALTER TABLE c1_bank_expenses ADD COLUMN IF NOT EXISTS basis_doc_ref VARCHAR(50)",
    "ALTER TABLE c1_bank_expenses ADD COLUMN IF NOT EXISTS basis_doc_type VARCHAR(150)",
    "ALTER TABLE c1_bank_expenses ADD COLUMN IF NOT EXISTS bsg_order_ref VARCHAR(50)",
    "ALTER TABLE c1_bank_expenses ADD COLUMN IF NOT EXISTS bsg_order_type VARCHAR(150)",
    "ALTER TABLE c1_bank_expenses ADD COLUMN IF NOT EXISTS posted_by_bank BOOLEAN",
    "ALTER TABLE c1_bank_expenses ADD COLUMN IF NOT EXISTS bank_post_date DATE",
    "CREATE INDEX IF NOT EXISTS idx_bnk_exp_author ON c1_bank_expenses(author_key)",
    "CREATE INDEX IF NOT EXISTS idx_bnk_exp_basis  ON c1_bank_expenses(basis_doc_ref)",
    "CREATE INDEX IF NOT EXISTS idx_bnk_exp_bsg    ON c1_bank_expenses(bsg_order_ref)",
    # Catalog_Пользователи 1С — на него ссылается Автор_Key (и Ответственный_Key,
    # и Автор у других документов)
    """
    CREATE TABLE IF NOT EXISTS c1_users (
        id              SERIAL PRIMARY KEY,
        ref_key         VARCHAR(50) NOT NULL UNIQUE,
        description     VARCHAR(500),
        code            VARCHAR(50),
        is_deleted      BOOLEAN DEFAULT FALSE,
        is_invalid      BOOLEAN DEFAULT FALSE,
        person_key      VARCHAR(50),
        contact_info    JSONB,
        created_at      TIMESTAMP DEFAULT NOW(),
        updated_at      TIMESTAMP DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_c1_users_desc ON c1_users(description)",
    "CREATE INDEX IF NOT EXISTS idx_c1_users_person ON c1_users(person_key)",
]


def ensure_schema(conn) -> None:
    log("Ensure schema (ALTER + indexes)...")
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()
    log("  ok.")


def sync_users(conn) -> int:
    """Catalog_Пользователи 1С — на это ссылается Автор_Key документов."""
    log("sync_users (Catalog_Пользователи)...")
    encoded = quote("Catalog_Пользователи", safe="_")
    out: list[dict] = []
    skip = 0
    top = 500
    while True:
        # без $select — кириллические поля в $select ломают OData
        url = f"{ODATA_BASE}/{encoded}?$format=json&$top={top}&$skip={skip}"
        r = S.get(url, timeout=120)
        if r.status_code != 200:
            log(f"  HTTP {r.status_code}"); break
        try:
            data = r.json()
        except Exception:
            break
        if "odata.error" in data:
            log(f"  err: {data['odata.error']}"); break
        batch = data.get("value", [])
        if not batch: break
        out.extend(batch)
        if len(batch) < top: break
        skip += top
    log(f"  fetched: {len(out)}")
    if not out: return 0
    import json as _json
    with conn.cursor() as cur:
        for u in out:
            cur.execute(
                """
                INSERT INTO c1_users (ref_key, description, code, is_deleted, is_invalid,
                                      person_key, contact_info, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                ON CONFLICT (ref_key) DO UPDATE SET
                    description = EXCLUDED.description,
                    code = EXCLUDED.code,
                    is_deleted = EXCLUDED.is_deleted,
                    is_invalid = EXCLUDED.is_invalid,
                    person_key = EXCLUDED.person_key,
                    contact_info = EXCLUDED.contact_info,
                    updated_at = NOW()
                """,
                (
                    u["Ref_Key"],
                    u.get("Description") or None,
                    u.get("Code") or None,
                    bool(u.get("DeletionMark")),
                    bool(u.get("Недействителен")),
                    _val(u, "ФизическоеЛицо_Key"),
                    _json.dumps(u.get("КонтактнаяИнформация") or [], ensure_ascii=False),
                ),
            )
    conn.commit()
    log(f"  upserted {len(out)} users")
    return len(out)


def fetch_period(date_from: date, date_to: date) -> list[dict]:
    """Тянем нужные поля Document_СписаниеБезналичныхДенежныхСредств за период."""
    df = date_from.strftime("%Y-%m-%dT00:00:00")
    dt = date_to.strftime("%Y-%m-%dT23:59:59")
    flt = (
        f"Date%20ge%20datetime'{df}'"
        f"%20and%20Date%20le%20datetime'{dt}'"
        f"%20and%20Posted%20eq%20true"
        f"%20and%20DeletionMark%20eq%20false"
    )
    select = (
        "Ref_Key,"
        "Автор_Key,Ответственный_Key,"
        "ДокументОснование,ДокументОснование_Type,"
        "BSG_Заказ,BSG_Заказ_Type,"
        "ПроведеноБанком,ДатаПроведенияБанком"
    )
    encoded = quote("Document_СписаниеБезналичныхДенежныхСредств", safe="_")
    out: list[dict] = []
    skip = 0
    top = 200
    while True:
        url = (
            f"{ODATA_BASE}/{encoded}"
            f"?$format=json&$top={top}&$skip={skip}"
            f"&$filter={flt}&$select={quote(select, safe=',')}"
        )
        r = S.get(url, timeout=180)
        if r.status_code != 200:
            log(f"  HTTP {r.status_code} skip={skip}: {r.text[:200]}")
            break
        try:
            data = r.json()
        except Exception:
            break
        if "odata.error" in data:
            log(f"  err: {data['odata.error']}")
            break
        batch = data.get("value", [])
        if not batch:
            break
        out.extend(batch)
        if len(out) % 1000 == 0:
            log(f"  fetched: {len(out)}")
        if len(batch) < top:
            break
        skip += top
        time.sleep(0.05)
    return out


def backfill(conn, date_from: date, date_to: date) -> int:
    log(f"backfill bank_expenses authors {date_from}..{date_to}")
    docs = fetch_period(date_from, date_to)
    log(f"  fetched: {len(docs)}")
    if not docs:
        return 0

    updated = 0
    skipped = 0
    with conn.cursor() as cur:
        for d in docs:
            ref_key = d.get("Ref_Key")
            if not ref_key:
                continue
            cur.execute(
                """
                UPDATE c1_bank_expenses SET
                  author_key = %s,
                  responsible_key = %s,
                  basis_doc_ref = %s,
                  basis_doc_type = %s,
                  bsg_order_ref = %s,
                  bsg_order_type = %s,
                  posted_by_bank = %s,
                  bank_post_date = %s,
                  updated_at = NOW()
                WHERE ref_key = %s
                """,
                (
                    _val(d, "Автор_Key"),
                    _val(d, "Ответственный_Key"),
                    _val(d, "ДокументОснование"),
                    d.get("ДокументОснование_Type") or None,
                    _val(d, "BSG_Заказ"),
                    d.get("BSG_Заказ_Type") or None,
                    bool(d.get("ПроведеноБанком")),
                    _date10(d.get("ДатаПроведенияБанком")),
                    ref_key,
                ),
            )
            if cur.rowcount > 0:
                updated += 1
            else:
                skipped += 1
    conn.commit()
    log(f"  updated={updated} skipped(не было в c1_bank_expenses)={skipped}")
    return updated


DEFAULT_FULL_FROM = date(2025, 1, 1)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true", help="последние 14 дней")
    p.add_argument("--daily", action="store_true", help="последние 60 дней")
    p.add_argument("--full", action="store_true", help=f"с {DEFAULT_FULL_FROM.isoformat()}")
    p.add_argument("--date-from", type=lambda s: date.fromisoformat(s))
    p.add_argument("--date-to", type=lambda s: date.fromisoformat(s))
    p.add_argument("--alter-only", action="store_true")
    args = p.parse_args()

    if not (args.alter_only or args.quick or args.daily or args.full or args.date_from):
        p.error("укажи: --quick / --daily / --full / --date-from / --alter-only")

    today = date.today()
    if args.date_from:
        df, dt = args.date_from, args.date_to or today
    elif args.quick:
        df, dt = today - timedelta(days=14), today
    elif args.daily:
        df, dt = today - timedelta(days=60), today
    elif args.full:
        df, dt = DEFAULT_FULL_FROM, today
    else:
        df, dt = today, today

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)
    try:
        ensure_schema(conn)
        if args.alter_only:
            log("alter-only → done.")
            return 0
        sync_users(conn)
        backfill(conn, df, dt)
    finally:
        conn.close()
    log("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
