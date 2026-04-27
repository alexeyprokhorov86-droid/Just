#!/usr/bin/env python3
"""Бэкфилл полей автора/менеджера/согласования в c1_supplier_orders.

sync_1c_full.sync_supplier_orders заливает только базовые поля (ref/number/
date/org/partner/warehouse/amount/status/comment) и оставляет пустыми
author_key, manager_key, counterparty_key, agreement_key, currency_key,
nds_mode, и прочее. payment_audit.py из-за этого не может определить
реального создателя заказа и сваливается на роль-fallback.

Поля backfill (Document_ЗаказПоставщику):
  - author_key                 ← Автор_Key            (уже колонка)
  - manager_key                ← Менеджер_Key         (уже колонка)
  - counterparty_key           ← Контрагент_Key       (уже колонка)
  - agreement_key              ← Соглашение_Key       (уже колонка)
  - contract_key               ← Договор_Key          (уже колонка)
  - currency_key               ← Валюта_Key           (уже колонка)
  - nds_mode                   ← НалогообложениеНДС   (уже колонка)
  - delivery_method            ← СпособДоставки       (уже колонка)
  - desired_arrival_date       ← ЖелаемаяДатаПоступления (уже колонка)
  - approved                   ← Согласован           (уже колонка)
  - price_includes_vat         ← ЦенаВключаетНДС      (уже колонка)
  - operation                  ← ХозяйственнаяОперация (уже колонка)
  - purchase_for_activity      ← ЗакупкаПодДеятельность (уже колонка)
  - payment_form               ← ФормаОплаты          (уже колонка)
  - registrar_supplier_prices  ← РегистрироватьЦеныПоставщика (уже колонка)
  - return_multi_turn_containers ← ВернутьМногооборотнуюТару (уже колонка)
  - contact_person_key         ← КонтактноеЛицо_Key   (уже колонка)
Новые колонки:
  - approved_date              ← ДатаСогласования
  - cash_flow_item_key         ← EVG_СтатьяДДС_Key   (мост ДДС из самого заказа)
  - bsg_purchase_plan_key      ← BSG_ПланЗакупок_Key (план закупок BSG)
  - evg_closed                 ← EVG_СтатусЗакрыт    (флаг «закрыт» от BSG)
  - department_key             ← Подразделение_Key

Запуск:
  python3 sync_supplier_order_authors.py --alter-only
  python3 sync_supplier_order_authors.py --quick      # 14 дней
  python3 sync_supplier_order_authors.py --daily      # 60 дней
  python3 sync_supplier_order_authors.py --full       # с 2025-01-01
  python3 sync_supplier_order_authors.py --date-from 2025-01-01 --date-to 2026-04-30
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
    "ALTER TABLE c1_supplier_orders ADD COLUMN IF NOT EXISTS approved_date DATE",
    "ALTER TABLE c1_supplier_orders ADD COLUMN IF NOT EXISTS cash_flow_item_key VARCHAR(50)",
    "ALTER TABLE c1_supplier_orders ADD COLUMN IF NOT EXISTS bsg_purchase_plan_key VARCHAR(50)",
    "ALTER TABLE c1_supplier_orders ADD COLUMN IF NOT EXISTS evg_closed BOOLEAN",
    "ALTER TABLE c1_supplier_orders ADD COLUMN IF NOT EXISTS department_key VARCHAR(50)",
    "CREATE INDEX IF NOT EXISTS idx_so_author    ON c1_supplier_orders(author_key)",
    "CREATE INDEX IF NOT EXISTS idx_so_manager   ON c1_supplier_orders(manager_key)",
    "CREATE INDEX IF NOT EXISTS idx_so_cfi       ON c1_supplier_orders(cash_flow_item_key)",
    "CREATE INDEX IF NOT EXISTS idx_so_bsg_plan  ON c1_supplier_orders(bsg_purchase_plan_key)",
]


def ensure_schema(conn) -> None:
    log("Ensure schema (ALTER + indexes)...")
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()
    log("  ok.")


# Список полей, которые тянем из OData. Без $select — чтобы не ловить
# проблемы с кириллицей в URL (как в sync_users). Выгрузка чуть толще,
# зато стабильнее.

def fetch_period(date_from: date, date_to: date) -> list[dict]:
    df = date_from.strftime("%Y-%m-%dT00:00:00")
    dt = date_to.strftime("%Y-%m-%dT23:59:59")
    flt = (
        f"Date%20ge%20datetime'{df}'"
        f"%20and%20Date%20le%20datetime'{dt}'"
        f"%20and%20Posted%20eq%20true"
    )
    encoded = quote("Document_ЗаказПоставщику", safe="_")
    out: list[dict] = []
    skip = 0
    top = 200
    while True:
        url = (
            f"{ODATA_BASE}/{encoded}"
            f"?$format=json&$top={top}&$skip={skip}"
            f"&$filter={flt}"
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
    log(f"backfill supplier_orders authors {date_from}..{date_to}")
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
                UPDATE c1_supplier_orders SET
                  author_key                  = %s,
                  manager_key                 = %s,
                  counterparty_key            = %s,
                  agreement_key               = %s,
                  contract_key                = %s,
                  currency_key                = %s,
                  nds_mode                    = %s,
                  delivery_method             = %s,
                  desired_arrival_date        = %s,
                  approved                    = %s,
                  approved_date               = %s,
                  price_includes_vat          = %s,
                  operation                   = %s,
                  purchase_for_activity       = %s,
                  payment_form                = %s,
                  registrar_supplier_prices   = %s,
                  return_multi_turn_containers= %s,
                  contact_person_key          = %s,
                  cash_flow_item_key          = %s,
                  bsg_purchase_plan_key       = %s,
                  evg_closed                  = %s,
                  department_key              = %s,
                  updated_at                  = NOW()
                WHERE ref_key = %s
                """,
                (
                    _val(d, "Автор_Key"),
                    _val(d, "Менеджер_Key"),
                    _val(d, "Контрагент_Key"),
                    _val(d, "Соглашение_Key"),
                    _val(d, "Договор_Key"),
                    _val(d, "Валюта_Key"),
                    d.get("НалогообложениеНДС") or None,
                    d.get("СпособДоставки") or None,
                    _date10(d.get("ЖелаемаяДатаПоступления")),
                    bool(d.get("Согласован")),
                    _date10(d.get("ДатаСогласования")),
                    bool(d.get("ЦенаВключаетНДС")),
                    d.get("ХозяйственнаяОперация") or None,
                    d.get("ЗакупкаПодДеятельность") or None,
                    d.get("ФормаОплаты") or None,
                    bool(d.get("РегистрироватьЦеныПоставщика")),
                    bool(d.get("ВернутьМногооборотнуюТару")),
                    _val(d, "КонтактноеЛицо_Key"),
                    _val(d, "EVG_СтатьяДДС_Key"),
                    _val(d, "BSG_ПланЗакупок_Key"),
                    bool(d.get("EVG_СтатусЗакрыт")),
                    _val(d, "Подразделение_Key"),
                    ref_key,
                ),
            )
            if cur.rowcount > 0:
                updated += 1
            else:
                skipped += 1
    conn.commit()
    log(f"  updated={updated} skipped(не было в c1_supplier_orders)={skipped}")
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
        backfill(conn, df, dt)
    finally:
        conn.close()
    log("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
