#!/usr/bin/env python3
"""Синхронизация дополнительных типов "приобретений" из 1С через OData.

Расширение по образцу sync_procurement.py — отдельный скрипт для нового домена,
не громоздим sync_1c_full.py.

Покрываемые типы документов 1С:
  - Document_ПриобретениеУслугПрочихАктивов
  - Document_АвансовыйОтчет (с двумя табличными частями)

Создаваемые таблицы:
  c1_purchases_other_assets         — header
  c1_purchases_other_assets_items   — стр. "Расходы"
  c1_advance_reports                — header АО
  c1_advance_report_other_expenses  — стр. "ПрочиеРасходы" (с прямой ДДС!)
  c1_advance_report_supplier_pmts   — стр. "ОплатаПоставщикам" (с прямой ДДС!)

Запуск:
  python3 sync_acquisitions_extra.py --quick      # 14 дней
  python3 sync_acquisitions_extra.py --daily      # 60 дней
  python3 sync_acquisitions_extra.py --full       # с 2025-01-01
  python3 sync_acquisitions_extra.py --date-from 2025-01-01 --date-to 2026-04-30
  python3 sync_acquisitions_extra.py --alter-only # только DDL
  python3 sync_acquisitions_extra.py --only purchases_other,advance_reports
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
SESSION = requests.Session()
SESSION.auth = HTTPBasicAuth(ODATA_USER, ODATA_PASS)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _val(d: dict, key: str):
    """Достать поле, заменив empty UUID на None."""
    v = d.get(key)
    if v == EMPTY_UUID or v == "":
        return None
    return v


def _date10(s: str | None) -> str | None:
    if not s:
        return None
    s = s[:10]
    if s.startswith("0001"):
        return None
    return s


def odata_paginated(entity: str, filt: str, top: int = 200) -> list[dict]:
    """Скачать страницами все записи, удовлетворяющие filt."""
    encoded = quote(entity, safe="_")
    out: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{ODATA_BASE}/{encoded}"
            f"?$format=json"
            f"&$top={top}"
            f"&$skip={skip}"
        )
        if filt:
            url += f"&$filter={filt}"
        r = SESSION.get(url, timeout=180)
        if r.status_code != 200:
            log(f"  HTTP {r.status_code} on {entity} skip={skip}: {r.text[:200]}")
            break
        try:
            data = r.json()
        except Exception as e:
            log(f"  JSON err on {entity} skip={skip}: {e}")
            break
        if "odata.error" in data:
            log(f"  OData err on {entity}: {data['odata.error']}")
            break
        batch = data.get("value", [])
        if not batch:
            break
        out.extend(batch)
        if len(batch) < top:
            break
        skip += top
        time.sleep(0.1)
    return out


def odata_all_no_filter(entity: str, top: int = 1000) -> list[dict]:
    """Полная выгрузка табличной части (без $filter, без $orderby — у некоторых
    «виртуальных» табличных частей фильтр по Ref_Key/$orderby ломается)."""
    encoded = quote(entity, safe="_")
    out: list[dict] = []
    skip = 0
    while True:
        url = (
            f"{ODATA_BASE}/{encoded}"
            f"?$format=json"
            f"&$top={top}"
            f"&$skip={skip}"
        )
        r = SESSION.get(url, timeout=300)
        if r.status_code != 200:
            log(f"  HTTP {r.status_code} {entity} skip={skip}")
            break
        try:
            data = r.json()
        except Exception:
            break
        if "odata.error" in data:
            log(f"  OData err {entity}: {data['odata.error']}")
            break
        batch = data.get("value", [])
        if not batch:
            break
        out.extend(batch)
        if len(batch) < top:
            break
        skip += top
        time.sleep(0.1)
    return out


# ─────────────────────────────────────────────────────────────────────
#  DDL
# ─────────────────────────────────────────────────────────────────────

DDL = [
    # Document_ПриобретениеУслугПрочихАктивов — header
    """
    CREATE TABLE IF NOT EXISTS c1_purchases_other_assets (
        id                  SERIAL PRIMARY KEY,
        ref_key             VARCHAR(50) NOT NULL UNIQUE,
        doc_number          VARCHAR(50),
        doc_date            DATE,
        posted              BOOLEAN DEFAULT FALSE,
        is_deleted          BOOLEAN DEFAULT FALSE,
        organization_key    VARCHAR(50),
        partner_key         VARCHAR(50),
        counterparty_key    VARCHAR(50),
        agreement_key       VARCHAR(50),
        contract_key        VARCHAR(50),
        department_key      VARCHAR(50),
        cash_flow_item_key  VARCHAR(50),
        amount              NUMERIC(15,2),
        amount_settlement   NUMERIC(15,2),
        currency_key        VARCHAR(50),
        price_includes_vat  BOOLEAN,
        vat_taxation        VARCHAR(50),
        operation           VARCHAR(100),
        incoming_number     VARCHAR(100),
        incoming_date       DATE,
        incoming_doc_name   VARCHAR(200),
        purchase_activity   VARCHAR(100),
        payment_form        VARCHAR(100),
        comment             TEXT,
        created_at          TIMESTAMP DEFAULT NOW(),
        updated_at          TIMESTAMP DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_poa_date ON c1_purchases_other_assets(doc_date)",
    "CREATE INDEX IF NOT EXISTS idx_poa_org  ON c1_purchases_other_assets(organization_key)",
    "CREATE INDEX IF NOT EXISTS idx_poa_partner ON c1_purchases_other_assets(partner_key)",
    "CREATE INDEX IF NOT EXISTS idx_poa_cfi  ON c1_purchases_other_assets(cash_flow_item_key)",

    # items "Расходы"
    """
    CREATE TABLE IF NOT EXISTS c1_purchases_other_assets_items (
        id                  SERIAL PRIMARY KEY,
        doc_key             VARCHAR(50) NOT NULL,
        line_number         INT,
        content             TEXT,
        quantity            NUMERIC(15,3),
        price               NUMERIC(15,2),
        sum_total           NUMERIC(15,2),
        sum_with_vat        NUMERIC(15,2),
        vat_rate_key        VARCHAR(50),
        vat_sum             NUMERIC(15,2),
        expense_item_key    VARCHAR(50),
        expense_item_type   VARCHAR(150),
        analytics_key       VARCHAR(50),
        analytics_type      VARCHAR(150),
        department_key      VARCHAR(50),
        UNIQUE (doc_key, line_number)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_poa_items_doc ON c1_purchases_other_assets_items(doc_key)",
    "CREATE INDEX IF NOT EXISTS idx_poa_items_eik ON c1_purchases_other_assets_items(expense_item_key)",

    # Document_АвансовыйОтчет — header
    """
    CREATE TABLE IF NOT EXISTS c1_advance_reports (
        id                      SERIAL PRIMARY KEY,
        ref_key                 VARCHAR(50) NOT NULL UNIQUE,
        doc_number              VARCHAR(50),
        doc_date                DATE,
        posted                  BOOLEAN DEFAULT FALSE,
        is_deleted              BOOLEAN DEFAULT FALSE,
        organization_key        VARCHAR(50),
        accountable_person_key  VARCHAR(50),
        department_key          VARCHAR(50),
        currency_key            VARCHAR(50),
        sum_spent               NUMERIC(15,2),
        sum_rejected            NUMERIC(15,2),
        status                  VARCHAR(50),
        purpose                 TEXT,
        approval_date           DATE,
        author_key              VARCHAR(50),
        head_key                VARCHAR(50),
        chief_accountant_key    VARCHAR(50),
        comment                 TEXT,
        created_at              TIMESTAMP DEFAULT NOW(),
        updated_at              TIMESTAMP DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ar_date ON c1_advance_reports(doc_date)",
    "CREATE INDEX IF NOT EXISTS idx_ar_org  ON c1_advance_reports(organization_key)",
    "CREATE INDEX IF NOT EXISTS idx_ar_pers ON c1_advance_reports(accountable_person_key)",

    # АвансовыйОтчет_ПрочиеРасходы (с прямой ДДС на строке!)
    """
    CREATE TABLE IF NOT EXISTS c1_advance_report_other_expenses (
        id                      SERIAL PRIMARY KEY,
        doc_key                 VARCHAR(50) NOT NULL,
        line_number             INT,
        cash_flow_item_key      VARCHAR(50),
        incoming_doc_name       VARCHAR(200),
        incoming_number         VARCHAR(100),
        incoming_date           DATE,
        sum_total               NUMERIC(15,2),
        vat_rate_key            VARCHAR(50),
        vat_sum                 NUMERIC(15,2),
        sum_with_vat            NUMERIC(15,2),
        sum_grand_total         NUMERIC(15,2),
        expense_item_key        VARCHAR(50),
        expense_item_type       VARCHAR(150),
        analytics_key           VARCHAR(50),
        analytics_type          VARCHAR(150),
        counterparty_key        VARCHAR(50),
        department_key          VARCHAR(50),
        cancelled               BOOLEAN,
        cancel_reason           TEXT,
        content                 TEXT,
        sf_presented            BOOLEAN,
        sf_number               VARCHAR(100),
        sf_date                 DATE,
        comment                 TEXT,
        UNIQUE (doc_key, line_number)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_aroe_doc ON c1_advance_report_other_expenses(doc_key)",
    "CREATE INDEX IF NOT EXISTS idx_aroe_cfi ON c1_advance_report_other_expenses(cash_flow_item_key)",
    "CREATE INDEX IF NOT EXISTS idx_aroe_eik ON c1_advance_report_other_expenses(expense_item_key)",

    # АвансовыйОтчет_ОплатаПоставщикам (с прямой ДДС на строке!)
    """
    CREATE TABLE IF NOT EXISTS c1_advance_report_supplier_pmts (
        id                          SERIAL PRIMARY KEY,
        doc_key                     VARCHAR(50) NOT NULL,
        line_number                 INT,
        cash_flow_item_key          VARCHAR(50),
        supplier_key                VARCHAR(50),
        counterparty_key            VARCHAR(50),
        sum_total                   NUMERIC(15,2),
        sum_grand_total             NUMERIC(15,2),
        sum_settlement              NUMERIC(15,2),
        currency_key                VARCHAR(50),
        currency_settlement_key     VARCHAR(50),
        incoming_number             VARCHAR(100),
        incoming_date               DATE,
        incoming_doc_name           VARCHAR(200),
        comment                     TEXT,
        UNIQUE (doc_key, line_number)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_arsp_doc ON c1_advance_report_supplier_pmts(doc_key)",
    "CREATE INDEX IF NOT EXISTS idx_arsp_cfi ON c1_advance_report_supplier_pmts(cash_flow_item_key)",
    "CREATE INDEX IF NOT EXISTS idx_arsp_sup ON c1_advance_report_supplier_pmts(supplier_key)",
]


def ensure_tables(conn) -> None:
    log("Ensure tables (DDL)...")
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()
    log("  ok.")


# ─────────────────────────────────────────────────────────────────────
#  Sync ПриобретениеУслугПрочихАктивов
# ─────────────────────────────────────────────────────────────────────

def sync_purchases_other_assets(conn, date_from: date, date_to: date) -> int:
    """ПриобретениеУслугПрочихАктивов за период."""
    log(f"sync_purchases_other_assets {date_from}..{date_to}")
    df = date_from.strftime("%Y-%m-%dT00:00:00")
    dt = date_to.strftime("%Y-%m-%dT23:59:59")
    flt = (
        f"Date%20ge%20datetime'{df}'"
        f"%20and%20Date%20le%20datetime'{dt}'"
        f"%20and%20Posted%20eq%20true"
        f"%20and%20DeletionMark%20eq%20false"
    )
    headers = odata_paginated("Document_ПриобретениеУслугПрочихАктивов", flt, top=200)
    log(f"  headers: {len(headers)}")
    if not headers:
        return 0

    ref_keys = {h["Ref_Key"] for h in headers}

    # items: пагинированно весь раздел (filter по Ref_Key in dot-notation у 1С OData
    # не работает, только in-memory фильтр после fetch; но можно передавать
    # filter по самому ref_key документа в табличной части — попробуем).
    # Безопаснее: тянем все строки документа поодиночке через nav-link.
    # Для скорости — пробуем фильтр Ref_Key, если не получится — fallback.
    items_all: list[dict] = []
    for rk in ref_keys:
        url = (
            f"{ODATA_BASE}/Document_ПриобретениеУслугПрочихАктивов_Расходы"
            f"?$format=json&$top=500"
            f"&$filter=Ref_Key%20eq%20guid'{rk}'"
        )
        r = SESSION.get(url, timeout=120)
        if r.status_code != 200:
            continue
        try:
            data = r.json()
        except Exception:
            continue
        if "odata.error" in data:
            continue
        items_all.extend(data.get("value", []))
        time.sleep(0.05)
    log(f"  items: {len(items_all)}")

    with conn.cursor() as cur:
        # Удаляем существующие данные за период (по doc_date), чтобы upsert был чистым
        cur.execute(
            "DELETE FROM c1_purchases_other_assets_items WHERE doc_key IN "
            "(SELECT ref_key FROM c1_purchases_other_assets "
            " WHERE doc_date BETWEEN %s AND %s)",
            (date_from, date_to),
        )
        cur.execute(
            "DELETE FROM c1_purchases_other_assets WHERE doc_date BETWEEN %s AND %s",
            (date_from, date_to),
        )

        for h in headers:
            cur.execute(
                """
                INSERT INTO c1_purchases_other_assets
                  (ref_key, doc_number, doc_date, posted, is_deleted,
                   organization_key, partner_key, counterparty_key,
                   agreement_key, contract_key, department_key,
                   cash_flow_item_key, amount, amount_settlement, currency_key,
                   price_includes_vat, vat_taxation, operation,
                   incoming_number, incoming_date, incoming_doc_name,
                   purchase_activity, payment_form, comment, updated_at)
                VALUES (%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,
                        %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, NOW())
                ON CONFLICT (ref_key) DO UPDATE SET
                    doc_number = EXCLUDED.doc_number,
                    doc_date = EXCLUDED.doc_date,
                    posted = EXCLUDED.posted,
                    is_deleted = EXCLUDED.is_deleted,
                    organization_key = EXCLUDED.organization_key,
                    partner_key = EXCLUDED.partner_key,
                    counterparty_key = EXCLUDED.counterparty_key,
                    agreement_key = EXCLUDED.agreement_key,
                    contract_key = EXCLUDED.contract_key,
                    department_key = EXCLUDED.department_key,
                    cash_flow_item_key = EXCLUDED.cash_flow_item_key,
                    amount = EXCLUDED.amount,
                    amount_settlement = EXCLUDED.amount_settlement,
                    currency_key = EXCLUDED.currency_key,
                    price_includes_vat = EXCLUDED.price_includes_vat,
                    vat_taxation = EXCLUDED.vat_taxation,
                    operation = EXCLUDED.operation,
                    incoming_number = EXCLUDED.incoming_number,
                    incoming_date = EXCLUDED.incoming_date,
                    incoming_doc_name = EXCLUDED.incoming_doc_name,
                    purchase_activity = EXCLUDED.purchase_activity,
                    payment_form = EXCLUDED.payment_form,
                    comment = EXCLUDED.comment,
                    updated_at = NOW()
                """,
                (
                    h["Ref_Key"],
                    (h.get("Number") or "").strip(),
                    _date10(h.get("Date")),
                    bool(h.get("Posted")),
                    bool(h.get("DeletionMark")),
                    _val(h, "Организация_Key"),
                    _val(h, "Партнер_Key"),
                    _val(h, "Контрагент_Key"),
                    _val(h, "Соглашение_Key"),
                    _val(h, "Договор_Key"),
                    _val(h, "Подразделение_Key"),
                    _val(h, "СтатьяДвиженияДенежныхСредств_Key"),
                    h.get("СуммаДокумента") or 0,
                    h.get("СуммаВзаиморасчетов") or 0,
                    _val(h, "Валюта_Key"),
                    h.get("ЦенаВключаетНДС"),
                    h.get("НалогообложениеНДС"),
                    h.get("ХозяйственнаяОперация"),
                    h.get("НомерВходящегоДокумента"),
                    _date10(h.get("ДатаВходящегоДокумента")),
                    h.get("НаименованиеВходящегоДокумента"),
                    h.get("ЗакупкаПодДеятельность"),
                    h.get("ФормаОплаты"),
                    h.get("Комментарий"),
                ),
            )

        for it in items_all:
            cur.execute(
                """
                INSERT INTO c1_purchases_other_assets_items
                  (doc_key, line_number, content, quantity, price,
                   sum_total, sum_with_vat, vat_rate_key, vat_sum,
                   expense_item_key, expense_item_type,
                   analytics_key, analytics_type, department_key)
                VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s, %s,%s,%s)
                ON CONFLICT (doc_key, line_number) DO UPDATE SET
                    content = EXCLUDED.content,
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    sum_total = EXCLUDED.sum_total,
                    sum_with_vat = EXCLUDED.sum_with_vat,
                    vat_rate_key = EXCLUDED.vat_rate_key,
                    vat_sum = EXCLUDED.vat_sum,
                    expense_item_key = EXCLUDED.expense_item_key,
                    expense_item_type = EXCLUDED.expense_item_type,
                    analytics_key = EXCLUDED.analytics_key,
                    analytics_type = EXCLUDED.analytics_type,
                    department_key = EXCLUDED.department_key
                """,
                (
                    it["Ref_Key"],
                    int(it.get("LineNumber") or 0),
                    it.get("Содержание"),
                    it.get("Количество"),
                    it.get("Цена"),
                    it.get("Сумма"),
                    it.get("СуммаСНДС"),
                    _val(it, "СтавкаНДС_Key"),
                    it.get("СуммаНДС"),
                    _val(it, "СтатьяРасходов"),
                    it.get("СтатьяРасходов_Type"),
                    _val(it, "АналитикаРасходов"),
                    it.get("АналитикаРасходов_Type"),
                    _val(it, "Подразделение_Key"),
                ),
            )
    conn.commit()
    log(f"  ✓ ПриобретениеУслугПрочихАктивов: headers={len(headers)} items={len(items_all)}")
    return len(headers)


# ─────────────────────────────────────────────────────────────────────
#  Sync АвансовыйОтчет (header + 2 subtables)
# ─────────────────────────────────────────────────────────────────────

def sync_advance_reports(conn, date_from: date, date_to: date) -> int:
    log(f"sync_advance_reports {date_from}..{date_to}")
    df = date_from.strftime("%Y-%m-%dT00:00:00")
    dt = date_to.strftime("%Y-%m-%dT23:59:59")
    flt = (
        f"Date%20ge%20datetime'{df}'"
        f"%20and%20Date%20le%20datetime'{dt}'"
        f"%20and%20Posted%20eq%20true"
        f"%20and%20DeletionMark%20eq%20false"
    )
    headers = odata_paginated("Document_АвансовыйОтчет", flt, top=200)
    log(f"  headers: {len(headers)}")
    if not headers:
        return 0

    ref_keys = list({h["Ref_Key"] for h in headers})

    other_exp: list[dict] = []
    sup_pmts: list[dict] = []
    for rk in ref_keys:
        for ent, sink in [
            ("Document_АвансовыйОтчет_ПрочиеРасходы", other_exp),
            ("Document_АвансовыйОтчет_ОплатаПоставщикам", sup_pmts),
        ]:
            url = (
                f"{ODATA_BASE}/{quote(ent, safe='_')}"
                f"?$format=json&$top=500"
                f"&$filter=Ref_Key%20eq%20guid'{rk}'"
            )
            r = SESSION.get(url, timeout=120)
            if r.status_code != 200:
                continue
            try:
                data = r.json()
            except Exception:
                continue
            if "odata.error" in data:
                continue
            sink.extend(data.get("value", []))
        time.sleep(0.05)
    log(f"  other_expenses: {len(other_exp)}, supplier_pmts: {len(sup_pmts)}")

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM c1_advance_report_other_expenses WHERE doc_key IN "
            "(SELECT ref_key FROM c1_advance_reports WHERE doc_date BETWEEN %s AND %s)",
            (date_from, date_to),
        )
        cur.execute(
            "DELETE FROM c1_advance_report_supplier_pmts WHERE doc_key IN "
            "(SELECT ref_key FROM c1_advance_reports WHERE doc_date BETWEEN %s AND %s)",
            (date_from, date_to),
        )
        cur.execute(
            "DELETE FROM c1_advance_reports WHERE doc_date BETWEEN %s AND %s",
            (date_from, date_to),
        )

        for h in headers:
            cur.execute(
                """
                INSERT INTO c1_advance_reports
                  (ref_key, doc_number, doc_date, posted, is_deleted,
                   organization_key, accountable_person_key, department_key,
                   currency_key, sum_spent, sum_rejected, status, purpose,
                   approval_date, author_key, head_key, chief_accountant_key,
                   comment, updated_at)
                VALUES (%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s,
                        %s,%s,%s,%s, %s, NOW())
                ON CONFLICT (ref_key) DO UPDATE SET
                    doc_number = EXCLUDED.doc_number,
                    doc_date = EXCLUDED.doc_date,
                    posted = EXCLUDED.posted,
                    is_deleted = EXCLUDED.is_deleted,
                    organization_key = EXCLUDED.organization_key,
                    accountable_person_key = EXCLUDED.accountable_person_key,
                    department_key = EXCLUDED.department_key,
                    currency_key = EXCLUDED.currency_key,
                    sum_spent = EXCLUDED.sum_spent,
                    sum_rejected = EXCLUDED.sum_rejected,
                    status = EXCLUDED.status,
                    purpose = EXCLUDED.purpose,
                    approval_date = EXCLUDED.approval_date,
                    author_key = EXCLUDED.author_key,
                    head_key = EXCLUDED.head_key,
                    chief_accountant_key = EXCLUDED.chief_accountant_key,
                    comment = EXCLUDED.comment,
                    updated_at = NOW()
                """,
                (
                    h["Ref_Key"],
                    (h.get("Number") or "").strip(),
                    _date10(h.get("Date")),
                    bool(h.get("Posted")),
                    bool(h.get("DeletionMark")),
                    _val(h, "Организация_Key"),
                    _val(h, "ПодотчетноеЛицо_Key"),
                    _val(h, "Подразделение_Key"),
                    _val(h, "Валюта_Key"),
                    h.get("СуммаИзрасходовано") or 0,
                    h.get("СуммаОтклонено") or 0,
                    h.get("Статус"),
                    h.get("НазначениеАванса"),
                    _date10(h.get("ДатаУтверждения")),
                    _val(h, "Автор_Key"),
                    _val(h, "Руководитель_Key"),
                    _val(h, "ГлавныйБухгалтер_Key"),
                    h.get("Комментарий"),
                ),
            )

        for it in other_exp:
            cur.execute(
                """
                INSERT INTO c1_advance_report_other_expenses
                  (doc_key, line_number, cash_flow_item_key,
                   incoming_doc_name, incoming_number, incoming_date,
                   sum_total, vat_rate_key, vat_sum, sum_with_vat, sum_grand_total,
                   expense_item_key, expense_item_type,
                   analytics_key, analytics_type,
                   counterparty_key, department_key,
                   cancelled, cancel_reason, content,
                   sf_presented, sf_number, sf_date, comment)
                VALUES (%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s,
                        %s,%s, %s,%s, %s,%s,
                        %s,%s,%s, %s,%s,%s, %s)
                ON CONFLICT (doc_key, line_number) DO UPDATE SET
                    cash_flow_item_key = EXCLUDED.cash_flow_item_key,
                    incoming_doc_name = EXCLUDED.incoming_doc_name,
                    incoming_number = EXCLUDED.incoming_number,
                    incoming_date = EXCLUDED.incoming_date,
                    sum_total = EXCLUDED.sum_total,
                    vat_rate_key = EXCLUDED.vat_rate_key,
                    vat_sum = EXCLUDED.vat_sum,
                    sum_with_vat = EXCLUDED.sum_with_vat,
                    sum_grand_total = EXCLUDED.sum_grand_total,
                    expense_item_key = EXCLUDED.expense_item_key,
                    expense_item_type = EXCLUDED.expense_item_type,
                    analytics_key = EXCLUDED.analytics_key,
                    analytics_type = EXCLUDED.analytics_type,
                    counterparty_key = EXCLUDED.counterparty_key,
                    department_key = EXCLUDED.department_key,
                    cancelled = EXCLUDED.cancelled,
                    cancel_reason = EXCLUDED.cancel_reason,
                    content = EXCLUDED.content,
                    sf_presented = EXCLUDED.sf_presented,
                    sf_number = EXCLUDED.sf_number,
                    sf_date = EXCLUDED.sf_date,
                    comment = EXCLUDED.comment
                """,
                (
                    it["Ref_Key"],
                    int(it.get("LineNumber") or 0),
                    _val(it, "СтатьяДвиженияДенежныхСредств_Key"),
                    it.get("НаименованиеВходящегоДокумента"),
                    it.get("НомерВходящегоДокумента"),
                    _date10(it.get("ДатаВходящегоДокумента")),
                    it.get("Сумма"),
                    _val(it, "СтавкаНДС_Key"),
                    it.get("СуммаНДС"),
                    it.get("СуммаСНДС"),
                    it.get("СуммаИтог"),
                    _val(it, "СтатьяРасходов"),
                    it.get("СтатьяРасходов_Type"),
                    _val(it, "АналитикаРасходов"),
                    it.get("АналитикаРасходов_Type"),
                    _val(it, "Контрагент_Key"),
                    _val(it, "Подразделение_Key"),
                    bool(it.get("Отменено")),
                    it.get("ПричинаОтмены"),
                    it.get("Содержание"),
                    bool(it.get("ПредъявленСФ")),
                    it.get("НомерСФ"),
                    _date10(it.get("ДатаСФ")),
                    it.get("Комментарий"),
                ),
            )

        for it in sup_pmts:
            cur.execute(
                """
                INSERT INTO c1_advance_report_supplier_pmts
                  (doc_key, line_number, cash_flow_item_key,
                   supplier_key, counterparty_key,
                   sum_total, sum_grand_total, sum_settlement,
                   currency_key, currency_settlement_key,
                   incoming_number, incoming_date, incoming_doc_name, comment)
                VALUES (%s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s,%s)
                ON CONFLICT (doc_key, line_number) DO UPDATE SET
                    cash_flow_item_key = EXCLUDED.cash_flow_item_key,
                    supplier_key = EXCLUDED.supplier_key,
                    counterparty_key = EXCLUDED.counterparty_key,
                    sum_total = EXCLUDED.sum_total,
                    sum_grand_total = EXCLUDED.sum_grand_total,
                    sum_settlement = EXCLUDED.sum_settlement,
                    currency_key = EXCLUDED.currency_key,
                    currency_settlement_key = EXCLUDED.currency_settlement_key,
                    incoming_number = EXCLUDED.incoming_number,
                    incoming_date = EXCLUDED.incoming_date,
                    incoming_doc_name = EXCLUDED.incoming_doc_name,
                    comment = EXCLUDED.comment
                """,
                (
                    it["Ref_Key"],
                    int(it.get("LineNumber") or 0),
                    _val(it, "СтатьяДвиженияДенежныхСредств_Key"),
                    _val(it, "Поставщик_Key"),
                    _val(it, "Контрагент_Key"),
                    it.get("Сумма"),
                    it.get("СуммаИтог"),
                    it.get("СуммаВзаиморасчетов"),
                    _val(it, "Валюта_Key"),
                    _val(it, "ВалютаВзаиморасчетов_Key"),
                    it.get("НомерВходящегоДокумента"),
                    _date10(it.get("ДатаВходящегоДокумента")),
                    it.get("НаименованиеВходящегоДокумента"),
                    it.get("Комментарий"),
                ),
            )
    conn.commit()
    log(
        f"  ✓ АвансовыйОтчет: headers={len(headers)} "
        f"other_expenses={len(other_exp)} supplier_pmts={len(sup_pmts)}"
    )
    return len(headers)


# ─────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────

DEFAULT_FULL_FROM = date(2025, 1, 1)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true",
                   help="последние 14 дней")
    p.add_argument("--daily", action="store_true",
                   help="последние 60 дней")
    p.add_argument("--full", action="store_true",
                   help=f"с {DEFAULT_FULL_FROM.isoformat()} до сегодня")
    p.add_argument("--date-from", type=lambda s: date.fromisoformat(s))
    p.add_argument("--date-to", type=lambda s: date.fromisoformat(s))
    p.add_argument("--alter-only", action="store_true",
                   help="только DDL, без OData")
    p.add_argument("--only", default="purchases_other,advance_reports",
                   help="csv: purchases_other, advance_reports")
    args = p.parse_args()

    if not (args.alter_only or args.quick or args.daily or args.full or args.date_from):
        p.error("укажи один из: --quick / --daily / --full / --date-from / --alter-only")

    today = date.today()
    if args.date_from:
        date_from = args.date_from
        date_to = args.date_to or today
    elif args.quick:
        date_from = today - timedelta(days=14)
        date_to = today
    elif args.daily:
        date_from = today - timedelta(days=60)
        date_to = today
    elif args.full:
        date_from = DEFAULT_FULL_FROM
        date_to = today
    else:
        date_from = today
        date_to = today

    only = {x.strip() for x in args.only.split(",") if x.strip()}

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )
    try:
        ensure_tables(conn)
        if args.alter_only:
            log("alter-only mode → done.")
            return 0
        log(f"sync window: {date_from} .. {date_to}")
        if "purchases_other" in only:
            sync_purchases_other_assets(conn, date_from, date_to)
        if "advance_reports" in only:
            sync_advance_reports(conn, date_from, date_to)
    finally:
        conn.close()
    log("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
