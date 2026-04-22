#!/usr/bin/env python3
"""Фаза 0.4 — досинхрон таблиц для procurement UPD.

Создаёт новые таблицы и расширяет существующие под сценарий
«сканирование УПД → создание ПТУ в 1С»:

Новые таблицы:
  - c1_supplier_agreements     — Соглашения с поставщиками
  - c1_series                  — Серии номенклатуры
  - c1_vat_rates               — Ставки НДС (% из Description)
  - c1_counterparties          — Контрагенты (отдельно от partners)

Расширение существующих:
  - nomenclature_types         — +use_series, +use_series_expiration,
                                 +use_series_production_date, +vat_rate_key,
                                 +archive_date, +nomenclature_type,
                                 +measurement_unit_key
  - c1_supplier_orders         — +agreement_key, +nds_mode, +currency_key,
                                 +registrar_supplier_prices, +return_multi_turn_containers,
                                 +delivery_method, +contact_person_key,
                                 +desired_arrival_date, +approved, +author_key,
                                 +contract_key, +counterparty_key
  - c1_supplier_order_items    — +vat_rate_key, +vat_sum, +sum_with_vat,
                                 +characteristic_key, +package_key, +package_qty,
                                 +expense_item_key

Запуск:
  python3 sync_procurement.py                  # всё
  python3 sync_procurement.py --only agreements,series
  python3 sync_procurement.py --alter-only     # только ALTER TABLE, без OData
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys
import time
from datetime import datetime

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from requests.auth import HTTPBasicAuth

REPO = pathlib.Path(__file__).resolve().parent
load_dotenv(REPO / ".env")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])

CONFIG_PG = {
    "host": os.getenv("DB_HOST", "172.20.0.2"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "knowledge_base"),
    "user": os.getenv("DB_USER", "knowledge"),
    "password": os.getenv("DB_PASSWORD", ""),
}

EMPTY = "00000000-0000-0000-0000-000000000000"
PAGE = 1000


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── OData paginator ─────────────────────────────────────────────────────

def odata_all(entity: str, params: dict | None = None, select: str | None = None) -> list[dict]:
    """Забирает всю коллекцию с пагинацией по Ref_Key."""
    params = dict(params or {})
    params.setdefault("$format", "json")
    params.setdefault("$orderby", "Ref_Key asc")
    if select:
        params["$select"] = select
    params["$top"] = PAGE
    out: list[dict] = []
    skip = 0
    while True:
        params["$skip"] = skip
        r = requests.get(f"{BASE}/{entity}", params=params, auth=AUTH, timeout=120)
        r.raise_for_status()
        chunk = r.json().get("value", [])
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < PAGE:
            break
        skip += PAGE
        log(f"  {entity}: {len(out)} so far…")
    return out


# ── Schema ──────────────────────────────────────────────────────────────

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        # Соглашения с поставщиками
        cur.execute("""
            CREATE TABLE IF NOT EXISTS c1_supplier_agreements (
                id SERIAL PRIMARY KEY,
                ref_key VARCHAR(50) UNIQUE NOT NULL,
                code VARCHAR(50),
                name VARCHAR(500),
                number VARCHAR(50),
                doc_date TIMESTAMP,
                partner_key VARCHAR(50),
                counterparty_key VARCHAR(50),
                organization_key VARCHAR(50),
                currency_key VARCHAR(50),
                warehouse_key VARCHAR(50),
                manager_key VARCHAR(50),
                status VARCHAR(100),
                approved BOOLEAN DEFAULT FALSE,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                price_includes_vat BOOLEAN DEFAULT FALSE,
                payment_order VARCHAR(100),
                payment_form VARCHAR(100),
                registrar_supplier_prices BOOLEAN DEFAULT FALSE,
                return_multi_turn_containers BOOLEAN DEFAULT FALSE,
                delivery_method VARCHAR(100),
                reception_variant VARCHAR(100),
                is_deleted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_supp_agr_partner ON c1_supplier_agreements(partner_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_supp_agr_org ON c1_supplier_agreements(organization_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_supp_agr_valid ON c1_supplier_agreements(valid_from, valid_to)")

        # Серии номенклатуры
        cur.execute("""
            CREATE TABLE IF NOT EXISTS c1_series (
                id SERIAL PRIMARY KEY,
                ref_key VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(500),
                number VARCHAR(100),
                nomenclature_kind_key VARCHAR(50),
                production_date TIMESTAMP,
                expiration_date TIMESTAMP,
                manufacturer_vetis_key VARCHAR(50),
                is_deleted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_series_kind ON c1_series(nomenclature_kind_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_series_expir ON c1_series(expiration_date)")

        # nomenclature_types уже существует (sync_1c_full.sync_nomenclature_types),
        # расширяем её недостающими полями — use_series*, vat_rate_key и т.д.
        for col, ddl in [
            ("use_series",                "BOOLEAN DEFAULT FALSE"),
            ("use_series_number",         "BOOLEAN DEFAULT FALSE"),
            ("use_series_qty",            "BOOLEAN DEFAULT FALSE"),
            ("use_series_expiration",     "BOOLEAN DEFAULT FALSE"),
            ("use_series_production_date","BOOLEAN DEFAULT FALSE"),
            ("auto_generate_series",      "BOOLEAN DEFAULT FALSE"),
            ("nomenclature_type",         "VARCHAR(100)"),
            ("vat_rate_key",              "VARCHAR(50)"),
            ("measurement_unit_key",      "VARCHAR(50)"),
            ("archive_date",              "TIMESTAMP"),
        ]:
            cur.execute(
                f"ALTER TABLE nomenclature_types ADD COLUMN IF NOT EXISTS {col} {ddl}"
            )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nom_types_use_series ON nomenclature_types(use_series) WHERE use_series=TRUE")

        # Ставки НДС
        cur.execute("""
            CREATE TABLE IF NOT EXISTS c1_vat_rates (
                id SERIAL PRIMARY KEY,
                ref_key VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(200),
                rate NUMERIC(5,2),
                is_deleted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Контрагенты (юр.лица с ИНН/КПП; Catalog_Контрагенты в 1С).
        # Флаги is_customer/is_supplier тут ПОЛУЧАЮТ ИЗ Партнёра через partner_key.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS c1_counterparties (
                id SERIAL PRIMARY KEY,
                ref_key VARCHAR(50) UNIQUE NOT NULL,
                code VARCHAR(50),
                name VARCHAR(500),
                full_name VARCHAR(1000),
                inn VARCHAR(20),
                kpp VARCHAR(20),
                ogrn VARCHAR(20),
                partner_key VARCHAR(50),
                is_deleted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ctp_inn ON c1_counterparties(inn)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ctp_partner ON c1_counterparties(partner_key)")

        # Партнёры (бизнес-сущности, Catalog_Партнеры в 1С).
        # Здесь живут флаги Клиент/Поставщик и «чёрный список».
        cur.execute("""
            CREATE TABLE IF NOT EXISTS c1_partners (
                id SERIAL PRIMARY KEY,
                ref_key VARCHAR(50) UNIQUE NOT NULL,
                code VARCHAR(50),
                name VARCHAR(500),
                full_name VARCHAR(1000),
                is_customer BOOLEAN DEFAULT FALSE,
                is_supplier BOOLEAN DEFAULT FALSE,
                is_other_relations BOOLEAN DEFAULT FALSE,
                is_competitor BOOLEAN DEFAULT FALSE,
                is_holder BOOLEAN DEFAULT FALSE,
                is_carrier BOOLEAN DEFAULT FALSE,
                is_working_with BOOLEAN DEFAULT TRUE,
                main_manager_key VARCHAR(50),
                parent_key VARCHAR(50),
                is_deleted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_partners_supplier ON c1_partners(is_supplier) WHERE is_supplier=TRUE")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_partners_name ON c1_partners USING gin(to_tsvector('russian', coalesce(name,'')))")

        # ── ALTER TABLE c1_supplier_orders ──────────────────────────
        for col, ddl in [
            ("agreement_key",            "VARCHAR(50)"),
            ("nds_mode",                 "VARCHAR(100)"),
            ("currency_key",             "VARCHAR(50)"),
            ("registrar_supplier_prices","BOOLEAN DEFAULT FALSE"),
            ("return_multi_turn_containers","BOOLEAN DEFAULT FALSE"),
            ("delivery_method",          "VARCHAR(100)"),
            ("contact_person_key",       "VARCHAR(50)"),
            ("desired_arrival_date",     "TIMESTAMP"),
            ("approved",                 "BOOLEAN DEFAULT FALSE"),
            ("author_key",               "VARCHAR(50)"),
            ("contract_key",             "VARCHAR(50)"),
            ("counterparty_key",         "VARCHAR(50)"),
            ("price_includes_vat",       "BOOLEAN DEFAULT FALSE"),
            ("manager_key",              "VARCHAR(50)"),
            ("operation",                "VARCHAR(200)"),
            ("purchase_for_activity",    "VARCHAR(100)"),
            ("payment_form",             "VARCHAR(100)"),
        ]:
            cur.execute(
                f"ALTER TABLE c1_supplier_orders ADD COLUMN IF NOT EXISTS {col} {ddl}"
            )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_so_agreement ON c1_supplier_orders(agreement_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_so_counterparty ON c1_supplier_orders(counterparty_key)")

        # ── ALTER TABLE c1_supplier_order_items ─────────────────────
        for col, ddl in [
            ("vat_rate_key",        "VARCHAR(50)"),
            ("vat_sum",             "NUMERIC(15,2)"),
            ("sum_with_vat",        "NUMERIC(15,2)"),
            ("characteristic_key",  "VARCHAR(50)"),
            ("package_key",         "VARCHAR(50)"),
            ("package_qty",         "NUMERIC(15,3)"),
            ("expense_item_key",    "VARCHAR(50)"),
            ("warehouse_key",       "VARCHAR(50)"),
        ]:
            cur.execute(
                f"ALTER TABLE c1_supplier_order_items ADD COLUMN IF NOT EXISTS {col} {ddl}"
            )

        conn.commit()
        log("✅ Schema готова")


# ── Upsert helpers ──────────────────────────────────────────────────────

def _d(rec: dict, k: str):
    """ref с EMPTY → None для чистоты БД."""
    v = rec.get(k)
    if isinstance(v, str) and v == EMPTY:
        return None
    return v


def upsert(conn, table: str, rows: list[tuple], cols: list[str], conflict_key: str = "ref_key") -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(cols)
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != conflict_key)
    updates += ", updated_at=NOW()"
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {table} ({cols_sql}) VALUES %s "
            f"ON CONFLICT ({conflict_key}) DO UPDATE SET {updates}",
            rows,
        )
        conn.commit()
    return len(rows)


# ── Sync functions ──────────────────────────────────────────────────────

def sync_agreements(conn) -> int:
    log("→ Соглашения с поставщиками")
    data = odata_all(
        "Catalog_СоглашенияСПоставщиками",
        params={"$filter": "DeletionMark eq false"},
    )
    log(f"  получено {len(data)}")
    rows = [(
        r["Ref_Key"], r.get("Code"), r.get("Description"),
        r.get("Номер"), r.get("Дата"),
        _d(r, "Партнер_Key"), _d(r, "Контрагент_Key"), _d(r, "Организация_Key"),
        _d(r, "Валюта_Key"), _d(r, "Склад_Key"), _d(r, "Менеджер_Key"),
        r.get("Статус"), bool(r.get("Согласован")),
        r.get("ДатаНачалаДействия"), r.get("ДатаОкончанияДействия"),
        bool(r.get("ЦенаВключаетНДС")),
        r.get("ПорядокРасчетов"), r.get("ФормаОплаты"),
        bool(r.get("РегистрироватьЦеныПоставщика")),
        bool(r.get("ВозвращатьМногооборотнуюТару")),
        r.get("СпособДоставки"), r.get("ВариантПриемкиТоваров"),
        bool(r.get("DeletionMark")),
    ) for r in data]
    cols = [
        "ref_key", "code", "name", "number", "doc_date",
        "partner_key", "counterparty_key", "organization_key",
        "currency_key", "warehouse_key", "manager_key",
        "status", "approved", "valid_from", "valid_to", "price_includes_vat",
        "payment_order", "payment_form",
        "registrar_supplier_prices", "return_multi_turn_containers",
        "delivery_method", "reception_variant", "is_deleted",
    ]
    n = upsert(conn, "c1_supplier_agreements", rows, cols)
    log(f"  ✓ upserted {n}")
    return n


def sync_nomenclature_kinds(conn) -> int:
    """Обновляет существующую nomenclature_types (sync_1c_full заливает базовые
    колонки id/parent_id/name/is_folder, мы доливаем use_series*, vat_rate_key
    и т.д.). Фильтр DeletionMark eq false ломает конкретно этот Catalog —
    фильтруем в Python."""
    log("→ Виды номенклатуры (обновляем nomenclature_types)")
    data = odata_all("Catalog_ВидыНоменклатуры")
    data = [r for r in data if not r.get("DeletionMark")]
    log(f"  получено {len(data)} активных")

    with conn.cursor() as cur:
        for r in data:
            ref = r["Ref_Key"]
            if not ref or ref == EMPTY:
                continue
            cur.execute(
                """
                UPDATE nomenclature_types SET
                  use_series                  = %s,
                  use_series_number           = %s,
                  use_series_qty              = %s,
                  use_series_expiration       = %s,
                  use_series_production_date  = %s,
                  auto_generate_series        = %s,
                  nomenclature_type           = %s,
                  vat_rate_key                = %s,
                  measurement_unit_key        = %s,
                  archive_date                = %s,
                  updated_at                  = NOW()
                WHERE id = %s
                """,
                (
                    bool(r.get("ИспользоватьСерии")),
                    bool(r.get("ИспользоватьНомерСерии")),
                    bool(r.get("ИспользоватьКоличествоСерии")),
                    bool(r.get("ИспользоватьСрокГодностиСерии")),
                    bool(r.get("ИспользоватьДатуПроизводстваСерии")),
                    bool(r.get("АвтоматическиГенерироватьСерии")),
                    r.get("ТипНоменклатуры"),
                    _d(r, "СтавкаНДС_Key"),
                    _d(r, "ЕдиницаИзмерения_Key"),
                    r.get("ДатаПереносаВАрхив"),
                    ref,
                ),
            )
        conn.commit()
    log(f"  ✓ обновлено {len(data)} записей в nomenclature_types")
    return len(data)


def sync_series(conn) -> int:
    log("→ Серии номенклатуры (все — может быть много)")
    # Фильтр DeletionMark eq false ломает этот Catalog (как и ВидыНоменклатуры),
    # фильтруем в Python.
    data = odata_all("Catalog_СерииНоменклатуры")
    data = [r for r in data if not r.get("DeletionMark")]
    log(f"  получено {len(data)} активных")
    rows = [(
        r["Ref_Key"], r.get("Description"), r.get("Номер"),
        _d(r, "ВидНоменклатуры_Key"),
        r.get("ДатаПроизводства"), r.get("ГоденДо"),
        _d(r, "ПроизводительВЕТИС_Key"),
        bool(r.get("DeletionMark")),
    ) for r in data]
    cols = [
        "ref_key", "name", "number", "nomenclature_kind_key",
        "production_date", "expiration_date", "manufacturer_vetis_key",
        "is_deleted",
    ]
    n = upsert(conn, "c1_series", rows, cols)
    log(f"  ✓ upserted {n}")
    return n


def sync_vat_rates(conn) -> int:
    log("→ Ставки НДС")
    data = odata_all("Catalog_СтавкиНДС")
    log(f"  получено {len(data)}")
    rows = []
    for r in data:
        # В 1С rate обычно в Description или в отдельном поле Ставка
        desc = (r.get("Description") or "").strip()
        # Парсим процент из названия: "НДС 20%", "НДС 22%", "Без НДС", "0%"
        rate = None
        if "Без НДС" in desc or desc.lower() == "без ндс":
            rate = None
        else:
            import re
            m = re.search(r"(\d+(?:[.,]\d+)?)\s*%", desc)
            if m:
                rate = float(m.group(1).replace(",", "."))
        rows.append((
            r["Ref_Key"], desc, rate, bool(r.get("DeletionMark")),
        ))
    cols = ["ref_key", "name", "rate", "is_deleted"]
    n = upsert(conn, "c1_vat_rates", rows, cols)
    log(f"  ✓ upserted {n}")
    return n


def sync_counterparties(conn) -> int:
    log("→ Контрагенты (Catalog_Контрагенты, ~2915 с удалёнными)")
    # $filter DeletionMark ломает (как и с ВидыНоменклатуры/Сериями),
    # фильтруем в Python. Также отсекаем совсем пустые записи.
    data = odata_all("Catalog_Контрагенты")
    active = [r for r in data if not r.get("DeletionMark") and (r.get("ИНН") or r.get("Description"))]
    log(f"  получено {len(data)} всего, {len(active)} активных с ИНН/именем")
    rows = [(
        r["Ref_Key"], r.get("Code"), r.get("Description"),
        r.get("НаименованиеПолное"),
        r.get("ИНН"), r.get("КПП"), r.get("ОГРН"),
        _d(r, "Партнер_Key"),
        bool(r.get("DeletionMark")),
    ) for r in active]
    cols = [
        "ref_key", "code", "name", "full_name",
        "inn", "kpp", "ogrn", "partner_key",
        "is_deleted",
    ]
    n = upsert(conn, "c1_counterparties", rows, cols)
    log(f"  ✓ upserted {n}")
    return n


def sync_partners(conn) -> int:
    log("→ Партнёры (Catalog_Партнеры, ~2805)")
    data = odata_all("Catalog_Партнеры")
    active = [r for r in data if not r.get("DeletionMark") and r.get("Description")]
    log(f"  получено {len(data)} всего, {len(active)} активных")
    rows = [(
        r["Ref_Key"], r.get("Code"), r.get("Description"),
        r.get("НаименованиеПолное"),
        bool(r.get("Клиент")), bool(r.get("Поставщик")),
        bool(r.get("ПрочиеОтношения")), bool(r.get("Конкурент")),
        bool(r.get("Холдинг")), bool(r.get("Перевозчик")),
        not bool(r.get("НеВестиРаботу")),
        _d(r, "ОсновнойМенеджер_Key"),
        _d(r, "Parent_Key"),
        bool(r.get("DeletionMark")),
    ) for r in active]
    cols = [
        "ref_key", "code", "name", "full_name",
        "is_customer", "is_supplier", "is_other_relations",
        "is_competitor", "is_holder", "is_carrier",
        "is_working_with", "main_manager_key", "parent_key",
        "is_deleted",
    ]
    n = upsert(conn, "c1_partners", rows, cols)
    log(f"  ✓ upserted {n}")
    return n


# ── Main ────────────────────────────────────────────────────────────────

# ВАЖНО: sync_series НЕ включён в ALL_TASKS по умолчанию. В 1С серий 150k+,
# полная выгрузка бессмысленна. Таблица c1_series нужна как лог наших
# собственных POST'ов при создании ПТУ (phase 3+). Запускать вручную
# `--only series` только если реально нужен полный срез.
ALL_TASKS = {
    "vat_rates":           sync_vat_rates,
    "nomenclature_kinds":  sync_nomenclature_kinds,
    "agreements":          sync_agreements,
    "partners":            sync_partners,
    "counterparties":      sync_counterparties,
}
EXTRA_TASKS = {
    "series":              sync_series,
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None, help="Comma-separated: " + ",".join(ALL_TASKS))
    ap.add_argument("--alter-only", action="store_true", help="Только DDL, без OData")
    args = ap.parse_args()

    conn = psycopg2.connect(**CONFIG_PG)
    try:
        ensure_tables(conn)
        if args.alter_only:
            log("--alter-only: done.")
            return 0

        tasks = ALL_TASKS
        if args.only:
            keys = [k.strip() for k in args.only.split(",")]
            combined = {**ALL_TASKS, **EXTRA_TASKS}
            tasks = {k: combined[k] for k in keys if k in combined}

        started = time.time()
        totals = {}
        for name, fn in tasks.items():
            t0 = time.time()
            try:
                totals[name] = fn(conn)
                log(f"  {name} done in {time.time()-t0:.1f}s")
            except Exception as e:
                log(f"  ❌ {name} failed: {e}")
                totals[name] = f"ERROR: {e}"

        log("")
        log("═══ Summary ═══")
        for name, n in totals.items():
            log(f"  {name}: {n}")
        log(f"Total time: {time.time()-started:.1f}s")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
