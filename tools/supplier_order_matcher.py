"""
tools/supplier_order_matcher — поиск Заказов поставщику кандидатов под распознанный УПД.

Основная функция:
  find_matching_orders(upd: UpdExtractResult) -> MatchResult

Критерии (из ТЗ):
  1. Поставщик совпадает (по ИНН → c1_counterparties → partner_key)
  2. Partner не в чёрном списке (c1_partners.is_working_with=true)
  3. Posted=true
  4. Статус = 'Подтвержден'
  5. Не старше 3 месяцев
  6. Сумма заказа ≥ сумма уже принятого ПТУ + сумма этого УПД

Fetch — on-demand через OData (без полной локальной выгрузки). Для фильтра
по дате/Posted — серверный $filter; по статусу 'Подтвержден' — клиент (кириллица
ломает $filter у 1С, 500).

Архив-номенклатура — warning (не блокер в MVP), вернётся в Фазе 3.
"""
from __future__ import annotations

import logging
import os
import pathlib
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from requests.auth import HTTPBasicAuth

from .vision_upd import UpdExtractResult

_REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(_REPO / ".env")

logger = logging.getLogger("supplier_order_matcher")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])
EMPTY_UUID = "00000000-0000-0000-0000-000000000000"

ORDER_MAX_AGE_DAYS = 90  # ≤3 месяцев
STATUS_CONFIRMED = "Подтвержден"


# ─── DB helpers ─────────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "172.20.0.2"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "knowledge_base"),
        user=os.getenv("DB_USER", "knowledge"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def resolve_partner_by_inn(inn: str) -> Optional[dict]:
    """Ищет партнёра (и контрагента) по ИНН поставщика из УПД.
    Возвращает: {partner_key, counterparty_key, partner_name, is_working_with, is_supplier}
    или None если не найден.
    """
    if not inn:
        return None
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.partner_key, c.ref_key AS counterparty_key, c.name AS counterparty_name,
                       p.name AS partner_name, p.is_working_with, p.is_supplier
                FROM c1_counterparties c
                LEFT JOIN c1_partners p ON p.ref_key = c.partner_key
                WHERE c.inn = %s AND c.partner_key IS NOT NULL
                ORDER BY c.is_deleted NULLS LAST
                LIMIT 1
                """,
                (inn,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "partner_key": row[0],
                "counterparty_key": row[1],
                "counterparty_name": row[2],
                "partner_name": row[3],
                "is_working_with": bool(row[4]) if row[4] is not None else True,
                "is_supplier": bool(row[5]) if row[5] is not None else False,
            }
    finally:
        conn.close()


def already_received_amount(order_ref_key: str) -> float:
    """Сколько уже принято по этому заказу (сумма ПТУ).
    Идёт по связке c1_purchase_items.supplier_order_key → c1_purchases.amount.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(p.amount), 0)
                FROM c1_purchases p
                WHERE p.posted = TRUE
                  AND p.is_deleted = FALSE
                  AND EXISTS(
                    SELECT 1 FROM c1_purchase_items pi
                    WHERE pi.doc_key = p.ref_key
                      AND pi.supplier_order_key = %s
                  )
                """,
                (order_ref_key,),
            )
            return float(cur.fetchone()[0] or 0)
    finally:
        conn.close()


# ─── OData fetch ────────────────────────────────────────────────────────

def fetch_candidate_orders(partner_key: str, days: int = ORDER_MAX_AGE_DAYS) -> list[dict]:
    """Из 1С: заказы поставщику от этого партнёра за последние N дней, Posted=true.
    Статус фильтруется в Python (кириллица в $filter ломает 1С OData → 500).
    """
    from urllib.parse import quote
    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
    # 1С OData требует pre-encoded URL: requests.params кодирует ":" в "%3A",
    # на чём сервер спотыкается и даёт 500. Собираем вручную как в sync_1c_full.
    encoded_entity = quote("Document_ЗаказПоставщику", safe="_")
    base_url = (
        f"{BASE}/{encoded_entity}"
        f"?$format=json"
        f"&$filter=Date%20ge%20datetime'{date_from}'%20and%20Posted%20eq%20true"
        f"&$orderby=Date%20desc"
        f"&$top=100"
    )
    all_orders: list[dict] = []
    skip = 0
    while True:
        url = base_url + (f"&$skip={skip}" if skip else "")
        r = requests.get(url, auth=AUTH, timeout=60)
        r.raise_for_status()
        chunk = r.json().get("value", [])
        if not chunk:
            break
        all_orders.extend(chunk)
        if len(chunk) < 100:
            break
        skip += 100
    # Клиентский фильтр
    result = [
        o for o in all_orders
        if o.get("Партнер_Key") == partner_key
        and (o.get("Статус") or "").strip() == STATUS_CONFIRMED
    ]
    logger.info(
        "fetch_candidate_orders: partner=%s → %d orders total server-filtered, %d after client filter",
        partner_key, len(all_orders), len(result),
    )
    return result


# ─── Candidate model + main matcher ─────────────────────────────────────

class OrderCandidate(BaseModel):
    ref_key: str
    number: str
    date: str
    amount: float
    already_received: float
    remaining: float
    fits_upd: bool  # remaining ≥ upd_total
    partner_key: Optional[str] = None
    counterparty_key: Optional[str] = None
    organization_key: Optional[str] = None
    warehouse_key: Optional[str] = None
    agreement_key: Optional[str] = None
    raw: dict = Field(default_factory=dict)  # для будущего создания ПТУ


class MatchResult(BaseModel):
    supplier_inn: Optional[str] = None
    supplier_name_from_db: Optional[str] = None
    partner_key: Optional[str] = None
    found: bool = False           # нашли ли партнёра по ИНН
    blacklisted: bool = False     # партнёр в ЧС
    candidates: list[OrderCandidate] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


def _d(v):
    return None if v == EMPTY_UUID else v


def find_matching_orders(upd: UpdExtractResult) -> MatchResult:
    """Ищет заказы-кандидаты под УПД."""
    out = MatchResult(supplier_inn=upd.supplier.inn)

    if not upd.supplier.inn:
        out.errors.append("В УПД не распознан ИНН поставщика — невозможно найти заказ.")
        return out

    partner = resolve_partner_by_inn(upd.supplier.inn)
    if not partner:
        out.errors.append(
            f"Поставщик с ИНН {upd.supplier.inn} не найден в базе контрагентов. "
            "Возможно, это новый поставщик — его нужно завести в 1С."
        )
        return out

    out.found = True
    out.partner_key = partner["partner_key"]
    out.supplier_name_from_db = partner["partner_name"] or partner["counterparty_name"]

    if not partner["is_working_with"]:
        out.blacklisted = True
        out.errors.append(
            f"Поставщик «{out.supplier_name_from_db}» помечен как «не вести работу» "
            "в 1С (чёрный список)."
        )
        return out

    if not partner["is_supplier"]:
        out.warnings.append(
            f"У партнёра «{out.supplier_name_from_db}» не стоит флаг «Поставщик» в 1С."
        )

    try:
        orders = fetch_candidate_orders(partner["partner_key"])
    except Exception as e:
        logger.exception("fetch_candidate_orders failed: %s", e)
        out.errors.append(f"Ошибка запроса заказов из 1С: {e}")
        return out

    upd_total = upd.document.total_amount or 0.0

    for o in orders:
        ref = o.get("Ref_Key")
        if not ref:
            continue
        amount = float(o.get("СуммаДокумента") or 0)
        received = already_received_amount(ref)
        remaining = amount - received
        fits = remaining + 0.01 >= upd_total  # толеранс 1 коп
        out.candidates.append(OrderCandidate(
            ref_key=ref,
            number=(o.get("Number") or "").strip(),
            date=(o.get("Date") or "")[:10],
            amount=amount,
            already_received=received,
            remaining=remaining,
            fits_upd=fits,
            partner_key=_d(o.get("Партнер_Key")),
            counterparty_key=_d(o.get("Контрагент_Key")),
            organization_key=_d(o.get("Организация_Key")),
            warehouse_key=_d(o.get("Склад_Key")),
            agreement_key=_d(o.get("Соглашение_Key")),
            raw=o,
        ))

    if not out.candidates:
        out.errors.append(
            f"У поставщика «{out.supplier_name_from_db}» нет проведённых подтверждённых заказов "
            f"за последние {ORDER_MAX_AGE_DAYS} дней."
        )
        return out

    fitting = [c for c in out.candidates if c.fits_upd]
    if not fitting:
        out.warnings.append(
            f"Ни один из {len(out.candidates)} заказов не имеет достаточного остатка "
            f"для суммы УПД ({upd_total:.2f} ₽). Показываю все — кладовщик выберет."
        )

    # Порядок: подходящие по сумме → сверху, внутри — от новых к старым
    out.candidates.sort(key=lambda c: (not c.fits_upd, c.date), reverse=False)
    # .sort: False < True, и по дате от старых к новым; хочу наоборот
    out.candidates.sort(key=lambda c: (c.fits_upd, c.date), reverse=True)

    logger.info(
        "find_matching_orders: inn=%s partner=%s → %d candidates (%d fit)",
        upd.supplier.inn, partner["partner_key"], len(out.candidates), len(fitting),
    )
    return out


def format_match_for_tg(result: MatchResult) -> str:
    """Красивое представление для TG."""
    lines = []
    if not result.found:
        lines.append("🔍 <b>Поиск заказа поставщику</b>\n")
        for e in result.errors:
            lines.append(f"❌ {e}")
        return "\n".join(lines)

    lines.append(f"🔍 <b>Найден партнёр:</b> {result.supplier_name_from_db}")
    if result.blacklisted:
        for e in result.errors:
            lines.append(f"🚫 {e}")
        return "\n".join(lines)

    for w in result.warnings:
        lines.append(f"⚠️ {w}")

    if not result.candidates:
        for e in result.errors:
            lines.append(f"❌ {e}")
        return "\n".join(lines)

    lines.append("")
    lines.append(f"📋 <b>Заказы-кандидаты ({len(result.candidates)}):</b>")
    for i, c in enumerate(result.candidates, 1):
        mark = "✅" if c.fits_upd else "⚠️"
        lines.append(
            f"{mark} <b>{i}.</b> № {c.number} от {c.date}\n"
            f"   Сумма: {c.amount:.2f} ₽  │  принято: {c.already_received:.2f} ₽  │  "
            f"остаток: <b>{c.remaining:.2f} ₽</b>"
        )
    return "\n".join(lines)
