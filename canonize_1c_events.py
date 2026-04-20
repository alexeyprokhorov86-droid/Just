#!/usr/bin/env python3
"""
Canonize 1C Events — селективная канонизация значимых 1С-документов в
source_documents с source_kind='c1_event'.

В отличие от synthesize_1c_facts.py (агрегаты), здесь — конкретные
документы для точечных вопросов: "что купили у Х в марте", "крупные
платежи за последний месяц".

MVP (2026-04-20): 3 категории
  - purchase_large   — c1_purchases, amount > 300 000 ₽
  - sale_large       — mart_sales, doc_number с SUM(sum_with_vat) > 200 000 ₽
  - payment_large    — c1_bank_expenses, amount > 500 000 ₽

Использование:
  python canonize_1c_events.py                       # incremental (updated_at за 1 час)
  python canonize_1c_events.py --backfill-days 180   # разовый backfill
  python canonize_1c_events.py --category purchase_large --backfill-days 30

Idempotent: source_ref = "c1_event:<category>:<ref_key_or_docnum>".
При UPDATE — DELETE source_chunks + пере-embed.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv("/home/admin/telegram_logger_bot/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("canonize_1c_events")

DB = {
    "host": "172.20.0.2",
    "port": 5432,
    "dbname": "knowledge_base",
    "user": "knowledge",
    "password": os.getenv("DB_PASSWORD"),
}

PURCHASE_THRESHOLD = 300_000
SALE_THRESHOLD = 200_000
PAYMENT_THRESHOLD = 500_000


def conn():
    return psycopg2.connect(**DB)


def fmt_rub(v):
    if v is None:
        return "0"
    return f"{float(v):,.0f}".replace(",", " ")


def fetch_purchase_events(cur, since: datetime, limit: int = 5000):
    cur.execute("""
        SELECT p.ref_key, p.doc_number, p.doc_date, p.amount, p.comment,
               p.incoming_number, p.incoming_date,
               c.name AS partner_name,
               p.updated_at
        FROM c1_purchases p
        LEFT JOIN clients c ON c.id::text = p.partner_key
        WHERE p.posted = true
          AND p.is_deleted = false
          AND p.amount >= %s
          AND p.updated_at >= %s
        ORDER BY p.doc_date DESC
        LIMIT %s
    """, (PURCHASE_THRESHOLD, since, limit))
    events = []
    for row in cur.fetchall():
        ref_key, doc_num, doc_date, amount, comment, inc_num, inc_date, partner, updated = row
        partner_name = (partner or "неизв.")
        title = f"Закупка {fmt_rub(amount)} ₽ у {partner_name[:80]}"
        lines = [
            f"Документ закупки № {doc_num or '—'} от {doc_date}",
            f"Поставщик: {partner_name}",
            f"Сумма: {fmt_rub(amount)} ₽",
        ]
        if inc_num or inc_date:
            lines.append(f"Входящий № {inc_num or '—'} от {inc_date or '—'}")
        if comment:
            lines.append(f"Комментарий: {comment[:500]}")

        # Позиции (top-5 по сумме)
        cur2 = cur.connection.cursor()
        try:
            cur2.execute("""
                SELECT n.name, pi.quantity, pi.sum_total
                FROM c1_purchase_items pi
                LEFT JOIN nomenclature n ON n.id::text = pi.nomenclature_key
                WHERE pi.doc_key = %s
                ORDER BY pi.sum_total DESC NULLS LAST
                LIMIT 5
            """, (ref_key,))
            items = cur2.fetchall()
        finally:
            cur2.close()
        if items:
            lines.append("Позиции (топ-5):")
            for name, qty, ssum in items:
                lines.append(f"  - {name or '—'}: {qty or 0} × = {fmt_rub(ssum)} ₽")

        events.append({
            "category": "purchase_large",
            "source_ref": f"c1_event:purchase_large:{ref_key}",
            "title": title[:200],
            "body": "\n".join(lines),
            "doc_date": doc_date,
            "meta": {
                "event_type": "purchase_large",
                "ref_key": ref_key,
                "doc_number": doc_num,
                "amount": float(amount or 0),
                "partner": partner_name,
            },
        })
    return events


def fetch_sale_events(cur, since_date: date, limit: int = 10000):
    """mart_sales сгруппировать по doc_number; порог по сумме документа."""
    cur.execute("""
        WITH doc_sum AS (
          SELECT doc_number, MIN(doc_date) AS doc_date,
                 MIN(client_name) AS client_name,
                 SUM(sum_with_vat) AS total,
                 MIN(doc_type) AS doc_type
          FROM mart_sales
          WHERE doc_date >= %s
          GROUP BY doc_number
          HAVING SUM(sum_with_vat) >= %s
          ORDER BY MIN(doc_date) DESC
          LIMIT %s
        )
        SELECT doc_number, doc_date, client_name, total, doc_type FROM doc_sum
    """, (since_date, SALE_THRESHOLD, limit))
    events = []
    for doc_num, doc_date, client, total, doc_type in cur.fetchall():
        client_name = client or "неизв."
        title = f"Продажа {fmt_rub(total)} ₽ клиенту {client_name[:80]}"
        lines = [
            f"{doc_type or 'Реализация'} № {doc_num or '—'} от {doc_date}",
            f"Клиент: {client_name}",
            f"Сумма: {fmt_rub(total)} ₽",
        ]
        # Топ-5 позиций по этому doc_number
        cur2 = cur.connection.cursor()
        try:
            cur2.execute("""
                SELECT nomenclature_name, SUM(quantity), SUM(sum_with_vat)
                FROM mart_sales
                WHERE doc_number = %s
                GROUP BY nomenclature_name
                ORDER BY SUM(sum_with_vat) DESC NULLS LAST
                LIMIT 5
            """, (doc_num,))
            items = cur2.fetchall()
        finally:
            cur2.close()
        if items:
            lines.append("Позиции (топ-5):")
            for name, qty, ssum in items:
                lines.append(f"  - {name or '—'}: {qty or 0} × = {fmt_rub(ssum)} ₽")

        events.append({
            "category": "sale_large",
            "source_ref": f"c1_event:sale_large:{doc_num}",
            "title": title[:200],
            "body": "\n".join(lines),
            "doc_date": doc_date,
            "meta": {
                "event_type": "sale_large",
                "doc_number": doc_num,
                "amount": float(total or 0),
                "client": client_name,
            },
        })
    return events


def fetch_payment_events(cur, since: datetime, limit: int = 5000):
    cur.execute("""
        SELECT be.ref_key, be.doc_number, be.doc_date, be.amount, be.purpose,
               be.comment, be.operation,
               c.name AS counterparty_name,
               be.updated_at
        FROM c1_bank_expenses be
        LEFT JOIN clients c ON c.id::text = be.counterparty_key
        WHERE be.posted = true
          AND be.is_deleted = false
          AND be.amount >= %s
          AND be.updated_at >= %s
        ORDER BY be.doc_date DESC
        LIMIT %s
    """, (PAYMENT_THRESHOLD, since, limit))
    events = []
    for row in cur.fetchall():
        ref_key, doc_num, doc_date, amount, purpose, comment, op, cpty, updated = row
        cpty_name = cpty or "неизв."
        title = f"Платёж {fmt_rub(amount)} ₽ контрагенту {cpty_name[:80]}"
        lines = [
            f"Платёжное поручение № {doc_num or '—'} от {doc_date}",
            f"Операция: {op or '—'}",
            f"Контрагент: {cpty_name}",
            f"Сумма: {fmt_rub(amount)} ₽",
        ]
        if purpose:
            lines.append(f"Назначение: {purpose[:500]}")
        if comment:
            lines.append(f"Комментарий: {comment[:300]}")

        events.append({
            "category": "payment_large",
            "source_ref": f"c1_event:payment_large:{ref_key}",
            "title": title[:200],
            "body": "\n".join(lines),
            "doc_date": doc_date,
            "meta": {
                "event_type": "payment_large",
                "ref_key": ref_key,
                "doc_number": doc_num,
                "amount": float(amount or 0),
                "counterparty": cpty_name,
            },
        })
    return events


def upsert_events(events: list, embedder):
    if not events:
        return 0
    c = conn()
    inserted = 0
    updated_ = 0
    last_log = time.time()
    try:
        with c.cursor() as cur:
            for i, ev in enumerate(events):
                # UPSERT source_documents
                cur.execute(
                    "SELECT id FROM source_documents WHERE source_ref = %s LIMIT 1",
                    (ev["source_ref"],),
                )
                row = cur.fetchone()
                body = ev["body"]
                if len(body) > 3000:
                    body = body[:3000]

                if row:
                    doc_id = row[0]
                    cur.execute(
                        """UPDATE source_documents
                           SET title=%s, body_text=%s, doc_date=%s, meta=%s::jsonb,
                               updated_at=now()
                           WHERE id=%s""",
                        (ev["title"], body, ev["doc_date"],
                         json.dumps(ev["meta"], ensure_ascii=False), doc_id),
                    )
                    cur.execute("DELETE FROM source_chunks WHERE document_id=%s",
                                (doc_id,))
                    updated_ += 1
                else:
                    cur.execute(
                        """INSERT INTO source_documents
                             (source_kind, source_ref, title, body_text, doc_date,
                              language, is_deleted, confidence, meta, created_at, updated_at)
                           VALUES ('c1_event', %s, %s, %s, %s, 'ru', false, 0.98, %s::jsonb, now(), now())
                           RETURNING id""",
                        (ev["source_ref"], ev["title"], body, ev["doc_date"],
                         json.dumps(ev["meta"], ensure_ascii=False)),
                    )
                    doc_id = cur.fetchone()[0]
                    inserted += 1

                try:
                    emb = embedder(body[:2000])
                except Exception as e:
                    logger.warning(f"embed failed for {ev['source_ref']}: {e}")
                    continue
                if not emb:
                    continue
                emb_str = "[" + ",".join(str(x) for x in emb) + "]"

                cur.execute(
                    """INSERT INTO source_chunks
                         (document_id, chunk_no, chunk_text, embedding_v2,
                          chunk_type, source_kind, chunk_date, confidence,
                          importance_score, created_at)
                       VALUES (%s, 0, %s, %s::vector, 'c1_event', 'c1_event',
                               %s, 0.98, 0.95, now())""",
                    (doc_id, body[:2000], emb_str, ev["doc_date"]),
                )
                # commit каждые 20 событий + прогресс-лог каждые 30 сек
                if (i + 1) % 20 == 0:
                    c.commit()
                if time.time() - last_log > 30:
                    logger.info(f"progress: {i+1}/{len(events)} (inserted={inserted}, updated={updated_})")
                    last_log = time.time()
            c.commit()
    finally:
        c.close()
    logger.info(f"Upsert: inserted={inserted}, updated={updated_}")
    return inserted + updated_


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill-days", type=int, default=None,
                    help="backfill за последние N дней (updated_at фильтр)")
    ap.add_argument("--category", choices=["purchase_large", "sale_large", "payment_large"],
                    help="только одна категория")
    args = ap.parse_args()

    t0 = time.time()
    logger.info("=== canonize_1c_events start ===")

    if args.backfill_days is not None:
        since = datetime.now() - timedelta(days=args.backfill_days)
        since_date = since.date()
        logger.info(f"BACKFILL режим: с {since.isoformat()} ({args.backfill_days} дней)")
    else:
        since = datetime.now() - timedelta(hours=1)
        since_date = since.date() - timedelta(days=1)  # dateonly безопасность
        logger.info(f"INCREMENTAL режим: updated_at >= {since.isoformat()}")

    from chunkers.embedder import embed_document_v2

    all_events = []
    c = conn()
    try:
        with c.cursor() as cur:
            if args.category in (None, "purchase_large"):
                pe = fetch_purchase_events(cur, since)
                logger.info(f"  purchase_large: {len(pe)}")
                all_events += pe
            if args.category in (None, "sale_large"):
                se = fetch_sale_events(cur, since_date)
                logger.info(f"  sale_large: {len(se)}")
                all_events += se
            if args.category in (None, "payment_large"):
                pye = fetch_payment_events(cur, since)
                logger.info(f"  payment_large: {len(pye)}")
                all_events += pye
    finally:
        c.close()

    logger.info(f"Всего событий: {len(all_events)}")
    upsert_events(all_events, embed_document_v2)

    logger.info(f"=== done in {time.time() - t0:.1f}s ===")


if __name__ == "__main__":
    main()
