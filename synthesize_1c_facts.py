#!/usr/bin/env python3
"""
Periodic Synthesis — ежедневный cron, агрегирует ключевые срезы из 1С
в source_chunks с confidence=0.98 и embedding_v2 (Qwen3).

Страховочный канал: если Router промахнётся и не выберет 1С-tool,
retrieval через Qwen3 HNSW найдёт эти синтезированные факты.

Запуск: cron `0 6 * * *` (после sync_1c, до daily_report).

Что записываем (~20-40 фраз в день):
- Топ-10 SKU по продажам за вчера / неделю / месяц
- Топ-10 SKU по производству за вчера / неделю / месяц
- Топ-10 номенклатур по закупкам за неделю / месяц
- Топ-5 клиентов по выручке за месяц
- Топ-5 поставщиков по закупкам за месяц
- Критичные остатки (>500 кг) по складам
- План/факт за прошлую неделю

Формат: "[synthesis 2026-04-17] <категория>: <текст факта>"
— тег синтеза позволяет легко дропнуть старые синтетики.

Dedup: по source_ref = "synth:<category>:<period>". При повторном прогоне
обновляет ту же запись (или пропускает если не изменилось).
"""

import hashlib
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
logger = logging.getLogger("synthesize_1c")

DB = {
    "host": "172.20.0.2",
    "port": 5432,
    "dbname": "knowledge_base",
    "user": "knowledge",
    "password": os.getenv("DB_PASSWORD"),
}


def conn():
    return psycopg2.connect(**DB)


def fmt_rub(v):
    if v is None:
        return "0"
    return f"{float(v):,.0f}".replace(",", " ")


def fmt_qty(v):
    if v is None:
        return "0"
    return f"{float(v):,.1f}".replace(",", " ").rstrip("0").rstrip(".")


def build_synthesis_facts() -> list:
    """
    Собирает список синтезированных фактов. Каждый — dict:
      {category, period_key, text}
    period_key — устойчивый ключ для dedup.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)
    week_start = today - timedelta(days=7)
    month_start = today - timedelta(days=30)

    facts = []
    c = conn()
    try:
        with c.cursor() as cur:

            def run_top(category, period_start, period_end, period_key, sql, params):
                cur.execute(sql, params)
                rows = cur.fetchall()
                if not rows:
                    return
                period_str = f"{period_start}..{period_end}"
                lines = [f"Топ за период {period_str} ({category}):"]
                for i, row in enumerate(rows, 1):
                    lines.append(f"  {i}. {row[0]} — {row[1]}")
                facts.append({
                    "category": category,
                    "period_key": period_key,
                    "text": "\n".join(lines),
                })

            # === ПРОДАЖИ ===
            for period_start, period_end, pk in [
                (yesterday, yesterday, "sales_day"),
                (week_start, today, "sales_week"),
                (month_start, today, "sales_month"),
            ]:
                run_top(
                    "топ-10 SKU по продажам",
                    period_start, period_end, pk,
                    """
                    SELECT nomenclature_name,
                           'продано ' || COALESCE(SUM(quantity)::text, '0') || ' шт/кг, выручка ' ||
                           COALESCE(SUM(sum_with_vat)::text, '0') || ' руб'
                    FROM mart_sales
                    WHERE doc_type = 'Реализация'
                      AND doc_date >= %s AND doc_date <= %s
                    GROUP BY nomenclature_name
                    ORDER BY SUM(sum_with_vat) DESC NULLS LAST
                    LIMIT 10
                    """,
                    (period_start, period_end),
                )

            # === ТОП КЛИЕНТОВ ===
            run_top(
                "топ-5 клиентов по выручке",
                month_start, today, "clients_month",
                """
                SELECT client_name,
                       'выручка ' || COALESCE(SUM(sum_with_vat)::text, '0') || ' руб, ' ||
                       COUNT(DISTINCT doc_number) || ' реализаций'
                FROM mart_sales
                WHERE doc_type = 'Реализация'
                  AND doc_date >= %s AND doc_date <= %s
                GROUP BY client_name
                ORDER BY SUM(sum_with_vat) DESC NULLS LAST
                LIMIT 5
                """,
                (month_start, today),
            )

            # === ЗАКУПКИ ===
            for period_start, period_end, pk in [
                (week_start, today, "purchases_week"),
                (month_start, today, "purchases_month"),
            ]:
                run_top(
                    "топ-10 номенклатур по закупкам",
                    period_start, period_end, pk,
                    """
                    SELECT nomenclature_name,
                           'закуплено ' || COALESCE(SUM(quantity)::text, '0') || ' ед., сумма ' ||
                           COALESCE(SUM(sum_total)::text, '0') || ' руб'
                    FROM mart_purchases
                    WHERE doc_date >= %s AND doc_date <= %s
                    GROUP BY nomenclature_name
                    ORDER BY SUM(sum_total) DESC NULLS LAST
                    LIMIT 10
                    """,
                    (period_start, period_end),
                )

            # === ТОП ПОСТАВЩИКОВ ===
            run_top(
                "топ-5 поставщиков по сумме закупок",
                month_start, today, "suppliers_month",
                """
                SELECT contractor_name,
                       'сумма закупок ' || COALESCE(SUM(sum_total)::text, '0') || ' руб, ' ||
                       COUNT(DISTINCT doc_number) || ' документов'
                FROM mart_purchases
                WHERE doc_date >= %s AND doc_date <= %s
                GROUP BY contractor_name
                ORDER BY SUM(sum_total) DESC NULLS LAST
                LIMIT 5
                """,
                (month_start, today),
            )

            # === ПРОИЗВОДСТВО ===
            for period_start, period_end, pk in [
                (week_start, today, "production_week"),
                (month_start, today, "production_month"),
            ]:
                run_top(
                    "топ-10 SKU по производству",
                    period_start, period_end, pk,
                    """
                    SELECT COALESCE(n.name, mp.nomenclature_key),
                           'произведено ' || COALESCE(SUM(mp.quantity)::text, '0') || ' ед.'
                    FROM mart_production mp
                    LEFT JOIN nomenclature n ON n.id::text = mp.nomenclature_key
                    WHERE mp.doc_date >= %s AND mp.doc_date <= %s
                    GROUP BY COALESCE(n.name, mp.nomenclature_key)
                    ORDER BY SUM(mp.quantity) DESC NULLS LAST
                    LIMIT 10
                    """,
                    (period_start, period_end),
                )

            # === ОСТАТКИ (снимок) — только крупные ≥ 500 кг/ед ===
            cur.execute(
                """
                SELECT n.name, w.name,
                       SUM(sb.quantity) AS qty,
                       MAX(n.weight_unit) AS wu
                FROM c1_stock_balance sb
                JOIN nomenclature n ON n.id::text = sb.nomenclature_key
                LEFT JOIN c1_warehouses w ON w.ref_key = sb.warehouse_key
                WHERE sb.quantity >= 500
                GROUP BY n.name, w.name
                ORDER BY qty DESC NULLS LAST
                LIMIT 30
                """
            )
            rows = cur.fetchall()
            if rows:
                lines = [f"Крупные остатки на складах (snapshot {today}, ≥500 ед.):"]
                for nom, wh, qty, wu in rows:
                    lines.append(f"  - {nom} на {wh or '?'}: {fmt_qty(qty)} {wu or 'ед.'}")
                facts.append({
                    "category": "остатки по складам",
                    "period_key": "stock_snapshot",
                    "text": "\n".join(lines),
                })

            # === ПЛАН-ФАКТ ЗА ПРОШЛУЮ НЕДЕЛЮ ===
            try:
                cur.execute(
                    """
                    SELECT "Неделя", "Заказы (план)", "Ордера (факт)",
                           "Отклонение", "Выполнение %"
                    FROM v_plan_fact_weekly
                    WHERE "Неделя" >= %s
                    ORDER BY "Неделя" DESC
                    LIMIT 4
                    """,
                    (week_start - timedelta(days=14),),
                )
                rows = cur.fetchall()
                if rows:
                    lines = ["План-факт по неделям (последние 4):"]
                    for w, plan, fact, dev, pct in rows:
                        lines.append(
                            f"  - {w}: план {fmt_rub(plan)}, факт {fmt_rub(fact)}, "
                            f"отклонение {dev}, выполнение {pct}%"
                        )
                    facts.append({
                        "category": "план/факт по неделям",
                        "period_key": "plan_fact_weekly",
                        "text": "\n".join(lines),
                    })
            except Exception as e:
                logger.warning(f"plan_fact: {e}")

    finally:
        c.close()

    logger.info(f"Построено синтезированных фактов: {len(facts)}")
    return facts


def upsert_facts_as_source_chunks(facts: list):
    """Сохраняет факты в source_documents + source_chunks.
    source_ref = "synth:<period_key>" → upsert (DELETE+INSERT) при каждом прогоне."""
    if not facts:
        return
    try:
        from chunkers.embedder import embed_document_v2
    except Exception as e:
        logger.error(f"Не могу загрузить embedder: {e}")
        return

    today = date.today()
    c = conn()
    inserted = 0
    try:
        with c.cursor() as cur:
            for f in facts:
                source_ref = f"synth:{f['period_key']}"
                text = f"[synthesis {today}] {f['category']}:\n{f['text']}"
                if len(text) > 3000:
                    text = text[:3000]

                cur.execute(
                    "SELECT id FROM source_documents WHERE source_ref = %s LIMIT 1",
                    (source_ref,),
                )
                row = cur.fetchone()
                if row:
                    doc_id = row[0]
                    cur.execute(
                        """UPDATE source_documents
                           SET body_text = %s, doc_date = %s, updated_at = now()
                           WHERE id = %s""",
                        (text, today, doc_id),
                    )
                    cur.execute(
                        "DELETE FROM source_chunks WHERE document_id = %s",
                        (doc_id,),
                    )
                else:
                    cur.execute(
                        """INSERT INTO source_documents
                             (source_kind, source_ref, title, body_text, doc_date,
                              language, is_deleted, confidence, meta, created_at, updated_at)
                           VALUES ('synthesized_1c', %s, %s, %s, %s, 'ru',
                                   false, 0.98, %s::jsonb, now(), now())
                           RETURNING id""",
                        (
                            source_ref,
                            f["category"][:200],
                            text,
                            today,
                            json.dumps({"period_key": f["period_key"]}, ensure_ascii=False),
                        ),
                    )
                    doc_id = cur.fetchone()[0]

                try:
                    emb = embed_document_v2(text[:2000])
                except Exception as e:
                    logger.warning(f"embed_document_v2 failed on '{f['period_key']}': {e}")
                    continue
                if not emb:
                    continue
                emb_str = "[" + ",".join(str(x) for x in emb) + "]"

                cur.execute(
                    """INSERT INTO source_chunks
                         (document_id, chunk_no, chunk_text, embedding_v2,
                          chunk_type, source_kind, confidence, importance_score, created_at)
                       VALUES (%s, 0, %s, %s::vector, 'synthesized_1c', 'synthesized_1c',
                               0.98, 0.95, now())""",
                    (doc_id, text[:2000], emb_str),
                )
                inserted += 1
            c.commit()
        logger.info(f"Upsert завершён: {inserted} source_chunks")
    finally:
        c.close()


def main():
    t0 = time.time()
    logger.info("=== synthesize_1c_facts start ===")
    try:
        facts = build_synthesis_facts()
        upsert_facts_as_source_chunks(facts)
    except Exception as e:
        logger.error(f"synthesize_1c_facts FAILED: {e}", exc_info=True)
        sys.exit(1)
    logger.info(f"=== done in {time.time() - t0:.1f}s ===")


if __name__ == "__main__":
    main()
