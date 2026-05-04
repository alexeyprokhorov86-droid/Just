"""
Full RAG battery test — 30 эталонных вопросов через process_rag_query.

Для каждого вопроса фиксирует:
- model_used (gpt-4.1 / opus-4.7)
- retry_count (0 = без escalation)
- eval_good (post-answer evaluator)
- has_1c_evidence (есть ли 1С в источниках)
- latency_ms
- первая строка ответа (для quick eyeballing)

Запуск:  venv/bin/python tests/full_rag_battery.py
         [--limit N]  (ограничить число вопросов)

Вывод: табличка + агрегаты + сохранение JSON в tests/full_rag_battery_result.json
"""

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rag_agent import process_rag_query


QUESTIONS = [
    # --- Количественные (закупки/продажи/производство) ---
    "Сколько муки купили в феврале 2026?",
    "Сколько сахара закупили за март 2026?",
    "Сколько тортов Медовик произвели в марте 2026?",
    "Сколько Наполеона продали в первом квартале 2026?",
    "Какая выручка по пирожным за март 2026?",
    "Сколько упаковки закупили за январь 2026?",
    "Сколько коржей произвели в феврале 2026?",

    # --- Остатки ---
    "Остатки сахара на складе",
    "Сколько муки сейчас на складе СЫРЬЯ СКЛАД?",
    "Какие у нас остатки глазури?",
    "Сколько упаковочной плёнки на складе УПАКОВКИ СКЛАД?",

    # --- Клиенты / поставщики ---
    "Что мы продали клиенту ДИКСИ за 4 квартал 2025?",
    "Что мы продали Магниту в марте 2026?",
    "Что купили у ИП Кутабаевой за последний месяц?",
    "Топ 5 поставщиков за март 2026 по сумме",
    "Топ 10 клиентов за первый квартал 2026",
    "Самые продаваемые товары за март 2026",

    # --- План-факт / динамика ---
    "Выполнение плана производства за март 2026",
    "План-факт по неделям за последний месяц",
    "Какой средний чек у клиента ДИКСИ в 4 квартале 2025?",

    # --- Обсуждения и решения (чаты/knowledge) ---
    "Как решили вопрос с доп соглашением Магнита?",
    "Что обсуждали по качеству упаковки в феврале 2026?",
    "Кто руководитель отдела бухгалтерии?",
    "Политика возврата бракованной продукции",

    # --- Сложные аналитические (нужен escalation или text-to-SQL) ---
    "Какая маржинальность торта Медовик 500г?",
    "Как изменились продажи Медовика между январём и мартом 2026?",
    "Сколько тратится на молоко ежемесячно?",

    # --- Редкое / вероятно пустое ---
    "Остатки упаковки на складе СЫРЬЯ СКЛАД",  # намеренно несоответствие
    "Какие новые клиенты появились в 2026?",
    "Какие поставщики муки есть у нас?",

    # --- Новые: алиасы клиентов ---
    "Что продали Пятёрочке в первом квартале 2026?",       # Q31: alias Пятёрочка→Агроторг
    "Что продали Перекрёстку в марте 2026?",               # Q32: ё→е Перекрёсток→Перекресток

    # --- Новые: банк и сводки ---
    "Сколько денег на банковских счетах?",                 # Q33: bank_balance
    "Какая общая выручка за апрель 2026?",                 # Q34: sales_summary
    "Общая сумма закупок за март 2026",                    # Q35: purchase_summary

    # --- Новые: stock_balance ---
    "Остатки масла сливочного на складе",                  # Q36: stock_balance масло
    "Сколько яиц на складе СЫРЬЯ?",                       # Q37: stock_balance яйца

    # --- Новые: закупки с поставщиком ---
    "Что купили у ООО АГРО 020 за март 2026?",             # Q38: purchases_by_nomenclature supplier
    "Сколько сгущённого молока закупили за первый квартал 2026?",  # Q39

    # --- Новые: производство ---
    "Что произвели за апрель 2026?",                       # Q40: production_summary
    "Сколько произвели Благородной птички в марте 2026?",  # Q41: production_by_nomenclature

    # --- Новые: динамика / custom_sql ---
    "Динамика продаж Медовика по месяцам за 2026",         # Q42: custom_sql помесячно
    "Какой средний размер заказа у Магнита?",              # Q43: custom_sql avg order

    # --- Новые: HR / оргструктура (knowledge) ---
    "Кто директор Фрумелад?",                              # Q44: km_facts
    "Сколько сотрудников в компании?",                     # Q45: 1С staff или knowledge

    # --- Новые: план-факт ---
    "Выполнение плана производства за апрель 2026",        # Q46: plan_vs_fact
    "Топ 5 клиентов за апрель 2026",                       # Q47: top_clients

    # --- Новые: смешанные (аналитика + контекст) ---
    "Почему упали продажи в феврале 2026?",                # Q48: mixed — аналитика + чаты
    "Есть ли задолженность у клиентов?",                   # Q49: custom_sql или knowledge
    "Какие SKU перестали продаваться в 2026?",             # Q50: custom_sql
]


def has_1c_evidence(text: str) -> bool:
    # "1С:" citation, "по данным 1С", "из 1С", "analytics_" type markers
    return bool(re.search(r"1С[:\s]|по данным 1С|из 1С|данные 1С|analytics_", text))


def count_citations(text: str) -> int:
    return len(re.findall(r"\[\d+\]", text))


async def run_one(q: str) -> dict:
    t0 = time.time()
    error = None
    answer = ""
    try:
        answer = await process_rag_query(q)
        if not isinstance(answer, str):
            answer = str(answer)
    except Exception as e:
        error = str(e)
    elapsed = time.time() - t0

    # split для аналитики
    first_line = ""
    body = answer.split("📎")[0].strip() if answer else ""
    for ln in body.split("\n"):
        ln = ln.strip()
        if ln and len(ln) > 20 and not ln.startswith("#"):
            first_line = ln[:180]
            break

    return {
        "question": q,
        "elapsed_sec": round(elapsed, 1),
        "error": error,
        "answer_length": len(answer),
        "has_1c_evidence": has_1c_evidence(answer),
        "citations_count": count_citations(body),
        "first_line": first_line,
    }


async def amain(args):
    questions = QUESTIONS[: args.limit] if args.limit else QUESTIONS
    total = len(questions)
    print(f"=== Full RAG battery: {total} вопросов ===\n")

    results = []
    for i, q in enumerate(questions, 1):
        print(f"[{i}/{total}] {q[:80]}")
        r = await run_one(q)
        results.append(r)
        status = "ERR" if r["error"] else (
            "OK" if r["has_1c_evidence"] or r["citations_count"] >= 3 else "THIN"
        )
        print(f"    {status} {r['elapsed_sec']}s | 1C={r['has_1c_evidence']} | cites={r['citations_count']} | {r['first_line'][:100]}")

    # Агрегаты
    ok = [r for r in results if not r["error"]]
    with_1c = [r for r in ok if r["has_1c_evidence"]]
    good_citations = [r for r in ok if r["citations_count"] >= 3]
    avg_time = sum(r["elapsed_sec"] for r in ok) / max(len(ok), 1)
    errors = [r for r in results if r["error"]]

    print("\n=== ИТОГ ===")
    print(f"Всего: {total}")
    print(f"Ошибок: {len(errors)}")
    print(f"С 1С-источником: {len(with_1c)}/{total} ({len(with_1c)*100//total}%)")
    print(f"С ≥3 ссылками: {len(good_citations)}/{total} ({len(good_citations)*100//total}%)")
    print(f"Средняя latency: {avg_time:.1f} сек")

    out_path = Path(__file__).parent / "full_rag_battery_result.json"
    out_path.write_text(
        json.dumps({
            "timestamp": datetime.now().isoformat(),
            "questions_total": total,
            "with_1c_evidence": len(with_1c),
            "good_citations": len(good_citations),
            "errors": len(errors),
            "avg_latency_sec": round(avg_time, 1),
            "results": results,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nПодробно: {out_path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    asyncio.run(amain(args))


if __name__ == "__main__":
    main()
