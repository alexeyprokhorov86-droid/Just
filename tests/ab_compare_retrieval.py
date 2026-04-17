"""
A/B сравнение retrieval: legacy km_* (e5) vs source_chunks.embedding_v2 (Qwen3).

Запускать вручную перед переключением RAG на USE_EMBEDDING_V2=true.
Для каждого вопроса печатает top-5 от каждого индекса с similarity —
вы сами оцениваете relevance глазами и решаете, какой лучше.

Использование:
    python tests/ab_compare_retrieval.py                   # все 10 вопросов
    python tests/ab_compare_retrieval.py --questions 3-5   # только 3-й, 4-й, 5-й
    python tests/ab_compare_retrieval.py --top 3           # top-3 вместо top-5

Вопросы фиксированные (для воспроизводимости). Дополнить список в
QUESTIONS по вкусу — покрывают продажи, персонал, рецептуры, 1С, email,
задачи, инциденты, политики.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rag_agent import search_knowledge, search_source_chunks


QUESTIONS = [
    "Продажи торта Медовик за март 2026",
    "Кто руководитель отдела бухгалтерии?",
    "Рецептура шоколадной глазури для пирожного",
    "Остатки муки пшеничной на складе НФ",
    "План производства на следующую неделю",
    "Договор с Магнит — статус поставок",
    "Отпуска сотрудников отдела продаж в апреле",
    "Проблемы с качеством упаковки в феврале",
    "Новые клиенты за первый квартал 2026",
    "Политика возврата бракованной продукции",
]


def print_separator(char: str = "=", length: int = 90):
    print(char * length)


def print_results(label: str, results: list, top: int):
    print(f"--- {label} ---")
    if not results:
        print("  (нет результатов)")
        return
    for i, r in enumerate(results[:top], 1):
        sim = r.get("similarity", 0.0)
        source = r.get("source", "?")
        title = r.get("title", "")
        content = r.get("content", "")[:150].replace("\n", " ")
        head = f"[{i}] sim={sim:.4f} | {source}"
        if title:
            head += f" | {title[:50]}"
        print(head)
        print(f"    {content}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--questions", default="1-10", help="Range (напр. 1-10 или 3-5)")
    parser.add_argument("--top", type=int, default=5)
    args = parser.parse_args()

    start_s, _, end_s = args.questions.partition("-")
    start = int(start_s)
    end = int(end_s) if end_s else start
    questions = QUESTIONS[start - 1 : end]

    print_separator("=")
    print(f"A/B compare retrieval — {len(questions)} вопросов, top-{args.top} каждого")
    print_separator("=")

    for idx, q in enumerate(questions, start=start):
        print()
        print_separator("=")
        print(f"Q{idx}: {q}")
        print_separator("=")

        r_km = search_knowledge(q, limit=20)
        print_results(f"A: search_knowledge (km_* e5 legacy) — {len(r_km)} results", r_km, args.top)

        print()

        r_sc = search_source_chunks(q, limit=20)
        print_results(f"B: search_source_chunks (Qwen3 embedding_v2) — {len(r_sc)} results", r_sc, args.top)

    print()
    print_separator("=")
    print("Готово. Оцените глазами какая ветвь даёт более релевантный результат.")


if __name__ == "__main__":
    main()
