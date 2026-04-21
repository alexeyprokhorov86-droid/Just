"""
Tool: synthesize_1c_snapshot — on-demand пересборка срезов 1С в source_chunks.

Обёртка над synthesize_1c_facts.build_synthesis_facts + опциональный upsert.
Полезна когда:
- RAG хочет свежие агрегаты без ожидания cron 06:00 (persist=False).
- Нужно форсировать обновление синтетики (например после sync_1c вне расписания).

Scope/period параметризация (пересчёт только sales / clients / конкретного периода)
не реализована — build_synthesis_facts монолитный, его разбиение на под-функции
в backlog'е.
"""
from __future__ import annotations

from pydantic import BaseModel, Field

from .registry import tool


class SynthesizeSnapshotInput(BaseModel):
    persist: bool = Field(
        default=False,
        description=(
            "True — после сборки записать в source_documents+source_chunks "
            "(upsert по source_ref='synth:<period_key>', обновляет embedding_v2). "
            "False — только вернуть факты в памяти, без побочных эффектов."
        ),
    )


@tool(
    name="synthesize_1c_snapshot",
    domain="c1",
    description=(
        "Пересобирает дневной срез агрегатов из 1С (mart_sales, mart_purchases, "
        "mart_production, c1_stock_balance, v_plan_fact_weekly): топ-SKU, "
        "топ-клиенты, топ-поставщики, крупные остатки, план-факт по неделям. "
        "Возвращает {facts_count, categories (словарь category->count), facts "
        "(list of {category, period_key, text}), persisted (bool), persisted_count}. "
        "При persist=True — записывает результат в source_documents/source_chunks "
        "с confidence=0.98 и embedding_v2 (Qwen3), делая факты находимыми через "
        "векторный поиск в RAG. Без persist — просто вернёт в памяти (для "
        "одноразового использования LLM или показа пользователю)."
    ),
    input_model=SynthesizeSnapshotInput,
)
def synthesize_1c_snapshot(persist: bool) -> dict:
    # Импорт внутри функции: synthesize_1c_facts тянет chunkers.embedder
    # (Qwen3), не стоит грузить при `import tools`.
    from synthesize_1c_facts import (
        build_synthesis_facts,
        upsert_facts_as_source_chunks,
    )

    facts = build_synthesis_facts()
    categories: dict[str, int] = {}
    for f in facts:
        categories[f["category"]] = categories.get(f["category"], 0) + 1

    result = {
        "facts_count": len(facts),
        "categories": categories,
        "facts": facts,
        "persisted": False,
        "persisted_count": 0,
    }

    if persist and facts:
        upsert_facts_as_source_chunks(facts)
        result["persisted"] = True
        result["persisted_count"] = len(facts)

    return result
