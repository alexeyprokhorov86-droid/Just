"""
Tools: km_filter_rules — управление правилами фильтрации мусора из km_*.

- search_filter_rules: ILIKE по value (для /rules_find в боте, ревью в review_knowledge).
- deactivate_filter_rule: UPDATE is_active=false + инвалидация кэша distillation.

Схема km_filter_rules:
  id, rule_type ('junk_word'|'safe_word'|'min_length'), target ('facts'|'decisions'|'all'),
  value, is_active, approval_status, hit_count, added_by, created_at, updated_at.
"""
from __future__ import annotations

from typing import Literal

import psycopg2.extras
from pydantic import BaseModel, Field

from ._db import get_conn
from .registry import tool


class SearchFilterRulesInput(BaseModel):
    query: str = Field(
        description="Подстрока для ILIKE поиска по полю value (регистр не важен)",
        min_length=1,
    )
    only_active: bool = Field(
        default=True,
        description="True — только is_active=true; False — включая отключённые",
    )
    limit: int = Field(default=10, ge=1, le=100)


@tool(
    name="search_filter_rules",
    domain="km_rules",
    description=(
        "Ищет правила фильтрации km_* в таблице km_filter_rules по подстроке value "
        "(ILIKE). Возвращает список правил с id, value, rule_type, target, added_by, "
        "created_at. Используется для: /rules_find в боте (админ-просмотр активных "
        "правил), ручного ревью, инструментов управления KM. По умолчанию только "
        "активные; можно включить отключённые через only_active=False."
    ),
    input_model=SearchFilterRulesInput,
)
def search_filter_rules(query: str, only_active: bool, limit: int) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            where = "value ILIKE %s"
            params: list = [f"%{query}%"]
            if only_active:
                where += " AND is_active = true"
            cur.execute(
                f"""
                SELECT id, value, rule_type, target, added_by, created_at, is_active
                FROM km_filter_rules
                WHERE {where}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                params + [limit],
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


class DeactivateFilterRuleInput(BaseModel):
    rule_ids: list[int] = Field(
        description="Список ID правил для деактивации (is_active → false)",
        min_length=1,
    )
    reason: str = Field(
        default="",
        description=(
            "Причина деактивации — пока только логируется в stdout; в будущем может "
            "уехать в audit-таблицу."
        ),
    )


@tool(
    name="deactivate_filter_rule",
    domain="km_rules",
    description=(
        "Деактивирует правила фильтрации в km_filter_rules (is_active=false, "
        "updated_at=NOW()). Принимает список rule_ids. Игнорирует уже неактивные. "
        "После успеха инвалидирует кэш правил в distillation.py (если модуль "
        "загружен в процессе), чтобы следующий is_junk() подхватил изменения без "
        "рестарта. Возвращает {deactivated:int, rules:[{id, value, rule_type}, ...]}."
    ),
    input_model=DeactivateFilterRuleInput,
)
def deactivate_filter_rule(rule_ids: list[int], reason: str) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE km_filter_rules
                SET is_active = false, updated_at = NOW()
                WHERE id = ANY(%s) AND is_active = true
                RETURNING id, value, rule_type
                """,
                (rule_ids,),
            )
            updated = cur.fetchall()
            conn.commit()
    finally:
        conn.close()

    # Инвалидация кэша distillation: если модуль ещё не импортирован в этом
    # процессе — import триггернёт его, что бесполезно. Защищаемся от этого:
    # трогаем кэш только если модуль уже в sys.modules.
    import sys as _sys
    if "distillation" in _sys.modules:
        _sys.modules["distillation"].invalidate_filter_rules_cache()

    return {
        "deactivated": len(updated),
        "reason": reason,
        "rules": [{"id": r[0], "value": r[1], "rule_type": r[2]} for r in updated],
    }
