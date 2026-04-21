"""
Tool: identify_employee_by_text — LLM-матчинг произвольного ответа пользователя
к записи в v_current_staff.

Use case: user прислал «я Иванов Иван, бухгалтерия НФ» — нужно найти его в 1С.
Простой fuzzy/transliteration не справляется (имена бывают неформальные,
должности формулируются по-разному). LLM хорошо с этим работает.

Возвращает топ-3 кандидата по релевантности, admin выбирает финал через
inline-кнопки в bot.py.
"""
from __future__ import annotations

import json
import os

import psycopg2.extras
from openai import OpenAI
from pydantic import BaseModel, Field

from ._db import get_conn
from .registry import tool


def _gpt_client() -> OpenAI:
    return OpenAI(
        api_key=os.getenv("ROUTERAI_API_KEY"),
        base_url=os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1"),
    )


class IdentifyEmployeeInput(BaseModel):
    user_answer: str = Field(
        description="Произвольный текст пользователя с ФИО, должностью, отделом и т.п.",
        min_length=1,
    )
    top_k: int = Field(
        default=3,
        ge=1,
        le=5,
        description="Сколько кандидатов вернуть для admin approval",
    )


@tool(
    name="identify_employee_by_text",
    domain="identification",
    description=(
        "Находит топ-N наиболее подходящих сотрудников из v_current_staff "
        "по произвольному текстовому ответу пользователя (ФИО, должность, "
        "отдел в свободной форме). Использует GPT-4.1 через RouterAI. "
        "Возвращает {candidates: [{employee_ref_key, full_name, position_name, "
        "department_name, confidence, reasoning}], best_confidence: str}. "
        "Если никто не подходит — candidates пустой, best_confidence='none' "
        "(значит user скорее всего external). Admin подтверждает выбор "
        "inline-кнопками в боте."
    ),
    input_model=IdentifyEmployeeInput,
)
def identify_employee_by_text(user_answer: str, top_k: int) -> dict:
    # Загружаем всех actionable сотрудников из 1С (не уволенные).
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ref_key::text, full_name, position_name, department_name
                FROM v_current_staff
                WHERE dismissal_date IS NULL
                ORDER BY full_name
                """
            )
            staff = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    if not staff:
        return {"candidates": [], "best_confidence": "none", "staff_pool_size": 0}

    # Компактный список для LLM (экономим токены).
    staff_text = "\n".join(
        f"{i+1}. ref={s['ref_key']} | {s['full_name']} | {s['position_name'] or '?'} | {s['department_name'] or '?'}"
        for i, s in enumerate(staff)
    )

    prompt = f"""Ты помогаешь определить сотрудника компании по его ответу.

ОТВЕТ ПОЛЬЗОВАТЕЛЯ:
{user_answer}

СПИСОК ДЕЙСТВУЮЩИХ СОТРУДНИКОВ (ref_key | ФИО | должность | отдел):
{staff_text}

ЗАДАЧА: найти до {top_k} наиболее вероятных кандидатов из списка. Ответ пользователя
может быть в любой форме: латиницей, с опечатками, без отчества, с неформальной
должностью. Имя собственное — главный сигнал; должность и отдел — дополнительные.

Верни СТРОГО JSON (ничего кроме JSON):
{{
  "candidates": [
    {{"employee_ref_key": "uuid", "full_name": "...", "position_name": "...",
      "department_name": "...", "confidence": "high"|"medium"|"low",
      "reasoning": "короткое обоснование на русском"}}
  ],
  "best_confidence": "high"|"medium"|"low"|"none"
}}

Если НИКТО не подходит — candidates=[], best_confidence="none" (пользователь
скорее всего внешний, не из компании). Только РЕАЛЬНЫЕ совпадения — не гадай.
"""

    client = _gpt_client()
    response = client.chat.completions.create(
        model="openai/gpt-4.1",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content or "{}"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "candidates": [],
            "best_confidence": "none",
            "staff_pool_size": len(staff),
            "error": f"LLM вернул невалидный JSON: {raw[:200]}",
        }

    parsed.setdefault("candidates", [])
    parsed.setdefault("best_confidence", "none")
    parsed["staff_pool_size"] = len(staff)
    # Обрезаем top_k (LLM мог вернуть больше)
    parsed["candidates"] = parsed["candidates"][:top_k]
    return parsed
