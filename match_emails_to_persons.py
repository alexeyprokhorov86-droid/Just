#!/usr/bin/env python3
"""LLM-матч emails → персоны (km_entities).

Для каждого email в email_employee_mapping (где employee_name пуст) или
найденного в email_messages.from_address (внутренний домен, не в маппинге) —
LLM с контекстом (локальная часть email + sample sent messages + список
кандидатов-сотрудников + их ФИО/роли) определяет владельца.

Результат: добавляет email в km_entities.attrs.emails соответствующего
человека + апдейтит email_employee_mapping.tg_user_id, c1_employee_key,
employee_name_1c.

Запуск:
  python3 match_emails_to_persons.py --dry-run
  python3 match_emails_to_persons.py --max 30   # ограничить
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
from datetime import datetime

import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

REPO = pathlib.Path(__file__).resolve().parent
load_dotenv(REPO / ".env")

ROUTER_AI_URL = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")
ROUTER_AI_KEY = os.getenv("ROUTERAI_API_KEY")
LLM_MODEL = "anthropic/claude-opus-4.7"

DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASS = os.getenv("DB_PASSWORD")

INTERNAL_DOMAINS = ("totsamiy.com", "lacannelle.ru", "frumelad.ru")


def log(m: str): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)


def conn_db():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


def fetch_unmapped_emails(conn) -> list[dict]:
    """Emails в email_employee_mapping без c1_employee_key и без отметки is_functional/is_system."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, email, tg_user_id, employee_name, employee_name_1c,
                   role_description, department, is_functional, is_internal
            FROM email_employee_mapping
            WHERE is_active
              AND c1_employee_key IS NULL
              AND email NOT LIKE 'admin@%'
              AND email NOT LIKE 'noreply@%'
              AND email NOT LIKE 'info@%'
              AND email NOT LIKE 'support@%'
            ORDER BY email
        """)
        return [dict(r) for r in cur.fetchall()]


def fetch_sample_messages(conn, email: str, limit: int = 3) -> list[str]:
    """Несколько последних исходящих от этого email — даёт контекст подписи / тематики."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT subject, LEFT(body_text, 300)
            FROM email_messages
            WHERE from_address = %s AND direction='out'
              AND body_text IS NOT NULL AND LENGTH(body_text) > 30
            ORDER BY received_at DESC LIMIT %s
        """, (email, limit))
        return [f"Тема: {s}\nТело: {b}" for s, b in cur.fetchall()]


def fetch_candidates(conn, top_n: int = 200) -> list[dict]:
    """Активные сотрудники + их роли (через km_entities)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT e.source_ref AS ref_key, e.canonical_name AS name,
                   COALESCE(e.attrs->>'tg_user_ids', '[]') AS tg_ids,
                   (
                     SELECT STRING_AGG(DISTINCT tr.role, '; ')
                     FROM tg_user_roles tr
                     WHERE tr.user_id::text = ANY (
                       SELECT jsonb_array_elements_text(
                         CASE WHEN jsonb_typeof(e.attrs->'tg_user_ids') = 'array'
                              THEN e.attrs->'tg_user_ids' ELSE '[]'::jsonb END))
                       AND tr.is_active
                   ) AS roles
            FROM km_entities e
            JOIN c1_employees emp ON emp.ref_key = e.source_ref
            WHERE e.entity_type='person'
              AND COALESCE(emp.is_archived,false) = false
            LIMIT %s
        """, (top_n,))
        return [dict(r) for r in cur.fetchall()]


def llm_match_email(email: str, samples: list[str], hint_name: str | None,
                    candidates: list[dict]) -> str | None:
    """LLM решает — кому из кандидатов принадлежит email. Возвращает ref_key или None."""
    if not ROUTER_AI_KEY:
        return None
    cand_lines = "\n".join(
        f"  - {c['ref_key']}: {c['name']}"
        + (f" | роли: {c['roles'][:80]}" if c.get('roles') else "")
        for c in candidates
    )
    samples_block = "\n---\n".join(samples) if samples else "(нет образцов сообщений)"
    hint_block = f"\nПодсказка из mapping-таблицы: employee_name={hint_name!r}" if hint_name else ""
    prompt = f"""Сматчи email-адрес с одним из сотрудников 1С.

Email: {email}
Локальная часть: {email.split('@')[0]}{hint_block}

Образцы исходящих сообщений (могут содержать подпись с ФИО):
{samples_block}

Кандидаты-сотрудники:
{cand_lines}

Учитывай:
- локальную часть (alex/alexey → Алексей; ip → Ирина Прохорова; ip — инициалы;
  zakupki1 → закупщик #1; sklad → кладовщик и т.п.)
- роли в чатах для функциональных адресов (rukprod → руководитель производства)
- подпись в теле письма

Если уверенно матчишь — верни ровно один ref_key.
Если не уверен / нет в списке / функциональный без явного владельца — верни NONE.
Ответ только ref_key или NONE, без пояснений."""
    try:
        r = requests.post(
            f"{ROUTER_AI_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTER_AI_KEY}"},
            json={"model": LLM_MODEL,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0, "max_tokens": 64},
            timeout=60,
        )
        ans = r.json()["choices"][0]["message"]["content"].strip()
        if ans == "NONE": return None
        if re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", ans):
            return ans
        return None
    except Exception as e:
        log(f"  LLM err: {e}"); return None


def main(max_count: int | None, dry: bool) -> int:
    log(f"match_emails_to_persons (dry={dry}, max={max_count})")
    conn = conn_db()
    emails = fetch_unmapped_emails(conn)
    log(f"  unmapped emails (active, no c1_employee_key): {len(emails)}")
    if max_count:
        emails = emails[:max_count]

    candidates = fetch_candidates(conn, top_n=300)
    log(f"  candidates (active employees with km_entity): {len(candidates)}")

    matched = 0
    skipped = 0
    for em in emails:
        email = em["email"]
        # Skip явно функциональные (вкл. чужие домены — пока не трогаем)
        domain = email.split("@", 1)[-1].lower()
        if domain not in INTERNAL_DOMAINS:
            skipped += 1
            continue
        samples = fetch_sample_messages(conn, email, limit=2)
        ref_key = llm_match_email(email, samples, em.get("employee_name"), candidates)
        if not ref_key:
            log(f"  {email} → NONE")
            skipped += 1
            continue
        # Найти ФИО сотрудника
        emp_name = next((c["name"] for c in candidates if c["ref_key"] == ref_key), None)
        log(f"  {email} → {ref_key} ({emp_name})")
        matched += 1
        if dry:
            continue
        with conn.cursor() as cur:
            # Обновить mapping
            cur.execute("""
                UPDATE email_employee_mapping
                SET c1_employee_key = %s, employee_name_1c = COALESCE(%s, employee_name_1c),
                    updated_at = NOW()
                WHERE id = %s
            """, (ref_key, emp_name, em["id"]))
            # Найти km_entity — добавить email в attrs.emails (если нет)
            cur.execute("SELECT id FROM km_entities WHERE entity_type='person' AND source_ref=%s",
                        (ref_key,))
            row = cur.fetchone()
            if row:
                cur.execute("""
                    UPDATE km_entities SET
                      attrs = jsonb_set(
                        COALESCE(attrs,'{}'::jsonb), '{emails}',
                        ((SELECT COALESCE(jsonb_agg(DISTINCT v), '[]'::jsonb)
                          FROM jsonb_array_elements(
                            COALESCE(attrs->'emails','[]'::jsonb) || to_jsonb(%s::text)
                          ) v))
                      ),
                      updated_at = NOW()
                    WHERE id = %s
                """, (email, row[0]))
            # Если у mapping есть tg_user_id, и km_entity не имеет TG — добавить
            if em.get("tg_user_id") and row:
                cur.execute("""
                    UPDATE km_entities SET attrs = jsonb_set(
                      COALESCE(attrs,'{}'::jsonb), '{tg_user_ids}',
                      ((SELECT COALESCE(jsonb_agg(DISTINCT v), '[]'::jsonb)
                        FROM jsonb_array_elements(
                          COALESCE(attrs->'tg_user_ids','[]'::jsonb) || to_jsonb(%s::bigint)
                        ) v))
                    ), updated_at=NOW() WHERE id=%s
                """, (em["tg_user_id"], row[0]))
                cur.execute("UPDATE comm_users SET km_entity_id=%s, employee_ref_key=%s "
                            "WHERE tg_user_id=%s AND km_entity_id IS NULL",
                            (row[0], ref_key, em["tg_user_id"]))
        conn.commit()

    log(f"matched={matched} skipped={skipped}")
    conn.close()
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    sys.exit(main(a.max, a.dry_run))
