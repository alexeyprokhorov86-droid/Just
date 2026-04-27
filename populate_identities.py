#!/usr/bin/env python3
"""Унификация идентичностей через KG.

Связывает 1С-сотрудников, 1С-пользователей, TG-юзеров и email-адреса в единые
person-узлы km_entities. Обновляет:
  - km_entities (entity_type='person', aliases+attrs)
  - comm_users.km_entity_id + employee_ref_key
  - email_employee_mapping.tg_user_id (где пустует)

Источники:
  - c1_employees (1180)         — name = ФИО сотрудника
  - c1_users (246)              — description = ФИО системного пользователя 1С
  - comm_users (56 TG юзеров)   — display_name, username, employee_ref_key, km_entity_id
  - tg_user_roles               — first_name + last_name + role
  - email_employee_mapping (81) — email + tg_user_id + c1_employee_key

Алгоритм:
  Pass 1 — c1_employees → km_entities (person), 1:1 по ref_key
  Pass 2 — c1_users → name match → дописать в attrs.c1_user_ref_key
  Pass 3 — comm_users (TG) → name match → comm_users.km_entity_id + employee_ref_key
  Pass 4 — email_employee_mapping → подтянуть tg_user_id из comm_users
  Pass 5 — отчёт по покрытию

Запуск:
  python3 populate_identities.py --dry-run    # показать что будет изменено, без записи
  python3 populate_identities.py              # применить
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
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

REPO = pathlib.Path(__file__).resolve().parent
load_dotenv(REPO / ".env")

DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASS = os.getenv("DB_PASSWORD")

try:
    from rapidfuzz import fuzz, process
except ImportError:
    print("Need rapidfuzz: /home/admin/telegram_logger_bot/venv/bin/pip install rapidfuzz")
    sys.exit(1)


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def conn_db():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)


# ─────────────────────────────────────────────────────────────────────
#  Name normalization
# ─────────────────────────────────────────────────────────────────────

NAME_PUNCT = re.compile(r"[.\-_,()]+")
NAME_WHITESPACE = re.compile(r"\s+")

# Stop-words для имени — служебные/технические
NAME_STOPS = {
    "ип", "ооо", "ао", "зао", "пао",
    "не", "указан", "пользователь",
    "system", "admin", "test",
}


def normalize_name(s: str | None) -> str:
    """ФИО → 'last first middle' lowercased, сорт. слов."""
    if not s:
        return ""
    s = s.lower()
    s = NAME_PUNCT.sub(" ", s)
    s = NAME_WHITESPACE.sub(" ", s).strip()
    parts = [p for p in s.split() if p and p not in NAME_STOPS and len(p) > 1]
    if not parts:
        return ""
    parts.sort()
    return " ".join(parts)


# ─────────────────────────────────────────────────────────────────────
#  Loaders
# ─────────────────────────────────────────────────────────────────────

def load_employees(conn) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
          SELECT ref_key, name, organization_key, is_archived
          FROM c1_employees
          WHERE name IS NOT NULL AND name != ''
            AND COALESCE(is_deleted, FALSE) = FALSE
        """)
        return list(cur.fetchall())


def load_users_1c(conn) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
          SELECT ref_key, description, person_key, is_invalid, is_deleted
          FROM c1_users
          WHERE COALESCE(is_deleted, FALSE) = FALSE
            AND COALESCE(is_invalid, FALSE) = FALSE
            AND description IS NOT NULL
            AND description NOT LIKE '<%'
        """)
        return list(cur.fetchall())


def load_comm_users(conn) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
          SELECT tg_user_id, display_name, username, km_entity_id, employee_ref_key
          FROM comm_users
        """)
        return list(cur.fetchall())


def load_tg_user_roles(conn) -> dict[int, dict]:
    """Best first_name+last_name per tg_user_id."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
          SELECT user_id,
            MAX(first_name) AS first_name,
            MAX(last_name) AS last_name,
            STRING_AGG(DISTINCT role, ' | ') AS roles
          FROM tg_user_roles
          WHERE first_name IS NOT NULL OR last_name IS NOT NULL
          GROUP BY user_id
        """)
        return {r["user_id"]: dict(r) for r in cur.fetchall()}


def load_email_mapping(conn) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
          SELECT id, email, tg_user_id, c1_employee_key, employee_name, employee_name_1c
          FROM email_employee_mapping
          WHERE COALESCE(is_active, TRUE) = TRUE
        """)
        return list(cur.fetchall())


# ─────────────────────────────────────────────────────────────────────
#  Matching
# ─────────────────────────────────────────────────────────────────────

ROUTER_AI_URL = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")
ROUTER_AI_KEY = os.getenv("ROUTERAI_API_KEY")
LLM_MODEL = "anthropic/claude-opus-4.7"


def llm_match_tg_to_employee(tg_info: dict, employees_short: list[dict]) -> str | None:
    """LLM решает кому из сотрудников соответствует TG-юзер.
    Возвращает employee.ref_key или None."""
    if not ROUTER_AI_KEY:
        return None
    # Build prompt
    cand_lines = []
    for e in employees_short[:200]:
        cand_lines.append(f"  - {e['ref_key']}: {e['name']}")
    prompt = f"""Сматчи Telegram-пользователя к сотруднику 1С.

Telegram:
  display_name: {tg_info.get('display_name')!r}
  username:     {tg_info.get('username')!r}
  first_name:   {tg_info.get('first_name')!r}
  last_name:    {tg_info.get('last_name')!r}
  роли в чатах: {tg_info.get('roles')!r}

Кандидаты-сотрудники (ref_key: ФИО):
{chr(10).join(cand_lines)}

Учитывай транслит (Юра ↔ Юрий, Лиза ↔ Елизавета, Лена ↔ Елена, Ира ↔ Ирина, Алексей ↔ Alex/A./Lex и т.п.) и инициалы.
Если уверен — верни ровно один ref_key. Если не уверен или внешний человек — верни строку NONE.
Ответ — только ref_key или NONE, без объяснений."""
    try:
        r = requests.post(
            f"{ROUTER_AI_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTER_AI_KEY}"},
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
                "max_tokens": 64,
            },
            timeout=60,
        )
        data = r.json()
        ans = data["choices"][0]["message"]["content"].strip()
        if ans == "NONE" or len(ans) < 30:
            return None
        # Validate it looks like UUID
        if re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", ans):
            return ans
        return None
    except Exception as e:
        log(f"  LLM error: {e}")
        return None


def best_match(query_norm: str, candidates: dict[str, list[str]],
               threshold: int = 85) -> list[tuple[str, int]]:
    """candidates: norm_name → list of ref_keys. Returns [(ref_key, score), ...] sorted."""
    if not query_norm or not candidates:
        return []
    matches = process.extract(query_norm, candidates.keys(), scorer=fuzz.token_set_ratio,
                              limit=5, score_cutoff=threshold)
    out = []
    for cand_norm, score, _ in matches:
        for rk in candidates[cand_norm]:
            out.append((rk, int(score)))
    return out


# ─────────────────────────────────────────────────────────────────────
#  KG operations
# ─────────────────────────────────────────────────────────────────────

def upsert_person_entity(conn, employee_ref_key: str, name: str, aliases: list[str],
                         attrs: dict, dry: bool) -> int | None:
    """Создаёт/обновляет km_entities (entity_type='person', source_ref=employee_ref_key)."""
    aliases_clean = sorted(set(a.strip() for a in aliases if a and a.strip()))
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, aliases, attrs FROM km_entities "
            "WHERE entity_type='person' AND source_ref = %s",
            (employee_ref_key,),
        )
        row = cur.fetchone()
        if row:
            ent_id, existing_aliases, existing_attrs = row
            new_aliases = sorted(set((existing_aliases or []) + aliases_clean))
            merged_attrs = dict(existing_attrs or {})
            for k, v in attrs.items():
                if isinstance(v, list):
                    merged_attrs[k] = sorted(set((merged_attrs.get(k) or []) + v))
                else:
                    if v is not None:
                        merged_attrs[k] = v
            if dry:
                return ent_id
            cur.execute(
                "UPDATE km_entities SET aliases=%s, attrs=%s, updated_at=NOW(), "
                "canonical_name = COALESCE(NULLIF(canonical_name,''), %s) "
                "WHERE id=%s",
                (new_aliases, json.dumps(merged_attrs, ensure_ascii=False), name, ent_id),
            )
            return ent_id
        else:
            if dry:
                return -1
            cur.execute(
                "INSERT INTO km_entities (entity_type, canonical_name, source_ref, aliases, attrs) "
                "VALUES ('person', %s, %s, %s, %s::jsonb) RETURNING id",
                (name, employee_ref_key, aliases_clean,
                 json.dumps(attrs, ensure_ascii=False)),
            )
            return cur.fetchone()[0]


# ─────────────────────────────────────────────────────────────────────
#  Main flow
# ─────────────────────────────────────────────────────────────────────

def main(dry: bool = False) -> int:
    log(f"populate_identities (dry_run={dry})")
    conn = conn_db()

    # ── load all sources
    employees = load_employees(conn)
    users_1c = load_users_1c(conn)
    comm_us = load_comm_users(conn)
    tg_roles = load_tg_user_roles(conn)
    emails = load_email_mapping(conn)
    log(f"sources: employees={len(employees)} users_1c={len(users_1c)} "
        f"comm_users={len(comm_us)} tg_roles={len(tg_roles)} emails={len(emails)}")

    # ── build candidate index по нормализованному ФИО
    emp_idx: dict[str, list[str]] = {}      # norm → [employee_ref_key]
    emp_by_ref: dict[str, dict] = {}
    for e in employees:
        n = normalize_name(e["name"])
        if not n: continue
        emp_idx.setdefault(n, []).append(e["ref_key"])
        emp_by_ref[e["ref_key"]] = e

    log(f"normalized employee index: {len(emp_idx)} unique names ({len(employees)} rows)")

    # ── Pass 1: создать/проверить km_entity per employee
    log("Pass 1: km_entities per c1_employees...")
    created = 0
    updated = 0
    for e in employees:
        ent_id = upsert_person_entity(
            conn, e["ref_key"], e["name"], [e["name"]],
            {"employee_ref_key": e["ref_key"],
             "organization_key": e["organization_key"],
             "is_archived": e["is_archived"]},
            dry,
        )
        if ent_id == -1:
            created += 1
        else:
            updated += 1
    log(f"  created={created} updated={updated}")
    if not dry: conn.commit()

    # ── Pass 2: c1_users → match → update attrs
    log("Pass 2: c1_users → match → attrs.c1_user_ref_key...")
    matched_u = 0
    ambiguous_u = 0
    nomatch_u = 0
    for u in users_1c:
        n = normalize_name(u["description"])
        if not n:
            nomatch_u += 1; continue
        cands = best_match(n, emp_idx, threshold=88)
        if not cands:
            nomatch_u += 1; continue
        # Если top score >= 92 — берём top
        if cands[0][1] >= 92:
            emp_rk = cands[0][0]
            matched_u += 1
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE km_entities SET attrs = jsonb_set(COALESCE(attrs,'{}'::jsonb), "
                    "'{c1_user_ref_key}', to_jsonb(%s::text)), "
                    "aliases = (SELECT array_agg(DISTINCT a) FROM unnest(COALESCE(aliases,'{}') || %s::text[]) a), "
                    "updated_at = NOW() "
                    "WHERE entity_type='person' AND source_ref=%s",
                    (u["ref_key"], [u["description"]], emp_rk),
                )
        else:
            # Ambiguous — top match есть, но score не уверенный или multiple equals
            ambiguous_u += 1
    log(f"  matched={matched_u} ambiguous={ambiguous_u} nomatch={nomatch_u}")
    if not dry: conn.commit()

    # ── Pass 3: comm_users (TG) → match → comm_users.km_entity_id + employee_ref_key
    log("Pass 3: comm_users (TG) → match → comm_users.km_entity_id...")
    matched_t = 0
    ambig_t = 0
    nomatch_t = 0
    for cu in comm_us:
        # Compose name from comm_users.display_name + tg_user_roles
        candidate_names = []
        if cu.get("display_name"):
            candidate_names.append(cu["display_name"])
        tr = tg_roles.get(cu["tg_user_id"])
        if tr:
            full = " ".join(filter(None, [tr.get("first_name"), tr.get("last_name")]))
            if full.strip():
                candidate_names.append(full)
        if not candidate_names:
            nomatch_t += 1; continue
        # Try each candidate; pick best
        best = None
        for name in candidate_names:
            n = normalize_name(name)
            if not n: continue
            cands = best_match(n, emp_idx, threshold=85)
            if cands and (best is None or cands[0][1] > best[1]):
                best = (cands[0][0], cands[0][1], name)
        if not best or best[1] < 90:
            if best:
                ambig_t += 1
            else:
                nomatch_t += 1
            continue
        emp_rk = best[0]
        # Set comm_users.km_entity_id + employee_ref_key, and add aliases
        if not dry:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM km_entities WHERE entity_type='person' AND source_ref=%s",
                            (emp_rk,))
                row = cur.fetchone()
                if not row: continue
                ent_id = row[0]
                cur.execute(
                    "UPDATE comm_users SET km_entity_id=%s, employee_ref_key=%s "
                    "WHERE tg_user_id=%s",
                    (ent_id, emp_rk, cu["tg_user_id"]),
                )
                # Append tg_user_id to attrs.tg_user_ids and aliases
                new_aliases = candidate_names
                cur.execute(
                    "UPDATE km_entities SET "
                    "attrs = jsonb_set("
                    "  COALESCE(attrs,'{}'::jsonb), "
                    "  '{tg_user_ids}', "
                    "  ((SELECT COALESCE(jsonb_agg(DISTINCT v), '[]'::jsonb) "
                    "      FROM jsonb_array_elements(COALESCE(attrs->'tg_user_ids','[]'::jsonb) || to_jsonb(%s::bigint)) v))"
                    "), "
                    "aliases = (SELECT array_agg(DISTINCT a) FROM unnest(COALESCE(aliases,'{}') || %s::text[]) a), "
                    "updated_at=NOW() "
                    "WHERE id=%s",
                    (cu["tg_user_id"], new_aliases, ent_id),
                )
        matched_t += 1
    log(f"  matched={matched_t} ambig={ambig_t} nomatch={nomatch_t}")
    if not dry: conn.commit()

    # ── Pass 3.5: LLM-добор для нематченных TG юзеров (опционально, требует ROUTER_AI_KEY)
    if ROUTER_AI_KEY:
        log("Pass 3.5: LLM-match для нематченных TG-юзеров...")
        # Только активные сотрудники как кандидаты
        active_emps = [e for e in employees if not e.get("is_archived")]
        llm_matched = 0
        llm_skipped = 0
        for cu in comm_us:
            # Skip уже сматченные
            with conn.cursor() as cur:
                cur.execute("SELECT km_entity_id FROM comm_users WHERE tg_user_id=%s",
                            (cu["tg_user_id"],))
                row = cur.fetchone()
                if row and row[0]:
                    continue
            # Skip явно служебные
            if cu["tg_user_id"] in (777000,) or (cu.get("display_name") or "").startswith(("Imported", "Group")):
                continue
            tr = tg_roles.get(cu["tg_user_id"], {})
            tg_info = {
                "display_name": cu.get("display_name"),
                "username": cu.get("username"),
                "first_name": tr.get("first_name"),
                "last_name": tr.get("last_name"),
                "roles": tr.get("roles"),
            }
            ref = llm_match_tg_to_employee(tg_info, active_emps)
            if not ref:
                llm_skipped += 1
                continue
            log(f"  LLM: {cu['display_name']!r} → {ref}")
            if not dry:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM km_entities WHERE entity_type='person' AND source_ref=%s",
                                (ref,))
                    row = cur.fetchone()
                    if not row:
                        continue
                    ent_id = row[0]
                    cur.execute(
                        "UPDATE comm_users SET km_entity_id=%s, employee_ref_key=%s "
                        "WHERE tg_user_id=%s",
                        (ent_id, ref, cu["tg_user_id"]),
                    )
                    aliases = list(filter(None, [cu.get("display_name"), cu.get("username"),
                                                  f"{tr.get('first_name','')} {tr.get('last_name','')}".strip()]))
                    cur.execute(
                        "UPDATE km_entities SET "
                        "attrs = jsonb_set("
                        "  COALESCE(attrs,'{}'::jsonb), '{tg_user_ids}', "
                        "  COALESCE(attrs->'tg_user_ids','[]'::jsonb) || to_jsonb(%s::bigint)"
                        "), "
                        "aliases = (SELECT array_agg(DISTINCT a) FROM unnest(COALESCE(aliases,'{}') || %s::text[]) a), "
                        "updated_at=NOW() "
                        "WHERE id=%s "
                        "AND NOT (COALESCE(attrs->'tg_user_ids','[]'::jsonb) @> to_jsonb(%s::bigint))",
                        (cu["tg_user_id"], aliases, ent_id, cu["tg_user_id"]),
                    )
            llm_matched += 1
        log(f"  LLM matched={llm_matched} skipped={llm_skipped}")
        if not dry: conn.commit()
    else:
        log("Pass 3.5 SKIPPED — no ROUTER_AI_KEY")

    # ── Pass 4: email_employee_mapping → подтянуть tg_user_id и c1_employee_key
    log("Pass 4: email_employee_mapping → fill missing tg_user_id / c1_employee_key...")
    filled_tg = 0
    filled_emp = 0
    for em in emails:
        if em.get("tg_user_id") and em.get("c1_employee_key"):
            continue
        # Match by employee_name_1c first, fallback to employee_name
        n = normalize_name(em.get("employee_name_1c") or em.get("employee_name") or "")
        if not n: continue
        cands = best_match(n, emp_idx, threshold=88)
        if not cands or cands[0][1] < 92: continue
        emp_rk = cands[0][0]
        # Get km_entity for that employee → its tg_user_ids
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, attrs FROM km_entities "
                "WHERE entity_type='person' AND source_ref=%s",
                (emp_rk,),
            )
            row = cur.fetchone()
            if not row: continue
            ent_id, attrs = row
            tg_ids = (attrs or {}).get("tg_user_ids", [])
            new_tg = tg_ids[0] if tg_ids and not em.get("tg_user_id") else None
            new_emp = emp_rk if not em.get("c1_employee_key") else None
            if not (new_tg or new_emp): continue
            if not dry:
                sets = []
                vals = []
                if new_tg:
                    sets.append("tg_user_id=%s"); vals.append(new_tg); filled_tg += 1
                if new_emp:
                    sets.append("c1_employee_key=%s"); vals.append(new_emp); filled_emp += 1
                vals.append(em["id"])
                cur.execute(f"UPDATE email_employee_mapping SET {', '.join(sets)}, "
                            f"updated_at=NOW() WHERE id=%s", vals)
                # also store email in km_entity attrs
                cur.execute(
                    "UPDATE km_entities SET "
                    "attrs = jsonb_set("
                    "  COALESCE(attrs,'{}'::jsonb), '{emails}', "
                    "  COALESCE(attrs->'emails','[]'::jsonb) || to_jsonb(%s::text)"
                    "), updated_at=NOW() "
                    "WHERE id=%s "
                    "AND NOT (COALESCE(attrs->'emails','[]'::jsonb) ? %s)",
                    (em["email"], ent_id, em["email"]),
                )
            else:
                if new_tg: filled_tg += 1
                if new_emp: filled_emp += 1
    log(f"  filled tg_user_id={filled_tg} c1_employee_key={filled_emp}")
    if not dry: conn.commit()

    # ── Pass 5: report
    log("Pass 5: coverage report")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM km_entities WHERE entity_type='person'")
        n_persons = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM km_entities WHERE entity_type='person' "
                    "AND attrs ? 'c1_user_ref_key'")
        n_with_user = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM km_entities WHERE entity_type='person' "
                    "AND jsonb_array_length(COALESCE(attrs->'tg_user_ids','[]'::jsonb)) > 0")
        n_with_tg = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM km_entities WHERE entity_type='person' "
                    "AND jsonb_array_length(COALESCE(attrs->'emails','[]'::jsonb)) > 0")
        n_with_email = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*), COUNT(km_entity_id), COUNT(employee_ref_key) FROM comm_users")
        cu_total, cu_kg, cu_emp = cur.fetchone()
        cur.execute("SELECT COUNT(*), COUNT(tg_user_id), COUNT(c1_employee_key) "
                    "FROM email_employee_mapping WHERE is_active")
        em_total, em_tg, em_emp = cur.fetchone()
    log(f"  km_entities person={n_persons}, with c1_user={n_with_user}, "
        f"with tg={n_with_tg}, with email={n_with_email}")
    log(f"  comm_users: {cu_total} total, {cu_kg} with km_entity_id, {cu_emp} with employee_ref_key")
    log(f"  email_employee_mapping: {em_total} active, {em_tg} with tg_user_id, {em_emp} with c1_employee_key")

    conn.close()
    log("done.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    sys.exit(main(dry=args.dry_run))
