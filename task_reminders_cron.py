#!/usr/bin/env python3
"""Phase 3 — cron-эскалации напоминаний по km_tasks.

Уровни (в днях от created_at, для не-resolved/declined):
  L0 (initial DM)        — сразу при создании, если ещё не отправляли
  L1 (повторный DM)      — через 2+ дня без реакции
  L2 (тег в общем чате)  — через 5+ дней без реакции

AI-текст: RouterAI claude-opus-4.7. Промпт зависит от уровня (нейтрально / жёстче).

skip-условия:
  - status != 'open' → пропускаем
  - snoozed_until > now() → пропускаем
  - assignee_tg_user_id NULL → пропускаем (некому DM)

Запуск:
  python3 task_reminders_cron.py             # обычный прогон
  python3 task_reminders_cron.py --dry-run   # без отправки

Cron (рекомендация): каждый час в рабочее время.
  0 9-19 * * 1-5 python3 task_reminders_cron.py >> /home/admin/knowledge-base/task_reminders.log 2>&1
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from datetime import datetime, timedelta

import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

REPO = pathlib.Path(__file__).resolve().parent
load_dotenv(REPO / ".env")

DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASS = os.getenv("DB_PASSWORD")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ROUTER_AI_URL = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")
ROUTER_AI_KEY = os.getenv("ROUTERAI_API_KEY")
LLM_MODEL = "anthropic/claude-opus-4.7"

L1_DAYS = 2
L2_DAYS = 5


def log(m: str): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)


def conn_db():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


# ─────────────────────────────────────────────────────────────────────
#  AI text generation
# ─────────────────────────────────────────────────────────────────────

LEVEL_TONE = {
    0: "Дружелюбный, информативный. «Создал задачу: ...».",
    1: "Напоминающий, чуть настойчивый. «Прошло 2 дня, не забудь...».",
    2: "Публичный, уважительный, но прямой. «Уже 5 дней висит, требуется ваше внимание». В групповом чате.",
}


def generate_reminder_text(task: dict, level: int) -> str:
    """AI-текст напоминания. Возврат — готовая строка для Telegram."""
    if not ROUTER_AI_KEY:
        return _fallback_text(task, level)
    ctx = task.get("context_data") or {}
    if isinstance(ctx, str):
        ctx = json.loads(ctx)
    days_open = (datetime.now() - task["created_at"]).days
    prompt = f"""Сгенерируй короткое (3-5 строк) напоминание о задаче для Telegram.

Задача: {task.get('title')}
Описание: {task.get('task_text')}
Поставщик: {ctx.get('partner_name')}
Сумма расхождения: {ctx.get('gap_amount')} ₽
Период: {ctx.get('period_from')}…{ctx.get('period_to')}
Уровень эскалации: L{level} ({LEVEL_TONE.get(level)})
Открыта дней: {days_open}

Требования:
- Текст на русском.
- Без воды и фольклора, по делу.
- Не повторяй «задача», «уважаемый коллега» и т.п. шаблоны.
- HTML-разметка Telegram допустима (<b>, <i>, <code>).
- В конце — короткий призыв (открыть /tasks, ответить и т.п.).
- НЕ упоминай слова «бот», «AI», «искусственный интеллект».
"""
    try:
        r = requests.post(
            f"{ROUTER_AI_URL}/chat/completions",
            headers={"Authorization": f"Bearer {ROUTER_AI_KEY}"},
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.4,
                "max_tokens": 300,
            },
            timeout=60,
        )
        text = r.json()["choices"][0]["message"]["content"].strip()
        return text or _fallback_text(task, level)
    except Exception as e:
        log(f"  LLM err: {e}")
        return _fallback_text(task, level)


def _fallback_text(task: dict, level: int) -> str:
    ctx = task.get("context_data") or {}
    if isinstance(ctx, str): ctx = json.loads(ctx)
    days = (datetime.now() - task["created_at"]).days
    intro = {
        0: "Новая задача:",
        1: f"⏰ Прошло {days} дн., задача не закрыта:",
        2: f"⚠ {days} дн. без действий по задаче:",
    }.get(level, "")
    return (
        f"{intro}\n\n"
        f"<b>{task.get('title') or 'Задача'}</b>\n"
        f"Поставщик: {ctx.get('partner_name')}\n"
        f"Расхождение: {float(ctx.get('gap_amount', 0)):,.0f} ₽\n\n"
        f"Открой /tasks для деталей и ответа."
    ).replace(",", " ")


# ─────────────────────────────────────────────────────────────────────
#  Telegram send
# ─────────────────────────────────────────────────────────────────────

def tg_send(chat_id: int, text: str, parse_mode: str = "HTML") -> dict | None:
    if not BOT_TOKEN:
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": chat_id, "text": text, "parse_mode": parse_mode,
                  "disable_web_page_preview": "true"},
            timeout=15,
        )
        d = r.json()
        if not d.get("ok"):
            log(f"  tg send err: {d}")
            return None
        return d.get("result")
    except Exception as e:
        log(f"  tg send err: {e}")
        return None


def find_group_chat_for_user(conn, tg_user_id: int) -> tuple[int, str] | None:
    """Любой деловой групповой чат, где есть assignee. (kind=1: для L2 тега)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tr.chat_id, COUNT(*) OVER (PARTITION BY tr.chat_id) AS chat_size
            FROM tg_user_roles tr
            WHERE tr.user_id = %s AND tr.is_active
              AND tr.chat_id < 0  -- group chats имеют отрицательный id
            ORDER BY tr.chat_id LIMIT 1
        """, (tg_user_id,))
        row = cur.fetchone()
        if not row:
            return None
        chat_id, _ = row
        cur.execute("SELECT MAX(first_name||' '||COALESCE(last_name,''))::text "
                    "FROM tg_user_roles WHERE user_id=%s AND is_active LIMIT 1",
                    (tg_user_id,))
        return chat_id, cur.fetchone()[0] or ""


def get_username(conn, tg_user_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT username FROM tg_user_roles WHERE user_id=%s "
                    "  AND username IS NOT NULL LIMIT 1", (tg_user_id,))
        r = cur.fetchone()
        return r[0] if r else None


# ─────────────────────────────────────────────────────────────────────
#  Reminder logic
# ─────────────────────────────────────────────────────────────────────

def fetch_open_tasks(conn) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT * FROM km_tasks
            WHERE status = 'open'
              AND kind != 'extracted_from_text'
              AND assignee_tg_user_id IS NOT NULL
              AND (snoozed_until IS NULL OR snoozed_until <= NOW())
            ORDER BY created_at ASC
        """)
        return [dict(r) for r in cur.fetchall()]


def log_reminder(conn, task_id: int, level: int, channel: str, chat_id: int,
                 message_id: int | None, text: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO task_reminders (task_id, level, channel, chat_id, message_id, text)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (task_id, level, channel, chat_id, message_id, text))


def update_task(conn, task_id: int, escalation_level: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE km_tasks SET escalation_level=%s, last_reminder_at=NOW(), "
            "updated_at=NOW() WHERE id=%s",
            (escalation_level, task_id),
        )


def compute_target_level(task: dict) -> int | None:
    """Какой уровень напоминания нужно отправить сейчас? None — ничего не нужно."""
    days_open = (datetime.now() - task["created_at"]).days
    cur_level = task.get("escalation_level") or 0
    last_reminder = task.get("last_reminder_at")

    # Initial L0 — если ещё не отправляли
    if cur_level == 0 and last_reminder is None:
        return 0
    # Повтор L0 не делаем — после initial ждём 2 дня → L1
    if days_open >= L2_DAYS and cur_level < 2:
        return 2
    if days_open >= L1_DAYS and cur_level < 1:
        return 1
    return None


def process(conn, dry: bool) -> tuple[int, int, int]:
    tasks = fetch_open_tasks(conn)
    log(f"  open tasks for assignees: {len(tasks)}")
    sent = {0: 0, 1: 0, 2: 0}
    # Правило: одна задача на пользователя за прогон. Сначала старшая по
    # priority/escalation, потом остальные на следующих прогонах.
    sent_to_users: set[int] = set()
    # Сортировка для приоритезации: priority DESC, days_open DESC, created_at ASC
    tasks.sort(
        key=lambda t: (-(t.get("priority") or 1),
                       -(datetime.now() - t["created_at"]).days,
                       t["created_at"]),
    )
    for task in tasks:
        tg = task["assignee_tg_user_id"]
        if tg in sent_to_users:
            continue  # одна за прогон на user
        level = compute_target_level(task)
        if level is None:
            continue
        text = generate_reminder_text(task, level)

        if level <= 1:
            # DM
            tg_id = task["assignee_tg_user_id"]
            log(f"  task#{task['id']} L{level} DM → tg={tg_id}")
            if dry:
                sent[level] += 1
                sent_to_users.add(tg)
                continue
            res = tg_send(tg_id, text)
            if res:
                log_reminder(conn, task["id"], level, "dm", tg_id,
                             res.get("message_id"), text)
                update_task(conn, task["id"], level)
                sent[level] += 1
                sent_to_users.add(tg)
        else:
            # L2 — пост в групповой чат
            target = find_group_chat_for_user(conn, tg)
            if not target:
                log(f"  task#{task['id']} L2 — нет групп. чата для tg={tg}")
                continue
            chat_id, name = target
            uname = get_username(conn, task["assignee_tg_user_id"])
            mention = f"@{uname}" if uname else (name or "коллега")
            full_text = f"{mention}, {text}"
            log(f"  task#{task['id']} L2 group → chat={chat_id} mention={mention}")
            if dry:
                sent[level] += 1
                sent_to_users.add(tg)
                continue
            res = tg_send(chat_id, full_text)
            if res:
                log_reminder(conn, task["id"], level, "group", chat_id,
                             res.get("message_id"), full_text)
                update_task(conn, task["id"], level)
                sent[level] += 1
                sent_to_users.add(tg)
    if not dry:
        conn.commit()
    return sent[0], sent[1], sent[2]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    log(f"task_reminders_cron (dry_run={args.dry_run})")
    conn = conn_db()
    try:
        l0, l1, l2 = process(conn, args.dry_run)
        log(f"sent: L0={l0} L1={l1} L2={l2}")
    finally:
        conn.close()
    log("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
