#!/usr/bin/env python3
"""Конвертер: payment_audit_gaps → km_tasks (Phase 2).

Расширяет km_tasks дополнительными колонками для structured-задач
(kind, context_data, snooze, decline, escalation, tg-привязка) и
конвертирует открытые gap'ы в задачи (1 gap = 1 task).

Отдельная таблица task_reminders — лог отправленных напоминаний
+ ответов пользователя.

Запуск:
  python3 payment_audit_to_tasks.py --alter-only
  python3 payment_audit_to_tasks.py             # конвертирует open gaps в tasks
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

REPO = pathlib.Path(__file__).resolve().parent
load_dotenv(REPO / ".env")

DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASS = os.getenv("DB_PASSWORD")


def log(m: str): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)


DDL = [
    # km_tasks: добавляем поля для structured tasks
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS kind VARCHAR(50) DEFAULT 'extracted_from_text'",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS context_data JSONB",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS source_table VARCHAR(50)",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS source_id INTEGER",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS title TEXT",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS assignee_tg_user_id BIGINT",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS snoozed_until TIMESTAMP",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS declined_at TIMESTAMP",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS decline_reason TEXT",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS escalation_level INTEGER DEFAULT 0",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS last_reminder_at TIMESTAMP",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS transferred_from_entity_id INTEGER",
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS transferred_at TIMESTAMP",
    "CREATE INDEX IF NOT EXISTS idx_km_tasks_kind ON km_tasks(kind)",
    "CREATE INDEX IF NOT EXISTS idx_km_tasks_tg ON km_tasks(assignee_tg_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_km_tasks_source ON km_tasks(source_table, source_id)",
    "CREATE INDEX IF NOT EXISTS idx_km_tasks_snooze ON km_tasks(snoozed_until)",

    # Лог напоминаний и ответов
    """
    CREATE TABLE IF NOT EXISTS task_reminders (
        id           SERIAL PRIMARY KEY,
        task_id      INTEGER NOT NULL REFERENCES km_tasks(id) ON DELETE CASCADE,
        level        INTEGER NOT NULL,
        channel      VARCHAR(20) NOT NULL,
        chat_id      BIGINT,
        message_id   BIGINT,
        text         TEXT,
        sent_at      TIMESTAMP DEFAULT NOW(),
        user_response VARCHAR(50),
        responded_at TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_treminders_task ON task_reminders(task_id)",
    "CREATE INDEX IF NOT EXISTS idx_treminders_sent ON task_reminders(sent_at DESC)",
]


def ensure_schema(conn):
    log("ensure schema...")
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()
    log("  ok.")


def convert_gaps_to_tasks(conn) -> int:
    """Создаёт km_tasks из открытых payment_audit_gaps (без существующих)."""
    log("convert payment_audit_gaps → km_tasks...")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT g.*
            FROM payment_audit_gaps g
            WHERE g.status = 'open'
              AND NOT EXISTS (
                  SELECT 1 FROM km_tasks t
                  WHERE t.source_table = 'payment_audit_gaps' AND t.source_id = g.id
              )
        """)
        gaps = cur.fetchall()
    log(f"  open gaps without tasks: {len(gaps)}")
    if not gaps:
        return 0

    created = 0
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for g in gaps:
            tg_ids = g.get("assignee_tg_user_ids")
            if isinstance(tg_ids, str):
                tg_ids = json.loads(tg_ids)
            primary_tg = tg_ids[0] if tg_ids else None
            if not primary_tg:
                # без TG-привязки задачу не делаем (некому DM-ить)
                continue

            ctx = {
                "partner_name": g["partner_name"],
                "organization_name": g["organization_name"],
                "gap_amount": float(g["gap_amount"]),
                "total_paid": float(g["total_paid"]),
                "total_acquired": float(g["total_acquired"]),
                "period_from": g["period_from"].isoformat() if g["period_from"] else None,
                "period_to": g["period_to"].isoformat() if g["period_to"] else None,
                "case_type": g["case_type"],
                "supplier_order_number": g.get("supplier_order_number"),
                "supplier_order_desired_arrival": (
                    g["supplier_order_desired_arrival"].isoformat()
                    if g.get("supplier_order_desired_arrival") else None
                ),
            }
            title = (
                f"Платёж > приёмки: {g['partner_name'] or '(без партнёра)'} "
                f"({g['organization_name']})"
            )
            task_text = (
                f"По партнёру «{g['partner_name']}» ({g['organization_name']}) "
                f"за период {g['period_from']}…{g['period_to']} "
                f"оплачено {float(g['total_paid']):.2f} ₽, "
                f"но принято документами только {float(g['total_acquired']):.2f} ₽. "
                f"Расхождение {float(g['gap_amount']):.2f} ₽."
            )
            cur.execute("""
                INSERT INTO km_tasks
                  (kind, source_table, source_id,
                   title, task_text, context_data,
                   assignee_entity_id, assignee_tg_user_id,
                   status, verification_status, confidence, created_at, updated_at)
                VALUES ('payment_no_acquisition', 'payment_audit_gaps', %s,
                        %s, %s, %s::jsonb,
                        %s, %s,
                        'open', 'auto', 1.0, NOW(), NOW())
                RETURNING id
            """, (
                g["id"], title, task_text, json.dumps(ctx, ensure_ascii=False),
                g["assignee_km_entity_id"], primary_tg,
            ))
            new_task_id = cur.fetchone()["id"]
            # отметить gap как переведённый в task
            cur.execute(
                "UPDATE payment_audit_gaps SET status='task_created', "
                "notes = COALESCE(notes,'') || E'\\nкonvертирован в km_tasks#' || %s "
                "WHERE id=%s",
                (str(new_task_id), g["id"]),
            )
            created += 1
    conn.commit()
    log(f"  created tasks: {created}")
    return created


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--alter-only", action="store_true")
    args = p.parse_args()

    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    try:
        ensure_schema(conn)
        if args.alter_only:
            log("alter-only → done.")
            return 0
        convert_gaps_to_tasks(conn)
    finally:
        conn.close()
    log("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
