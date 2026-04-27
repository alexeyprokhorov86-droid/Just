#!/usr/bin/env python3
"""Расширение km_tasks: priority, result_text + audit-trail таблица task_history.

Создаёт триггер на UPDATE km_tasks, который записывает diff (status, assignee,
deadline, snoozed_until, decline_reason, transferred_*) в task_history.

Запуск:
  python3 extend_km_tasks_fields.py
"""
from __future__ import annotations

import os
import pathlib
import sys
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

REPO = pathlib.Path(__file__).resolve().parent
load_dotenv(REPO / ".env")

DDL = [
    # priority: 0=low, 1=normal (default), 2=high, 3=critical
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS priority SMALLINT DEFAULT 1",
    # текст что сделано (заполняется при ✅ Сделано)
    "ALTER TABLE km_tasks ADD COLUMN IF NOT EXISTS result_text TEXT",
    # индексы
    "CREATE INDEX IF NOT EXISTS idx_km_tasks_priority ON km_tasks(priority DESC, deadline ASC NULLS LAST)",

    # task_history — audit trail
    """
    CREATE TABLE IF NOT EXISTS task_history (
        id              SERIAL PRIMARY KEY,
        task_id         INTEGER NOT NULL REFERENCES km_tasks(id) ON DELETE CASCADE,
        changed_at      TIMESTAMP DEFAULT NOW(),
        changed_by_tg   BIGINT,
        changed_by_entity_id INTEGER,
        field_name      VARCHAR(100) NOT NULL,
        old_value       TEXT,
        new_value       TEXT,
        comment         TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_task_history_task ON task_history(task_id, changed_at DESC)",

    # Trigger function — записывает diff
    """
    CREATE OR REPLACE FUNCTION fn_km_tasks_audit() RETURNS TRIGGER AS $$
    BEGIN
        IF NEW.status IS DISTINCT FROM OLD.status THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'status', OLD.status::text, NEW.status::text);
        END IF;
        IF NEW.assignee_entity_id IS DISTINCT FROM OLD.assignee_entity_id THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'assignee_entity_id',
                    OLD.assignee_entity_id::text, NEW.assignee_entity_id::text);
        END IF;
        IF NEW.assignee_tg_user_id IS DISTINCT FROM OLD.assignee_tg_user_id THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'assignee_tg_user_id',
                    OLD.assignee_tg_user_id::text, NEW.assignee_tg_user_id::text);
        END IF;
        IF NEW.deadline IS DISTINCT FROM OLD.deadline THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'deadline', OLD.deadline::text, NEW.deadline::text);
        END IF;
        IF NEW.snoozed_until IS DISTINCT FROM OLD.snoozed_until THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'snoozed_until',
                    OLD.snoozed_until::text, NEW.snoozed_until::text);
        END IF;
        IF NEW.decline_reason IS DISTINCT FROM OLD.decline_reason THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'decline_reason',
                    OLD.decline_reason, NEW.decline_reason);
        END IF;
        IF NEW.priority IS DISTINCT FROM OLD.priority THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'priority', OLD.priority::text, NEW.priority::text);
        END IF;
        IF NEW.result_text IS DISTINCT FROM OLD.result_text THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'result_text', OLD.result_text, NEW.result_text);
        END IF;
        IF NEW.escalation_level IS DISTINCT FROM OLD.escalation_level THEN
            INSERT INTO task_history (task_id, field_name, old_value, new_value)
            VALUES (NEW.id, 'escalation_level',
                    OLD.escalation_level::text, NEW.escalation_level::text);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """,
    "DROP TRIGGER IF EXISTS trg_km_tasks_audit ON km_tasks",
    """
    CREATE TRIGGER trg_km_tasks_audit
    AFTER UPDATE ON km_tasks
    FOR EACH ROW EXECUTE FUNCTION fn_km_tasks_audit()
    """,
]


def main():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "172.20.0.2"),
        dbname=os.getenv("DB_NAME", "knowledge_base"),
        user=os.getenv("DB_USER", "knowledge"),
        password=os.getenv("DB_PASSWORD"),
    )
    print(f"[{datetime.now().strftime('%H:%M:%S')}] DDL...")
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()
    print("  ok.")

    # Backfill priority for payment_no_acquisition based on gap amount
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE km_tasks SET priority = CASE
                WHEN (context_data->>'gap_amount')::numeric >= 1000000 THEN 3
                WHEN (context_data->>'gap_amount')::numeric >= 200000  THEN 2
                WHEN (context_data->>'gap_amount')::numeric >= 50000   THEN 1
                ELSE 0
            END
            WHERE kind = 'payment_no_acquisition' AND priority = 1
        """)
        print(f"  backfilled priority on {cur.rowcount} payment_no_acquisition tasks")
    conn.commit()
    conn.close()
    print("done.")


if __name__ == "__main__":
    sys.exit(main())
