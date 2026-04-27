#!/usr/bin/env python3
"""Phase 1 — детектор расхождений «оплачено vs принято».

За окно (--days N, по умолчанию 30) для каждой пары (организация × контрагент)
сравнивает сумму платежей (c1_bank_expenses, bank_date в окне) с суммой
приобретений (c1_purchases + c1_purchases_other_assets + c1_advance_report_other_expenses,
doc_date в окне). Если оплачено > принято → пишет gap в таблицу payment_audit_gaps.

Атрибуция case:
  case_with_order — если у любого из «лишних» платежей есть basis_doc_type=ЗаказПоставщику
                    или BSG_Заказ непуст. Тянем заказ, ответственный = автор заказа.
                    Если desired_arrival_date < today → флаг overdue.
  case_no_order   — иначе. Ответственный = автор Списания (bank_expense.author_key).
                    Если автор = bot — пропускаем (бот сам всё контролирует).

Скрипт пока ТОЛЬКО детектирует и пишет в таблицу. Tasks/reminders — Phase 2.

Запуск:
  python3 payment_audit.py --days 30          # окно
  python3 payment_audit.py --alter-only       # только DDL
  python3 payment_audit.py --dry-run          # показать без записи
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

REPO = pathlib.Path(__file__).resolve().parent
load_dotenv(REPO / ".env")

DB_HOST = os.getenv("DB_HOST", "172.20.0.2")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASS = os.getenv("DB_PASSWORD")

# Какие операции c1_bank_expenses считать «оплатой поставщику»
# (зарплаты/налоги/кредиты — не сюда)
TRACKED_OPERATIONS = (
    'ОплатаПоставщику',
    'ОплатаАрендодателю',
    'ОплатаДенежныхСредствВДругуюОрганизацию',
    'ВозвратОплатыКлиенту',
)

# Внутригрупповые контрагенты — игнорируем (intercompany)
INTERNAL_PARTNERS = (
    '9812504d-2293-11ee-8e18-000c299cc968',  # НОВЭЛ ФУД ООО
    '64e2f4c5-5441-11ec-bf20-000c29247c35',  # НФ ООО
    '57c4d522-a14b-11e4-80c8-005056a80686',  # НЬЮ БРЕНДС ООО
    '220d7948-25e2-11ec-bf1e-000c29247c35',  # ФРУМЕЛАД ООО
    '0ed451c6-81cc-11ec-bf26-000c29247c35',  # Прохоров А.Е. ИП
    'de579663-81cc-11ec-bf26-000c29247c35',  # Прохорова И.В. ИП
)


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def conn_db():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)


DDL = [
    """
    CREATE TABLE IF NOT EXISTS payment_audit_gaps (
        id                     SERIAL PRIMARY KEY,
        detected_at            TIMESTAMP DEFAULT NOW(),
        period_from            DATE NOT NULL,
        period_to              DATE NOT NULL,
        organization_key       VARCHAR(50) NOT NULL,
        organization_name      VARCHAR(200),
        partner_key            VARCHAR(50),
        partner_name           VARCHAR(300),
        total_paid             NUMERIC(15,2) NOT NULL,
        total_acquired         NUMERIC(15,2) NOT NULL,
        gap_amount             NUMERIC(15,2) NOT NULL,
        case_type              VARCHAR(50) NOT NULL,
        supplier_order_ref     VARCHAR(50),
        supplier_order_number  VARCHAR(50),
        supplier_order_date    DATE,
        supplier_order_author_key VARCHAR(50),
        supplier_order_desired_arrival DATE,
        is_overdue             BOOLEAN,
        sample_payment_ref     VARCHAR(50),
        sample_payment_author_key VARCHAR(50),
        assignee_user_ref_key  VARCHAR(50),
        assignee_employee_ref_key VARCHAR(50),
        assignee_km_entity_id  BIGINT,
        assignee_tg_user_ids   JSONB,
        assignee_name          VARCHAR(300),
        status                 VARCHAR(30) DEFAULT 'open',
        notes                  TEXT,
        UNIQUE (period_from, period_to, organization_key, partner_key, case_type)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pag_status ON payment_audit_gaps(status)",
    "CREATE INDEX IF NOT EXISTS idx_pag_assignee ON payment_audit_gaps(assignee_employee_ref_key)",
    "CREATE INDEX IF NOT EXISTS idx_pag_period ON payment_audit_gaps(period_from, period_to)",
    "CREATE INDEX IF NOT EXISTS idx_pag_partner ON payment_audit_gaps(partner_key)",
]


def ensure_schema(conn) -> None:
    log("Ensure schema...")
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()
    log("  ok.")


def get_default_bookkeeper(conn) -> dict | None:
    """Внутренний бухгалтер по роли в tg_user_roles."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT tr.user_id, MAX(tr.first_name||' '||COALESCE(tr.last_name,'')) AS name,
                   cu.km_entity_id, cu.employee_ref_key
            FROM tg_user_roles tr
            LEFT JOIN comm_users cu ON cu.tg_user_id = tr.user_id
            WHERE tr.role = 'Бухгалтер' AND tr.is_active
            GROUP BY tr.user_id, cu.km_entity_id, cu.employee_ref_key
            ORDER BY tr.user_id LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None


# Админ — DM-получатель когда реальный автор заказа известен по имени,
# но не сматчен с tg_user_id в km_entities. Лучше DM админу с явной пометкой
# «обсудить с <ФИО автора>», чем угадывать «закупщика по роли» (Раис попадал
# из-за «Главный по закупкам» в неподходящем чате).
ADMIN_TG_USER_ID = 805598873


def find_open_supplier_order(conn, organization_key: str,
                              partner_key: str | None, counterparty_key: str | None,
                              gap_amount: float | None) -> dict | None:
    """Ищет лучший open supplier_order для данной пары (org, partner|counterparty).
    Приоритеты:
      1. по partner_key + status='Подтвержден' + просрочена доставка (или старый)
      2. по counterparty_key (если partner-match не дал)
    Из найденных предпочтение по совпадению суммы с gap.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        candidates = []
        for filter_field, filter_val in [("partner_key", partner_key),
                                          ("counterparty_key", counterparty_key)]:
            if not filter_val:
                continue
            cur.execute(f"""
                SELECT ref_key, doc_number, doc_date, author_key, status, amount,
                       desired_arrival_date::date AS desired_arrival_date,
                       partner_key, counterparty_key
                FROM c1_supplier_orders
                WHERE organization_key = %s AND {filter_field} = %s
                  AND posted=true AND is_deleted=false
                  AND status IN ('Подтвержден','Согласован')
                ORDER BY doc_date DESC LIMIT 30
            """, (organization_key, filter_val))
            for r in cur.fetchall():
                candidates.append(dict(r))
            if candidates:
                break  # партнёр-матч приоритетнее
    if not candidates:
        return None
    # Скоринг: совпадение по amount + просрочка
    today = date.today()

    def score(o: dict) -> tuple:
        amt_match = 0.0
        if gap_amount and o.get("amount"):
            ratio = abs(float(o["amount"]) - gap_amount) / max(gap_amount, 1)
            amt_match = 1.0 - min(ratio, 1.0)  # 1 при точном совпадении
        # просрочка (по desired_arrival или по возрасту 30+ дней)
        overdue = 0.0
        if o.get("desired_arrival_date") and o["desired_arrival_date"] < today:
            overdue = 1.0
        elif o.get("doc_date") and (today - o["doc_date"]).days > 30:
            overdue = 0.5
        return (overdue + amt_match,)
    candidates.sort(key=score, reverse=True)
    return candidates[0]


def detect_gaps(conn, period_from: date, period_to: date, dry: bool) -> int:
    log(f"detect_gaps {period_from}..{period_to}")
    INTERNAL_SQL = "(" + ",".join(f"'{p}'" for p in INTERNAL_PARTNERS) + ")"
    OPS_SQL = "(" + ",".join(f"'{op}'" for op in TRACKED_OPERATIONS) + ")"

    # Aggregate payments per (org, partner) for the window — only tracked operations,
    # only external partners.
    # Use bank_date for cash-basis matching with reality.
    # Note: amount could be negative for refunds; keep as is.
    SQL_PAY = f"""
    SELECT
      be.organization_key,
      be.counterparty_key,
      bei.partner_key,
      SUM(bei.amount)::numeric(15,2) AS total_paid,
      COUNT(*) AS payments_cnt
    FROM c1_bank_expense_items bei
    JOIN c1_bank_expenses be ON bei.doc_key = be.ref_key
    WHERE be.posted=true AND be.is_deleted=false
      AND be.bank_date >= %s AND be.bank_date <= %s
      AND be.operation IN {OPS_SQL}
      AND COALESCE(bei.partner_key, '') NOT IN {INTERNAL_SQL}
    GROUP BY be.organization_key, be.counterparty_key, bei.partner_key
    HAVING SUM(bei.amount) > 0 AND bei.partner_key IS NOT NULL
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL_PAY, (period_from, period_to))
        payments = cur.fetchall()
    log(f"  payment groups (org, partner): {len(payments)}")

    # Aggregate acquisitions per (org, partner) for the same window
    # 1) c1_purchases (Приобретение товаров и услуг) — gross sum
    # 2) c1_purchases_other_assets (Приобретение услуг и прочих активов)
    # 3) c1_advance_report_other_expenses → headers ar (organization), counterparty_key on items
    SQL_ACQ = f"""
    WITH a1 AS (
      SELECT p.organization_key, p.partner_key,
        SUM(CASE WHEN pi.sum_total = pi.sum_with_vat THEN pi.sum_total
                 ELSE pi.sum_total + pi.vat_sum END)::numeric(15,2) AS gross
      FROM c1_purchase_items pi
      JOIN c1_purchases p ON pi.doc_key = p.ref_key
      WHERE p.doc_date >= %s AND p.doc_date <= %s
        AND p.posted=true AND p.is_deleted=false
        AND COALESCE(p.partner_key,'') NOT IN {INTERNAL_SQL}
      GROUP BY p.organization_key, p.partner_key
    ),
    a2 AS (
      SELECT h.organization_key, h.partner_key,
        SUM(it.sum_with_vat)::numeric(15,2) AS gross
      FROM c1_purchases_other_assets_items it
      JOIN c1_purchases_other_assets h ON it.doc_key = h.ref_key
      WHERE h.doc_date >= %s AND h.doc_date <= %s
        AND h.posted=true AND h.is_deleted=false
        AND COALESCE(h.partner_key,'') NOT IN {INTERNAL_SQL}
      GROUP BY h.organization_key, h.partner_key
    ),
    a3 AS (
      SELECT ar.organization_key, NULL::varchar AS partner_key,
        SUM(COALESCE(oe.sum_with_vat, oe.sum_total))::numeric(15,2) AS gross
      FROM c1_advance_report_other_expenses oe
      JOIN c1_advance_reports ar ON oe.doc_key = ar.ref_key
      WHERE ar.doc_date >= %s AND ar.doc_date <= %s
        AND ar.posted=true AND ar.is_deleted=false
      GROUP BY ar.organization_key
    )
    SELECT organization_key, partner_key, SUM(gross)::numeric(15,2) AS total_acquired
    FROM (SELECT * FROM a1 UNION ALL SELECT * FROM a2 UNION ALL SELECT * FROM a3) u
    GROUP BY organization_key, partner_key
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL_ACQ, (period_from, period_to,
                              period_from, period_to,
                              period_from, period_to))
        acqs = {(r["organization_key"], r["partner_key"]): float(r["total_acquired"] or 0)
                for r in cur.fetchall()}
    log(f"  acquisition groups: {len(acqs)}")

    # Compute gaps
    gaps_to_write = []
    for p in payments:
        key = (p["organization_key"], p["partner_key"])
        acquired = acqs.get(key, 0.0)
        paid = float(p["total_paid"])
        if paid <= acquired + 0.01:
            continue
        gap = round(paid - acquired, 2)
        gaps_to_write.append({
            "org": p["organization_key"],
            "partner": p["partner_key"],
            "ctp": p["counterparty_key"],
            "paid": paid,
            "acquired": acquired,
            "gap": gap,
            "payments_cnt": p["payments_cnt"],
        })
    log(f"  gaps (paid > acquired): {len(gaps_to_write)}")

    bookkeeper = get_default_bookkeeper(conn)
    if bookkeeper:
        log(f"  default bookkeeper: {bookkeeper['name']} (tg={bookkeeper['user_id']})")

    # Enrich each gap: fetch sample payment, look for supplier_order link, get authors
    enriched = []
    today = date.today()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for g in gaps_to_write:
            # Pull payments details for this group
            cur.execute(f"""
                SELECT be.ref_key, be.doc_number, be.bank_date, be.bank_number,
                       be.amount, be.author_key, be.responsible_key,
                       be.basis_doc_ref, be.basis_doc_type,
                       be.bsg_order_ref, be.bsg_order_type, be.purpose
                FROM c1_bank_expenses be
                JOIN c1_bank_expense_items bei ON bei.doc_key = be.ref_key
                WHERE be.organization_key = %s
                  AND COALESCE(bei.partner_key,'') = COALESCE(%s,'')
                  AND be.posted=true AND be.is_deleted=false
                  AND be.bank_date >= %s AND be.bank_date <= %s
                  AND be.operation IN {OPS_SQL}
                ORDER BY be.bank_date ASC
            """, (g["org"], g["partner"], period_from, period_to))
            be_rows = cur.fetchall()

            # 1) Direct linkage via basis_doc_type или bsg_order_ref
            so_ref = None
            for be in be_rows:
                if be["basis_doc_type"] and "ЗаказПоставщику" in (be["basis_doc_type"] or ""):
                    so_ref = be["basis_doc_ref"]; break
                if be["bsg_order_ref"]:
                    so_ref = be["bsg_order_ref"]; break

            # 2) Эвристика: ищем открытый supplier_order для партнёра ИЛИ контрагента
            so_info = None
            if not so_ref:
                so_info = find_open_supplier_order(
                    conn, g["org"], g["partner"], g["ctp"], gap_amount=g["gap"],
                )
            else:
                cur.execute("""
                    SELECT ref_key, doc_number, doc_date, author_key, status,
                           desired_arrival_date::date AS desired_arrival_date,
                           organization_key, amount
                    FROM c1_supplier_orders WHERE ref_key = %s
                """, (so_ref,))
                so_info = cur.fetchone()

            # Determine case + assignee
            so_overdue = False
            if so_info:
                if so_info.get("desired_arrival_date") and \
                   so_info["desired_arrival_date"] < today:
                    so_overdue = True
                elif so_info.get("doc_date") and (today - so_info["doc_date"]).days > 30:
                    so_overdue = True

            if so_info and so_overdue:
                case = "case_with_order_overdue"
                assignee_user_ref = so_info.get("author_key")
            elif so_info:
                case = "case_with_order_in_progress"
                assignee_user_ref = so_info.get("author_key")
            else:
                case = "case_no_order"
                assignee_user_ref = (be_rows[0]["author_key"] if be_rows else None)
                if not assignee_user_ref and be_rows:
                    assignee_user_ref = be_rows[0]["responsible_key"]

            # Resolve assignee → c1_users → km_entity → tg_user_ids
            tg_ids = None
            ent_id = None
            emp_ref = None
            assignee_name = None
            if assignee_user_ref:
                cur.execute("""
                    SELECT u.description, e.id AS km_entity_id,
                           e.attrs->'tg_user_ids' AS tg_user_ids,
                           (e.attrs->>'employee_ref_key') AS employee_ref_key
                    FROM c1_users u
                    LEFT JOIN km_entities e ON e.entity_type='person'
                         AND (e.attrs->>'c1_user_ref_key') = u.ref_key
                    WHERE u.ref_key = %s
                """, (assignee_user_ref,))
                row = cur.fetchone()
                if row:
                    assignee_name = row["description"]
                    ent_id = row["km_entity_id"]
                    tg_ids = row["tg_user_ids"]
                    emp_ref = row["employee_ref_key"]
            # Логика выбора DM-получателя:
            #  • автор известен И сматчен с TG → DM прямо ему
            #  • автор известен по имени, но без TG-привязки в km_entities →
            #    DM админу с пометкой «обсудить с <ФИО>» (НЕ дефолт-закупщик —
            #    Раис попадал из-за чужого описания роли)
            #  • case_no_order без автора → дефолт-бухгалтер (импорт из клиент-банка)
            if assignee_name and not tg_ids:
                # Автор есть в 1С, но не сматчен с tg_user_id
                assignee_name = f"{assignee_name} (через админа — нет TG-привязки)"
                tg_ids = [ADMIN_TG_USER_ID]
            elif case == "case_no_order" and not assignee_name and bookkeeper:
                assignee_name = bookkeeper["name"] + " (дефолт-бухгалтер)"
                ent_id = bookkeeper["km_entity_id"]
                emp_ref = bookkeeper["employee_ref_key"]
                tg_ids = [bookkeeper["user_id"]] if bookkeeper["user_id"] else None
            elif not assignee_name:
                # case_with_order_* без автора (редкий: 0.14% после backfill)
                assignee_name = "Не определён (через админа)"
                tg_ids = [ADMIN_TG_USER_ID]

            # Names
            cur.execute("SELECT name FROM c1_organizations WHERE ref_key=%s", (g["org"],))
            r = cur.fetchone(); org_name = r["name"] if r else None
            partner_name = None
            if g["partner"]:
                cur.execute("SELECT name FROM c1_partners WHERE ref_key=%s", (g["partner"],))
                r = cur.fetchone(); partner_name = r["name"] if r else None

            enriched.append({
                **g,
                "org_name": org_name,
                "partner_name": partner_name,
                "case": case,
                "so": so_info,
                "sample_payment": be_rows[0] if be_rows else None,
                "assignee_user_ref": assignee_user_ref,
                "assignee_emp_ref": emp_ref,
                "assignee_ent_id": ent_id,
                "assignee_tg_ids": tg_ids,
                "assignee_name": assignee_name,
            })

    # Write to DB
    by_case: dict[str, int] = {}
    if not dry:
        with conn.cursor() as cur:
            # Mark previous open gaps in this period as superseded — replace
            cur.execute("""
                DELETE FROM payment_audit_gaps
                WHERE period_from = %s AND period_to = %s AND status = 'open'
            """, (period_from, period_to))
            for g in enriched:
                so = g["so"] or {}
                sp = g["sample_payment"] or {}
                cur.execute("""
                    INSERT INTO payment_audit_gaps
                      (period_from, period_to, organization_key, organization_name,
                       partner_key, partner_name,
                       total_paid, total_acquired, gap_amount, case_type,
                       supplier_order_ref, supplier_order_number, supplier_order_date,
                       supplier_order_author_key, supplier_order_desired_arrival, is_overdue,
                       sample_payment_ref, sample_payment_author_key,
                       assignee_user_ref_key, assignee_employee_ref_key,
                       assignee_km_entity_id, assignee_tg_user_ids, assignee_name,
                       status, notes)
                    VALUES (%s,%s,%s,%s, %s,%s, %s,%s,%s, %s,
                            %s,%s,%s, %s,%s,%s,
                            %s,%s, %s,%s, %s,%s,%s,
                            %s, %s)
                    ON CONFLICT (period_from, period_to, organization_key, partner_key, case_type) DO UPDATE SET
                      total_paid = EXCLUDED.total_paid,
                      total_acquired = EXCLUDED.total_acquired,
                      gap_amount = EXCLUDED.gap_amount,
                      detected_at = NOW(),
                      sample_payment_ref = EXCLUDED.sample_payment_ref,
                      sample_payment_author_key = EXCLUDED.sample_payment_author_key,
                      assignee_user_ref_key = EXCLUDED.assignee_user_ref_key,
                      assignee_employee_ref_key = EXCLUDED.assignee_employee_ref_key,
                      assignee_km_entity_id = EXCLUDED.assignee_km_entity_id,
                      assignee_tg_user_ids = EXCLUDED.assignee_tg_user_ids,
                      assignee_name = EXCLUDED.assignee_name,
                      notes = EXCLUDED.notes
                """, (
                    period_from, period_to, g["org"], g["org_name"],
                    g["partner"], g["partner_name"],
                    g["paid"], g["acquired"], g["gap"], g["case"],
                    so.get("ref_key"), so.get("doc_number"), so.get("doc_date"),
                    so.get("author_key"), so.get("desired_arrival_date"),
                    bool(so.get("desired_arrival_date") and so["desired_arrival_date"] < date.today()) if so else None,
                    sp.get("ref_key"), sp.get("author_key"),
                    g["assignee_user_ref"], g["assignee_emp_ref"],
                    g["assignee_ent_id"], json.dumps(g["assignee_tg_ids"], ensure_ascii=False) if g["assignee_tg_ids"] else None,
                    g["assignee_name"],
                    "open", None,
                ))
                by_case[g["case"]] = by_case.get(g["case"], 0) + 1
        conn.commit()
    else:
        for g in enriched:
            by_case[g["case"]] = by_case.get(g["case"], 0) + 1
    log(f"  by case: {by_case}")
    return len(enriched)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--alter-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    conn = conn_db()
    try:
        ensure_schema(conn)
        if args.alter_only:
            log("alter-only → done.")
            return 0
        today = date.today()
        period_from = today - timedelta(days=args.days)
        detect_gaps(conn, period_from, today, dry=args.dry_run)
    finally:
        conn.close()
    log("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
