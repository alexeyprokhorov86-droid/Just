#!/usr/bin/env python3
"""
Daily Report v2 - Ежедневный отчёт о состоянии системы.
Запускается через cron раз в день.
"""

import os
import sys
import json
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import pathlib
from dotenv import load_dotenv
import subprocess

from proxy_config import get_proxy_dict

PROXY = get_proxy_dict()
env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
ROUTERAI_API_KEY = os.getenv("ROUTERAI_API_KEY")
ROUTERAI_BASE_URL = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "knowledge_base")
DB_USER = os.getenv("DB_USER", "knowledge")
DB_PASSWORD = os.getenv("DB_PASSWORD")

LOG_FILE = pathlib.Path(__file__).parent / "daily_report.log"


def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}"
    print(log_line)
    with open(LOG_FILE, "a") as f:
        f.write(log_line + "\n")


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )


def _safe_query(cur, sql, params=None, default=None):
    """Execute query, return result or default on error."""
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception as e:
        log(f"Query error: {e}")
        cur.connection.rollback()
        return default if default is not None else []


def _safe_query_one(cur, sql, params=None, default=None):
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row if row else default
    except Exception as e:
        log(f"Query error: {e}")
        cur.connection.rollback()
        return default


# ── Service checks ──

def check_service_status(service_name: str) -> tuple:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service_name],
            capture_output=True, text=True, timeout=10
        )
        is_active = result.stdout.strip() == "active"
        return is_active, result.stdout.strip()
    except Exception as e:
        return False, str(e)


def get_disk_usage() -> dict:
    try:
        result = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=10)
        lines = result.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            return {"total": parts[1], "used": parts[2], "available": parts[3], "percent": parts[4]}
    except Exception as e:
        log(f"Disk usage error: {e}")
    return {}


# ── Cleanup tasks (kept from v1) ──

def cleanup_orphan_embeddings() -> int:
    deleted = 0
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM embeddings
                WHERE source_type = 'email'
                AND source_id NOT IN (SELECT id FROM email_messages)
            """)
            deleted += cur.rowcount
            cur.execute("""
                DELETE FROM embeddings
                WHERE content IS NULL OR content = '' OR LENGTH(content) < 10
            """)
            deleted += cur.rowcount
            conn.commit()
    except Exception as e:
        log(f"Orphan cleanup error: {e}")
        conn.rollback()
    finally:
        conn.close()
    return deleted


def cleanup_old_backups(keep_count: int = 3) -> dict:
    backup_dir = pathlib.Path("/home/admin/telegram_logger_bot/backups")
    result = {"deleted": 0, "freed_mb": 0, "kept": 0}
    if not backup_dir.exists():
        return result
    try:
        backups = sorted(backup_dir.glob("backup_*.sql.gz"), key=lambda f: f.stat().st_mtime, reverse=True)
        result["kept"] = min(len(backups), keep_count)
        for backup in backups[keep_count:]:
            size_mb = backup.stat().st_size / (1024 * 1024)
            backup.unlink()
            result["deleted"] += 1
            result["freed_mb"] += size_mb
            log(f"Deleted backup: {backup.name} ({size_mb:.0f} MB)")
        result["freed_mb"] = round(result["freed_mb"], 0)
    except Exception as e:
        log(f"Backup cleanup error: {e}")
    return result


# ── Report blocks ──

def get_rag_stats() -> dict:
    stats = {"total": 0, "private": 0, "group": 0, "users": [], "intents": [],
             "avg_time": 0, "insufficient": 0, "web_search": 0}
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        row = _safe_query_one(cur,
            "SELECT count(*) as total, "
            "count(*) FILTER (WHERE chat_type = 'private') as private, "
            "count(*) FILTER (WHERE chat_type != 'private') as grp, "
            "coalesce(avg(response_time_ms), 0) as avg_ms, "
            "count(*) FILTER (WHERE evaluator_sufficient = false) as insuf, "
            "count(*) FILTER (WHERE web_search_used = true) as web "
            "FROM rag_query_log WHERE created_at > NOW() - INTERVAL '24 hours'")
        if row:
            stats["total"] = row["total"]
            stats["private"] = row["private"]
            stats["group"] = row["grp"]
            stats["avg_time"] = round(row["avg_ms"] / 1000, 1) if row["avg_ms"] else 0
            stats["insufficient"] = row["insuf"]
            stats["web_search"] = row["web"]

        rows = _safe_query(cur,
            "SELECT coalesce(first_name, username, user_id::text) as name, count(*) as cnt "
            "FROM rag_query_log WHERE created_at > NOW() - INTERVAL '24 hours' "
            "GROUP BY name ORDER BY cnt DESC LIMIT 5")
        stats["users"] = [(r["name"], r["cnt"]) for r in rows]

        rows = _safe_query(cur,
            "SELECT primary_intent, count(*) as cnt "
            "FROM rag_query_log WHERE created_at > NOW() - INTERVAL '24 hours' AND primary_intent IS NOT NULL "
            "GROUP BY primary_intent ORDER BY cnt DESC LIMIT 5")
        stats["intents"] = [(r["primary_intent"], r["cnt"]) for r in rows]

        cur.close()
        conn.close()
    except Exception as e:
        log(f"RAG stats error: {e}")
    return stats


def get_document_analysis_stats() -> dict:
    stats = {"total": 0, "by_type": {}, "by_chat": [], "by_sender": [], "full_analysis_button": 0}
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get all tg_chat_ tables
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'tg_chat_%'")
        tables = [r[0] for r in cur.fetchall()]

        total = 0
        type_counts = {}
        chat_counts = {}
        sender_counts = {}

        for table in tables:
            try:
                cur.execute(f"""
                    SELECT message_type, first_name, count(*) FROM {table}
                    WHERE media_analysis IS NOT NULL AND media_analysis != ''
                      AND timestamp > NOW() - INTERVAL '24 hours'
                    GROUP BY message_type, first_name
                """)
                for msg_type, sender, cnt in cur.fetchall():
                    total += cnt
                    type_counts[msg_type or "other"] = type_counts.get(msg_type or "other", 0) + cnt
                    chat_title = table.replace("tg_chat_", "").split("_", 1)[-1][:20]
                    chat_counts[chat_title] = chat_counts.get(chat_title, 0) + cnt
                    sender_counts[sender or "?"] = sender_counts.get(sender or "?", 0) + cnt
            except Exception:
                conn.rollback()
                continue

        stats["total"] = total
        stats["by_type"] = type_counts
        stats["by_chat"] = sorted(chat_counts.items(), key=lambda x: -x[1])[:5]
        stats["by_sender"] = sorted(sender_counts.items(), key=lambda x: -x[1])[:5]

        # Button clicks
        row = _safe_query_one(cur,
            "SELECT count(*) FROM bot_button_log WHERE button_type='full_analysis' AND created_at > NOW() - INTERVAL '24 hours'")
        stats["full_analysis_button"] = row[0] if row else 0

        cur.close()
        conn.close()
    except Exception as e:
        log(f"Doc analysis stats error: {e}")
    return stats


def get_email_stats() -> dict:
    stats = {"total": 0, "by_category": {}, "threads_opened": 0, "threads_closed": 0,
             "attachments": {}, "error_mailboxes": 0}
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        rows = _safe_query(cur,
            "SELECT category, count(*) FROM email_messages "
            "WHERE received_at > NOW() - INTERVAL '24 hours' GROUP BY category ORDER BY count(*) DESC")
        total = 0
        for cat, cnt in rows:
            stats["by_category"][cat or "unknown"] = cnt
            total += cnt
        stats["total"] = total

        row = _safe_query_one(cur,
            "SELECT count(*) FROM email_threads WHERE started_at > NOW() - INTERVAL '24 hours'")
        stats["threads_opened"] = row[0] if row else 0

        row = _safe_query_one(cur,
            "SELECT count(*) FROM email_threads WHERE resolution_detected_at > NOW() - INTERVAL '24 hours' AND summary_short IS NOT NULL")
        stats["threads_closed"] = row[0] if row else 0

        rows = _safe_query(cur,
            "SELECT analysis_status, count(*) FROM email_attachments "
            "WHERE created_at > NOW() - INTERVAL '24 hours' GROUP BY analysis_status")
        for status, cnt in rows:
            stats["attachments"][status or "unknown"] = cnt

        row = _safe_query_one(cur,
            "SELECT count(*) FROM monitored_mailboxes WHERE is_active = true AND last_error IS NOT NULL AND last_error != ''")
        stats["error_mailboxes"] = row[0] if row else 0

        cur.close()
        conn.close()
    except Exception as e:
        log(f"Email stats error: {e}")
    return stats


def get_km_stats() -> dict:
    stats = {"health": None, "pending_rules": 0,
             "facts_total": 0, "facts_new": 0, "decisions_total": 0, "decisions_new": 0}
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        row = _safe_query_one(cur, "SELECT * FROM km_health_metrics WHERE metric_date = CURRENT_DATE")
        stats["health"] = dict(row) if row else None

        row = _safe_query_one(cur,
            "SELECT count(*) as cnt FROM km_filter_rules WHERE approval_status = 'pending' AND is_active = true")
        stats["pending_rules"] = row["cnt"] if row else 0

        row = _safe_query_one(cur,
            "SELECT count(*) as total FROM km_facts WHERE verification_status NOT IN ('rejected','duplicate')")
        stats["facts_total"] = row["total"] if row else 0

        row = _safe_query_one(cur,
            "SELECT count(*) as cnt FROM km_facts WHERE created_at > NOW() - INTERVAL '24 hours' "
            "AND verification_status NOT IN ('rejected','duplicate')")
        stats["facts_new"] = row["cnt"] if row else 0

        row = _safe_query_one(cur,
            "SELECT count(*) as total FROM km_decisions WHERE verification_status NOT IN ('rejected','duplicate')")
        stats["decisions_total"] = row["total"] if row else 0

        row = _safe_query_one(cur,
            "SELECT count(*) as cnt FROM km_decisions WHERE created_at > NOW() - INTERVAL '24 hours' "
            "AND verification_status NOT IN ('rejected','duplicate')")
        stats["decisions_new"] = row["cnt"] if row else 0

        cur.close()
        conn.close()
    except Exception as e:
        log(f"KM stats error: {e}")
    return stats


def get_1c_sync_stats() -> dict:
    stats = {"syncs": [], "bom": None, "odata_errors": 0}
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        rows = _safe_query(cur,
            "SELECT entity_type, last_sync_at, records_synced, last_error FROM sync_status ORDER BY last_sync_at DESC")
        stats["syncs"] = [dict(r) for r in rows]
        stats["odata_errors"] = sum(1 for r in rows if r.get("last_error"))

        row = _safe_query_one(cur,
            "SELECT id, started_at, products_processed, changes_summary FROM bom_calculations ORDER BY id DESC LIMIT 1")
        if row:
            stats["bom"] = dict(row)

        cur.close()
        conn.close()
    except Exception as e:
        log(f"1C sync stats error: {e}")
    return stats


def get_nutrition_stats() -> dict:
    stats = {"by_status": {}, "sent_24h": 0, "written_24h": 0, "skipped_24h": 0, "rejected_24h": 0,
             "pending": 0, "deferred": 0}
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        rows = _safe_query(cur, "SELECT status, count(*) FROM nutrition_requests GROUP BY status")
        for status, cnt in rows:
            stats["by_status"][status] = cnt
        stats["pending"] = stats["by_status"].get("pending", 0)
        stats["deferred"] = stats["by_status"].get("deferred", 0)

        row = _safe_query_one(cur,
            "SELECT "
            "count(*) FILTER (WHERE status = 'sent') as sent, "
            "count(*) FILTER (WHERE status = 'written') as written, "
            "count(*) FILTER (WHERE status = 'skipped') as skipped, "
            "count(*) FILTER (WHERE status = 'rejected') as rejected "
            "FROM nutrition_requests WHERE updated_at > NOW() - INTERVAL '24 hours'")
        if row:
            stats["sent_24h"] = row[0] or 0
            stats["written_24h"] = row[1] or 0
            stats["skipped_24h"] = row[2] or 0
            stats["rejected_24h"] = row[3] or 0

        cur.close()
        conn.close()
    except Exception as e:
        log(f"Nutrition stats error: {e}")
    return stats


def get_matrix_stats() -> dict:
    stats = {"messages_24h": 0, "listener_active": False}
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        row = _safe_query_one(cur,
            "SELECT count(*) FROM source_documents WHERE source_kind = 'matrix_message' AND created_at > NOW() - INTERVAL '24 hours'")
        stats["messages_24h"] = row[0] if row else 0
        cur.close()
        conn.close()
    except Exception as e:
        log(f"Matrix stats error: {e}")

    active, _ = check_service_status("matrix-listener")
    stats["listener_active"] = active
    return stats


def get_routerai_usage() -> dict:
    if not ROUTERAI_API_KEY:
        return {}
    try:
        response = requests.get(
            f"{ROUTERAI_BASE_URL}/key",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}"},
            timeout=10
        )
        result = response.json()
        return result.get("data", result)
    except Exception as e:
        log(f"RouterAI error: {e}")
        return {}


# ── Report generation ──

def generate_report() -> str:
    parts = []
    parts.append("<b>📊 Ежедневный отчёт</b>")
    parts.append(f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n")

    # ── RAG ──
    rag = get_rag_stats()
    if rag["total"] > 0:
        parts.append("<b>🤖 RAG-агент:</b>")
        parts.append(f"  Запросов: {rag['total']} (личка: {rag['private']}, группы: {rag['group']})")
        if rag["users"]:
            user_str = ", ".join(f"{n} ({c})" for n, c in rag["users"])
            parts.append(f"  Пользователи: {user_str}")
        if rag["intents"]:
            intent_str = ", ".join(f"{i} {c}" for i, c in rag["intents"])
            parts.append(f"  Интенты: {intent_str}")
        parts.append(f"  Ср. время ответа: {rag['avg_time']} сек")
        if rag["insufficient"]:
            parts.append(f"  Evaluator insufficient: {rag['insufficient']} из {rag['total']}")
        if rag["web_search"]:
            parts.append(f"  Web search: {rag['web_search']}")
        parts.append("")
    else:
        parts.append("<b>🤖 RAG-агент:</b> нет запросов за 24ч\n")

    # ── Document analysis ──
    docs = get_document_analysis_stats()
    if docs["total"] > 0:
        parts.append("<b>📄 Анализ документов:</b>")
        type_str = ", ".join(f"{t}: {c}" for t, c in sorted(docs["by_type"].items(), key=lambda x: -x[1]))
        parts.append(f"  Проанализировано: {docs['total']} ({type_str})")
        if docs["by_chat"]:
            chat_str = ", ".join(f"{n} ({c})" for n, c in docs["by_chat"][:3])
            parts.append(f"  Чаты: {chat_str}")
        if docs["by_sender"]:
            sender_str = ", ".join(f"{n} ({c})" for n, c in docs["by_sender"][:3])
            parts.append(f"  Отправители: {sender_str}")
        if docs["full_analysis_button"]:
            parts.append(f'  Кнопка "Полный анализ": {docs["full_analysis_button"]}')
        parts.append("")

    # ── Email ──
    email = get_email_stats()
    parts.append("<b>📧 Email:</b>")
    if email["total"] > 0:
        cat_str = ", ".join(f"{k}: {v}" for k, v in email["by_category"].items())
        parts.append(f"  Новых: {email['total']} ({cat_str})")
    else:
        parts.append("  Новых: 0")
    parts.append(f"  Цепочек открыто: {email['threads_opened']}, закрыто: {email['threads_closed']}")
    if email["attachments"]:
        att_str = ", ".join(f"{k}: {v}" for k, v in email["attachments"].items())
        parts.append(f"  Вложений: {att_str}")
    if email["error_mailboxes"]:
        parts.append(f"  Ящиков с ошибками: {email['error_mailboxes']}")
    parts.append("")

    # ── Knowledge Management ──
    km = get_km_stats()
    parts.append("<b>🧠 Knowledge Management:</b>")
    h = km["health"]
    if h:
        created = h.get("facts_created", 0)
        verified = h.get("facts_verified", 0)
        rejected = h.get("facts_rejected", 0)
        dedup = h.get("facts_deduplicated", 0)
        parts.append(f"  Distillation: facts +{created}, decisions +{h.get('decisions_created', 0)}, tasks +{h.get('tasks_created', 0)}")
        parts.append(f"  Ревизия: verified {verified}, rejected {rejected}, duplicate {dedup}")
    if km["pending_rules"]:
        parts.append(f"  Новые правила: {km['pending_rules']} (pending)")
    parts.append(f"  База: facts {km['facts_total']:,} (+{km['facts_new']}), decisions {km['decisions_total']:,} (+{km['decisions_new']})")
    parts.append("")

    # ── 1C sync ──
    sync = get_1c_sync_stats()
    parts.append("<b>📦 1С синхронизация:</b>")
    for s in sync["syncs"]:
        ts = s["last_sync_at"].strftime("%H:%M") if s.get("last_sync_at") else "?"
        emoji = "✅" if not s.get("last_error") else "❌"
        parts.append(f"  {emoji} {s['entity_type']}: {ts} ({s.get('records_synced', 0)} записей)")
    bom = sync["bom"]
    if bom:
        parts.append(f"  BOM: расчёт #{bom['id']}, {bom.get('products_processed', 0)} продуктов")
    if sync["odata_errors"]:
        parts.append(f"  Ошибки OData: {sync['odata_errors']}")
    parts.append("")

    # ── Nutrition ──
    nutr = get_nutrition_stats()
    if any(v for v in [nutr["sent_24h"], nutr["written_24h"], nutr["pending"]]):
        parts.append("<b>🍎 Nutrition:</b>")
        parts.append(f"  Отправлено технологам: {nutr['sent_24h']}")
        parts.append(f"  Ответов: written {nutr['written_24h']}, skipped {nutr['skipped_24h']}, rejected {nutr['rejected_24h']}")
        parts.append(f"  Очередь: {nutr['pending']} pending, {nutr['deferred']} deferred")
        parts.append("")

    # ── Matrix ──
    mx = get_matrix_stats()
    parts.append("<b>💬 Matrix/Element:</b>")
    parts.append(f"  Сообщений через listener: {mx['messages_24h']}")
    parts.append(f"  matrix-listener: {'✅ active' if mx['listener_active'] else '❌ inactive'}")
    parts.append("")

    # ── Infrastructure ──
    parts.append("<b>🔧 Инфраструктура:</b>")
    for svc in ["telegram-logger", "email-sync", "matrix-listener", "auth-bom"]:
        active, status = check_service_status(svc)
        parts.append(f"  {svc}: {'✅' if active else '❌'} {status}")

    disk = get_disk_usage()
    if disk:
        parts.append(f"  Диск: {disk.get('percent', '?')} ({disk.get('used', '?')} / {disk.get('total', '?')})")

    routerai = get_routerai_usage()
    if routerai:
        usage = routerai.get('usage_monthly', 0)
        limit = routerai.get('limit', 0)
        if limit and limit > 0:
            parts.append(f"  RouterAI: {usage:.2f}P / {limit:.2f}P")
        else:
            parts.append(f"  RouterAI: {usage:.2f}P / безлимит")
    parts.append("")

    # ── Cleanup ──
    orphans = cleanup_orphan_embeddings()
    backups = cleanup_old_backups(keep_count=3)
    parts.append("<b>🧹 Очистка:</b>")
    parts.append(f"  Сирот удалено: {orphans}")
    if backups["deleted"] > 0:
        parts.append(f"  Бэкапов удалено: {backups['deleted']} (освобождено {backups['freed_mb']:.0f} MB)")
    parts.append(f"  Бэкапов сохранено: {backups['kept']}")

    return "\n".join(parts)


def send_report(report: str):
    if not BOT_TOKEN or not ADMIN_USER_ID:
        log("BOT_TOKEN or ADMIN_USER_ID not set")
        return
    try:
        if len(report) > 4000:
            chunks = [report[i:i+4000] for i in range(0, len(report), 4000)]
        else:
            chunks = [report]

        for chunk in chunks:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": ADMIN_USER_ID, "text": chunk, "parse_mode": "HTML"},
                timeout=30,
                proxies=PROXY,
            )
        log("Report sent to Telegram")
    except Exception as e:
        log(f"Send error: {e}")


def main():
    log("=" * 50)
    log("Daily Report v2 starting...")
    report = generate_report()
    print(report)
    send_report(report)
    log("Daily Report v2 finished")
    log("=" * 50)


if __name__ == "__main__":
    main()
