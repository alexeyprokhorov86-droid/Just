#!/usr/bin/env python3
"""
Daily Report - Ежедневный отчёт о состоянии системы.
Запускается через cron раз в день.

Функции:
- Чистка "сирот" в embeddings
- Статистика прироста данных
- Проверка здоровья сервисов
- Расход токенов RouterAI
- Отправка саммари в Telegram
"""

import os
import sys
import json
import requests
import psycopg2
from datetime import datetime, timedelta
import pathlib
from dotenv import load_dotenv
import subprocess

# Загружаем переменные окружения
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
    """Записывает в лог."""
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


def check_service_status(service_name: str) -> tuple[bool, str]:
    """Проверяет статус systemd сервиса."""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service_name],
            capture_output=True, text=True, timeout=10
        )
        is_active = result.stdout.strip() == "active"
        return is_active, result.stdout.strip()
    except Exception as e:
        return False, str(e)


def get_routerai_usage() -> dict:
    """Получает статистику расхода токенов RouterAI."""
    if not ROUTERAI_API_KEY:
        return {}
    
    try:
        response = requests.get(
            f"{ROUTERAI_BASE_URL}/key",
            headers={"Authorization": f"Bearer {ROUTERAI_API_KEY}"},
            timeout=10
        )
        result = response.json()
        # Данные в поле "data"
        return result.get("data", result)
    except Exception as e:
        log(f"Ошибка получения статистики RouterAI: {e}")
        return {}


def get_db_stats() -> dict:
    """Получает статистику базы данных."""
    stats = {}
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            # Размер БД
            cur.execute("SELECT pg_size_pretty(pg_database_size(%s))", (DB_NAME,))
            stats["db_size"] = cur.fetchone()[0]
            
            # Количество embeddings
            cur.execute("SELECT COUNT(*) FROM embeddings")
            stats["embeddings_total"] = cur.fetchone()[0]
            
            # Embeddings за последние 24 часа
            cur.execute("""
                SELECT COUNT(*) FROM embeddings 
                WHERE created_at > NOW() - INTERVAL '24 hours'
            """)
            stats["embeddings_24h"] = cur.fetchone()[0]
            
            # Количество email
            cur.execute("SELECT COUNT(*) FROM email_messages")
            stats["emails_total"] = cur.fetchone()[0]
            
            # Email за последние 24 часа
            cur.execute("""
                SELECT COUNT(*) FROM email_messages 
                WHERE processed_at > NOW() - INTERVAL '24 hours'
            """)
            stats["emails_24h"] = cur.fetchone()[0]
            
            # Количество сообщений Telegram (сумма по всем таблицам tg_chat_*)
            cur.execute("""
                SELECT SUM(n_live_tup) 
                FROM pg_stat_user_tables 
                WHERE relname LIKE 'tg_chat_%'
            """)
            result = cur.fetchone()[0]
            stats["telegram_total"] = int(result) if result else 0
            
            # Закрытые цепочки за 24 часа
            cur.execute("""
                SELECT COUNT(*) FROM email_threads 
                WHERE resolution_detected_at > NOW() - INTERVAL '24 hours'
            """)
            stats["threads_closed_24h"] = cur.fetchone()[0]
            
            # Активные почтовые ящики
            cur.execute("SELECT COUNT(*) FROM monitored_mailboxes WHERE is_active = true")
            stats["active_mailboxes"] = cur.fetchone()[0]
            
    except Exception as e:
        log(f"Ошибка получения статистики БД: {e}")
    finally:
        conn.close()
    
    return stats


def cleanup_orphan_embeddings() -> int:
    """Удаляет 'сироты' — embeddings без связанных записей."""
    deleted = 0
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            # Удаляем email embeddings без связанных email_messages
            cur.execute("""
                DELETE FROM embeddings 
                WHERE source_type = 'email' 
                AND source_id NOT IN (SELECT id FROM email_messages)
            """)
            deleted += cur.rowcount
            
            # Удаляем пустые или битые embeddings
            cur.execute("""
                DELETE FROM embeddings 
                WHERE content IS NULL OR content = '' OR LENGTH(content) < 10
            """)
            deleted += cur.rowcount
            
            conn.commit()
            
    except Exception as e:
        log(f"Ошибка очистки сирот: {e}")
        conn.rollback()
    finally:
        conn.close()
    
    return deleted


def check_sync_1c_status() -> dict:
    """Проверяет статус синхронизации 1С."""
    sync_log = pathlib.Path("/home/admin/knowledge-base/sync.log")
    
    if not sync_log.exists():
        return {"status": "no_log", "message": "Лог не найден"}
    
    try:
        # Читаем последние 20 строк лога
        result = subprocess.run(
            ["tail", "-20", str(sync_log)],
            capture_output=True, text=True, timeout=10
        )
        last_lines = result.stdout
        
        # Ищем последнюю дату синхронизации
        if "Синхронизация завершена" in last_lines:
            if "ОШИБКА" in last_lines or "error" in last_lines.lower():
                return {"status": "error", "message": "Ошибка подключения к 1С"}
            else:
                return {"status": "ok", "message": "Синхронизация работает"}
        else:
            return {"status": "unknown", "message": "Не удалось определить статус"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_disk_usage() -> dict:
    """Получает информацию о диске."""
    try:
        result = subprocess.run(
            ["df", "-h", "/"],
            capture_output=True, text=True, timeout=10
        )
        lines = result.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            return {
                "total": parts[1],
                "used": parts[2],
                "available": parts[3],
                "percent": parts[4]
            }
    except Exception as e:
        log(f"Ошибка получения информации о диске: {e}")
    
    return {}


def send_report(report: str):
    """Отправляет отчёт в Telegram."""
    if not BOT_TOKEN or not ADMIN_USER_ID:
        log("Не настроен BOT_TOKEN или ADMIN_USER_ID")
        return
    
    try:
        # Разбиваем если слишком длинный
        if len(report) > 4000:
            parts = [report[i:i+4000] for i in range(0, len(report), 4000)]
        else:
            parts = [report]
        
        for part in parts:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": ADMIN_USER_ID,
                    "text": part,
                    "parse_mode": "HTML"
                },
                timeout=30
            )
        
        log("Отчёт отправлен в Telegram")
        
    except Exception as e:
        log(f"Ошибка отправки отчёта: {e}")


def generate_report() -> str:
    """Генерирует полный ежедневный отчёт."""
    
    report_parts = []
    report_parts.append("📊 <b>Ежедневный отчёт</b>")
    report_parts.append(f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n")
    
    # === Сервисы ===
    report_parts.append("🔧 <b>Сервисы:</b>")
    
    tg_active, tg_status = check_service_status("telegram-logger")
    report_parts.append(f"  • telegram-logger: {'✅' if tg_active else '❌'} {tg_status}")
    
    email_active, email_status = check_service_status("email-sync")
    report_parts.append(f"  • email-sync: {'✅' if email_active else '❌'} {email_status}")
    
    sync_1c = check_sync_1c_status()
    sync_emoji = "✅" if sync_1c["status"] == "ok" else "⚠️" if sync_1c["status"] == "error" else "❓"
    report_parts.append(f"  • sync-1c: {sync_emoji} {sync_1c['message']}\n")
    
    # === База данных ===
    db_stats = get_db_stats()
    report_parts.append("💾 <b>База данных:</b>")
    report_parts.append(f"  • Размер: {db_stats.get('db_size', 'N/A')}")
    report_parts.append(f"  • Embeddings: {db_stats.get('embeddings_total', 0):,} (+{db_stats.get('embeddings_24h', 0)} за 24ч)")
    report_parts.append(f"  • Email: {db_stats.get('emails_total', 0):,} (+{db_stats.get('emails_24h', 0)} за 24ч)")
    report_parts.append(f"  • Telegram: {db_stats.get('telegram_total', 0):,} сообщений")
    report_parts.append(f"  • Закрыто цепочек: {db_stats.get('threads_closed_24h', 0)} за 24ч")
    report_parts.append(f"  • Активных ящиков: {db_stats.get('active_mailboxes', 0)}\n")
    
    # === RouterAI ===
    routerai = get_routerai_usage()
    if routerai:
        report_parts.append("🤖 <b>RouterAI:</b>")
        report_parts.append(f"  • Использовано: {routerai.get('usage_monthly', 'N/A')}")
        report_parts.append(f"  • Лимит: {routerai.get('limit', 'N/A')}")
        report_parts.append(f"  • Осталось: {routerai.get('limit_remaining', 'N/A')}\n")
    
    # === Диск ===
    disk = get_disk_usage()
    if disk:
        report_parts.append("💿 <b>Диск:</b>")
        report_parts.append(f"  • Занято: {disk.get('used', 'N/A')} / {disk.get('total', 'N/A')} ({disk.get('percent', 'N/A')})")
        report_parts.append(f"  • Свободно: {disk.get('available', 'N/A')}\n")
    
    # === Очистка ===
    orphans_deleted = cleanup_orphan_embeddings()
    report_parts.append("🧹 <b>Очистка:</b>")
    report_parts.append(f"  • Удалено сирот: {orphans_deleted}\n")
    
    # === Предупреждения ===
    warnings = []
    
    if not tg_active:
        warnings.append("❌ telegram-logger не работает!")
    
    if not email_active:
        warnings.append("❌ email-sync не работает!")
    
    if sync_1c["status"] == "error":
        warnings.append("⚠️ Проблемы с синхронизацией 1С")
    
    if db_stats.get("embeddings_24h", 0) == 0 and db_stats.get("emails_24h", 0) > 0:
        warnings.append("⚠️ Новые письма не индексируются!")
    
    disk_percent = int(disk.get("percent", "0%").replace("%", ""))
    if disk_percent > 85:
        warnings.append(f"⚠️ Диск заполнен на {disk_percent}%!")
    
    if warnings:
        report_parts.append("⚠️ <b>Предупреждения:</b>")
        for w in warnings:
            report_parts.append(f"  {w}")
    else:
        report_parts.append("✅ Всё работает нормально")
    
    return "\n".join(report_parts)


def main():
    log("=" * 50)
    log("Daily Report starting...")
    
    report = generate_report()
    print(report)
    
    send_report(report)
    
    log("Daily Report finished")
    log("=" * 50)


if __name__ == "__main__":
    main()
